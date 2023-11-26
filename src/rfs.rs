use std::{
    ffi::OsStr,
    fs,
    fs::{read_dir, DirEntry, File},
    mem::ManuallyDrop,
    ops::Add,
    os::{
        fd::{FromRawFd, IntoRawFd},
        unix::fs::{MetadataExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::{Duration, SystemTime},
};

use anyhow::Context;
use clamav_rs::engine::ScanResult;
use fuser::{FileAttr, FileType};
use log::{debug, error, info, trace, warn};
use petgraph::stable_graph::NodeIndex;
use sys_mount::{Mount, Unmount, UnmountFlags};
use tempdir::TempDir;

use crate::{
    error::FuseError,
    inode::{FileAttrBuilder, Inode, InodeList, OpenedHandlers},
    scanner::ClamAV,
};

type FuseResult<T> = Result<T, FuseError>;

pub struct Rfs {
    inode_list: RwLock<InodeList>,
    proxy_mount: PathBuf,
    origin_mount: TempDir,
    mount: Mount,
    clamav: ClamAV,
}

impl Rfs {
    pub fn new(source: PathBuf, mount_point: PathBuf) -> anyhow::Result<Self> {
        let clamav = ClamAV::new().with_context(|| "Failed to create ClamAV scanner")?;

        let file_name = source
            .file_name()
            .expect("mount point is expected to be valid Path")
            .to_str()
            .unwrap();
        let origin_mount = TempDir::new_in("/mnt", file_name).unwrap();
        debug!("Real mount point: {:?}", origin_mount.as_ref());

        let mount = Mount::builder()
            .explicit_loopback()
            .mount(source, origin_mount.as_ref())?;
        Ok(Self {
            inode_list: RwLock::new(InodeList::default()),
            proxy_mount: mount_point,
            origin_mount,
            mount,
            clamav,
        })
    }

    pub fn inode_list(&self) -> RwLockReadGuard<InodeList> {
        self.inode_list.read().unwrap()
    }

    pub fn inode_list_write(&self) -> RwLockWriteGuard<InodeList> {
        self.inode_list.write().unwrap()
    }

    pub fn init(&mut self) {
        let attr = self.stat(&self.origin_mount).unwrap();
        let root_ino = 1;
        let attr = attr.with_ino(root_ino).build();

        let mut inode_list = self.inode_list.write().unwrap();

        let root_node = inode_list.list.add_node(Inode::new(
            self.proxy_mount.clone(),
            self.origin_mount.path().to_path_buf(),
            attr,
        ));
        let self_reference = Inode::new(
            PathBuf::from("."),
            self.origin_mount.path().to_path_buf(),
            attr,
        );
        inode_list.insert(self_reference, root_node);

        let upper_folder = Inode::new(
            PathBuf::from(".."),
            self.proxy_mount.parent().unwrap().to_path_buf(),
            attr, // TODO: wrong attributes here
        );
        inode_list.insert(upper_folder, root_node);
    }

    fn stat<P: AsRef<Path>>(&self, item: P) -> FuseResult<FileAttrBuilder> {
        debug!("Stat with {:?}", item.as_ref());

        let file = File::open(item).map_err(|_| FuseError::last())?;
        let meta = file.metadata().map_err(|_| FuseError::last())?;

        Ok(FileAttrBuilder::new()
            .with_size(meta.size())
            .with_blocks(meta.blocks())
            .with_atime(
                SystemTime::UNIX_EPOCH
                    .add(Duration::from_secs(u64::try_from(meta.atime()).unwrap())),
            )
            .with_mtime(
                SystemTime::UNIX_EPOCH
                    .add(Duration::from_secs(u64::try_from(meta.mtime()).unwrap())),
            )
            .with_ctime(
                SystemTime::UNIX_EPOCH
                    .add(Duration::from_secs(u64::try_from(meta.ctime()).unwrap())),
            )
            .with_crtime(meta.created().map_err(|_| FuseError::last())?)
            .with_kind(std_file_type_to_fuse_file_type(meta.file_type()))
            .with_perm(u16::try_from(meta.permissions().mode()).unwrap())
            .with_nlink(u32::try_from(meta.nlink()).unwrap())
            .with_uid(meta.uid())
            .with_gid(meta.gid())
            .with_rdev(u32::try_from(meta.rdev()).unwrap())
            .with_blksize(u32::try_from(meta.blksize()).unwrap())
            .with_flags(0))
    }

    pub fn create(
        &mut self,
        name: &OsStr,
        parent_ino: u64,
        mode: u32,
        kind: FileType,
    ) -> FuseResult<FileAttr> {
        let mut inode_list = self.inode_list.write().unwrap();

        let (parent_node, parent_inode) = inode_list
            .find_by_id(parent_ino)
            .ok_or(FuseError::NO_EXIST)?;

        if inode_list.find_child_by_name(parent_node, name).is_some() {
            return Err(FuseError::FILE_EXISTS);
        };

        let proxy_path = parent_inode.proxy_path.join(name);
        let origin_path = parent_inode.origin_path.join(name);

        match kind {
            FileType::RegularFile => {
                if let Err(err) = File::create(&origin_path) {
                    error!("Failed to create {origin_path:?} file: {err}");
                    return Err(FuseError::last());
                }
            }
            FileType::Directory => {
                if let Err(err) = fs::create_dir(&origin_path) {
                    error!("Failed to create {origin_path:?} directory: {err}");
                    return Err(FuseError::last());
                }
            }
            _ => {
                error!("{kind:?} creating is not implemented!");
                return Err(FuseError::NOT_IMPLEMENTED);
            }
        };

        let attr = FileAttrBuilder::new()
            .with_kind(kind)
            .with_perm(mode as u16)
            .build();

        let inode = Inode::new(proxy_path, origin_path, attr);
        let attr = inode_list.insert(inode, parent_node);

        Ok(attr)
    }

    fn insert_item(&mut self, item: PathBuf, parent_node: NodeIndex) -> FuseResult<()> {
        let proxy_path = self.origin_path_to_proxy_path(&item);
        match self.clamav.scan(&item) {
            Ok(scan_result) => match scan_result {
                ScanResult::Clean => {}
                ScanResult::Whitelisted => {
                    warn!("{item:?} is whitelisted")
                }
                ScanResult::Virus(_) => {
                    error!("{item:?} is a virus!!!");
                    return Err(FuseError::OPERATION_NOT_PERMITTED);
                }
            },
            Err(err) => {
                error!("Failed to scan {:?} file: {err}", item);
                return Err(FuseError::IO);
            }
        }

        let attr = self.stat(&item)?.build();

        trace!("Added {:?} item", proxy_path);
        let inode = Inode::new(proxy_path, item, attr);

        let mut inode_list = self.inode_list.write().unwrap();
        inode_list.insert(inode, parent_node);
        Ok(())
    }

    pub fn add_folder<P: AsRef<Path>>(&mut self, folder: P, ino: u64) -> FuseResult<()> {
        trace!("Adding folder: {:?}...", folder.as_ref());
        let (items, parent_node) = {
            let inode_list = self.inode_list.read().unwrap();

            let parent_node = inode_list.find_by_id(ino).ok_or(FuseError::NO_EXIST)?.0;

            let items = read_dir(folder)
                .map_err(|_| FuseError::last())?
                .filter_map(|item| match item {
                    Ok(item) => {
                        if inode_list.childs(parent_node).any(|child| {
                            child.origin_path.file_name().unwrap_or(OsStr::new(".."))
                                == item.file_name()
                        }) {
                            None
                        } else {
                            Some(Ok(item))
                        }
                    }
                    Err(err) => {
                        error!("readdir item error: {err}");
                        Some(Err(FuseError::last()))
                    }
                })
                .collect::<Result<Vec<DirEntry>, FuseError>>()?;

            (items, parent_node)
        };

        for item in items {
            match self.insert_item(item.path(), parent_node) {
                Ok(()) => {}
                Err(err) if err == FuseError::OPERATION_NOT_PERMITTED => {
                    warn!("Operation is not permitted for {:?}", item.path());
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(())
    }

    pub fn allocate_fh(&mut self, inode: u64, read: bool, write: bool) -> FuseResult<u64> {
        let mut write_view = self.inode_list.write().unwrap();
        let (_, inode) = write_view
            .find_by_id_mut(inode)
            .ok_or(FuseError::NO_EXIST)?;

        let count = if let Some(open_handlers) = inode.open_handles.as_mut() {
            open_handlers.count += 1;

            open_handlers.count
        } else {
            let file = match File::options()
                .read(read)
                .write(write)
                .open(&inode.origin_path)
            {
                Ok(file) => file,
                Err(err) => {
                    error!("Failed to open {:?}: {err}", inode.origin_path);
                    return Err(FuseError::last());
                }
            };

            inode.open_handles = Some(OpenedHandlers {
                fh: file.into_raw_fd(),
                count: 1,
            });

            1
        };

        let fh = (count << 2) | u64::from(read) | (u64::from(write) << 1);

        Ok(fh)
    }

    pub fn open_file(
        &self,
        node: &Inode,
        fh: u64,
        read: bool,
        write: bool,
    ) -> FuseResult<ManuallyDrop<File>> {
        if read && !fn_check_access_read(fh) {
            error!("Read is not allowed!");
            return Err(FuseError::OPERATION_NOT_PERMITTED);
        }

        if write && !fn_check_access_write(fh) {
            error!("Write is not allowed!");
            return Err(FuseError::OPERATION_NOT_PERMITTED);
        }

        let open_handlers = node.open_handles.as_ref().ok_or(FuseError::BAD_FD)?;

        Ok(ManuallyDrop::new(unsafe {
            File::from_raw_fd(open_handlers.fh)
        }))
    }

    fn proxy_path_to_origin_path<P: AsRef<Path>>(&self, item: P) -> PathBuf {
        self.origin_mount
            .path()
            .join(item.as_ref().strip_prefix(&self.proxy_mount).unwrap())
    }

    fn origin_path_to_proxy_path<P: AsRef<Path>>(&self, item: P) -> PathBuf {
        self.proxy_mount
            .as_path()
            .join(item.as_ref().strip_prefix(&self.origin_mount).unwrap())
    }

    pub fn remove(&mut self, ino: u64) -> FuseResult<()> {
        let mut inode_view = self.inode_list.write().unwrap();
        let (node_index, inode) = inode_view.find_by_id(ino).ok_or(FuseError::NO_EXIST)?;

        match inode.attr.kind {
            FileType::RegularFile => {
                fs::remove_file(&inode.origin_path).map_err(|_| FuseError::last())?;
            }
            FileType::Directory => {
                fs::remove_dir_all(&inode.origin_path).map_err(|_| FuseError::last())?;
            }
            other => {
                error!("Remove is not implemented for {other:?}");
                return Err(FuseError::NOT_IMPLEMENTED);
            }
        }

        let _ = inode_view.list.remove_node(node_index);

        Ok(())
    }

    pub fn rename(
        &mut self,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
    ) -> FuseResult<()> {
        let mut inode_list = self.inode_list.write().unwrap();

        let (parent_node, _) = inode_list.find_by_id(parent).ok_or(FuseError::NO_EXIST)?;

        let (newparent_node, newparent_inode) = inode_list
            .find_by_id(newparent)
            .ok_or(FuseError::NO_EXIST)?;
        let new_path = newparent_inode.proxy_path.join(newname);

        let (node_index, inode) = inode_list
            .find_child_by_name_mut(parent_node, name)
            .ok_or(FuseError::NO_EXIST)?;

        let new = self.proxy_path_to_origin_path(new_path.as_path());
        fs::rename(&inode.origin_path, &new).map_err(|_| FuseError::last())?;

        inode.proxy_path = new_path;
        inode.origin_path = new;

        let edge = inode_list
            .list
            .find_edge(parent_node, node_index)
            .expect("We found a child above so we shouldn't fail here");
        let _ = inode_list.list.remove_edge(edge);

        inode_list.list.add_edge(newparent_node, node_index, ());

        Ok(())
    }
}

impl Drop for Rfs {
    fn drop(&mut self) {
        match self.mount.unmount(UnmountFlags::DETACH) {
            Ok(()) => {
                info!("Unmounted origin {:?} mount", self.origin_mount.path());
            }
            Err(err) => {
                error!(
                    "Failed to unmounted origin {:?} mount: {err}",
                    self.origin_mount
                );
            }
        }
    }
}

fn fn_check_access_read(fh: u64) -> bool {
    (fh & 1) != 0
}

fn fn_check_access_write(fh: u64) -> bool {
    (fh & (1 << 1)) != 0
}

fn std_file_type_to_fuse_file_type(tp: fs::FileType) -> FileType {
    if tp.is_symlink() {
        return FileType::Symlink;
    }

    if tp.is_dir() {
        return FileType::Directory;
    }

    FileType::RegularFile
}
