use std::{
    collections::HashMap,
    ffi::OsStr,
    fs,
    fs::{read_dir, File},
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
use sys_mount::{Mount, Unmount, UnmountFlags};
use tempdir::TempDir;

use crate::{
    error::FuseError,
    inode::{FileAttrBuilder, Inode, OpenedHandlers},
    scanner::ClamAV,
};

type FuseResult<T> = Result<T, FuseError>;

struct InodeList {
    list: RwLock<HashMap<u64, RwLock<Inode>>>,
}

impl InodeList {
    fn insert(
        &mut self,
        path: PathBuf,
        origin_path: PathBuf,
        parent_id: u64,
        attr: FileAttrBuilder,
    ) -> u64 {
        let mut list = self.list.write().unwrap();
        let id = u64::try_from(list.len() + 1).unwrap();
        list.insert(
            id,
            RwLock::new(Inode::new(
                path,
                origin_path,
                parent_id,
                attr.with_ino(id).build(),
            )),
        );

        id
    }

    fn read_view(&self) -> InodeListReadView {
        InodeListReadView(self.list.read().unwrap())
    }

    fn write_view(&self) -> InodeListWriteView {
        InodeListWriteView(self.list.write().unwrap())
    }
}

pub struct InodeListReadView<'a>(RwLockReadGuard<'a, HashMap<u64, RwLock<Inode>>>);

impl<'a> InodeListReadView<'a> {
    pub(crate) fn iter_read(&'a self) -> impl Iterator<Item = RwLockReadGuard<'a, Inode>> {
        self.0.iter().map(|(_, entry)| entry.read().unwrap())
    }
    fn find_by_path<P: AsRef<Path>>(&'a self, path: P) -> FuseResult<&'a RwLock<Inode>> {
        self.0
            .iter()
            .find(|(_, node)| {
                let node = node.read().unwrap();
                node.proxy_path == path.as_ref().as_os_str()
            })
            .map(|(_, inode)| inode)
            .ok_or(FuseError::NO_EXIST)
    }

    pub fn find_by_name<P: AsRef<Path>>(
        &'a self,
        parent: u64,
        name: P,
    ) -> FuseResult<&'a RwLock<Inode>> {
        self.0
            .iter()
            .find(|(_, node)| {
                let node = node.read().unwrap();

                node.parent_id == parent
                    && node.proxy_path.file_name().unwrap_or(OsStr::new(".."))
                        == name.as_ref().as_os_str()
            })
            .map(|(_, inode)| inode)
            .ok_or(FuseError::NO_EXIST)
    }

    pub fn find_by_id(&'a self, inode: u64) -> FuseResult<&'a RwLock<Inode>> {
        self.0
            .iter()
            .find(|(id, _)| **id == inode)
            .map(|(_, inode)| inode)
            .ok_or(FuseError::NO_EXIST)
    }

    fn get(&self, id: u64) -> Option<&RwLock<Inode>> {
        self.0.get(&id)
    }
}

struct InodeListWriteView<'a>(RwLockWriteGuard<'a, HashMap<u64, RwLock<Inode>>>);

pub struct Rfs {
    inode_list: InodeList,
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
            inode_list: InodeList {
                list: RwLock::new(HashMap::new()),
            },
            proxy_mount: mount_point,
            origin_mount,
            mount,
            clamav,
        })
    }

    pub fn inode_list(&self) -> InodeListReadView {
        self.inode_list.read_view()
    }

    pub fn init(&mut self) {
        let attr = self.stat(&self.origin_mount).unwrap();

        let root_id = self.inode_list.insert(
            self.proxy_mount.clone(),
            self.origin_mount.path().to_path_buf(),
            0,
            attr.clone(),
        );

        let _ = self.inode_list.insert(
            PathBuf::from("."),
            self.origin_mount.path().to_path_buf(),
            root_id,
            attr.clone(),
        );
        let _ = self.inode_list.insert(
            PathBuf::from(".."),
            self.proxy_mount.parent().unwrap().to_path_buf(),
            root_id,
            attr,
        );
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
        let (proxy_path, origin_path) = {
            let read_view = self.inode_list.read_view();

            match read_view.find_by_name(parent_ino, name) {
                Ok(_) => return Err(FuseError::FILE_EXISTS),
                Err(FuseError::NO_EXIST) => {}
                Err(error) => return Err(error),
            };

            let inode_lock = read_view.find_by_id(parent_ino)?;
            let inode = inode_lock.read().unwrap();
            (inode.proxy_path.join(name), inode.origin_path.join(name))
        };

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
            .with_perm(mode as u16);

        let id = self
            .inode_list
            .insert(proxy_path, origin_path, parent_ino, attr);

        let read_view = self.inode_list();
        let inode = read_view.get(id).ok_or(FuseError::IO)?;
        let inode = inode.read().unwrap();
        Ok(inode.attr)
    }

    fn insert_item(&mut self, item: PathBuf, parent_ino: u64) -> FuseResult<()> {
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

        let attr = self.stat(&item)?;

        trace!("Added {:?} item", proxy_path);
        self.inode_list.insert(proxy_path, item, parent_ino, attr);

        Ok(())
    }

    pub fn add_folder<P: AsRef<Path>>(&mut self, folder: P, ino: u64) -> FuseResult<()> {
        trace!("Adding folder: {:?}...", folder.as_ref());
        for item in read_dir(folder).map_err(|_| FuseError::last())? {
            match item {
                Ok(item) => match self.insert_item(item.path(), ino) {
                    Ok(()) => {}
                    Err(err) if err == FuseError::OPERATION_NOT_PERMITTED => {
                        warn!("Operation is not permitted for {:?}", item.path());
                        continue;
                    }
                    Err(err) => return Err(err),
                },
                Err(_) => {
                    return Err(FuseError::last());
                }
            }
        }

        Ok(())
    }

    pub fn allocate_fh(&mut self, inode: u64, read: bool, write: bool) -> FuseResult<u64> {
        let read_view = self.inode_list.read_view();
        let inode_lock = read_view.find_by_id(inode)?;
        let mut inode = inode_lock.write().unwrap();

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

    pub fn remove(&mut self, id: u64) -> FuseResult<()> {
        let mut inode_view = self.inode_list.write_view();

        let inode_lock = inode_view.0.remove(&id).ok_or(FuseError::NO_EXIST)?;
        let inode = inode_lock.read().unwrap();

        match inode.attr.kind {
            FileType::RegularFile => {
                fs::remove_file(&inode.origin_path).map_err(|_| FuseError::last())?;
            }
            FileType::Directory => {
                inode_view.0.retain(|_, node| {
                    let node = node.read().unwrap();
                    node.parent_id != inode.attr.ino
                }); // delete children
                fs::remove_dir_all(&inode.origin_path).map_err(|_| FuseError::last())?;
            }
            other => {
                error!("Remove is not implemented for {other:?}");
                return Err(FuseError::NOT_IMPLEMENTED);
            }
        }

        Ok(())
    }

    pub fn rename(
        &mut self,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
    ) -> FuseResult<()> {
        let read_view = self.inode_list.read_view();

        let new_path = match read_view.find_by_id(newparent) {
            Ok(inode) => {
                let inode = inode.read().unwrap();
                inode.proxy_path.join(newname)
            }
            Err(_) => {
                error!("Can't find newparent inode {}", parent);
                return Err(FuseError::INVALID_ARGUMENT);
            }
        };

        let inode_lock = read_view.find_by_name(parent, Path::new(name))?;
        let mut inode = inode_lock.write().unwrap();

        let new = self.proxy_path_to_origin_path(new_path.as_path());
        fs::rename(&inode.origin_path, &new).map_err(|_| FuseError::last())?;

        inode.parent_id = newparent;
        inode.proxy_path = new_path;
        inode.origin_path = new;

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
