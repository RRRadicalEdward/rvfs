use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fs,
    fs::{read_dir, File},
    ops::Add,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

use fuser::{FileAttr, FileType};
use sys_mount::{Mount, Unmount, UnmountFlags};
use tempdir::TempDir;
use tracing::{debug, error, info, trace};

use crate::{error::FuseError, inode::Inode};

pub const FUSE_ROOT_INODE_ID: u64 = 1;

type FuseResult<T> = Result<T, FuseError>;

#[derive(Debug)]
pub struct Rfs {
    pub(crate) inode_lists: BTreeMap<u64, Inode>,
    // sync access
    last_ino_id: AtomicU64,
    proxy_mount: PathBuf,
    origin_mount: TempDir,
    mount: Mount,
}

impl Rfs {
    pub fn new(source: PathBuf, mount_point: PathBuf) -> anyhow::Result<Self> {
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
            inode_lists: BTreeMap::new(),
            last_ino_id: AtomicU64::new(0),
            proxy_mount: mount_point,
            origin_mount,
            mount,
        })
    }
}

impl Rfs {
    pub fn init(&mut self) {
        let attr = self.stat(&self.origin_mount).unwrap();

        self.inode_lists.insert(
            FUSE_ROOT_INODE_ID,
            Inode::new(FUSE_ROOT_INODE_ID, self.proxy_mount.clone(), 0, attr),
        );
        self.last_ino_id
            .store(FUSE_ROOT_INODE_ID, Ordering::Relaxed);

        let id = self.last_ino_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.inode_lists.insert(
            id,
            Inode::new(id, PathBuf::from("."), FUSE_ROOT_INODE_ID, attr),
        );

        let id = self.last_ino_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.inode_lists.insert(
            id,
            Inode::new(id, PathBuf::from(".."), FUSE_ROOT_INODE_ID, attr),
        );
    }
    pub(crate) fn stat<P: AsRef<Path>>(&self, item: P) -> FuseResult<FileAttr> {
        debug!("Stat with {:?}", item.as_ref());

        let file = File::open(item).map_err(|_| FuseError::last())?;
        let meta = file.metadata().map_err(|_| FuseError::last())?;

        Ok(FileAttr {
            ino: 0,
            size: meta.size(),
            blocks: meta.blocks(),
            atime: SystemTime::UNIX_EPOCH
                .add(Duration::from_secs(u64::try_from(meta.atime()).unwrap())),
            mtime: SystemTime::UNIX_EPOCH
                .add(Duration::from_secs(u64::try_from(meta.mtime()).unwrap())),
            ctime: SystemTime::UNIX_EPOCH
                .add(Duration::from_secs(u64::try_from(meta.ctime()).unwrap())),
            crtime: meta.created().map_err(|_| FuseError::last())?,
            kind: std_file_type_to_fuse_file_type(meta.file_type()),
            perm: u16::try_from(meta.permissions().mode()).unwrap(),
            nlink: u32::try_from(meta.nlink()).unwrap(),
            uid: meta.uid(),
            gid: meta.gid(),
            rdev: u32::try_from(meta.rdev()).unwrap(),
            blksize: u32::try_from(meta.blksize()).unwrap(),
            flags: 0,
        })
    }

    pub fn create(&mut self, name: &OsStr, parent_ino: u64, mode: u32) -> FuseResult<FileAttr> {
        if self.inode_lists.iter().any(|entry| {
            entry.1.parent_id == parent_ino
                && entry.1.path.file_name().unwrap_or("..".as_ref()) == name
        }) {
            return Err(FuseError::FILE_EXISTS);
        }

        let inode = self.find_by_id(parent_ino)?;

        let path = inode.path.join(name);
        let origin_path = self.proxy_path_to_origin_path(&path);

        if origin_path.exists() {
            return Err(FuseError::FILE_EXISTS);
        }

        match File::create(&origin_path) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to create {origin_path:?} file: {err}");
                return Err(FuseError::last());
            }
        };

        let ino = self.last_ino_id.fetch_add(1, Ordering::Relaxed) + 1;
        let now = SystemTime::now();

        let attr = FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::RegularFile,
            perm: mode as u16,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
            flags: 0,
        };

        let _ = self.inode_lists.insert(
            ino,
            Inode {
                id: ino,
                path,
                parent_id: parent_ino,
                attr,
                open_handles: Default::default(),
            },
        );

        Ok(attr)
    }

    fn insert_item(&mut self, item: PathBuf, parent_ino: u64) -> FuseResult<()> {
        let attr = self.stat(&item)?;
        let proxy_path = self.origin_path_to_proxy_path(item);
        if !self
            .inode_lists
            .iter()
            .any(|entry| entry.1.parent_id == parent_ino && entry.1.path == proxy_path)
        {
            let inode = self.last_ino_id.fetch_add(1, Ordering::Relaxed) + 1;
            trace!("Added {:?} item", proxy_path);
            self.inode_lists
                .insert(inode, Inode::new(inode, proxy_path, parent_ino, attr));
        }

        Ok(())
    }

    pub fn add_folder<P: AsRef<Path>>(&mut self, folder: P) -> FuseResult<()> {
        let inode = self.find_by_path(&folder)?;

        let folder_ino = inode.id;

        let folder = self.proxy_path_to_origin_path(folder);
        trace!("Adding folder: {:?}...", folder);
        for item in read_dir(folder).map_err(|_| FuseError::last())? {
            match item {
                Ok(item) => {
                    self.insert_item(item.path(), folder_ino)?;
                }
                Err(_) => {
                    return Err(FuseError::last());
                }
            }
        }

        Ok(())
    }

    fn find_by_path<P: AsRef<Path>>(&self, path: P) -> FuseResult<&Inode> {
        self.inode_lists
            .iter()
            .find(|entry| entry.1.path == path.as_ref().as_os_str())
            .map(|(_, inode)| inode)
            .ok_or(FuseError::NO_EXIST)
    }

    pub fn find_by_name<P: AsRef<Path>>(&self, parent: P, name: P) -> FuseResult<&Inode> {
        let parent = self.find_by_path(parent)?;

        self.inode_lists
            .iter()
            .find(|entry| {
                entry.1.parent_id == parent.id
                    && entry.1.path.file_name().unwrap_or(OsStr::new(".."))
                        == name.as_ref().as_os_str()
            })
            .map(|(_, inode)| inode)
            .ok_or(FuseError::NO_EXIST)
    }

    pub fn find_by_id(&self, inode: u64) -> FuseResult<&Inode> {
        self.inode_lists
            .iter()
            .find(|(id, _)| **id == inode)
            .map(|(_, inode)| inode)
            .ok_or(FuseError::NO_EXIST)
    }

    pub fn find_mut_by_id(&mut self, inode: u64) -> FuseResult<&mut Inode> {
        self.inode_lists
            .iter_mut()
            .find(|(id, _)| **id == inode)
            .map(|(_, inode)| inode)
            .ok_or(FuseError::NO_EXIST)
    }

    pub fn allocate_fh(&mut self, inode: u64, read: bool, write: bool) -> FuseResult<u64> {
        let entry = self.find_mut_by_id(inode)?;

        let mut fh = entry.open_handles.fetch_add(1, Ordering::Relaxed) + 1;
        fh = (fh << 2) | u64::from(read) | (u64::from(write) << 1);

        Ok(fh)
    }

    pub fn open_file(&self, node: &Inode, fh: u64, read: bool, write: bool) -> FuseResult<File> {
        if read && !fn_check_access_read(fh) {
            error!("Read is not allowed!");
            return Err(FuseError::OPERATION_NOT_PERMITTED);
        }

        if write && !fn_check_access_write(fh) {
            error!("Write is not allowed!");
            return Err(FuseError::OPERATION_NOT_PERMITTED);
        }

        let path = self
            .origin_mount
            .path()
            .join(node.path.strip_prefix(&self.proxy_mount).unwrap());
        let file = match File::options().read(true).write(false).open(&path) {
            Ok(file) => file,
            Err(err) => {
                error!("Failed to open {:?}: {err}", path);
                return Err(FuseError::last());
            }
        };

        Ok(file)
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
