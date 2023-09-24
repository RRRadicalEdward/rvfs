use std::{path::PathBuf, sync::atomic::AtomicU64, time::SystemTime};

use fuser::{FileAttr, FileType};

#[derive(Default, Clone)]
pub struct FileAttrBuilder {
    ino: u64,
    size: u64,
    blocks: u64,
    atime: Option<SystemTime>,
    mtime: Option<SystemTime>,
    ctime: Option<SystemTime>,
    crtime: Option<SystemTime>,
    kind: Option<FileType>,
    perm: u16,
    nlink: u32,
    uid: u32,
    gid: u32,
    rdev: u32,
    blksize: u32,
    flags: u32,
}

impl FileAttrBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ino(mut self, ino: u64) -> Self {
        self.ino = ino;
        self
    }

    pub fn with_size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }

    pub fn with_blocks(mut self, blocks: u64) -> Self {
        self.blocks = blocks;
        self
    }

    pub fn with_atime(mut self, atime: SystemTime) -> Self {
        self.atime = Some(atime);
        self
    }

    pub fn with_mtime(mut self, mtime: SystemTime) -> Self {
        self.mtime = Some(mtime);
        self
    }

    pub fn with_ctime(mut self, ctime: SystemTime) -> Self {
        self.ctime = Some(ctime);
        self
    }

    pub fn with_crtime(mut self, crtime: SystemTime) -> Self {
        self.crtime = Some(crtime);
        self
    }

    pub fn with_kind(mut self, kind: FileType) -> Self {
        self.kind = Some(kind);
        self
    }

    pub fn with_perm(mut self, perm: u16) -> Self {
        self.perm = perm;
        self
    }

    pub fn with_nlink(mut self, nlink: u32) -> Self {
        self.nlink = nlink;
        self
    }

    pub fn with_uid(mut self, uid: u32) -> Self {
        self.uid = uid;
        self
    }

    pub fn with_gid(mut self, uid: u32) -> Self {
        self.uid = uid;
        self
    }

    pub fn with_rdev(mut self, rdev: u32) -> Self {
        self.rdev = rdev;
        self
    }

    pub fn with_blksize(mut self, blksize: u32) -> Self {
        self.blksize = blksize;
        self
    }

    pub fn with_flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    pub fn build(self) -> FileAttr {
        FileAttr {
            ino: self.ino,
            size: self.size,
            blocks: self.blocks,
            atime: self.atime.unwrap_or(SystemTime::now()),
            mtime: self.mtime.unwrap_or(SystemTime::now()),
            ctime: self.ctime.unwrap_or(SystemTime::now()),
            crtime: self.crtime.unwrap_or(SystemTime::now()),
            kind: self.kind.unwrap_or(FileType::RegularFile),
            perm: self.perm,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: self.rdev,
            blksize: self.blksize,
            flags: self.flags,
        }
    }
}

#[derive(Debug)]
pub struct Inode {
    pub proxy_path: PathBuf,
    pub origin_path: PathBuf,
    pub parent_id: u64,
    pub attr: FileAttr,
    pub open_handles: AtomicU64,
}

impl Inode {
    pub fn new(path: PathBuf, origin_path: PathBuf, parent_id: u64, attr: FileAttr) -> Self {
        Inode {
            proxy_path: path,
            origin_path,
            parent_id,
            attr,
            open_handles: AtomicU64::new(0),
        }
    }
}
