use std::{path::PathBuf, sync::atomic::AtomicU64};

use fuser::FileAttr;

#[derive(Debug)]
pub struct Inode {
    pub id: u64,
    pub path: PathBuf,
    pub parent_id: u64,
    pub attr: FileAttr,
    pub open_handles: AtomicU64,
}

impl Inode {
    pub fn new(id: u64, path: PathBuf, parent_id: u64, mut attr: FileAttr) -> Self {
        attr.ino = id;
        Inode {
            id,
            path,
            parent_id,
            attr,
            open_handles: AtomicU64::new(0),
        }
    }
}
