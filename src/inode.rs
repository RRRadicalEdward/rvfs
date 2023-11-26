use std::{
    ffi::OsStr,
    fs::File,
    os::fd::{FromRawFd, RawFd},
    path::{Path, PathBuf},
    time::SystemTime,
};

use fuser::{FileAttr, FileType};
use petgraph::{prelude::*, visit::Walker};

#[derive(Default)]
pub struct InodeList {
    pub list: Graph<Inode, ()>,
}

impl InodeList {
    pub fn insert(&mut self, mut node: Inode, parent_node: NodeIndex) -> FileAttr {
        let node_id = self.list.node_count() as u64 + 1;
        node.attr.ino = node_id;
        let attr = node.attr;
        let node = self.list.add_node(node);

        self.list.add_edge(parent_node, node, ());
        attr
    }

    pub fn find_child_by_name<P: AsRef<Path>>(
        &self,
        parent_node: NodeIndex,
        name: P,
    ) -> Option<(NodeIndex, &Inode)> {
        self.list
            .neighbors(parent_node)
            .map(|index| (index, self.list.node_weight(index).unwrap()))
            .find(|(_, node)| {
                node.proxy_path.file_name().unwrap_or(OsStr::new("..")) == name.as_ref().as_os_str()
            })
    }

    pub fn find_by_id(&self, inode: u64) -> Option<(NodeIndex, &Inode)> {
        Bfs::new(&self.list, NodeIndex::default())
            .iter(&self.list)
            .map(|index| (index, self.list.node_weight(index).unwrap()))
            .find(|(_, node)| node.attr.ino == inode)
    }

    pub fn find_child_by_name_mut<P: AsRef<Path>>(
        &mut self,
        parent_node: NodeIndex,
        name: P,
    ) -> Option<(NodeIndex, &mut Inode)> {
        self.list
            .neighbors(parent_node)
            .find(|&node_index| {
                let node = self
                    .list
                    .node_weight(node_index)
                    .expect("should be safe to unwrap as we within the valid index range");
                node.proxy_path.file_name().unwrap_or(OsStr::new("..")) == name.as_ref().as_os_str()
            })
            .map(|node_index| {
                (
                    node_index,
                    self.list
                        .node_weight_mut(node_index)
                        .expect("should be safe to unwrap as we within the valid index range"),
                )
            })
    }

    pub fn find_by_id_mut(&mut self, inode: u64) -> Option<(NodeIndex, &mut Inode)> {
        Bfs::new(&self.list, NodeIndex::default())
            .iter(&self.list)
            .find(|&node_index| {
                let node = self
                    .list
                    .node_weight(node_index)
                    .expect("should be safe to unwrap as we within the valid index range");
                node.attr.ino == inode
            })
            .map(|node_index| {
                (
                    node_index,
                    self.list
                        .node_weight_mut(node_index)
                        .expect("should be safe to unwrap as we within the valid index range"),
                )
            })
    }

    pub fn childs(&self, parent_node: NodeIndex) -> impl Iterator<Item = &Inode> {
        self.list
            .neighbors(parent_node)
            .map(|index| self.list.node_weight(index).unwrap())
    }
}

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

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone)]
pub struct OpenedHandlers {
    pub fh: RawFd,
    pub count: u64,
}

impl Drop for OpenedHandlers {
    fn drop(&mut self) {
        drop(unsafe { File::from_raw_fd(self.fh) });
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Inode {
    pub proxy_path: PathBuf,
    pub origin_path: PathBuf,
    pub attr: FileAttr,
    pub open_handles: Option<OpenedHandlers>,
}

impl Inode {
    pub fn new(path: PathBuf, origin_path: PathBuf, attr: FileAttr) -> Self {
        Inode {
            proxy_path: path,
            origin_path,
            attr,
            open_handles: None,
        }
    }
}
