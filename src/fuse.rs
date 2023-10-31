use std::{
    ffi::OsStr,
    io::{Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    time::{Duration, SystemTime},
};

use fuser::{
    Filesystem, FileType, KernelConfig, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use libc::c_int;
use log::{debug, error, trace};

use crate::{error::FuseError, rfs::Rfs};

const DEFUALT_TTL: Duration = Duration::from_secs(1);

macro_rules! fuse_reply_error {
    ($result:expr, $reply:ident, $message:expr) => {
        match $result {
            Ok(val) => val,
            Err(err) => {
                error!("{}: {}({})", line!(), $message, err.as_ref());
                $reply.error(err.into());
                return;
            }
        }
    };
}

macro_rules! fuse_reply_last_error {
    ($result:expr, $reply:ident, $message:expr) => {
        match $result {
            Ok(val) => val,
            Err(err) => {
                error!("{}: {}({err})", line!(), $message);
                $reply.error(FuseError::last().into());
                return;
            }
        }
    };
}

impl Filesystem for Rfs {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        debug!("Initialization...");
        self.init();
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let read_view = self.inode_list();

        let (parent_node, _) = read_view.find_by_id(parent).unwrap();

        let (_, inode) = fuse_reply_error!(
            read_view
                .find_child_by_name(parent_node, name)
                .ok_or(FuseError::NO_EXIST),
            reply,
            format!("Can't find item with {name:?} name")
        );

        reply.entry(&DEFUALT_TTL, &inode.attr, 0);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let read_view = self.inode_list();

        let (_, inode) = fuse_reply_error!(
            read_view.find_by_id(ino).ok_or(FuseError::NO_EXIST),
            reply,
            format!("Can't find inode with {ino} ino")
        );

        reply.attr(&Duration::new(0, 0), &inode.attr)
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let mut write_view = self.inode_list_write();

        let (_, inode) = fuse_reply_error!(
            write_view.find_by_id_mut(ino).ok_or(FuseError::NO_EXIST),
            reply,
            format!("Cannot find inode with {ino} ino")
        );

        if let Some(atime) = atime {
            let time = match atime {
                TimeOrNow::SpecificTime(time) => time,
                TimeOrNow::Now => SystemTime::now(),
            };

            inode.attr.atime = time;
        }

        if let Some(mtime) = mtime {
            let time = match mtime {
                TimeOrNow::SpecificTime(time) => time,
                TimeOrNow::Now => SystemTime::now(),
            };

            inode.attr.mtime = time;
        }

        if let Some(ctime) = ctime {
            inode.attr.mtime = ctime;
        }

        if let Some(crtime) = crtime {
            inode.attr.crtime = crtime;
        }

        reply.attr(&DEFUALT_TTL, &(inode.attr).clone())
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let attr = fuse_reply_error!(
            self.create(name, parent, mode, FileType::Directory),
            reply,
            format!("Can't create directory with {parent} parent")
        );

        reply.entry(&DEFUALT_TTL, &attr, 0);
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let ino = {
            let read_view = self.inode_list();

            let (parent_node, _) = read_view.find_by_id(parent).unwrap();

            let (_, inode) = fuse_reply_error!(
                read_view
                    .find_child_by_name(parent_node, name)
                    .ok_or(FuseError::NO_EXIST),
                reply,
                format!("Can't find inode with {parent} parent and {name:?} name")
            );

            if inode.attr.kind != FileType::RegularFile {
                reply.error(FuseError::IS_DIRECTORY.into());
                return;
            }

            inode.attr.ino
        };

        fuse_reply_error!(
            self.remove(ino),
            reply,
            format!("Failed to remove {ino} file")
        );

        reply.ok()
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let ino = {
            let inode_list = self.inode_list();

            let (parent_node, _) = inode_list.find_by_id(parent).unwrap();

            let (_, inode) = fuse_reply_error!(
                inode_list
                    .find_child_by_name(parent_node, name)
                    .ok_or(FuseError::NO_EXIST),
                reply,
                format!("Can't find inode with {parent} parent and {name:?} name")
            );

            if inode.attr.kind != FileType::Directory {
                reply.error(FuseError::NOT_DIRECTORY.into());
                return;
            }

            if inode_list.childs(parent_node).count() > 0 {
                reply.error(FuseError::DIRECTORY_NOT_EMPTY.into()); // We have to delete only empty folders
                return;
            }

            inode.attr.ino
        };

        fuse_reply_error!(self.remove(ino), reply, "Failed to remove {ino} directory");

        reply.ok()
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        fuse_reply_error!(
            self.rename(parent, name, newparent, newname),
            reply,
            format!("Failed to rename item {name:?} with {parent} parent to  {newname:?} newname with {newparent} newparent")
        );

        reply.ok()
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let (_, read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => (libc::R_OK, true, false),
            libc::O_WRONLY => (libc::W_OK, false, true),
            libc::O_RDWR => (libc::R_OK | libc::W_OK, true, true),
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let fh = match self.allocate_fh(ino, read, write) {
            Ok(fh) => fh,
            Err(error) => {
                error!("Cannot allocated fh for {ino}: open exit with {}", error);
                reply.error(error.into());
                return;
            }
        };

        reply.opened(fh, 0);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let read_view = self.inode_list();

        let (_, inode) = fuse_reply_error!(
            read_view.find_by_id(ino).ok_or(FuseError::NO_EXIST),
            reply,
            format!("Cannot find inode with {ino} ino")
        );

        let file = fuse_reply_error!(
            self.open_file(inode, fh, true, false),
            reply,
            format!(
                "Failed to open file with inode {} and {fh} fh",
                inode.attr.ino
            )
        );

        let offset = u64::try_from(offset).unwrap();
        let amount = usize::min(
            usize::try_from(inode.attr.size.saturating_sub(offset)).unwrap(),
            usize::try_from(size).unwrap(),
        );
        let mut buf = vec![0; amount];

        fuse_reply_last_error!(
            file.read_exact_at(&mut buf, offset),
            reply,
            format!("Failed to read {amount} bytes from file")
        );

        reply.data(&buf)
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let mut write_view = self.inode_list_write();
        let (_, inode) = fuse_reply_error!(
            write_view.find_by_id_mut(ino).ok_or(FuseError::NO_EXIST),
            reply,
            format!("Cannot find inode with {ino} ino")
        );

        let mut file = fuse_reply_error!(
            self.open_file(inode, fh, false, true),
            reply,
            format!("Failed to open file with {ino} inode and {fh} fh")
        );

        fuse_reply_last_error!(
            file.seek(SeekFrom::Start(offset as u64)),
            reply,
            format!("File seek failed for file with {ino} inode")
        );

        let written = fuse_reply_last_error!(
            file.write(data),
            reply,
            format!("Failed to write data to file with {ino} inode")
        );

        let (_, inode) = write_view.find_by_id_mut(ino).unwrap();

        let attr = &mut inode.attr;
        if data.len() + offset as usize > attr.size as usize {
            attr.size = (data.len() + offset as usize) as u64;
        }

        let time_now = SystemTime::now();
        attr.ctime = time_now;
        attr.mtime = time_now;

        reply.written(written as u32)
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let mut write_view = self.inode_list_write();

        let (_, inode) = fuse_reply_error!(
            write_view.find_by_id_mut(ino).ok_or(FuseError::NO_EXIST),
            reply,
            format!("Cannot find inode with {ino} ino")
        );

        if let Some(open_handlers) = inode.open_handles.as_mut() {
            open_handlers.count = open_handlers.count.saturating_sub(1);

            if open_handlers.count == 0 {
                inode.open_handles = None;
            }
        }

        reply.ok()
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let (dir, id) = {
            let read_view = self.inode_list();

            let (_, inode) = fuse_reply_error!(
                read_view.find_by_id(ino).ok_or(FuseError::NO_EXIST),
                reply,
                format!("Cannot find inode with {ino} ino")
            );

            (inode.origin_path.clone(), inode.attr.ino)
        };

        self.add_folder(dir, id).unwrap();

        reply.opened(id, 0);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let inode_list = self.inode_list();

        let (node_index, _) = fuse_reply_error!(
            inode_list.find_by_id(ino).ok_or(FuseError::NO_EXIST),
            reply,
            format!("Cannot find inode with {ino} ino")
        );

        for (i, child) in inode_list
            .childs(node_index)
            .enumerate()
            .skip(offset as usize)
            .map(|(i, item)| (i + 1, item))
        {
            trace!("Replying readdir with: {:?}", child.proxy_path);
            if reply.add(
                child.attr.ino,
                i as i64,
                child.attr.kind,
                child.proxy_path.file_name().unwrap_or("..".as_ref()),
            ) {
                break;
            }
        }

        reply.ok()
    }

    fn access(&mut self, _req: &Request<'_>, ino: u64, _mask: i32, reply: ReplyEmpty) {
        let read_view = self.inode_list();

        let _ = fuse_reply_error!(
            read_view.find_by_id(ino).ok_or(FuseError::NO_EXIST),
            reply,
            format!("Cannot find inode with {ino} ino")
        );
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let (read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => (true, false),
            libc::O_WRONLY => (false, true),
            libc::O_RDWR => (true, true),
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let attr = fuse_reply_error!(
            self.create(name, parent, mode, FileType::RegularFile),
            reply,
            format!("Can't create file from {parent} directory")
        );

        let fh = self.allocate_fh(attr.ino, read, write).unwrap();
        reply.created(&DEFUALT_TTL, &attr, 0, fh, 0);
    }
}
