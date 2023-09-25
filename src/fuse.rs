use std::{
    ffi::OsStr,
    io::{Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::Path,
    time::{Duration, SystemTime},
};

use fuser::{
    FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use libc::c_int;
use log::{debug, error, trace};

use crate::{error::FuseError, rfs::Rfs};

const DEFUALT_TTL: Duration = Duration::from_secs(1);

macro_rules! fuse_reply_error {
    ($result:expr, $reply:ident, $fmt:literal, $($fmt_args: tt)*) => {
        match $result  {
            Ok(val) => val,
            Err(err) => {
                error!(concat!($fmt, "({})"), $($fmt_args)*, err.as_ref());
                $reply.error(err.into());
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

        let inode_lock = fuse_reply_error!(
            read_view.find_by_name(parent, Path::new(name)),
            reply,
            "Can't find item with {:?} name",
            name
        );

        let inode = inode_lock.read().unwrap();

        reply.entry(&DEFUALT_TTL, &inode.attr, 0);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let read_view = self.inode_list();

        let inode_lock = fuse_reply_error!(
            read_view.find_by_id(ino),
            reply,
            "Can't find inode with {} ino",
            ino
        );

        let inode = inode_lock.read().unwrap();

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
        let read_view = self.inode_list();

        let inode_lock = fuse_reply_error!(
            read_view.find_by_id(ino),
            reply,
            "Cannot find inode with {} ino",
            ino
        );

        let mut inode = inode_lock.write().unwrap();

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

        reply.attr(&DEFUALT_TTL, &inode.attr)
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
            "Can't create directory(parent: {})",
            parent
        );

        reply.entry(&DEFUALT_TTL, &attr, 0);
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let ino = {
            let read_view = self.inode_list();

            let inode_lock = fuse_reply_error!(
                read_view.find_by_name(parent, Path::new(name)),
                reply,
                "Can't find inode with {} parent and {:?} name",
                parent,
                name
            );

            let inode = inode_lock.read().unwrap();

            if inode.attr.kind != FileType::Directory {
                reply.error(FuseError::NOT_DIRECTORY.into());
                return;
            }

            inode.attr.ino
        };

        if self.inode_list().iter_read().filter(|inode|
            inode.parent_id == ino && inode.proxy_path.file_name().is_some() /* we only have it when filename is ".." */ && inode.proxy_path.file_name() != Some(".".as_ref())
        ).count() != 0
        {
            reply.error(FuseError::DIRECTORY_NOT_EMPTY.into()); // We have to delete only empty folders
            return;
        }

        fuse_reply_error!(self.remove(ino), reply, "Failed to remove {}", ino);

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

        let inode_lock = fuse_reply_error!(
            read_view.find_by_id(ino),
            reply,
            "Cannot find inode with {} ino",
            ino
        );

        let inode = inode_lock.read().unwrap();

        let file = fuse_reply_error!(
            self.open_file(&inode, fh, true, false),
            reply,
            "Failed to open file with inode {} and fh {}",
            inode.attr.ino,
            fh
        );

        let len = match file.metadata() {
            Ok(meta) => meta.len(),
            Err(err) => {
                error!("Failed to get metadata: {err}");
                reply.error(FuseError::last().into());
                return;
            }
        };

        let offset = u64::try_from(offset).unwrap();
        let amount = usize::min(
            usize::try_from(len.saturating_sub(offset)).unwrap(),
            usize::try_from(size).unwrap(),
        );
        let mut buf = vec![0; amount];

        match file.read_exact_at(&mut buf, offset) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to read {amount} bytes from file: {err}");
                reply.error(FuseError::last().into());
                return;
            }
        }

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
        let read_view = self.inode_list();
        let mut file = {
            let inode_lock = fuse_reply_error!(
                read_view.find_by_id(ino),
                reply,
                "Cannot find inode with {} ino",
                ino
            );

            let inode = inode_lock.read().unwrap();

            fuse_reply_error!(
                self.open_file(&inode, fh, false, true),
                reply,
                "Failed to open file with {} and fh {}",
                ino,
                fh
            )
        };

        if offset > file.metadata().unwrap().len() as i64 {
            reply.error(FuseError::INVALID_ARGUMENT.into());
            return;
        }

        match file.seek(SeekFrom::Start(offset as u64)) {
            Ok(_) => {}
            Err(err) => {
                error!("File seek failed for file with {ino} inode: {err}");
                reply.error(FuseError::last().into());
                return;
            }
        }

        let written = match file.write(data) {
            Ok(written) => written,
            Err(err) => {
                error!("Failed to write data to file with {ino} inode: {err}");
                reply.error(FuseError::last().into());
                return;
            }
        };

        let inode_lock = read_view.find_by_id(ino).unwrap();

        let mut inode = inode_lock.write().unwrap();

        let attr = &mut inode.attr;
        if data.len() + offset as usize > attr.size as usize {
            attr.size = (data.len() + offset as usize) as u64;
        }

        attr.ctime = SystemTime::now();
        attr.mtime = SystemTime::now();

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
        let read_view = self.inode_list();

        let inode_lock = fuse_reply_error!(
            read_view.find_by_id(ino),
            reply,
            "Cannot find inode with {} ino",
            ino
        );

        let mut inode = inode_lock.write().unwrap();
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

            let inode_lock = fuse_reply_error!(
                read_view.find_by_id(ino),
                reply,
                "Cannot find inode with {} ino",
                ino
            );

            let inode = inode_lock.read().unwrap();
            (inode.proxy_path.clone(), inode.attr.ino)
        };

        self.add_folder(dir).unwrap();

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
        let read_view = self.inode_list();

        let inode_lock = fuse_reply_error!(
            read_view.find_by_id(ino),
            reply,
            "Cannot find inode with {} ino",
            ino
        );

        let inode = inode_lock.read().unwrap();
        let id = inode.attr.ino;

        for (i, inode) in read_view
            .iter_read()
            .filter(|inode| inode.parent_id == id)
            .enumerate()
            .skip(offset as usize)
            .map(|(i, item)| (i + 1, item))
        {
            trace!("Replying readdir with: {:?}", inode.proxy_path);
            if reply.add(
                inode.attr.ino,
                i as i64,
                inode.attr.kind,
                inode.proxy_path.file_name().unwrap_or("..".as_ref()),
            ) {
                break;
            }
        }
        reply.ok()
    }

    fn access(&mut self, _req: &Request<'_>, ino: u64, _mask: i32, reply: ReplyEmpty) {
        let read_view = self.inode_list();

        let _ = fuse_reply_error!(
            read_view.find_by_id(ino),
            reply,
            "Cannot find inode with {} ino",
            ino
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
            "Can't create file from {} directory",
            parent
        );

        let fh = self.allocate_fh(attr.ino, read, write).unwrap();
        reply.created(&DEFUALT_TTL, &attr, 0, fh, 0);
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let ino = {
            let read_view = self.inode_list();

            let inode_lock = fuse_reply_error!(
                read_view.find_by_name(parent, Path::new(name)),
                reply,
                "Can't find inode with {} parent and {:?} name",
                parent,
                name
            );

            let inode = inode_lock.read().unwrap();

            if inode.attr.kind != FileType::RegularFile {
                reply.error(FuseError::IS_DIRECTORY.into());
                return;
            }

            inode.attr.ino
        };

        fuse_reply_error!(self.remove(ino), reply, "Failed to remove {}", ino);

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
            "Failed to rename item {:?} parent {} to newname {:?} newparent {}",
            name,
            parent,
            newname,
            newparent
        );

        reply.ok()
    }
}
