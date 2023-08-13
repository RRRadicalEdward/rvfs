use std::{
    ffi::OsStr,
    io::{Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::Path,
    sync::atomic::Ordering,
    time::{Duration, SystemTime},
};

use fuser::{
    Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use libc::c_int;
use tracing::{debug, error, trace};

use crate::{error::FuseError, rfs::Rfs};

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

    #[tracing::instrument(skip(self))]
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent = fuse_reply_error!(
            self.find_by_id(parent),
            reply,
            "Can't find parent inode with {} ino",
            parent
        );

        let name = Path::new(name);
        let inode = fuse_reply_error!(
            self.find_by_name(parent.path.as_path(), name),
            reply,
            "Can't find item with {:?} name",
            name
        );

        reply.entry(&Duration::from_secs(1), &inode.attr, 0);
    }

    #[tracing::instrument(skip(self))]
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let node = fuse_reply_error!(
            self.find_by_id(ino),
            reply,
            "Can't find inode with {} ino",
            ino
        );

        reply.attr(&Duration::new(0, 0), &node.attr)
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
        let inode = fuse_reply_error!(
            self.find_mut_by_id(ino),
            reply,
            "Cannot find inode with {} ino",
            ino
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

        reply.attr(&Duration::from_secs(1), &inode.attr)
    }

    #[tracing::instrument(skip(self))]
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
        let node = fuse_reply_error!(
            self.find_by_id(ino),
            reply,
            "Cannot find inode with {} ino",
            ino
        );

        let file = fuse_reply_error!(
            self.open_file(node, fh, true, false),
            reply,
            "Failed to open file with inode {} and fh {}",
            node.id,
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

    #[tracing::instrument(skip(self))]
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
        let inode = fuse_reply_error!(
            self.find_by_id(ino),
            reply,
            "Cannot find inode with {} ino",
            ino
        );

        let mut file = fuse_reply_error!(
            self.open_file(inode, fh, false, true),
            reply,
            "Failed to open file with {} and fh {}",
            ino,
            fh
        );
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

        match file.write_all(data) {
            Ok(()) => {}
            Err(err) => {
                error!("Failed to write data to file with {ino} inode: {err}");
                reply.error(FuseError::last().into());
                return;
            }
        };

        let inode = self.find_mut_by_id(ino).unwrap();
        let attr = &mut inode.attr;
        if data.len() + offset as usize > attr.size as usize {
            attr.size = (data.len() + offset as usize) as u64;
        }

        attr.ctime = SystemTime::now();
        attr.mtime = SystemTime::now();

        reply.written(data.len() as u32)
    }

    #[tracing::instrument(skip(self))]
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
        let entry = fuse_reply_error!(
            self.find_mut_by_id(ino),
            reply,
            "Cannot find inode with {} ino",
            ino
        );

        entry.open_handles.fetch_min(1, Ordering::Relaxed);

        reply.ok()
    }

    #[tracing::instrument(skip(self))]
    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let (dir, id) = fuse_reply_error!(
            self.find_by_id(ino)
                .map(|entry| (entry.path.clone(), entry.id)),
            reply,
            "Cannot find inode with {} ino",
            ino
        );

        self.add_folder(dir).unwrap();

        reply.opened(id, 0);
    }

    #[tracing::instrument(skip(self))]
    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let id = fuse_reply_error!(
            self.find_by_id(ino).map(|entry| entry.id),
            reply,
            "Cannot find inode with {} ino",
            ino
        );

        for (i, inode) in self
            .inode_lists
            .iter()
            .filter(|(_, inode)| inode.parent_id == id)
            .enumerate()
            .skip(offset as usize)
            .map(|(i, item)| (i + 1, item.1))
        {
            trace!("Replying readdir with: {:?}", inode.path);
            if reply.add(
                inode.id,
                i as i64,
                inode.attr.kind,
                inode.path.file_name().unwrap_or("..".as_ref()),
            ) {
                break;
            }
        }
        reply.ok()
    }

    fn access(&mut self, _req: &Request<'_>, ino: u64, _mask: i32, reply: ReplyEmpty) {
        let _ = fuse_reply_error!(
            self.find_by_id(ino),
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
        let _ = fuse_reply_error!(
            self.find_by_id(parent),
            reply,
            "Can't find inode with {} ino",
            parent
        );

        let (read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => (true, false),
            libc::O_WRONLY => (false, true),
            libc::O_RDWR => (true, true),
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let attr = self.create(name, parent, mode).unwrap();
        let fh = self.allocate_fh(attr.ino, read, write).unwrap();
        reply.created(&Duration::from_secs(1), &attr, 0, fh, 0);
    }
}
