use std::path::PathBuf;

use anyhow::{ensure, Context};
use fuser::MountOption;

const HELP: &str = "\
Proxy VFS that focuses on forbitting access malwares

Usage: rvfs [OPTIONS] DEVICE MOUNTPOINT

OPTIONS
       -h  print help.

       -o OPTION[,OPTION...]
           mount options(see mount.fuse(8) for list of all options).
";

#[derive(Debug)]
pub struct Cli {
    pub device: PathBuf,
    pub mountpoint: PathBuf,
    pub options: Vec<MountOption>,
}

impl Cli {
    pub fn parse() -> anyhow::Result<Self> {
        let mut pargs = pico_args::Arguments::from_env();

        if pargs.contains(["-h", "--help"]) {
            print!("{}", HELP);
            std::process::exit(0);
        }

        let mut options = Vec::new();
        while let Some(raw_options) = pargs
            .opt_value_from_str::<&str, String>("-o")
            .context("Unable to get mount options")?
        {
            for option in raw_options.split(',').flat_map(str::split_whitespace) {
                options.push(mount_option_from_str(option))
            }
        }

        let device = pargs
            .free_from_str::<PathBuf>()
            .context("Unable to get device path")?;
        ensure!(device.exists(), "{:?} device path doesn't exists", device);

        let mountpoint = pargs
            .free_from_str::<PathBuf>()
            .context("Unable to get mountpoint path")?;

        ensure!(
            mountpoint.exists(),
            "{:?} mountpoint path doesn't exists",
            mountpoint
        );

        Ok(Cli {
            device,
            mountpoint,
            options,
        })
    }
}

pub fn mount_option_from_str(s: &str) -> MountOption {
    match s {
        "auto_unmount" => MountOption::AutoUnmount,
        "allow_other" => MountOption::AllowOther,
        "allow_root" => MountOption::AllowRoot,
        "default_permissions" => MountOption::DefaultPermissions,
        "dev" => MountOption::Dev,
        "nodev" => MountOption::NoDev,
        "suid" => MountOption::Suid,
        "nosuid" => MountOption::NoSuid,
        "ro" => MountOption::RO,
        "rw" => MountOption::RW,
        "exec" => MountOption::Exec,
        "noexec" => MountOption::NoExec,
        "atime" => MountOption::Atime,
        "noatime" => MountOption::NoAtime,
        "dirsync" => MountOption::DirSync,
        "sync" => MountOption::Sync,
        "async" => MountOption::Async,
        x if x.starts_with("fsname=") => MountOption::FSName(x[7..].into()),
        x if x.starts_with("subtype=") => MountOption::Subtype(x[8..].into()),
        x => MountOption::CUSTOM(x.into()),
    }
}
