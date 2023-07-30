use std::{
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
};

use clap::{CommandFactory, error::ErrorKind, Parser};
use fuser::MountOption;
use tracing_subscriber::{EnvFilter, fmt};

use rfs::Rfs;

mod error;
mod fuse;
mod inode;
mod rfs;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to device to mount
    #[arg(short, long, value_name = "PATH")]
    device: PathBuf,
    /// Path where the device should be mounted in system
    #[arg(short, long, value_name = "PATH")]
    mountpoint: PathBuf,
}

fn main() {
    let args = Args::parse();
    if !args.device.exists() {
        let mut cmd = Args::command();
        cmd.error(
            ErrorKind::InvalidValue,
            format!("{:?} device path doesn't exists", args.device),
        )
            .exit();
    }

    if !args.mountpoint.exists() {
        let mut cmd = Args::command();
        cmd.error(
            ErrorKind::InvalidValue,
            format!("{:?} mountpoint path doesn't exists", args.device),
        )
            .exit();
    }

    setup_logger();

    let options = vec![
        MountOption::FSName("rvfs".to_string()),
        MountOption::AutoUnmount,
        MountOption::AllowOther,
        MountOption::Dev,
        MountOption::RW,
        MountOption::Sync,
    ];

    let proxy_file_system = Rfs::new(args.device.clone(), args.mountpoint.clone()).unwrap();
    let session = fuser::spawn_mount2(proxy_file_system, args.mountpoint, &options)
        .expect("Fuse mount failed");

    let conv_var = Arc::new((Mutex::new(false), Condvar::new()));
    ctrlc::set_handler({
        let conv_var = conv_var.clone();
        move || {
            let (mtx, conv_var) = &*conv_var;
            let mut mtx = mtx.lock().unwrap();
            *mtx = true;
            conv_var.notify_one();
        }
    })
        .expect("Failed to set Ctrl-C handler");

    let (mtx, conv_var) = &*conv_var;
    let mut mtx = mtx.lock().unwrap();

    while !*mtx {
        mtx = conv_var.wait(mtx).unwrap();
    }

    session.join();
}

pub fn setup_logger() {
    let format = fmt::format()
        .with_ansi(true)
        .with_level(true)
        .with_target(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_source_location(true)
        .with_line_number(true)
        .pretty();

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("trace"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .event_format(format)
        .init();
}
