use fuser::Session;
use log::debug;
use simplelog::{Config, LevelFilter, SimpleLogger};

use rfs::Rfs;

use crate::cli::Cli;

mod cli;
mod error;
mod fuse;
mod inode;
mod rfs;
mod scanner;

fn main() {
    let Cli {
        device,
        mountpoint,
        options,
    } = Cli::parse().unwrap();

    setup_logger();

    debug!("Mount options: {options:?}");

    let proxy_file_system = Rfs::new(device.clone(), mountpoint.clone()).unwrap();
    let mut session = Session::new(proxy_file_system, mountpoint.as_ref(), &options)
        .expect("Failed to create FUSE session");

    let mut umount = session.unmount_callable();
    ctrlc::set_handler(move || {
        umount.unmount().expect("Failed to unmount FUSE mount");
    })
        .expect("Failed to set Ctrl-C handler");

    session.run().unwrap()
}

pub fn setup_logger() {
    let log_level = if cfg!(debug_assertions) {
        LevelFilter::Trace
    } else {
        LevelFilter::Info
    };

    SimpleLogger::init(log_level, Config::default()).expect("Failed to setup logger");
}
