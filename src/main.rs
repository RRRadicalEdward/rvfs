use fuser::Session;
use tracing::debug;
use tracing_subscriber::{fmt, EnvFilter};

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

    debug!(?options, "Mount options");

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
    let format = fmt::format()
        .with_ansi(true)
        .with_level(true)
        .with_target(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_source_location(true)
        .with_line_number(true)
        .pretty();

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        if cfg!(debug_assertions) {
            EnvFilter::new("trace")
        } else {
            EnvFilter::new("info")
        }
    });

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .event_format(format)
        .init();
}
