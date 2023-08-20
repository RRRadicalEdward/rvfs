use std::sync::{Arc, Condvar, Mutex};

use fuser::MountOption;
use rfs::Rfs;
use tracing::debug;
use tracing_subscriber::{EnvFilter, fmt};

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
    let session =
        fuser::spawn_mount2(proxy_file_system
                            , mountpoint, &options).expect("Fuse mount failed");

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
