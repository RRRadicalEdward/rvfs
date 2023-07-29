use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use clap::Parser;
use fuser::MountOption;
use rfs::Rfs;
use tempdir::TempDir;
use tracing::{debug, info};
use tracing_subscriber::{fmt, EnvFilter};

mod error;
mod fs;
mod inode;
mod rfs;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path where the device should be mounted in system
    #[arg(short, long, value_name = "PATH")]
    mount: PathBuf,
}

fn main() {
    let args = Args::parse();
    setup_logger();

    let options = vec![
        MountOption::FSName("rvfs".to_string()),
        MountOption::AutoUnmount,
        MountOption::AllowOther,
        MountOption::Dev,
        MountOption::RW,
        MountOption::Sync,
    ];

    let file_name = args
        .mount
        .file_name()
        .expect("mount point is expected to be valid Path")
        .to_str()
        .unwrap();
    let mount_point = TempDir::new_in("/mnt", file_name).unwrap();
    debug!("Mount point: {:?}", mount_point.as_ref());

    let session = fuser::spawn_mount2(
        Rfs::new(args.mount.clone(), mount_point.as_ref().to_path_buf()),
        mount_point.as_ref(),
        &options,
    )
    .expect("Fuse mount failed");

    let conv_var = Arc::new((Mutex::new(false), Condvar::new()));
    let mount = Arc::new(mount_point.path().to_path_buf());
    ctrlc::set_handler({
        let mount = mount.clone();
        let conv_var = conv_var.clone();
        move || {
            {
                let (mtx, conv_var) = &*conv_var;
                let mut mtx = mtx.lock().unwrap();
                *mtx = true;
                conv_var.notify_one();
            }

            info!("Cleaning up {:?}", mount.as_ref());
            while mount.exists() {
                match std::fs::remove_dir(mount.as_ref()) {
                    Ok(()) => {}
                    Err(err) => {
                        tracing::error!("Failed to delete mount point on exit: {err}");
                    }
                }
            }
        }
    })
    .expect("Failed to set Ctrl-C handler");

    let (mtx, conv_var) = &*conv_var;
    let mut mtx = mtx.lock().unwrap();

    while !*mtx {
        mtx = conv_var.wait(mtx).unwrap();
    }

    session.join();
    // extra wait to give remove_dir time to do its job
    std::thread::sleep(Duration::from_secs(1));
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
