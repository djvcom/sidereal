//! Filesystem setup for the guest VM.

use nix::mount::{mount, MsFlags};
use std::fs;
use std::path::Path;
use tracing::{debug, info};

pub fn mount_filesystems() -> Result<(), Box<dyn std::error::Error>> {
    info!("Mounting filesystems");

    mount_proc()?;
    mount_sys()?;
    mount_tmp()?;
    mount_dev()?;

    Ok(())
}

fn mount_proc() -> Result<(), Box<dyn std::error::Error>> {
    let target = Path::new("/proc");
    if !target.exists() {
        fs::create_dir_all(target)?;
    }

    debug!("Mounting /proc");
    mount(
        Some("proc"),
        target,
        Some("proc"),
        MsFlags::MS_NOSUID | MsFlags::MS_NOEXEC | MsFlags::MS_NODEV,
        None::<&str>,
    )?;

    Ok(())
}

fn mount_sys() -> Result<(), Box<dyn std::error::Error>> {
    let target = Path::new("/sys");
    if !target.exists() {
        fs::create_dir_all(target)?;
    }

    debug!("Mounting /sys");
    mount(
        Some("sysfs"),
        target,
        Some("sysfs"),
        MsFlags::MS_NOSUID | MsFlags::MS_NOEXEC | MsFlags::MS_NODEV,
        None::<&str>,
    )?;

    Ok(())
}

fn mount_tmp() -> Result<(), Box<dyn std::error::Error>> {
    let target = Path::new("/tmp");
    if !target.exists() {
        fs::create_dir_all(target)?;
    }

    debug!("Mounting /tmp");
    mount(
        Some("tmpfs"),
        target,
        Some("tmpfs"),
        MsFlags::MS_NOSUID | MsFlags::MS_NODEV,
        Some("size=64M"),
    )?;

    Ok(())
}

fn mount_dev() -> Result<(), Box<dyn std::error::Error>> {
    let target = Path::new("/dev");
    if !target.exists() {
        fs::create_dir_all(target)?;
    }

    debug!("Mounting /dev");
    mount(
        Some("devtmpfs"),
        target,
        Some("devtmpfs"),
        MsFlags::MS_NOSUID,
        None::<&str>,
    )?;

    Ok(())
}
