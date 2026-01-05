//! Signal handling for the guest runtime.
//!
//! As PID 1, we need to handle signals appropriately and reap zombie processes.

use nix::sys::signal::{signal, SigHandler, Signal};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;
use tracing::{debug, info, warn};

pub fn setup_signal_handlers() {
    info!("Setting up signal handlers");

    unsafe {
        let _ = signal(Signal::SIGCHLD, SigHandler::Handler(handle_sigchld));

        let _ = signal(Signal::SIGTERM, SigHandler::Handler(handle_sigterm));
        let _ = signal(Signal::SIGINT, SigHandler::Handler(handle_sigterm));

        let _ = signal(Signal::SIGHUP, SigHandler::SigIgn);
        let _ = signal(Signal::SIGPIPE, SigHandler::SigIgn);
    }
}

extern "C" fn handle_sigchld(_: libc::c_int) {
    loop {
        match waitpid(Pid::from_raw(-1), Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(pid, status)) => {
                debug!(pid = pid.as_raw(), status = status, "Child exited");
            }
            Ok(WaitStatus::Signaled(pid, sig, _)) => {
                debug!(pid = pid.as_raw(), signal = ?sig, "Child signaled");
            }
            Ok(WaitStatus::StillAlive) => break,
            Err(nix::errno::Errno::ECHILD) => break,
            Err(e) => {
                warn!("waitpid error: {}", e);
                break;
            }
            _ => {}
        }
    }
}

extern "C" fn handle_sigterm(_: libc::c_int) {
    info!("Received termination signal, shutting down");
    std::process::exit(0);
}
