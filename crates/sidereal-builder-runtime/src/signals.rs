// Allow unsafe code for POSIX signal handling
#![allow(unsafe_code)]

//! Signal handling for the builder runtime.
//!
//! As PID 1 in the VM, proper signal handling is required to reap zombie processes
//! and handle shutdown requests.

use nix::sys::signal::{signal, SigHandler, Signal};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;
use tracing::{debug, info, warn};

/// Set up signal handlers for the builder VM init process.
pub fn setup_signal_handlers() {
    info!("Setting up signal handlers");

    unsafe {
        // Reap zombie processes (important as PID 1)
        let _ = signal(Signal::SIGCHLD, SigHandler::Handler(handle_sigchld));

        // Handle termination signals
        let _ = signal(Signal::SIGTERM, SigHandler::Handler(handle_sigterm));
        let _ = signal(Signal::SIGINT, SigHandler::Handler(handle_sigterm));

        // Ignore these signals
        let _ = signal(Signal::SIGHUP, SigHandler::SigIgn);
        let _ = signal(Signal::SIGPIPE, SigHandler::SigIgn);
    }
}

/// Handle SIGCHLD - reap zombie child processes.
extern "C" fn handle_sigchld(_: libc::c_int) {
    loop {
        match waitpid(Pid::from_raw(-1), Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(pid, status)) => {
                debug!(pid = pid.as_raw(), status = status, "Child exited");
            }
            Ok(WaitStatus::Signaled(pid, sig, _)) => {
                debug!(pid = pid.as_raw(), signal = ?sig, "Child signaled");
            }
            Ok(WaitStatus::StillAlive) | Err(nix::errno::Errno::ECHILD) => break,
            Err(e) => {
                warn!("waitpid error: {}", e);
                break;
            }
            _ => {}
        }
    }
}

/// Handle SIGTERM/SIGINT - initiate graceful shutdown.
extern "C" fn handle_sigterm(_: libc::c_int) {
    info!("Received termination signal, shutting down");
    std::process::exit(0);
}
