// Allow unsafe code for POSIX signal handling
#![allow(unsafe_code)]

//! Signal handling for the builder runtime.
//!
//! As PID 1 in the VM, proper signal handling is required to reap zombie processes
//! and handle shutdown requests.

use nix::sys::signal::{signal, SigHandler, Signal};
use tracing::info;

/// Set up signal handlers for the builder VM init process.
pub fn setup_signal_handlers() {
    info!("Setting up signal handlers");

    unsafe {
        // NOTE: We intentionally do NOT install a SIGCHLD handler that reaps children.
        // While PID 1 normally needs to reap orphans, our builder VM only spawns cargo
        // as a child, and we wait on it explicitly. A reaping handler would cause ECHILD
        // when we try to wait. Since we have no orphan processes, this is safe.

        // Handle termination signals
        let _ = signal(Signal::SIGTERM, SigHandler::Handler(handle_sigterm));
        let _ = signal(Signal::SIGINT, SigHandler::Handler(handle_sigterm));

        // Ignore these signals
        let _ = signal(Signal::SIGHUP, SigHandler::SigIgn);
        let _ = signal(Signal::SIGPIPE, SigHandler::SigIgn);
    }
}

/// Handle SIGTERM/SIGINT - initiate graceful shutdown.
extern "C" fn handle_sigterm(_: libc::c_int) {
    info!("Received termination signal, shutting down");
    std::process::exit(0);
}
