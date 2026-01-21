//! Bubblewrap (bwrap) command builder.
//!
//! Provides a type-safe builder for constructing bubblewrap sandbox commands.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

/// Builder for constructing bubblewrap commands.
#[derive(Debug, Clone)]
pub struct BubblewrapBuilder {
    /// Read-only bind mounts (source, dest).
    binds_ro: Vec<(PathBuf, PathBuf)>,
    /// Read-write bind mounts (source, dest).
    binds_rw: Vec<(PathBuf, PathBuf)>,
    /// Environment variables.
    env: HashMap<String, String>,
    /// Working directory inside sandbox.
    cwd: Option<PathBuf>,
    /// Unshare network namespace.
    unshare_net: bool,
    /// Unshare PID namespace.
    unshare_pid: bool,
    /// Unshare IPC namespace.
    unshare_ipc: bool,
    /// Unshare UTS namespace.
    unshare_uts: bool,
    /// Create a new user namespace.
    new_session: bool,
    /// Die with parent process.
    die_with_parent: bool,
    /// Hostname inside sandbox.
    hostname: Option<String>,
}

impl Default for BubblewrapBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BubblewrapBuilder {
    /// Create a new bubblewrap builder with sensible defaults.
    #[must_use]
    pub fn new() -> Self {
        Self {
            binds_ro: Vec::new(),
            binds_rw: Vec::new(),
            env: HashMap::new(),
            cwd: None,
            unshare_net: true,
            unshare_pid: true,
            unshare_ipc: true,
            unshare_uts: true,
            new_session: true,
            die_with_parent: true,
            hostname: Some("build".to_owned()),
        }
    }

    /// Add a read-only bind mount.
    #[must_use]
    pub fn bind_ro(mut self, src: impl AsRef<Path>, dest: impl AsRef<Path>) -> Self {
        self.binds_ro
            .push((src.as_ref().to_owned(), dest.as_ref().to_owned()));
        self
    }

    /// Add a read-write bind mount.
    #[must_use]
    pub fn bind_rw(mut self, src: impl AsRef<Path>, dest: impl AsRef<Path>) -> Self {
        self.binds_rw
            .push((src.as_ref().to_owned(), dest.as_ref().to_owned()));
        self
    }

    /// Set an environment variable.
    #[must_use]
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    /// Set the working directory inside the sandbox.
    #[must_use]
    pub fn cwd(mut self, path: impl AsRef<Path>) -> Self {
        self.cwd = Some(path.as_ref().to_owned());
        self
    }

    /// Control network namespace isolation.
    #[must_use]
    pub const fn unshare_net(mut self, unshare: bool) -> Self {
        self.unshare_net = unshare;
        self
    }

    /// Control PID namespace isolation.
    #[must_use]
    pub const fn unshare_pid(mut self, unshare: bool) -> Self {
        self.unshare_pid = unshare;
        self
    }

    /// Control IPC namespace isolation.
    #[must_use]
    pub const fn unshare_ipc(mut self, unshare: bool) -> Self {
        self.unshare_ipc = unshare;
        self
    }

    /// Control UTS namespace isolation.
    #[must_use]
    pub const fn unshare_uts(mut self, unshare: bool) -> Self {
        self.unshare_uts = unshare;
        self
    }

    /// Control whether to die with parent process.
    #[must_use]
    pub const fn die_with_parent(mut self, die: bool) -> Self {
        self.die_with_parent = die;
        self
    }

    /// Set hostname inside sandbox.
    #[must_use]
    pub fn hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = Some(hostname.into());
        self
    }

    /// Build the command with the specified program and arguments.
    #[must_use]
    pub fn build<S: AsRef<OsStr>>(&self, program: &str, args: &[S]) -> Command {
        let mut cmd = Command::new("bwrap");

        // Namespace isolation
        if self.unshare_net {
            cmd.arg("--unshare-net");
        }
        if self.unshare_pid {
            cmd.arg("--unshare-pid");
        }
        if self.unshare_ipc {
            cmd.arg("--unshare-ipc");
        }
        if self.unshare_uts {
            cmd.arg("--unshare-uts");
        }

        // Session and parent handling
        if self.new_session {
            cmd.arg("--new-session");
        }
        if self.die_with_parent {
            cmd.arg("--die-with-parent");
        }

        // Hostname
        if let Some(hostname) = &self.hostname {
            cmd.arg("--hostname").arg(hostname);
        }

        // Create minimal /dev
        cmd.arg("--dev").arg("/dev");

        // Create /proc
        cmd.arg("--proc").arg("/proc");

        // Create tmpfs at /tmp
        cmd.arg("--tmpfs").arg("/tmp");

        // Read-only bind mounts
        for (src, dest) in &self.binds_ro {
            cmd.arg("--ro-bind").arg(src).arg(dest);
        }

        // Read-write bind mounts
        for (src, dest) in &self.binds_rw {
            cmd.arg("--bind").arg(src).arg(dest);
        }

        // Environment variables
        for (key, value) in &self.env {
            cmd.arg("--setenv").arg(key).arg(value);
        }

        // Working directory
        if let Some(cwd) = &self.cwd {
            cmd.arg("--chdir").arg(cwd);
        }

        // The command to run
        cmd.arg("--").arg(program);
        for arg in args {
            cmd.arg(arg);
        }

        cmd
    }
}

/// Configuration for sandbox resource limits.
#[derive(Debug, Clone)]
pub struct SandboxLimits {
    /// Maximum build duration.
    pub timeout: Duration,
    /// Memory limit in megabytes.
    pub memory_limit_mb: u32,
    /// CPU core limit (0 = unlimited).
    pub cpu_cores: u32,
}

impl Default for SandboxLimits {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(600),
            memory_limit_mb: 4096,
            cpu_cores: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let builder = BubblewrapBuilder::new();
        assert!(builder.unshare_net);
        assert!(builder.unshare_pid);
        assert!(builder.die_with_parent);
    }

    #[test]
    fn builder_bind_mounts() {
        let builder = BubblewrapBuilder::new()
            .bind_ro("/usr", "/usr")
            .bind_rw("/tmp/build", "/build");

        assert_eq!(builder.binds_ro.len(), 1);
        assert_eq!(builder.binds_rw.len(), 1);
    }

    #[test]
    fn builder_env() {
        let builder = BubblewrapBuilder::new()
            .env("PATH", "/usr/bin")
            .env("HOME", "/build");

        assert_eq!(builder.env.get("PATH"), Some(&"/usr/bin".to_owned()));
        assert_eq!(builder.env.get("HOME"), Some(&"/build".to_owned()));
    }

    #[test]
    fn builder_command_args() {
        let builder = BubblewrapBuilder::new()
            .bind_ro("/usr", "/usr")
            .cwd("/build");

        let cmd = builder.build("cargo", &["build", "--release"]);
        let args: Vec<_> = cmd.get_args().collect();

        // Check that essential args are present
        assert!(args.contains(&OsStr::new("--unshare-net")));
        assert!(args.contains(&OsStr::new("--die-with-parent")));
        assert!(args.contains(&OsStr::new("cargo")));
        assert!(args.contains(&OsStr::new("build")));
        assert!(args.contains(&OsStr::new("--release")));
    }
}
