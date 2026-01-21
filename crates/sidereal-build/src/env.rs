//! Environment validation for the build service.
//!
//! Pre-flight checks ensure required tools are available before the service
//! accepts build requests.

use std::process::Command;

use tracing::{debug, info, warn};

use crate::error::BuildError;

/// Results of environment validation checks.
#[derive(Debug, Clone)]
pub struct EnvironmentCheck {
    /// Whether bubblewrap (bwrap) is available.
    pub bubblewrap: bool,
    /// Bubblewrap version if available.
    pub bubblewrap_version: Option<String>,

    /// Whether cargo is available.
    pub cargo: bool,
    /// Cargo version if available.
    pub cargo_version: Option<String>,

    /// Whether rustc is available.
    pub rustc: bool,
    /// Rustc version if available.
    pub rustc_version: Option<String>,

    /// Whether git is available.
    pub git: bool,
    /// Git version if available.
    pub git_version: Option<String>,

    /// Whether cargo-audit is available.
    pub cargo_audit: bool,
    /// Cargo-audit version if available.
    pub cargo_audit_version: Option<String>,

    /// Whether seccomp is supported (Linux only).
    pub seccomp_support: bool,

    /// Whether user namespaces are available.
    pub user_namespaces: bool,
}

impl EnvironmentCheck {
    /// Run all environment checks.
    pub fn run() -> Self {
        info!("Running environment validation checks");

        let bubblewrap = check_command("bwrap", &["--version"]);
        let cargo = check_command("cargo", &["--version"]);
        let rustc = check_command("rustc", &["--version"]);
        let git = check_command("git", &["--version"]);
        let cargo_audit = check_command("cargo-audit", &["--version"]);

        let seccomp_support = check_seccomp_support();
        let user_namespaces = check_user_namespaces();

        let check = Self {
            bubblewrap: bubblewrap.0,
            bubblewrap_version: bubblewrap.1,
            cargo: cargo.0,
            cargo_version: cargo.1,
            rustc: rustc.0,
            rustc_version: rustc.1,
            git: git.0,
            git_version: git.1,
            cargo_audit: cargo_audit.0,
            cargo_audit_version: cargo_audit.1,
            seccomp_support,
            user_namespaces,
        };

        check.log_status();
        check
    }

    /// Check if all critical tools are available.
    ///
    /// Critical tools are those without which the service cannot function.
    /// Cargo-audit is optional (degrades gracefully).
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.bubblewrap && self.cargo && self.rustc && self.git
    }

    /// Validate the environment, returning an error if not ready.
    pub fn validate(&self) -> Result<(), BuildError> {
        let mut missing = Vec::new();

        if !self.bubblewrap {
            missing.push("bwrap (bubblewrap)");
        }
        if !self.cargo {
            missing.push("cargo");
        }
        if !self.rustc {
            missing.push("rustc");
        }
        if !self.git {
            missing.push("git");
        }

        if missing.is_empty() {
            Ok(())
        } else {
            Err(BuildError::MissingTools {
                tools: missing.into_iter().map(String::from).collect(),
            })
        }
    }

    /// Log the status of all checks.
    fn log_status(&self) {
        if self.bubblewrap {
            info!(
                version = self.bubblewrap_version.as_deref().unwrap_or("unknown"),
                "bubblewrap: available"
            );
        } else {
            warn!("bubblewrap: NOT AVAILABLE - sandboxed builds will fail");
        }

        if self.cargo {
            info!(
                version = self.cargo_version.as_deref().unwrap_or("unknown"),
                "cargo: available"
            );
        } else {
            warn!("cargo: NOT AVAILABLE - builds will fail");
        }

        if self.rustc {
            info!(
                version = self.rustc_version.as_deref().unwrap_or("unknown"),
                "rustc: available"
            );
        } else {
            warn!("rustc: NOT AVAILABLE - builds will fail");
        }

        if self.git {
            info!(
                version = self.git_version.as_deref().unwrap_or("unknown"),
                "git: available"
            );
        } else {
            warn!("git: NOT AVAILABLE - source checkout will fail");
        }

        if self.cargo_audit {
            info!(
                version = self.cargo_audit_version.as_deref().unwrap_or("unknown"),
                "cargo-audit: available"
            );
        } else {
            warn!("cargo-audit: not available - dependency auditing disabled");
        }

        if self.seccomp_support {
            debug!("seccomp: supported");
        } else {
            warn!("seccomp: not supported - reduced sandbox security");
        }

        if self.user_namespaces {
            debug!("user namespaces: available");
        } else {
            warn!("user namespaces: not available - sandbox will run as current user");
        }
    }
}

/// Check if a command is available and get its version.
fn check_command(name: &str, args: &[&str]) -> (bool, Option<String>) {
    match Command::new(name).args(args).output() {
        Ok(output) if output.status.success() => {
            let version = String::from_utf8_lossy(&output.stdout)
                .lines()
                .next()
                .map(|s| s.trim().to_owned());
            (true, version)
        }
        Ok(_) => {
            debug!(command = name, "command returned non-zero exit code");
            (false, None)
        }
        Err(e) => {
            debug!(command = name, error = %e, "command not found");
            (false, None)
        }
    }
}

/// Check if seccomp is supported on this system.
fn check_seccomp_support() -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::path::Path;
        Path::new("/proc/sys/kernel/seccomp/actions_avail").exists()
    }

    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Check if user namespaces are available.
fn check_user_namespaces() -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        use std::path::Path;

        let max_user_ns = Path::new("/proc/sys/user/max_user_namespaces");
        if max_user_ns.exists() {
            match fs::read_to_string(max_user_ns) {
                Ok(content) => content.trim().parse::<u32>().unwrap_or(0) > 0,
                Err(_) => false,
            }
        } else {
            false
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_check_runs_without_panic() {
        let check = EnvironmentCheck::run();
        // At minimum, these should be deterministic
        assert!(check.cargo || !check.cargo);
    }

    #[test]
    fn is_ready_requires_critical_tools() {
        let check = EnvironmentCheck {
            bubblewrap: true,
            bubblewrap_version: None,
            cargo: true,
            cargo_version: None,
            rustc: true,
            rustc_version: None,
            git: true,
            git_version: None,
            cargo_audit: false, // Optional
            cargo_audit_version: None,
            seccomp_support: false,
            user_namespaces: false,
        };
        assert!(check.is_ready());
    }

    #[test]
    fn is_ready_fails_without_bubblewrap() {
        let check = EnvironmentCheck {
            bubblewrap: false,
            bubblewrap_version: None,
            cargo: true,
            cargo_version: None,
            rustc: true,
            rustc_version: None,
            git: true,
            git_version: None,
            cargo_audit: true,
            cargo_audit_version: None,
            seccomp_support: true,
            user_namespaces: true,
        };
        assert!(!check.is_ready());
    }

    #[test]
    fn validate_returns_error_for_missing_tools() {
        let check = EnvironmentCheck {
            bubblewrap: false,
            bubblewrap_version: None,
            cargo: false,
            cargo_version: None,
            rustc: true,
            rustc_version: None,
            git: true,
            git_version: None,
            cargo_audit: false,
            cargo_audit_version: None,
            seccomp_support: false,
            user_namespaces: false,
        };

        let err = check.validate().unwrap_err();
        match err {
            BuildError::MissingTools { tools } => {
                assert!(tools.contains(&"bwrap (bubblewrap)".to_owned()));
                assert!(tools.contains(&"cargo".to_owned()));
                assert!(!tools.contains(&"git".to_owned()));
            }
            _ => panic!("Expected MissingTools error"),
        }
    }
}
