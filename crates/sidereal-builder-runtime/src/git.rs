//! Git repository cloning for the builder runtime.
//!
//! Handles cloning repositories with optional SSH key authentication.

use std::path::Path;
use std::process::Stdio;

use tokio::process::Command;
use tracing::{debug, info, warn};

use crate::protocol::GitConfig;

/// Error type for git operations.
#[derive(Debug, thiserror::Error)]
pub enum GitError {
    #[error("failed to write SSH key: {0}")]
    SshKeyWrite(std::io::Error),

    #[error("failed to set SSH key permissions: {0}")]
    SshKeyPermissions(std::io::Error),

    #[error("git clone failed: {0}")]
    CloneFailed(String),

    #[error("git fetch failed: {0}")]
    FetchFailed(String),

    #[error("git checkout failed: {0}")]
    CheckoutFailed(String),

    #[error("subpath '{0}' not found in repository")]
    SubpathNotFound(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Clone a git repository to the target directory.
///
/// Handles SSH key authentication if provided, and checks out the specific commit.
pub async fn clone_repository(config: &GitConfig, target_dir: &Path) -> Result<(), GitError> {
    info!(
        repo = %config.repo_url,
        branch = %config.branch,
        commit = %config.commit_sha,
        "Cloning repository"
    );

    // Set up SSH key if provided
    let ssh_key_path = if let Some(ref key) = config.ssh_key {
        Some(setup_ssh_key(key).await?)
    } else {
        None
    };

    // Build the clone result, ensuring we clean up the SSH key regardless of outcome
    let result = do_clone(config, target_dir, ssh_key_path.as_deref()).await;

    // Clean up SSH key
    if let Some(ref path) = ssh_key_path {
        cleanup_ssh_key(path).await;
    }

    result?;

    // Verify subpath exists if specified
    if let Some(ref subpath) = config.subpath {
        let full_path = target_dir.join(subpath);
        if !full_path.exists() {
            return Err(GitError::SubpathNotFound(subpath.clone()));
        }
        debug!(subpath = %subpath, "Subpath verified");
    }

    info!("Repository cloned successfully");
    Ok(())
}

/// Set up the SSH key for git operations.
async fn setup_ssh_key(key_contents: &str) -> Result<String, GitError> {
    let ssh_key_path = "/tmp/git-ssh-key";

    // Write key with restrictive permissions
    tokio::fs::write(ssh_key_path, key_contents)
        .await
        .map_err(GitError::SshKeyWrite)?;

    // Set permissions to 0600 (owner read/write only)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        tokio::fs::set_permissions(ssh_key_path, perms)
            .await
            .map_err(GitError::SshKeyPermissions)?;
    }

    debug!("SSH key written to {}", ssh_key_path);
    Ok(ssh_key_path.to_owned())
}

/// Clean up the SSH key file.
async fn cleanup_ssh_key(path: &str) {
    if let Err(e) = tokio::fs::remove_file(path).await {
        warn!(path = %path, error = %e, "Failed to remove SSH key file");
    } else {
        debug!(path = %path, "SSH key file removed");
    }
}

/// Perform the actual git clone and checkout.
async fn do_clone(
    config: &GitConfig,
    target_dir: &Path,
    ssh_key_path: Option<&str>,
) -> Result<(), GitError> {
    // Ensure target directory exists
    tokio::fs::create_dir_all(target_dir).await?;

    // Build SSH command if using SSH key
    let ssh_command = ssh_key_path.map(|path| {
        format!(
            "ssh -i {} -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null",
            path
        )
    });

    // Clone the repository with shallow depth for efficiency
    let mut cmd = Command::new("git");
    cmd.arg("clone")
        .arg("--depth")
        .arg("1")
        .arg("--branch")
        .arg(&config.branch)
        .arg("--single-branch")
        .arg(&config.repo_url)
        .arg(target_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(ref ssh_cmd) = ssh_command {
        cmd.env("GIT_SSH_COMMAND", ssh_cmd);
    }

    debug!(command = ?cmd, "Running git clone");

    let output = cmd.output().await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(GitError::CloneFailed(stderr.to_string()));
    }

    // If the commit SHA doesn't match HEAD, we need to fetch and checkout
    let head_sha = get_head_sha(target_dir).await?;
    if !head_sha.starts_with(&config.commit_sha) && !config.commit_sha.starts_with(&head_sha) {
        debug!(
            head = %head_sha,
            target = %config.commit_sha,
            "HEAD doesn't match target commit, fetching specific commit"
        );

        // Fetch the specific commit
        fetch_commit(target_dir, &config.commit_sha, ssh_key_path).await?;

        // Checkout the commit
        checkout_commit(target_dir, &config.commit_sha).await?;
    } else {
        debug!(commit = %head_sha, "HEAD matches target commit");
    }

    Ok(())
}

/// Get the HEAD commit SHA.
async fn get_head_sha(repo_dir: &Path) -> Result<String, GitError> {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("HEAD")
        .current_dir(repo_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(GitError::CheckoutFailed(format!(
            "failed to get HEAD: {stderr}"
        )));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Fetch a specific commit from the remote.
async fn fetch_commit(
    repo_dir: &Path,
    commit_sha: &str,
    ssh_key_path: Option<&str>,
) -> Result<(), GitError> {
    let mut cmd = Command::new("git");
    cmd.arg("fetch")
        .arg("--depth")
        .arg("1")
        .arg("origin")
        .arg(commit_sha)
        .current_dir(repo_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(path) = ssh_key_path {
        cmd.env(
            "GIT_SSH_COMMAND",
            format!(
                "ssh -i {} -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null",
                path
            ),
        );
    }

    let output = cmd.output().await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(GitError::FetchFailed(stderr.to_string()));
    }

    Ok(())
}

/// Checkout a specific commit.
async fn checkout_commit(repo_dir: &Path, commit_sha: &str) -> Result<(), GitError> {
    let output = Command::new("git")
        .arg("checkout")
        .arg(commit_sha)
        .current_dir(repo_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(GitError::CheckoutFailed(stderr.to_string()));
    }

    info!(commit = %commit_sha, "Checked out commit");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_error_display() {
        let err = GitError::CloneFailed("permission denied".to_owned());
        assert!(err.to_string().contains("git clone failed"));
        assert!(err.to_string().contains("permission denied"));
    }

    #[test]
    fn subpath_not_found_error() {
        let err = GitError::SubpathNotFound("packages/foo".to_owned());
        assert!(err.to_string().contains("packages/foo"));
        assert!(err.to_string().contains("not found"));
    }
}
