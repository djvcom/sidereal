//! Git repository cloning for the builder runtime.
//!
//! Handles cloning repositories with optional SSH key authentication.

use std::path::Path;
use std::process::Stdio;

use tokio::process::Command;
use tracing::{debug, info};

use crate::protocol::GitConfig;

/// HTTP proxy address for git operations.
const HTTP_PROXY: &str = "http://127.0.0.1:1080";

/// Convert SSH-style git URLs to HTTPS URLs.
///
/// Examples:
/// - `git@github.com:user/repo.git` -> `https://github.com/user/repo.git`
/// - `ssh://git@github.com/user/repo.git` -> `https://github.com/user/repo.git`
/// - `https://github.com/user/repo.git` -> unchanged
fn ssh_to_https(url: &str) -> String {
    // Handle git@host:path format
    if let Some(rest) = url.strip_prefix("git@") {
        if let Some(colon_pos) = rest.find(':') {
            let host = &rest[..colon_pos];
            let path = &rest[colon_pos + 1..];
            return format!("https://{host}/{path}");
        }
    }

    // Handle ssh://git@host/path format
    if let Some(rest) = url.strip_prefix("ssh://git@") {
        return format!("https://{rest}");
    }

    // Already HTTPS or other format, return as-is
    url.to_owned()
}

/// Error type for git operations.
#[derive(Debug, thiserror::Error)]
pub enum GitError {
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
/// SSH URLs are converted to HTTPS to work through the HTTP proxy.
pub async fn clone_repository(config: &GitConfig, target_dir: &Path) -> Result<(), GitError> {
    // Convert SSH URL to HTTPS for proxy compatibility
    let https_url = ssh_to_https(&config.repo_url);

    info!(
        repo = %https_url,
        branch = %config.branch,
        commit = %config.commit_sha,
        "Cloning repository"
    );

    // Build the clone result
    let result = do_clone(config, &https_url, target_dir).await;

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

/// Perform the actual git clone and checkout.
async fn do_clone(config: &GitConfig, https_url: &str, target_dir: &Path) -> Result<(), GitError> {
    // Ensure target directory exists
    tokio::fs::create_dir_all(target_dir).await?;

    // Clone the repository with shallow depth for efficiency
    // Configure git to use the HTTP proxy and mark directory as safe
    let mut cmd = Command::new("git");
    cmd.arg("-c")
        .arg(format!("http.proxy={HTTP_PROXY}"))
        .arg("-c")
        .arg(format!("https.proxy={HTTP_PROXY}"))
        .arg("-c")
        .arg(format!("safe.directory={}", target_dir.display()))
        .arg("clone")
        .arg("--depth")
        .arg("1")
        .arg("--branch")
        .arg(&config.branch)
        .arg("--single-branch")
        .arg(https_url)
        .arg(target_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

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
        fetch_commit(target_dir, &config.commit_sha).await?;

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
        .arg("-c")
        .arg(format!("safe.directory={}", repo_dir.display()))
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
async fn fetch_commit(repo_dir: &Path, commit_sha: &str) -> Result<(), GitError> {
    let mut cmd = Command::new("git");
    cmd.arg("-c")
        .arg(format!("http.proxy={HTTP_PROXY}"))
        .arg("-c")
        .arg(format!("https.proxy={HTTP_PROXY}"))
        .arg("-c")
        .arg(format!("safe.directory={}", repo_dir.display()))
        .arg("fetch")
        .arg("--depth")
        .arg("1")
        .arg("origin")
        .arg(commit_sha)
        .current_dir(repo_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

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
        .arg("-c")
        .arg(format!("safe.directory={}", repo_dir.display()))
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

    #[test]
    fn ssh_to_https_converts_git_at_format() {
        assert_eq!(
            ssh_to_https("git@github.com:user/repo.git"),
            "https://github.com/user/repo.git"
        );
        assert_eq!(
            ssh_to_https("git@gitlab.com:org/project.git"),
            "https://gitlab.com/org/project.git"
        );
    }

    #[test]
    fn ssh_to_https_converts_ssh_protocol() {
        assert_eq!(
            ssh_to_https("ssh://git@github.com/user/repo.git"),
            "https://github.com/user/repo.git"
        );
    }

    #[test]
    fn ssh_to_https_preserves_https() {
        assert_eq!(
            ssh_to_https("https://github.com/user/repo.git"),
            "https://github.com/user/repo.git"
        );
    }
}
