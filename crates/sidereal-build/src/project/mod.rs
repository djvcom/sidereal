//! Deployable project discovery.
//!
//! Scans a workspace for projects that can be deployed to Sidereal.
//! A project is deployable if it has both a compiled binary and a sidereal.toml file.

use std::path::{Path, PathBuf};

use tracing::{debug, info, warn};

use crate::config::SiderealConfig;
use crate::error::{BuildError, BuildResult};
use crate::sandbox::BinaryInfo;
use crate::source::SourceCheckout;

/// A project that can be deployed to Sidereal.
#[derive(Debug, Clone)]
pub struct DeployableProject {
    /// Project name (from sidereal.toml).
    pub name: String,
    /// Path to the project directory (containing sidereal.toml).
    pub path: PathBuf,
    /// Path to the compiled binary.
    pub binary_path: PathBuf,
    /// Parsed sidereal.toml configuration.
    pub sidereal_config: SiderealConfig,
}

/// Discover deployable projects in a workspace.
///
/// For each compiled binary, checks if the corresponding crate directory
/// contains a sidereal.toml file. Projects without sidereal.toml are
/// considered libraries or internal tools and are skipped.
pub fn discover_projects(
    _checkout: &SourceCheckout,
    binaries: &[BinaryInfo],
) -> BuildResult<Vec<DeployableProject>> {
    let mut projects = Vec::new();

    for binary in binaries {
        let sidereal_toml = binary.crate_dir.join("sidereal.toml");

        if !sidereal_toml.exists() {
            debug!(
                binary = %binary.name,
                crate_dir = %binary.crate_dir.display(),
                "skipping binary without sidereal.toml"
            );
            continue;
        }

        match parse_sidereal_config(&sidereal_toml) {
            Ok(config) => {
                let project = DeployableProject {
                    name: config.project.name.clone(),
                    path: binary.crate_dir.clone(),
                    binary_path: binary.path.clone(),
                    sidereal_config: config,
                };

                info!(
                    project = %project.name,
                    binary = %binary.name,
                    path = %project.path.display(),
                    "discovered deployable project"
                );

                projects.push(project);
            }
            Err(e) => {
                warn!(
                    binary = %binary.name,
                    path = %sidereal_toml.display(),
                    error = %e,
                    "failed to parse sidereal.toml, skipping project"
                );
            }
        }
    }

    Ok(projects)
}

/// Scan a directory for all sidereal.toml files.
///
/// Returns paths to all directories containing sidereal.toml files.
/// This is useful for discovering projects before compilation.
pub fn find_project_dirs(workspace_root: &Path) -> BuildResult<Vec<PathBuf>> {
    let mut project_dirs = Vec::new();
    find_project_dirs_recursive(workspace_root, workspace_root, &mut project_dirs)?;
    Ok(project_dirs)
}

fn find_project_dirs_recursive(
    workspace_root: &Path,
    dir: &Path,
    project_dirs: &mut Vec<PathBuf>,
) -> BuildResult<()> {
    let sidereal_toml = dir.join("sidereal.toml");

    if sidereal_toml.exists() {
        project_dirs.push(dir.to_owned());
    }

    let entries = std::fs::read_dir(dir).ok();
    if let Some(entries) = entries {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name != "target" && !name.starts_with('.') && name != "node_modules" {
                    find_project_dirs_recursive(workspace_root, &path, project_dirs)?;
                }
            }
        }
    }

    Ok(())
}

fn parse_sidereal_config(path: &Path) -> BuildResult<SiderealConfig> {
    let content = std::fs::read_to_string(path)?;
    let config: SiderealConfig = toml::from_str(&content).map_err(|e| {
        BuildError::ConfigParse(format!("failed to parse {}: {}", path.display(), e))
    })?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn discover_projects_finds_deployable() {
        let dir = TempDir::new().unwrap();

        std::fs::create_dir_all(dir.path().join("service-a")).unwrap();
        std::fs::write(
            dir.path().join("service-a/Cargo.toml"),
            r#"
            [package]
            name = "service-a"
            version = "0.1.0"
        "#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("service-a/sidereal.toml"),
            r#"
            [project]
            name = "service-a"
        "#,
        )
        .unwrap();

        std::fs::create_dir_all(dir.path().join("library")).unwrap();
        std::fs::write(
            dir.path().join("library/Cargo.toml"),
            r#"
            [package]
            name = "library"
            version = "0.1.0"

            [lib]
            name = "library"
        "#,
        )
        .unwrap();

        std::fs::create_dir_all(dir.path().join("target/release")).unwrap();
        std::fs::write(dir.path().join("target/release/service_a"), "binary").unwrap();
        std::fs::write(dir.path().join("target/release/library"), "binary").unwrap();

        let checkout = SourceCheckout {
            path: dir.path().to_owned(),
            commit_sha: "abc123".to_owned(),
            cargo_toml: Some(dir.path().join("Cargo.toml")),
            cargo_lock: None,
        };

        let binaries = vec![
            BinaryInfo {
                name: "service_a".to_owned(),
                path: dir.path().join("target/release/service_a"),
                crate_dir: dir.path().join("service-a"),
            },
            BinaryInfo {
                name: "library".to_owned(),
                path: dir.path().join("target/release/library"),
                crate_dir: dir.path().join("library"),
            },
        ];

        let projects = discover_projects(&checkout, &binaries).unwrap();
        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0].name, "service-a");
    }

    #[test]
    fn find_project_dirs_scans_recursively() {
        let dir = TempDir::new().unwrap();

        std::fs::create_dir_all(dir.path().join("functions/service-a")).unwrap();
        std::fs::write(
            dir.path().join("functions/service-a/sidereal.toml"),
            "[project]\nname = \"service-a\"",
        )
        .unwrap();

        std::fs::create_dir_all(dir.path().join("functions/service-b")).unwrap();
        std::fs::write(
            dir.path().join("functions/service-b/sidereal.toml"),
            "[project]\nname = \"service-b\"",
        )
        .unwrap();

        std::fs::create_dir_all(dir.path().join("libs/common")).unwrap();

        let project_dirs = find_project_dirs(dir.path()).unwrap();
        assert_eq!(project_dirs.len(), 2);
    }

    #[test]
    fn find_project_dirs_ignores_target() {
        let dir = TempDir::new().unwrap();

        std::fs::create_dir_all(dir.path().join("target/debug/build")).unwrap();
        std::fs::write(
            dir.path().join("target/debug/build/sidereal.toml"),
            "[project]\nname = \"fake\"",
        )
        .unwrap();

        std::fs::write(
            dir.path().join("sidereal.toml"),
            "[project]\nname = \"real\"",
        )
        .unwrap();

        let project_dirs = find_project_dirs(dir.path()).unwrap();
        assert_eq!(project_dirs.len(), 1);
        assert_eq!(project_dirs[0], dir.path());
    }
}
