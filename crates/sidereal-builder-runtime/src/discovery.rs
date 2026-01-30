//! Deployable project discovery for the builder runtime.
//!
//! Scans the built workspace for projects that can be deployed to Sidereal.
//! A project is deployable if it has both a compiled binary and a sidereal.toml file.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::protocol::{BinaryInfo, FunctionInfo};

/// Error type for discovery operations.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("failed to read sidereal.toml: {0}")]
    ConfigRead(std::io::Error),

    #[error("failed to parse sidereal.toml: {0}")]
    ConfigParse(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// A project that can be deployed to Sidereal.
#[derive(Debug, Clone)]
pub struct DeployableProject {
    /// Project name (from sidereal.toml).
    pub name: String,
    /// Path to the project directory (containing sidereal.toml).
    pub path: PathBuf,
    /// Path to the compiled binary.
    pub binary_path: PathBuf,
    /// Discovered functions in the project.
    pub functions: Vec<FunctionInfo>,
}

/// Root configuration structure for sidereal.toml.
#[derive(Debug, Clone, Deserialize)]
struct SiderealConfig {
    /// Project metadata.
    project: ProjectConfig,

    /// Function definitions (optional).
    #[serde(default)]
    functions: Option<FunctionsConfig>,

    /// Resource definitions (queues, databases, etc.).
    resources: Option<ResourcesConfig>,
}

/// Project metadata.
#[derive(Debug, Clone, Deserialize)]
struct ProjectConfig {
    /// The project name.
    name: String,
}

/// Functions configuration section.
#[derive(Debug, Clone, Deserialize, Default)]
struct FunctionsConfig {
    /// HTTP handler functions.
    #[serde(default)]
    http: Vec<HttpFunctionConfig>,

    /// Queue consumer functions.
    #[serde(default)]
    queue: Vec<QueueFunctionConfig>,
}

/// HTTP function configuration.
#[derive(Debug, Clone, Deserialize)]
struct HttpFunctionConfig {
    /// Function name.
    name: String,
    /// HTTP route pattern.
    route: String,
    /// HTTP method (GET, POST, etc.).
    #[serde(default = "default_http_method")]
    method: String,
}

fn default_http_method() -> String {
    "GET".to_owned()
}

/// Queue consumer function configuration.
#[derive(Debug, Clone, Deserialize)]
struct QueueFunctionConfig {
    /// Function name.
    name: String,
    /// Queue name to consume from.
    queue: String,
}

/// Resource definitions.
#[derive(Debug, Clone, Deserialize)]
struct ResourcesConfig {
    /// Queue definitions.
    #[serde(default)]
    queue: Option<std::collections::HashMap<String, serde_json::Value>>,
}

/// Discover deployable projects in a workspace.
///
/// For each compiled binary, checks if the corresponding crate directory
/// contains a sidereal.toml file. Projects without sidereal.toml are
/// considered libraries or internal tools and are skipped.
pub fn discover_projects(
    binaries: &[BinaryInfo],
) -> Result<Vec<DeployableProject>, DiscoveryError> {
    let mut projects = Vec::new();

    for binary in binaries {
        let crate_dir = Path::new(&binary.crate_dir);
        let sidereal_toml = crate_dir.join("sidereal.toml");

        if !sidereal_toml.exists() {
            debug!(
                binary = %binary.name,
                crate_dir = %binary.crate_dir,
                "skipping binary without sidereal.toml"
            );
            continue;
        }

        match parse_sidereal_config(&sidereal_toml) {
            Ok(config) => {
                let functions = extract_functions(&config);

                let project = DeployableProject {
                    name: config.project.name.clone(),
                    path: crate_dir.to_owned(),
                    binary_path: PathBuf::from(&binary.path),
                    functions,
                };

                info!(
                    project = %project.name,
                    binary = %binary.name,
                    path = %project.path.display(),
                    functions = project.functions.len(),
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

/// Parse a sidereal.toml configuration file.
fn parse_sidereal_config(path: &Path) -> Result<SiderealConfig, DiscoveryError> {
    let content = std::fs::read_to_string(path).map_err(DiscoveryError::ConfigRead)?;
    let config: SiderealConfig = toml::from_str(&content)
        .map_err(|e| DiscoveryError::ConfigParse(format!("{}: {}", path.display(), e)))?;
    Ok(config)
}

/// Extract function info from the sidereal.toml configuration.
fn extract_functions(config: &SiderealConfig) -> Vec<FunctionInfo> {
    let mut functions = Vec::new();

    if let Some(ref funcs) = config.functions {
        for http in &funcs.http {
            functions.push(FunctionInfo {
                name: http.name.clone(),
                route: Some(http.route.clone()),
                method: Some(http.method.clone()),
                queue: None,
            });
        }

        for queue in &funcs.queue {
            functions.push(FunctionInfo {
                name: queue.name.clone(),
                route: None,
                method: None,
                queue: Some(queue.queue.clone()),
            });
        }
    }

    functions
}

/// Scan a directory for all sidereal.toml files.
///
/// Returns paths to all directories containing sidereal.toml files.
/// Useful for discovering projects before compilation.
pub fn find_project_dirs(workspace_root: &Path) -> Result<Vec<PathBuf>, DiscoveryError> {
    let mut project_dirs = Vec::new();
    find_project_dirs_recursive(workspace_root, &mut project_dirs)?;
    Ok(project_dirs)
}

fn find_project_dirs_recursive(
    dir: &Path,
    project_dirs: &mut Vec<PathBuf>,
) -> Result<(), DiscoveryError> {
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
                    find_project_dirs_recursive(&path, project_dirs)?;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let toml = r#"
            [project]
            name = "test-project"
        "#;

        let config: SiderealConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.project.name, "test-project");
        assert!(config.functions.is_none());
    }

    #[test]
    fn parse_config_with_functions() {
        let toml = r#"
            [project]
            name = "api-service"

            [[functions.http]]
            name = "get_users"
            route = "/users"
            method = "GET"

            [[functions.http]]
            name = "create_user"
            route = "/users"
            method = "POST"

            [[functions.queue]]
            name = "process_order"
            queue = "orders"
        "#;

        let config: SiderealConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.project.name, "api-service");

        let funcs = config.functions.as_ref().unwrap();
        assert_eq!(funcs.http.len(), 2);
        assert_eq!(funcs.queue.len(), 1);

        let functions = extract_functions(&config);
        assert_eq!(functions.len(), 3);
        assert_eq!(functions[0].name, "get_users");
        assert_eq!(functions[0].route.as_deref(), Some("/users"));
        assert_eq!(functions[0].method.as_deref(), Some("GET"));
        assert!(functions[0].queue.is_none());

        assert_eq!(functions[2].name, "process_order");
        assert!(functions[2].route.is_none());
        assert_eq!(functions[2].queue.as_deref(), Some("orders"));
    }

    #[test]
    fn extract_functions_empty() {
        let config = SiderealConfig {
            project: ProjectConfig {
                name: "test".to_owned(),
            },
            functions: None,
            resources: None,
        };

        let functions = extract_functions(&config);
        assert!(functions.is_empty());
    }

    #[test]
    fn discovery_error_display() {
        let err = DiscoveryError::ConfigParse("invalid toml".to_owned());
        assert!(err.to_string().contains("parse"));
        assert!(err.to_string().contains("invalid toml"));
    }
}
