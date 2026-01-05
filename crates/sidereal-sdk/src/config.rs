//! Configuration management with layered loading and environment variable interpolation.
//!
//! Configuration is loaded from multiple sources in order of priority:
//! 1. Runtime overrides (highest priority)
//! 2. Environment variables (SIDEREAL_*)
//! 3. Environment-specific config (`[env.{SIDEREAL_ENV}]` section)
//! 4. Base sidereal.toml (lowest priority)
//!
//! Environment variables in TOML values can be interpolated using `${VAR}` syntax.

use figment::{
    providers::{Format, Serialized, Toml},
    value::{Dict, Map, Value},
    Error as FigmentError, Figment, Metadata, Profile, Provider,
};

use serde::{de::DeserializeOwned, Deserialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Configuration error: {0}")]
    Figment(#[from] FigmentError),

    #[error("Section not found: {0}")]
    SectionNotFound(String),

    #[error("Failed to deserialise section '{section}': {source}")]
    Deserialisation {
        section: String,
        #[source]
        source: FigmentError,
    },

    #[error("Configuration file not found: {0}")]
    FileNotFound(String),
}

struct ConfigManagerInner {
    figment: Figment,
    active_env: String,
}

#[derive(Clone)]
pub struct ConfigManager {
    inner: Arc<ConfigManagerInner>,
}

impl ConfigManager {
    pub fn load() -> Result<Self, ConfigError> {
        Self::load_from("sidereal.toml")
    }

    pub fn load_from(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(ConfigError::FileNotFound(path.display().to_string()));
        }

        let active_env =
            std::env::var("SIDEREAL_ENV").unwrap_or_else(|_| "development".to_string());

        let base_figment = Figment::new().merge(InterpolatingToml::file(path)?);

        let env_section_key = format!("env.{}", active_env);
        let with_env_overrides: Figment =
            if let Ok(env_overrides) = base_figment.extract_inner::<Value>(&env_section_key) {
                let env_dict = value_to_dict(env_overrides);
                base_figment.merge(Serialized::globals(env_dict))
            } else {
                base_figment
            };

        let final_figment = with_env_overrides.merge(
            figment::providers::Env::prefixed("SIDEREAL_")
                .split("__")
                .lowercase(false),
        );

        Ok(Self {
            inner: Arc::new(ConfigManagerInner {
                figment: final_figment,
                active_env,
            }),
        })
    }

    pub fn active_environment(&self) -> &str {
        &self.inner.active_env
    }

    pub fn section<T: DeserializeOwned>(&self, name: &str) -> Result<T, ConfigError> {
        let key = format!("app.{}", name);
        self.inner
            .figment
            .extract_inner::<T>(&key)
            .map_err(|e| match e.kind {
                figment::error::Kind::MissingField(_) => {
                    ConfigError::SectionNotFound(name.to_string())
                }
                _ => ConfigError::Deserialisation {
                    section: name.to_string(),
                    source: e,
                },
            })
    }

    pub fn project(&self) -> Result<ProjectConfig, ConfigError> {
        self.inner
            .figment
            .extract_inner::<ProjectConfig>("project")
            .map_err(ConfigError::from)
    }

    pub fn dev(&self) -> DevConfig {
        self.inner
            .figment
            .extract_inner::<DevConfig>("dev")
            .unwrap_or_default()
    }

    pub fn resources(&self) -> Option<ResourcesConfig> {
        self.inner
            .figment
            .extract_inner::<ResourcesConfig>("resources")
            .ok()
    }

    pub fn declared_queues(&self) -> Vec<String> {
        self.resources()
            .and_then(|r| r.queue)
            .map(|queues| queues.keys().cloned().collect())
            .unwrap_or_default()
    }
}

fn value_to_dict(value: Value) -> Dict {
    match value {
        Value::Dict(_, dict) => dict,
        _ => Dict::new(),
    }
}

struct EnvVarInterpolator;

impl EnvVarInterpolator {
    fn interpolate_value(value: Value) -> Value {
        match value {
            Value::String(_, s) => {
                let interpolated = Self::interpolate_string(&s);
                Value::from(interpolated)
            }
            Value::Dict(tag, dict) => {
                let new_dict: Dict = dict
                    .into_iter()
                    .map(|(k, v)| (k, Self::interpolate_value(v)))
                    .collect();
                Value::Dict(tag, new_dict)
            }
            Value::Array(tag, arr) => {
                let new_arr: Vec<Value> =
                    arr.into_iter().map(Self::interpolate_value).collect();
                Value::Array(tag, new_arr)
            }
            other => other,
        }
    }

    fn interpolate_string(s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        let mut chars = s.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '$' && chars.peek() == Some(&'{') {
                chars.next();
                let mut var_name = String::new();

                for ch in chars.by_ref() {
                    if ch == '}' {
                        break;
                    }
                    var_name.push(ch);
                }

                if let Ok(val) = std::env::var(&var_name) {
                    result.push_str(&val);
                }
            } else {
                result.push(c);
            }
        }

        result
    }
}

pub struct InterpolatingToml {
    content: String,
}

impl InterpolatingToml {
    pub fn file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|_| ConfigError::FileNotFound(path.as_ref().display().to_string()))?;
        Ok(Self { content })
    }

    pub fn string(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
        }
    }
}

impl Provider for InterpolatingToml {
    fn metadata(&self) -> Metadata {
        Metadata::named("Interpolating TOML")
    }

    fn data(&self) -> Result<Map<Profile, Dict>, FigmentError> {
        let base = Toml::string(&self.content);
        let base_data = base.data()?;

        let interpolated: Map<Profile, Dict> = base_data
            .into_iter()
            .map(|(profile, dict)| {
                let new_dict: Dict = dict
                    .into_iter()
                    .map(|(k, v)| (k, EnvVarInterpolator::interpolate_value(v)))
                    .collect();
                (profile, new_dict)
            })
            .collect();

        Ok(interpolated)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SiderealConfig {
    pub project: ProjectConfig,
    #[serde(default)]
    pub dev: DevConfig,
    pub resources: Option<ResourcesConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectConfig {
    pub name: String,
    #[serde(default = "default_version")]
    pub version: String,
}

fn default_version() -> String {
    "0.1.0".to_string()
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct DevConfig {
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_port() -> u16 {
    7850
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResourcesConfig {
    pub queue: Option<HashMap<String, QueueConfig>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueConfig {
    pub retention: Option<String>,
    #[serde(default)]
    pub dead_letter: bool,
}

impl SiderealConfig {
    pub fn load() -> Option<Self> {
        Self::load_from("sidereal.toml")
    }

    pub fn load_from(path: impl AsRef<Path>) -> Option<Self> {
        let content = std::fs::read_to_string(path).ok()?;
        toml::from_str(&content).ok()
    }

    pub fn declared_queues(&self) -> Vec<&str> {
        self.resources
            .as_ref()
            .and_then(|r| r.queue.as_ref())
            .map(|queues| queues.keys().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }
}
