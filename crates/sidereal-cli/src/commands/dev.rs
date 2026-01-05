//! Implementation of the `sidereal dev` command.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::post,
    Router,
};
use serde::Deserialize;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use thiserror::Error;
use wasmtime::{Config, Engine, Linker, Module, Store, TypedFunc};
use wasmtime_wasi::preview1::{self, WasiP1Ctx};
use wasmtime_wasi::WasiCtxBuilder;

#[derive(Error, Debug)]
pub enum DevError {
    #[error("Configuration file not found: sidereal.toml")]
    ConfigNotFound,

    #[error("Build failed: {0}")]
    BuildFailed(String),

    #[error("Missing WASM target. Run: rustup target add wasm32-wasip1")]
    MissingTarget,

    #[error("No functions found in compiled module")]
    NoFunctionsFound,

    #[error("Runtime error: {0}")]
    Runtime(String),

    #[error("Server error: {0}")]
    Server(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Wasmtime(#[from] wasmtime::Error),

    #[error(transparent)]
    MemoryAccess(#[from] wasmtime::MemoryAccessError),

    #[error(transparent)]
    Toml(#[from] toml::de::Error),
}

#[derive(Deserialize)]
struct SiderealConfig {
    project: ProjectConfig,
    #[serde(default)]
    build: BuildConfig,
    #[serde(default)]
    dev: DevConfig,
}

#[derive(Deserialize)]
struct ProjectConfig {
    name: String,
    #[allow(dead_code)]
    version: String,
}

#[derive(Deserialize, Default)]
struct BuildConfig {
    #[serde(default = "default_target")]
    target: String,
}

fn default_target() -> String {
    "wasm32-wasip1".to_string()
}

#[derive(Deserialize, Default)]
struct DevConfig {
    #[serde(default = "default_port")]
    port: u16,
}

fn default_port() -> u16 {
    3000
}

fn load_config() -> Result<SiderealConfig, DevError> {
    let config_path = PathBuf::from("sidereal.toml");
    if !config_path.exists() {
        return Err(DevError::ConfigNotFound);
    }

    let content = std::fs::read_to_string(config_path)?;
    let config: SiderealConfig = toml::from_str(&content)?;
    Ok(config)
}

fn check_wasm_target() -> Result<(), DevError> {
    // Try rustc --print target-list first (works with nix-managed toolchains)
    let output = Command::new("rustc")
        .args(["--print", "target-list"])
        .output();

    if let Ok(output) = output {
        let targets = String::from_utf8_lossy(&output.stdout);
        if targets.contains("wasm32-wasip1") {
            // Target is known, now check if we can actually compile to it
            // by checking if the target sysroot exists
            let sysroot_check = Command::new("rustc")
                .args(["--print", "sysroot"])
                .output();

            if let Ok(sysroot_output) = sysroot_check {
                let sysroot = String::from_utf8_lossy(&sysroot_output.stdout);
                let sysroot = sysroot.trim();
                let target_dir =
                    std::path::Path::new(sysroot).join("lib/rustlib/wasm32-wasip1");
                if target_dir.exists() {
                    return Ok(());
                }
            }
        }
    }

    // Fall back to rustup check for non-nix environments
    let output = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output();

    if let Ok(output) = output {
        let installed = String::from_utf8_lossy(&output.stdout);
        if installed.contains("wasm32-wasip1") {
            return Ok(());
        }
    }

    Err(DevError::MissingTarget)
}

fn get_target_directory() -> Result<PathBuf, DevError> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()?;

    if !output.status.success() {
        return Err(DevError::BuildFailed(
            "Failed to get cargo metadata".to_string(),
        ));
    }

    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| DevError::BuildFailed(format!("Failed to parse cargo metadata: {}", e)))?;

    let target_dir = metadata["target_directory"]
        .as_str()
        .ok_or_else(|| DevError::BuildFailed("No target_directory in cargo metadata".to_string()))?;

    Ok(PathBuf::from(target_dir))
}

fn build_wasm(config: &SiderealConfig) -> Result<PathBuf, DevError> {
    println!("Building project...");

    let status = Command::new("cargo")
        .args(["build", "--release", "--target", &config.build.target])
        .status()?;

    if !status.success() {
        return Err(DevError::BuildFailed(
            "cargo build failed".to_string(),
        ));
    }

    let target_dir = get_target_directory()?;

    // Convert project name to crate name (hyphens to underscores)
    let crate_name = config.project.name.replace('-', "_");
    let wasm_path = target_dir
        .join(&config.build.target)
        .join("release")
        .join(format!("{}.wasm", crate_name));

    if !wasm_path.exists() {
        return Err(DevError::BuildFailed(format!(
            "WASM file not found at {}",
            wasm_path.display()
        )));
    }

    println!("Build complete: {}", wasm_path.display());
    Ok(wasm_path)
}

const SIDEREAL_FUNCTION_PREFIX: &str = "__sidereal_call_";

fn discover_functions(module: &Module) -> Vec<String> {
    module
        .exports()
        .filter_map(|export| {
            let name = export.name();
            if name.starts_with(SIDEREAL_FUNCTION_PREFIX) {
                Some(
                    name.strip_prefix(SIDEREAL_FUNCTION_PREFIX)
                        .unwrap()
                        .to_string(),
                )
            } else {
                None
            }
        })
        .collect()
}

struct WasmRuntime {
    engine: Engine,
    module: Module,
    linker: Linker<WasiP1Ctx>,
}

impl WasmRuntime {
    fn new(wasm_path: &std::path::Path) -> Result<Self, DevError> {
        let mut config = Config::new();
        config.epoch_interruption(true);

        let engine = Engine::new(&config)?;
        let module = Module::from_file(&engine, wasm_path)?;

        let mut linker = Linker::new(&engine);
        preview1::add_to_linker_sync(&mut linker, |ctx| ctx)?;

        Ok(Self {
            engine,
            module,
            linker,
        })
    }

    fn call(&self, function_name: &str, input: &[u8]) -> Result<Vec<u8>, DevError> {
        let wasi = WasiCtxBuilder::new().build_p1();
        let mut store = Store::new(&self.engine, wasi);

        // Set epoch deadline for timeout (5 seconds worth of epochs)
        store.set_epoch_deadline(50);

        let instance = self.linker.instantiate(&mut store, &self.module)?;

        // Get memory and allocation functions
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| DevError::Runtime("Module must export 'memory'".to_string()))?;

        let alloc: TypedFunc<u32, u32> = instance
            .get_typed_func(&mut store, "__sidereal_alloc")
            .map_err(|_| {
                DevError::Runtime("Module must export '__sidereal_alloc'".to_string())
            })?;

        let dealloc: TypedFunc<(u32, u32), ()> = instance
            .get_typed_func(&mut store, "__sidereal_dealloc")
            .map_err(|_| {
                DevError::Runtime("Module must export '__sidereal_dealloc'".to_string())
            })?;

        let export_name = format!("{}{}", SIDEREAL_FUNCTION_PREFIX, function_name);
        let func: TypedFunc<(u32, u32), u64> = instance
            .get_typed_func(&mut store, &export_name)
            .map_err(|_| DevError::Runtime(format!("Function '{}' not found", function_name)))?;

        // Allocate and write input
        let input_len = input.len() as u32;
        let input_ptr = alloc.call(&mut store, input_len)?;
        memory.write(&mut store, input_ptr as usize, input)?;

        // Call the function
        let packed_result = func.call(&mut store, (input_ptr, input_len))?;

        // Unpack result (ptr in high 32 bits, len in low 32 bits)
        let output_ptr = (packed_result >> 32) as u32;
        let output_len = (packed_result & 0xFFFFFFFF) as u32;

        // Read output
        let mut output = vec![0u8; output_len as usize];
        memory.read(&store, output_ptr as usize, &mut output)?;

        // Clean up allocations
        dealloc.call(&mut store, (input_ptr, input_len))?;
        dealloc.call(&mut store, (output_ptr, output_len))?;

        Ok(output)
    }
}

struct AppState {
    runtime: WasmRuntime,
    functions: Vec<String>,
}

async fn handle_function(
    State(state): State<Arc<AppState>>,
    Path(function_name): Path<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !state.functions.contains(&function_name) {
        return Err((
            StatusCode::NOT_FOUND,
            format!("Function '{}' not found", function_name),
        ));
    }

    match state.runtime.call(&function_name, &body) {
        Ok(output) => {
            let value: serde_json::Value = serde_json::from_slice(&output).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to parse output: {}", e),
                )
            })?;
            Ok(Json(value))
        }
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn run_server(state: Arc<AppState>, port: u16) -> Result<(), DevError> {
    let app = Router::new()
        .route("/{function}", post(handle_function))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .map_err(|e| DevError::Server(e.to_string()))?;

    println!("Server listening on http://localhost:{}", port);

    axum::serve(listener, app)
        .await
        .map_err(|e| DevError::Server(e.to_string()))?;

    Ok(())
}

pub async fn run(port_override: Option<u16>) -> Result<(), DevError> {
    // Load configuration
    let config = load_config()?;

    // Check prerequisites
    check_wasm_target()?;

    // Build to WASM
    let wasm_path = build_wasm(&config)?;

    // Create runtime and discover functions
    let runtime = WasmRuntime::new(&wasm_path)?;
    let functions = discover_functions(&runtime.module);

    if functions.is_empty() {
        return Err(DevError::NoFunctionsFound);
    }

    println!("Functions available:");
    for func in &functions {
        println!("  POST /{}", func);
    }
    println!();

    // Start server
    let port = port_override.unwrap_or(config.dev.port);
    let state = Arc::new(AppState { runtime, functions });
    run_server(state, port).await?;

    Ok(())
}
