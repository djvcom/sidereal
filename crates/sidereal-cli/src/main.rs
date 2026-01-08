//! Sidereal CLI - build and run Sidereal applications.

mod commands;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "sidereal")]
#[command(about = "Build and run Sidereal applications")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new Sidereal project
    Init {
        /// Project name
        name: String,

        /// Output directory (defaults to project name)
        #[arg(short, long)]
        path: Option<PathBuf>,
    },

    /// Run the local development server
    Dev {
        /// Port to listen on (overrides sidereal.toml)
        #[arg(short, long)]
        port: Option<u16>,
    },

    /// Deploy to local Firecracker VM or remote environment
    Deploy {
        /// Deploy to local Firecracker VM
        #[arg(long, default_value = "true")]
        local: bool,

        /// Keep VM running after deployment (for debugging)
        #[arg(long)]
        keep_alive: bool,

        /// Skip building the project
        #[arg(long)]
        skip_build: bool,
    },
}

#[tokio::main]
async fn main() {
    let _otel_guard = opentelemetry_configuration::OtelSdkBuilder::new()
        .with_standard_env()
        .service_name(env!("CARGO_PKG_NAME"))
        .build()
        .ok();

    let cli = Cli::parse();

    let result: Result<(), anyhow::Error> = match cli.command {
        Commands::Init { name, path } => commands::init::run(&name, path).map_err(Into::into),
        Commands::Dev { port } => commands::dev::run(port).map_err(Into::into),
        Commands::Deploy {
            local,
            keep_alive,
            skip_build,
        } => {
            let args = commands::deploy::DeployArgs {
                local,
                keep_alive,
                skip_build,
            };
            commands::deploy::run(args).await.map_err(Into::into)
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
