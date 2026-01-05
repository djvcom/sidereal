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
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();

    let result: Result<(), anyhow::Error> = match cli.command {
        Commands::Init { name, path } => commands::init::run(&name, path).await.map_err(Into::into),
        Commands::Dev { port } => commands::dev::run(port).await.map_err(Into::into),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
