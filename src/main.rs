mod common;
mod engines;

use crate::engines::iceberg_writer::IcebergWriterManager;
use crate::engines::polars_engine::PolarsEngine;
use crate::engines::Engine;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "rust-data-pipeline")]
#[command(about = "ETL and Analytics Pipeline in Rust")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, default_value = "config.toml")]
    config: PathBuf,
}

#[derive(Subcommand)]
enum Commands {
    Polars,
    Datafusion,
}


fn main() -> anyhow::Result<()>  {
    let cli = Cli::parse();

    // reading config
    println!("Loading config from: {}", cli.config.display());
    let config = common::config::load_config(&cli.config);

    // setup logging
    let log_file = common::logging::init_logging(&config.log_file_dir);
    println!("Logging to: {}", log_file.display());

    // Create tokio runtime for async Iceberg operations
    let rt = tokio::runtime::Runtime::new()?;

    // Initialize Iceberg writer if configured
    let iceberg_writer = match &config.iceberg {
        Some(iceberg_config) if iceberg_config.enabled => {
            println!("Initializing Iceberg catalog...");
            let writer = rt.block_on(IcebergWriterManager::new(iceberg_config))?;
            println!("Iceberg catalog initialized successfully");
            Some(writer)
        }
        _ => {
            println!("Iceberg not configured, using local file output");
            None
        }
    };

    let engine: Box<dyn Engine> = match cli.command {
        Commands::Polars => Box::new(PolarsEngine),
        _ => panic!("Unknown engine"),
    };

    // Enter the tokio runtime context so that block_on calls inside
    // the pipeline (for Iceberg writes) can access the runtime handle
    let _guard = rt.enter();
    engine.run_pipeline(&config, iceberg_writer.as_ref())?;

    Ok(())
}
