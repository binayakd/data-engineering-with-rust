mod common;
mod engines;

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

    let engine: Box<dyn Engine> = match cli.command {
        Commands::Polars => Box::new(PolarsEngine),
        _ => panic!("Unknown engine"),
    };

    engine.run_pipeline(&config)?;

    Ok(())

}

















