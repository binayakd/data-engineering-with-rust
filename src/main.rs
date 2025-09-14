mod utils;
mod polars_functions;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::info;

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


fn main() {
    let cli = Cli::parse();

    // reading config
    println!("Loading config from: {}", cli.config.display());
    let config = utils::config::load_config(&cli.config);

    // setup logging
    let log_file = utils::logging::init_logging(&config.log_file_dir);
    println!("Logging to: {}", log_file.display());

    info!("Starting ETL pipeline...");

    info!("Step 0: Download data to bronze layer");
    let download_paths = utils::download_client::download_data_to_bronze(
        &config
    ).expect("Failed to download data");


    info!("Step 2: Siver layer");
    match cli.command {
        Commands::Polars => {
            info!("Running with Polars");
            for path in download_paths {
                polars_functions::silver_layer_transformations(
                    &path,
                    &config.dataset.data_id,
                    &config.dataset.timestamp_column,
                    &config.data_dir
                )
            }

        }
        Commands::Datafusion => {
            info!("Running with Datafusion")
        }
    }



    //
    // warn!("Step 3: Missing some columns, applying defaults");
    // info!("Step 4: Saving results");
    // info!("ETL pipeline completed successfully!");
    // error!("This is an example error log");
}



