use chrono::Local;
use std::fs;
use std::path::PathBuf;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

pub fn init_logging(log_dir_path: &String) -> PathBuf {
    // Create logs directory if missing
    let log_dir = PathBuf::from(log_dir_path);
    fs::create_dir_all(&log_dir).expect("Failed to create logs directory");

    // Generate unique log file name per run
    let now = Local::now();
    let log_file_name = format!("etl_run_{}.log", now.format("%Y-%m-%d_%H-%M-%S"));
    let log_file_path = log_dir.join(log_file_name);

    // Open log file
    let file = fs::File::create(&log_file_path).expect("Failed to create log file");

    // Set up logging subscriber
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Create a formatting layer for file output
    let file_layer = fmt::layer()
        .with_writer(file)
        .json()
        .with_timer(fmt::time::ChronoLocal::rfc_3339())
        .with_level(true)
        .with_ansi(false); // cleaner format

    // Create a formatting layer for console output
    let console_layer = fmt::layer()
        .with_timer(fmt::time::ChronoLocal::rfc_3339())
        .with_level(true)
        .with_ansi(true);

    // Build subscriber with both layers
    tracing_subscriber::registry()
        .with(env_filter)
        .with(file_layer)
        .with(console_layer)
        .init();

    log_file_path
}
