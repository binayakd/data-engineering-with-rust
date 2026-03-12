use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

#[derive(Deserialize)]
pub struct Config {
    pub log_file_dir: String,
    pub data_dir: String,
    pub download_data: bool,
    pub dataset: Dataset,
    pub iceberg: Option<IcebergConfig>,
}

#[derive(Deserialize)]
pub struct Dataset {
    pub data_id: String,
    pub timestamp_column: String,
    pub urls: Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct IcebergConfig {
    pub enabled: bool,
    pub catalog_uri: String,
    pub warehouse_path: String,
    pub namespace: String,
}

pub fn load_config(path: &PathBuf) -> Config {
    let content = fs::read_to_string(&path).expect("Unable to read config file");
    toml::from_str::<Config>(&content).expect("Failed to parse TOML in config file")
}
