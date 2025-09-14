use crate::utils::config::{Config};
use anyhow::Result;
use reqwest::blocking::Client;
use std::fs;
use std::fs::File;
use std::io::Write;
use tracing::info;


pub fn download_data_to_bronze(config: &Config) -> Result<Vec<String>>{

    let bronze_data_dir = format!("{}/bronze/{}", config.data_dir, config.dataset.data_id);
    fs::create_dir_all(&bronze_data_dir).expect("Failed to create bronze data directory");

    let mut downloaded_file_paths: Vec<String> = Vec::new();
    for url in &config.dataset.urls {
        let filename = url.split('/').last().unwrap();
        let file_path = format!("{}/{}", &bronze_data_dir, filename);
        downloaded_file_paths.push(file_path.clone());

        if config.download_data {
            download_file(url, file_path)?;
        }
    }

    Ok(downloaded_file_paths)

}

fn download_file(url: &String, file_path: String) -> Result<()> {
    let client = Client::new();

    let resp = client.get(url).send()?;
    let bytes = resp.bytes()?;

    let mut file = File::create(&file_path)?;
    file.write_all(&bytes)?;

    info!("Downloaded {} → {}", &url, &file_path);

    Ok(())
}

