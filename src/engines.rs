pub mod polars_engine;

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::Write;
use anyhow::{anyhow, Result};
use regex::Regex;
use reqwest::blocking::Client;
use tracing::info;
use crate::common::config::Config;


pub struct OutputLocations {
    ingest_data_dir: String,
    clean_data_dir: String,
    analytics_data_dir: String,
}


pub trait Engine {

    fn run_pipeline(&self, config: &Config) -> Result<()> {

        info!("Step 0: Setup");
        let output_locations = self.setup(config)?;

        info!("Step 1: Ingest");
        let ingest_data_map = self.ingest(config, &output_locations.ingest_data_dir)?;


        info!("Step 2: Clean");
        for (period, ingest_data_path) in ingest_data_map.iter() {
            self.clean(
                config,
                period,
                ingest_data_path,
                &output_locations.clean_data_dir
            )?;
        }

        // get the min period, to ensure processing is only done for that period or later
        // in an incremental way

        info!("Step 3: Analytics");
        let min_period = ingest_data_map.keys()
            .min()
            .ok_or(anyhow!("Failed to extract min period"))?;


        for period in ingest_data_map.keys() {

        }




        Ok(())
    }


    fn setup(&self, config: &Config) -> Result<OutputLocations> {

        // Create ingest data dir
        let ingest_data_dir = format!("{}/ingest/{}", config.data_dir, config.dataset.data_id);
        fs::create_dir_all(&ingest_data_dir)?;
        
        // create cleaned partitioned parquet dir
        let clean_data_dir = format!("{}/clean/{}.parquet", &config.data_dir, &config.dataset.data_id);
        fs::create_dir_all(&clean_data_dir)?;

        // create analytics partitioned parquet dir
        let analytics_data_dir = format!("{}/analytics/{}.parquet", &config.data_dir, &config.dataset.data_id);
        fs::create_dir_all(&analytics_data_dir)?;

        let output_locations = OutputLocations {
            ingest_data_dir,
            clean_data_dir,
            analytics_data_dir
        };

        Ok(output_locations)

    }

    fn ingest(&self, config: &Config, ingest_data_dir: &String) -> Result<HashMap<String, String>> {

        // get period partition key for each downloaded file
        let mut ingest_data_map = HashMap::new();

        let client = Client::new();

        for url in &config.dataset.urls {
            // get the period partition key from the filename, and the download location
            // and save it to the bronze_data_map for used by silver later transformation
            let filename = url.split('/')
                .last()
                .ok_or(anyhow!("Failed extracting filename from url: {:?}", url))?;

            let re = Regex::new(r"\d{4}-\d{2}")?;
            let period = re.find(filename)
                .map(|x| x.as_str().to_string())
                .ok_or(anyhow!("Failed extracting period from filename: {:?}", filename))?;

            let file_path = format!("{}/{}", ingest_data_dir, filename);
            ingest_data_map.insert(period, file_path.clone());

            // do the actual downloading
            if config.download_data {
                let resp = client.get(url).send()?;
                let bytes = resp.bytes()?;

                let mut file = File::create(&file_path)?;
                file.write_all(&bytes)?;
                info!("Downloaded {} → {}", &url, &file_path);
            }
        }

        Ok(ingest_data_map)
    }


    fn clean(
        &self,
        config: &Config,
        period: &String,
        ingest_data_path: &String,
        output_dir: &String
    ) -> Result<()>;

    fn analytics(
        &self,
        period: &String,
        clean_data_path: &String,
        output_dir: &String
    ) -> Result<()>;
}

