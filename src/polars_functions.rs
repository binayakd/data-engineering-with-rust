use polars::prelude::*;
use regex::Regex;
use std::fs;
use polars_io::utils::sync_on_close::SyncOnCloseType;
use tracing::{info};

pub fn silver_layer_transformations(
    file_path: &str,
    data_id: &str,
    timestamp_column: &str,
    output_data_dir: &str,

){

    info!("Processing: {file_path}");

    let year_month = extract_year_month(file_path);

    // lazy load parquet files
    let args = ScanArgsParquet::default();
    let mut lf = LazyFrame::scan_parquet(PlPath::new(file_path), args)
        .expect("unable to parse parquet file");

    // filter by year month
    lf = filter_by_year_month(lf, &year_month, timestamp_column);

    // write to silver layer
    let output_dir = format!("{}/silver/{}.parquet", output_data_dir, data_id);
    fs::create_dir_all(&output_dir).expect("Failed to create silver data directory");

    // let output_file_path = format!("{}/{}.parquet", output_dir, year_month);

    let partition_columns = vec!["period".to_string()];
    write_output_partitioned(lf, &output_dir, partition_columns);

}

fn extract_year_month(file_path: &str) -> String {
    let re = Regex::new(r"\d{4}-\d{2}").unwrap();
    re.find(file_path).map(|x| x.as_str().to_string()).expect("Unable to extract time partition")
}

fn filter_by_year_month(mut lf: LazyFrame, year_month: &str, timestamp_column: &str) -> LazyFrame {
    let parts: Vec<&str> = year_month.split('-').collect::<>();
    let year: i32 = parts[0].parse().unwrap();
    let month: i32 = parts[1].parse().unwrap();

    lf = lf.filter(
        col(timestamp_column)
        .dt()
        .year()
        .eq(lit(year))
        .and(col(timestamp_column).dt().month().eq(lit(month)))
    );

    lf.with_column(lit(year_month).alias("period"))
}

fn write_output_partitioned(lf: LazyFrame, output_path: &str, partition_columns: Vec<String>) {
    info!("starting sink to file: {output_path}");

    let sink_options = SinkOptions {
        sync_on_close: SyncOnCloseType::All,
        maintain_order: true,
        mkdir: true
    };

    let key_exprs = partition_columns.iter()
        .map(|x| col(x)).collect::<Vec<Expr>>();

    let partition_variant = PartitionVariant::ByKey {
        key_exprs,
        include_key: true,
    };

    let _ = lf.sink_parquet_partitioned(
        Arc::new(PlPath::new(output_path)),
        None,
        partition_variant,
        ParquetWriteOptions::default(),
        None,
        sink_options,
        None,
        None,
    ).unwrap().collect().unwrap();

}

