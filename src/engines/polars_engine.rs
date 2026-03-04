use crate::common::config::Config;
use crate::engines::Engine;
use anyhow::Result;
use polars::prelude::*;
use polars_io::utils::sync_on_close::SyncOnCloseType;
use tracing::info;

pub struct PolarsEngine;

impl Engine for PolarsEngine {
    fn clean(
        &self,
        config: &Config,
        period: &String,
        ingest_data_path: &String,
        output_dir: &String,
    ) -> Result<()> {
        info!("Processing: {ingest_data_path}");

        let args = ScanArgsParquet::default();
        let mut lf = LazyFrame::scan_parquet(PlPath::new(ingest_data_path), args)?;

        // filter only current period
        lf = filter_by_period(lf, &period, &config.dataset.timestamp_column)?;

        // add period column
        let partition_columns = vec!["period".to_string()];

        // save to output
        write_output_partitioned(lf, &output_dir, partition_columns)?;

        Ok(())
    }

    fn analytics(
        &self,
        period: &String,
        clean_data_path: &String,
        output_dir: &String,
    ) -> Result<()> {
        info!("Starting daily trip count analytics for period: {}", period);

        let args = ScanArgsParquet::default();
        let mut lf = LazyFrame::scan_parquet(PlPath::new(clean_data_path), args)?;

        // filter only current period
        lf = lf.filter(col("period").eq(lit(period.to_string())));

        // extract date from datetime
        let lf = lf.with_columns([col("tpep_pickup_datetime").dt().date().alias("date")]);

        // group by date and period and count rows
        let grouped_lf = lf
            .group_by([col("date"), col("period")])
            .agg([
                col("date").count().alias("total_trips")
            ]);

        // Prepare partition columns
        let partition_columns = vec!["period".to_string()];

        // Use the existing write_output_partitioned function
        write_output_partitioned(grouped_lf, output_dir, partition_columns)?;

        info!("Daily trip count analytics completed successfully");

        Ok(())
    }
}

fn filter_by_period(
    mut lf: LazyFrame,
    year_month: &str,
    timestamp_column: &str,
) -> Result<LazyFrame> {
    let parts: Vec<&str> = year_month.split('-').collect();
    let year: i32 = parts[0].parse()?;
    let month: i32 = parts[1].parse()?;

    lf = lf.filter(
        col(timestamp_column)
            .dt()
            .year()
            .eq(lit(year))
            .and(col(timestamp_column).dt().month().eq(lit(month))),
    );

    lf = lf.with_column(lit(year_month).alias("period"));

    Ok(lf)
}

fn write_output_partitioned(
    lf: LazyFrame,
    output_path: &str,
    partition_columns: Vec<String>,
) -> Result<()> {
    info!("starting sink to file: {output_path}");

    let sink_options = SinkOptions {
        sync_on_close: SyncOnCloseType::All,
        maintain_order: true,
        mkdir: true,
    };

    let key_exprs = partition_columns
        .iter()
        .map(|x| col(x))
        .collect::<Vec<Expr>>();

    let partition_variant = PartitionVariant::ByKey {
        key_exprs,
        include_key: true,
    };

    let _ = lf
        .sink_parquet_partitioned(
            Arc::new(PlPath::new(output_path)),
            None,
            partition_variant,
            ParquetWriteOptions::default(),
            None,
            sink_options,
            None,
            None,
        )?
        .collect()?;

    Ok(())
}
