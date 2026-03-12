use std::sync::Arc;

use crate::common::config::Config;
use crate::engines::iceberg_writer::IcebergWriterManager;
use crate::engines::Engine;
use anyhow::Result;
use arrow_array::RecordBatch;
use df_interchange::Interchange;
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
        iceberg_writer: Option<&IcebergWriterManager>,
    ) -> Result<()> {
        info!("Processing: {ingest_data_path}");

        let args = ScanArgsParquet::default();
        let mut lf = LazyFrame::scan_parquet(PlPath::new(ingest_data_path), args)?;

        // filter only current period
        lf = filter_by_period(lf, &period, &config.dataset.timestamp_column)?;

        if let Some(writer) = iceberg_writer {
            // Collect the LazyFrame and write to Iceberg
            let df = lf.collect()?;
            let table_name = format!("{}_clean", config.dataset.data_id);
            info!("Writing clean data to Iceberg table: {}", table_name);

            let batches = polars_df_to_arrow_batches(df)?;
            let rt = tokio::runtime::Handle::current();
            rt.block_on(writer.write_to_table(&table_name, &batches, "period"))?;
        } else {
            // Fallback: write to local partitioned parquet
            let partition_columns = vec!["period".to_string()];
            write_output_partitioned(lf, &output_dir, partition_columns)?;
        }

        Ok(())
    }

    fn analytics(
        &self,
        config: &Config,
        period: &String,
        clean_data_path: &String,
        output_dir: &String,
        iceberg_writer: Option<&IcebergWriterManager>,
    ) -> Result<()> {
        info!("Starting daily trip count analytics for period: {}", period);

        let lf = if let Some(writer) = iceberg_writer {
            // Read clean data from Iceberg table, filtered by period
            let clean_table_name = format!("{}_clean", config.dataset.data_id);
            info!(
                "Reading clean data from Iceberg table: {} (period={})",
                clean_table_name, period
            );

            let rt = tokio::runtime::Handle::current();
            let batches = rt.block_on(writer.read_from_table(
                &clean_table_name,
                Some("period"),
                Some(period),
            ))?;

            // Convert Arrow RecordBatches back to a Polars DataFrame
            let df = arrow_batches_to_polars_df(batches)?;
            info!(
                "Loaded {} rows from Iceberg clean table for period {}",
                df.height(),
                period
            );
            df.lazy()
        } else {
            // Fallback: read from local parquet
            let args = ScanArgsParquet::default();
            let lf = LazyFrame::scan_parquet(PlPath::new(clean_data_path), args)?;
            lf.filter(col("period").eq(lit(period.to_string())))
        };

        // extract date from datetime
        let lf = lf.with_columns([col("tpep_pickup_datetime").dt().date().alias("date")]);

        // group by date and period and count rows
        let grouped_lf = lf.group_by([col("date"), col("period")]).agg([col("date")
            .count()
            .cast(DataType::Int64)
            .alias("total_trips")]);

        if let Some(writer) = iceberg_writer {
            // Collect, convert to Arrow, write to Iceberg
            let df = grouped_lf.collect()?;
            let table_name = format!("{}_analytics", config.dataset.data_id);
            info!("Writing analytics data to Iceberg table: {}", table_name);

            let batches = polars_df_to_arrow_batches(df)?;
            let rt = tokio::runtime::Handle::current();
            rt.block_on(writer.write_to_table(&table_name, &batches, "period"))?;
        } else {
            // Fallback: write to local partitioned parquet
            let partition_columns = vec!["period".to_string()];
            write_output_partitioned(grouped_lf, output_dir, partition_columns)?;
        }

        info!("Daily trip count analytics completed successfully");

        Ok(())
    }
}

/// Convert a Polars DataFrame into Arrow RecordBatches using the df-interchange crate.
/// Uses the Arrow C Data Interface for zero-copy conversion.
/// Rechunks the DataFrame first to ensure contiguous memory layout.
fn polars_df_to_arrow_batches(df: DataFrame) -> Result<Vec<RecordBatch>> {
    let mut df = df;
    df.rechunk_mut();
    let batches: Vec<RecordBatch> = Interchange::from_polars_0_50(df)?.to_arrow_57()?;
    Ok(batches)
}

/// Convert Arrow RecordBatches back into a Polars DataFrame using the df-interchange crate.
/// Uses the Arrow C Data Interface for zero-copy conversion.
fn arrow_batches_to_polars_df(batches: Vec<RecordBatch>) -> Result<DataFrame> {
    let df: DataFrame = Interchange::from_arrow_57(batches)?.to_polars_0_50()?;
    Ok(df)
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
