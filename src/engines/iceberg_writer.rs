use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use futures::TryStreamExt;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, FieldMatchMode};
use iceberg::expr::Reference;
use iceberg::spec::{Datum, DataFileFormat, Literal, PartitionKey, Struct, Transform, UnboundPartitionSpec};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;
use tracing::info;
use uuid::Uuid;

use iceberg_catalog_sql::{
    SqlBindStyle, SqlCatalogBuilder, SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI,
    SQL_CATALOG_PROP_WAREHOUSE,
};

use crate::common::config::IcebergConfig;

/// Manages Iceberg catalog connection and table operations.
pub struct IcebergWriterManager {
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
    warehouse_path: String,
}

impl IcebergWriterManager {
    /// Initialize the catalog connection and ensure the namespace exists.
    pub async fn new(config: &IcebergConfig) -> Result<Self> {
        // Ensure the warehouse directory exists
        std::fs::create_dir_all(&config.warehouse_path)?;

        // Build absolute warehouse path for Iceberg
        let abs_warehouse = std::fs::canonicalize(&config.warehouse_path)
            .context("Failed to resolve warehouse path")?;
        let warehouse_uri = format!("file://{}", abs_warehouse.display());

        // Build absolute path for the SQLite DB
        let catalog_uri = resolve_sqlite_uri(&config.catalog_uri)?;

        info!("Initializing Iceberg SQL catalog at: {}", catalog_uri);
        info!("Warehouse location: {}", warehouse_uri);

        let catalog = SqlCatalogBuilder::default()
            .load(
                "sql",
                HashMap::from_iter([
                    (SQL_CATALOG_PROP_URI.to_string(), catalog_uri),
                    (SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_uri.clone()),
                    (
                        SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                        SqlBindStyle::QMark.to_string(),
                    ),
                ]),
            )
            .await
            .context("Failed to create Iceberg SQL catalog")?;

        let namespace = NamespaceIdent::from_vec(vec![config.namespace.clone()])?;

        // Create namespace if it doesn't exist
        let namespaces = catalog.list_namespaces(None).await?;
        if !namespaces.contains(&namespace) {
            info!("Creating Iceberg namespace: {}", config.namespace);
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await?;
        }

        Ok(Self {
            catalog: Arc::new(catalog),
            namespace,
            warehouse_path: warehouse_uri,
        })
    }

    /// Write Arrow RecordBatches to an Iceberg table, partitioned by the given column.
    /// Creates the table with an identity partition spec if it doesn't exist.
    pub async fn write_to_table(
        &self,
        table_name: &str,
        batches: &[RecordBatch],
        partition_column: &str,
    ) -> Result<()> {
        if batches.is_empty() {
            info!("No data to write for table: {}", table_name);
            return Ok(());
        }

        let arrow_schema = batches[0].schema();
        let table_ident = TableIdent::new(self.namespace.clone(), table_name.to_string());

        // Create or load the table
        let table = match self.catalog.load_table(&table_ident).await {
            Ok(existing) => {
                info!("Loaded existing Iceberg table: {}", table_name);
                existing
            }
            Err(_) => {
                info!("Creating new Iceberg table: {}", table_name);
                let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&arrow_schema)
                    .map_err(|e| anyhow::anyhow!("Failed to convert Arrow schema to Iceberg: {}", e))?;

                // Find the field ID for the partition column in the Iceberg schema
                let partition_field = iceberg_schema
                    .field_by_name(partition_column)
                    .ok_or_else(|| anyhow::anyhow!(
                        "Partition column '{}' not found in schema", partition_column
                    ))?;
                let partition_source_id = partition_field.id;

                // Build an identity partition spec on the partition column
                let partition_spec = UnboundPartitionSpec::builder()
                    .add_partition_field(partition_source_id, partition_column, Transform::Identity)
                    .map_err(|e| anyhow::anyhow!("Failed to add partition field: {}", e))?
                    .build();

                let creation = TableCreation::builder()
                    .name(table_name.to_string())
                    .schema(iceberg_schema)
                    .partition_spec(partition_spec)
                    .location(format!(
                        "{}/{}",
                        self.warehouse_path, table_name
                    ))
                    .build();

                self.catalog
                    .create_table(&self.namespace, creation)
                    .await
                    .context(format!("Failed to create Iceberg table: {}", table_name))?
            }
        };

        // Set up the partitioned writer pipeline:
        // ParquetWriterBuilder -> RollingFileWriterBuilder -> DataFileWriterBuilder -> FanoutWriter

        let location_generator =
            DefaultLocationGenerator::new(table.metadata().clone())
                .context("Failed to create location generator")?;

        let file_name_generator = DefaultFileNameGenerator::new(
            "data".to_string(),
            Some(Uuid::now_v7().to_string()),
            DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new_with_match_mode(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
            FieldMatchMode::Name,
        );

        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Use FanoutWriter for partitioned writes
        let mut fanout_writer = FanoutWriter::new(data_file_writer_builder);

        let bound_partition_spec = table.metadata().default_partition_spec().as_ref().clone();
        let schema_ref = table.metadata().current_schema().clone();

        // Find the partition column index in the Arrow schema
        let partition_col_idx = arrow_schema
            .index_of(partition_column)
            .map_err(|e| anyhow::anyhow!("Partition column '{}' not found in Arrow schema: {}", partition_column, e))?;

        // Write each batch, splitting by partition values
        for batch in batches {
            let partitioned = split_batch_by_partition(batch, partition_col_idx)?;

            for (partition_value, sub_batch) in partitioned {
                let partition_key = PartitionKey::new(
                    bound_partition_spec.clone(),
                    schema_ref.clone(),
                    Struct::from_iter([Some(Literal::string(&partition_value))]),
                );

                fanout_writer
                    .write(partition_key, sub_batch)
                    .await
                    .context("Failed to write partitioned RecordBatch to Iceberg")?;
            }
        }

        // Close the writer to get the data files
        let data_files = fanout_writer
            .close()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to close Iceberg fanout writer: {}", e))?;

        info!(
            "Wrote {} data file(s) for table: {}",
            data_files.len(),
            table_name
        );

        // Commit the data files via a transaction
        let tx = Transaction::new(&table);
        let append_action = tx.fast_append().add_data_files(data_files);
        let tx = append_action
            .apply(tx)
            .map_err(|e| anyhow::anyhow!("Failed to apply fast append: {}", e))?;
        let _table = tx
            .commit(&*self.catalog)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to commit transaction for table {}: {}", table_name, e))?;

        info!(
            "Successfully committed data to Iceberg table: {}",
            table_name
        );

        Ok(())
    }

    /// Read data from an Iceberg table, optionally filtered by a partition value.
    /// Returns Arrow RecordBatches. Uses Iceberg's partition pruning for efficiency.
    pub async fn read_from_table(
        &self,
        table_name: &str,
        filter_column: Option<&str>,
        filter_value: Option<&str>,
    ) -> Result<Vec<RecordBatch>> {
        let table_ident = TableIdent::new(self.namespace.clone(), table_name.to_string());

        let table = self
            .catalog
            .load_table(&table_ident)
            .await
            .context(format!("Failed to load Iceberg table: {}", table_name))?;

        let mut scan_builder = table.scan().select_all();

        // Apply filter if both column and value are provided
        if let (Some(col_name), Some(val)) = (filter_column, filter_value) {
            let predicate = Reference::new(col_name).equal_to(Datum::string(val));
            scan_builder = scan_builder.with_filter(predicate);
        }

        let scan = scan_builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build Iceberg table scan: {}", e))?;

        let batch_stream = scan
            .to_arrow()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create arrow stream from Iceberg scan: {}", e))?;

        let batches: Vec<RecordBatch> = batch_stream
            .try_collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to collect batches from Iceberg scan: {}", e))?;

        info!(
            "Read {} batch(es) from Iceberg table: {}",
            batches.len(),
            table_name
        );

        Ok(batches)
    }
}

/// Split a RecordBatch into sub-batches grouped by the partition column's distinct values.
/// Returns a Vec of (partition_value_string, RecordBatch) pairs.
fn split_batch_by_partition(
    batch: &RecordBatch,
    partition_col_idx: usize,
) -> Result<Vec<(String, RecordBatch)>> {
    let partition_array = batch.column(partition_col_idx);
    let num_rows = batch.num_rows();

    // Extract string values from the partition column, supporting Utf8, LargeUtf8, and Utf8View
    let string_values: Vec<Option<String>> = match partition_array.data_type() {
        arrow_schema::DataType::Utf8 => {
            let arr = partition_array.as_string::<i32>();
            (0..num_rows)
                .map(|i| {
                    if arr.is_valid(i) {
                        Some(arr.value(i).to_string())
                    } else {
                        None
                    }
                })
                .collect()
        }
        arrow_schema::DataType::LargeUtf8 => {
            let arr = partition_array.as_string::<i64>();
            (0..num_rows)
                .map(|i| {
                    if arr.is_valid(i) {
                        Some(arr.value(i).to_string())
                    } else {
                        None
                    }
                })
                .collect()
        }
        arrow_schema::DataType::Utf8View => {
            let arr = partition_array.as_string_view();
            (0..num_rows)
                .map(|i| {
                    if arr.is_valid(i) {
                        Some(arr.value(i).to_string())
                    } else {
                        None
                    }
                })
                .collect()
        }
        other => {
            anyhow::bail!(
                "Partition column at index {} must be a string type, got {:?}",
                partition_col_idx,
                other
            );
        }
    };

    // Collect distinct partition values (preserving order)
    let mut partition_values: Vec<String> = Vec::new();
    for val in &string_values {
        if let Some(v) = val {
            if !partition_values.contains(v) {
                partition_values.push(v.clone());
            }
        }
    }

    let mut results = Vec::new();
    let schema = batch.schema();

    for value in partition_values {
        // Collect row indices matching this partition value
        let indices: Vec<u32> = string_values
            .iter()
            .enumerate()
            .filter_map(|(i, v)| {
                if v.as_deref() == Some(&value) {
                    Some(i as u32)
                } else {
                    None
                }
            })
            .collect();

        let indices_array = arrow_array::UInt32Array::from(indices);
        let columns: Vec<arrow_array::ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| arrow_select::take::take(col, &indices_array, None).unwrap())
            .collect();

        let sub_batch = RecordBatch::try_new(schema.clone(), columns)?;
        results.push((value, sub_batch));
    }

    Ok(results)
}

/// Resolve a sqlite:// URI to use an absolute path for the DB file.
/// Input: "sqlite://local/iceberg/catalog.db"
/// Output: "sqlite:///absolute/path/to/local/iceberg/catalog.db?mode=rwc"
fn resolve_sqlite_uri(uri: &str) -> Result<String> {
    if let Some(path) = uri.strip_prefix("sqlite://") {
        // Ensure parent directory exists
        let db_path = std::path::Path::new(path);
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let abs_path = std::fs::canonicalize(
            db_path.parent().unwrap_or(std::path::Path::new(".")),
        )?;
        let filename = db_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy();
        // mode=rwc creates the database file if it doesn't exist
        Ok(format!("sqlite://{}/{}?mode=rwc", abs_path.display(), filename))
    } else {
        Ok(uri.to_string())
    }
}
