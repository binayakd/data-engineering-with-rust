# Proposal: Add Apache Iceberg Support

## What

Add Apache Iceberg table format support to the data engineering pipeline, enabling clean and analytics tables to be saved in Iceberg format instead of just Parquet. Include Iceberg REST API configuration in config.toml and provide a Docker Compose setup for local testing with Lakekeeper.

## Why

**Benefits:**
- **ACID Transactions**: Iceberg provides ACID guarantees for concurrent writes and updates
- **Time Travel**: Query historical table snapshots for data auditing and recovery
- **Schema Evolution**: Safely modify table schemas without data loss
- **Hidden Partitioning**: Automatic, efficient partitioning without manual management
- **Data Quality**: Built-in data versioning and snapshot management
- **Ecosystem Compatibility**: Better integration with modern data platforms (Spark, Trino, Flink)

**Current Gap:**
The project currently uses Parquet for batch processing. Adding Iceberg support modernizes the data lake architecture and enables advanced data management capabilities needed for production workloads.

## Scope

- Integrate Apache Iceberg table format support for clean and analytics tables
- Add Iceberg REST API endpoints configuration to config.toml
- Implement table creation and write operations using Iceberg format
- Add Docker Compose file for local Lakekeeper instance (reference Iceberg catalog)
- Maintain backward compatibility with existing Parquet workflows

## Non-Goals

- Streaming ingestion support (batch-only for now)
- Advanced Iceberg optimization features (compaction, rewriting)
- Multi-catalog support (single local/remote REST catalog)
- Iceberg ACID transaction rollback handlers
