# Design: Add Apache Iceberg Support

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│         Data Processing Pipeline                    │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Config (config.toml)                              │
│  ├── Iceberg REST API endpoint                      │
│  ├── Warehouse path                                 │
│  └── Namespace config                              │
│                                                     │
│  Data Engine (Polars/DataFusion)                   │
│  ├── Process source data                           │
│  └── Transform to clean/analytics tables           │
│                                                     │
│  Iceberg Integration Layer (NEW)                   │
│  ├── IcebergWriter                                 │
│  ├── Table metadata management                     │
│  └── REST catalog client                           │
│                                                     │
│  Storage Backend                                   │
│  ├── Lakekeeper (Local - Docker Compose)           │
│  └── File system / S3 (Production)                 │
│                                                     │
└─────────────────────────────────────────────────────┘
```

## Component Design

### 1. Configuration Extension (config.toml)

```toml
[iceberg]
enabled = true
catalog_type = "rest"  # or "hadoop"
rest_endpoint = "http://localhost:8181"
warehouse_path = "s3://my-warehouse"  # or file:///local/warehouse
namespace = "default"
use_parquet = true  # Use Parquet as file format
```

### 2. Iceberg Integration Module (New)

**File:** `src/engines/iceberg_writer.rs`

```rust
pub struct IcebergWriter {
    catalog_uri: String,
    warehouse_path: String,
    namespace: String,
}

impl IcebergWriter {
    pub async fn create_table(
        &self,
        table_name: &str,
        df: PolarsDataFrame,
    ) -> Result<()> {
        // Convert Polars schema to Iceberg schema
        // Create table via REST API
        // Write data to table
    }

    pub async fn append_data(
        &self,
        table_name: &str,
        df: PolarsDataFrame,
    ) -> Result<()> {
        // Append data to existing Iceberg table
    }
}
```

### 3. Config Management Update

**File:** `src/common/config.rs`

Add `IcebergConfig` struct:
```rust
#[derive(Debug, Deserialize)]
pub struct IcebergConfig {
    pub enabled: bool,
    pub catalog_type: String,
    pub rest_endpoint: String,
    pub warehouse_path: String,
    pub namespace: String,
    pub use_parquet: bool,
}
```

### 4. Engine Integration

**File:** `src/engines/polars_engine.rs`

Modify `PolarsEngine` to:
- Detect if Iceberg output is enabled
- Use `IcebergWriter` for table creation
- Fall back to Parquet if Iceberg disabled

### 5. Docker Compose for Lakekeeper

**File:** `docker-compose.yml`

```yaml
version: '3.8'
services:
  lakekeeper:
    image: lakekeeper:latest
    ports:
      - "8181:8181"
    environment:
      RUST_LOG: "debug"
    volumes:
      - ./warehouse:/warehouse
```

## Data Flow

```
Raw Data (Parquet URLs)
    ↓
Polars Engine (Process)
    ↓
Clean Table (DataFrame)
    ├─ Output: Iceberg Table (via IcebergWriter)
    └─ Metadata: Iceberg Catalog (REST API)
    ↓
Analytics Table (Aggregated DataFrame)
    ├─ Output: Iceberg Table (via IcebergWriter)
    └─ Metadata: Iceberg Catalog (REST API)
```

## Implementation Strategy

1. **Phase 1**: Add Iceberg configuration to config.toml
2. **Phase 2**: Create IcebergWriter module with REST catalog client
3. **Phase 3**: Integrate IcebergWriter into PolarsEngine
4. **Phase 4**: Create Docker Compose for Lakekeeper
5. **Phase 5**: Add tests and documentation

## Dependencies

Add to Cargo.toml:
- `iceberg` (Apache Iceberg Rust binding)
- `reqwest` (already present, for REST API calls)
- `tokio` (async runtime)

## Error Handling

- REST API connection failures → fallback to Parquet
- Schema validation errors → detailed error messages
- Table creation conflicts → skip if table exists
- Data write failures → retry with exponential backoff

## Testing Strategy

1. **Unit Tests**: Table schema conversion, config parsing
2. **Integration Tests**: Local Lakekeeper instance via Docker Compose
3. **E2E Tests**: Full pipeline with Iceberg output
4. **Manual Tests**: Verify table metadata in Lakekeeper UI
