# Tasks: Add Apache Iceberg Support

## Task 1: Update Dependencies

**Objective:** Add Iceberg and async runtime dependencies to Cargo.toml

**Steps:**
1. Add `iceberg` crate (latest version)
2. Add `tokio` with `full` features for async support
3. Optionally add `aws-sdk-s3` for S3 integration
4. Run `cargo check` to verify dependency resolution

**Acceptance Criteria:**
- [ ] Cargo.toml updated with new dependencies
- [ ] `cargo check` passes without conflicts
- [ ] All dependencies resolve correctly

---

## Task 2: Create Iceberg Configuration Schema

**Objective:** Extend config.toml with Iceberg REST API configuration

**Steps:**
1. Add `[iceberg]` section to config.toml:
   - `enabled = true`
   - `catalog_type = "rest"`
   - `rest_endpoint = "http://localhost:8181"`
   - `warehouse_path = "file:///local/warehouse"`
   - `namespace = "default"`
   - `use_parquet = true`

2. Update `src/common/config.rs`:
   - Add `IcebergConfig` struct with above fields
   - Add `iceberg: Option<IcebergConfig>` to main `Config` struct
   - Add deserialization logic

3. Update `src/common.rs` to re-export `IcebergConfig`

**Acceptance Criteria:**
- [ ] config.toml has Iceberg section with all required fields
- [ ] `IcebergConfig` struct compiles
- [ ] Config parsing tests pass
- [ ] Default configuration is sensible

---

## Task 3: Create IcebergWriter Module

**Objective:** Implement core Iceberg table writing functionality

**Steps:**
1. Create `src/engines/iceberg_writer.rs`:
   - Define `IcebergWriter` struct with catalog_uri, warehouse_path, namespace
   - Implement `SchemaConverter` trait to convert Polars schema → Iceberg schema
   - Implement `create_table()` method:
     - Accept table_name and PolarsDataFrame
     - Convert schema
     - Make REST API call to create table
     - Write Parquet file to warehouse
     - Return table metadata
   - Implement `append_data()` method for subsequent writes
   - Implement error handling for REST API failures

2. Create REST client wrapper:
   - Use `reqwest` client to communicate with Iceberg catalog
   - Handle JSON serialization/deserialization
   - Implement retry logic for transient failures

3. Add unit tests for schema conversion

**Acceptance Criteria:**
- [ ] IcebergWriter struct compiles
- [ ] REST API methods implemented
- [ ] Schema conversion logic handles Polars types
- [ ] Error handling is comprehensive
- [ ] Unit tests pass

---

## Task 4: Integrate IcebergWriter into PolarsEngine

**Objective:** Enable the data pipeline to write tables in Iceberg format

**Steps:**
1. Update `src/engines/polars_engine.rs`:
   - Import `IcebergWriter` and config
   - Add `iceberg_writer: Option<IcebergWriter>` field to `PolarsEngine`
   - Update constructor to initialize IcebergWriter if Iceberg is enabled
   - Modify table output methods (clean_table, analytics_table) to:
     - Check if Iceberg is enabled
     - If yes, use IcebergWriter to write to Iceberg catalog
     - If no, fall back to Parquet output (existing behavior)

2. Update main.rs or engine initialization to pass config to engine

3. Add integration tests

**Acceptance Criteria:**
- [ ] PolarsEngine compiles with IcebergWriter integration
- [ ] Engine can be initialized with/without Iceberg
- [ ] Table output respects Iceberg configuration
- [ ] Fallback to Parquet works if Iceberg disabled
- [ ] Integration tests pass

---

## Task 5: Create Docker Compose for Lakekeeper

**Objective:** Provide local Iceberg testing environment

**Steps:**
1. Create `docker-compose.yml` in project root:
   - Define `lakekeeper` service
   - Use official or community Lakekeeper image
   - Expose port 8181 for REST API
   - Mount volume for warehouse directory
   - Add environment variables for logging

2. Create `warehouse/.gitkeep` directory (for persistence)

3. Create `docker-compose.yml` documentation in README:
   - How to start: `docker-compose up -d`
   - How to verify: `curl http://localhost:8181/healthz`
   - How to stop: `docker-compose down`

4. Update main config.toml default to use `http://localhost:8181`

**Acceptance Criteria:**
- [ ] docker-compose.yml valid YAML
- [ ] Lakekeeper service starts successfully
- [ ] REST API accessible on port 8181
- [ ] Warehouse directory persists data
- [ ] Documentation updated

---

## Task 6: End-to-End Testing

**Objective:** Verify complete Iceberg integration with pipeline

**Steps:**
1. Start Lakekeeper: `docker-compose up -d`
2. Run pipeline with Iceberg enabled
3. Verify tables created in Iceberg catalog:
   - Check REST API: `curl http://localhost:8181/v1/namespaces/default/tables`
   - Inspect table metadata
   - List table snapshots and metadata files

4. Query results to ensure data integrity

5. Test fallback behavior (disable Iceberg, run again)

6. Create test script: `scripts/test-iceberg.sh`

**Acceptance Criteria:**
- [ ] Tables successfully written to Iceberg format
- [ ] Metadata correctly stored in catalog
- [ ] Data integrity verified (row counts, schemas match)
- [ ] Fallback to Parquet works
- [ ] Test script is executable and passes

---

## Task 7: Documentation and Cleanup

**Objective:** Document the new Iceberg support and ensure code quality

**Steps:**
1. Update README.md:
   - Add Iceberg to "Technology Stack"
   - Document Iceberg configuration section
   - Add "Local Testing with Lakekeeper" guide

2. Add inline code documentation:
   - Document IcebergWriter public methods
   - Document config structure
   - Add examples in docs

3. Run code formatting: `cargo fmt`

4. Run clippy: `cargo clippy -- -D warnings`

5. Update CHANGELOG or version notes

6. Review all files for consistency

**Acceptance Criteria:**
- [ ] README updated with Iceberg information
- [ ] Code passes `cargo fmt` check
- [ ] Code passes `cargo clippy` with no warnings
- [ ] All public APIs documented
- [ ] No debug prints or TODO comments left

---

## Task 8: Build and Validate

**Objective:** Final validation that all changes compile and work

**Steps:**
1. Run `cargo build --release`
2. Run full test suite: `cargo test`
3. Run integration tests with Lakekeeper
4. Manual smoke test of full pipeline
5. Verify no regressions in existing functionality

**Acceptance Criteria:**
- [ ] Release build succeeds
- [ ] All tests pass
- [ ] No new warnings
- [ ] Existing Parquet output still works
- [ ] Iceberg output works with local Lakekeeper
