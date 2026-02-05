# Technical Debt & Roadmap Backlog

| Priority | Category | Description | Status |
| :--- | :--- | :--- | :--- |
| **Critical** | **Scalability** | **OOM (Out-of-Memory) Management:** Implement Polars Streaming mode to process 5GB datasets in environments with 2GB RAM. Shift from `read_csv/parquet` to `scan_csv/parquet` + `collect(streaming=True)`. | **New** |
| **Critical** | **Data Quality** | **Quarantine Zone (Sink):** Implement physical persistence for invalid records. Rejected rows from Pandera must be saved to a `gs://.../quarantine/` path with error metadata for auditing. | **New** |
| **High** | **Cloud Refactor** | **Infrastructure Agnosticism:** Refactor `source_validator.py` and `transforms.py` to use `fsspec` or the new `IOManager` instead of local libraries (`os.path`, `pathlib`). | Pending |
| **High** | **Resilience** | **Pre-flight Checks:** Implement GCS object existence validation before Job execution to prevent empty-path failures. | Pending |
| **Medium** | **Medallion Architecture** | **Ingestion Layers:** Implement physical persistence logic for Bronze (raw landing) and Silver (cleansed/technical) layers. | Pending |
| **Medium** | **Testing** | **Edge Case Coverage:** Expand test suite to handle empty files, malformed schemas, and extreme null values (partially addressed with Pandera). | In Progress |
| **Low** | **Observability** | **Cloud Logging:** Integrate GCP Cloud Logging for custom metrics regarding data volume and processing time. | Pending |
