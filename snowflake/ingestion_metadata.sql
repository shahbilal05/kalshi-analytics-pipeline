CREATE TABLE ingestion_metadata (
    pipeline_name STRING PRIMARY KEY,
    last_processed_ts TIMESTAMP
);