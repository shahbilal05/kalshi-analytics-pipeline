CREATE TABLE IF NOT EXISTS ingestion_metadata (
    kalshi_pipeline VARCHAR PRIMARY KEY,
    last_processed_ts TIMESTAMP_NTZ NOT NULL,
    last_successful_run TIMESTAMP_NTZ,
    records_processed NUMBER(40,0),
    status VARCHAR(50) 
);