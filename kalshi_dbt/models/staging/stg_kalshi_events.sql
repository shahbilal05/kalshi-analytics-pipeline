WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_events') }}
),

typed AS (
    SELECT
        raw_json:event_ticker::VARCHAR AS event_ticker,
        raw_json:series_ticker::VARCHAR AS series_ticker,
        raw_json:category::VARCHAR AS category,
        raw_json:title::VARCHAR AS title,
        raw_json:sub_title::VARCHAR AS sub_title,
        raw_json:mutually_exclusive::BOOLEAN AS mutually_exclusive,
        raw_json:available_on_brokers::BOOLEAN AS available_on_brokers,
        raw_json:strike_period::VARCHAR AS strike_period,
        raw_json:collateral_return_type::VARCHAR AS collateral_return_type,
        
        --include metadata
        ingested_at,
        batch_id
    FROM source
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_ticker
            ORDER BY ingested_at DESC 
        ) AS row_num
    FROM typed
)
-- new unique data to be transformed by dbt
SELECT * EXCLUDE(row_num)
FROM deduplicated
WHERE row_num = 1