WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_markets') }}
),

typed AS (
    SELECT
        -- identifiers
        raw_json:ticker::VARCHAR AS market_ticker,
        raw_json:event_ticker::VARCHAR AS event_ticker,
        raw_json:series_ticker::VARCHAR AS series_ticker,
        
        -- basic market info
        raw_json:title::VARCHAR AS title,
        raw_json:subtitle::VARCHAR AS subtitle,
        raw_json:category::VARCHAR AS category,
        raw_json:market_type::VARCHAR AS market_type,
        raw_json:status::VARCHAR AS status,
        
        -- timing
        raw_json:open_time::TIMESTAMP_NTZ AS open_time,
        raw_json:close_time::TIMESTAMP_NTZ AS close_time,
        raw_json:created_time::TIMESTAMP_NTZ AS created_time,
        raw_json:expiration_time::TIMESTAMP_NTZ AS expiration_time,
        raw_json:expected_expiration_time::TIMESTAMP_NTZ AS expected_expiration_time,
        
        -- prices (yes side)
        raw_json:yes_bid::NUMBER(10,4) AS yes_bid,
        raw_json:yes_ask::NUMBER(10,4) AS yes_ask,
        raw_json:last_price::NUMBER(10,4) AS last_price,
        raw_json:previous_price::NUMBER(10,4) AS previous_price,
        
        -- prices (no side)
        raw_json:no_bid::NUMBER(10,4) AS no_bid,
        raw_json:no_ask::NUMBER(10,4) AS no_ask,
        
        -- market metrics
        raw_json:volume::NUMBER(20,0) AS volume,
        raw_json:volume_24h::NUMBER(20,0) AS volume_24h,
        raw_json:open_interest::NUMBER(20,0) AS open_interest,
        raw_json:liquidity::NUMBER(20,4) AS liquidity,
        
        -- market settlement
        raw_json:result::VARCHAR AS result,
        raw_json:settlement_value::NUMBER(10,4) AS settlement_value,
        
        -- include metadata
        ingested_at,
        batch_id
    FROM source
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY market_ticker
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM typed
)

-- new unique data to be transformed by dbt
SELECT * EXCLUDE(row_num)
FROM deduplicated
WHERE row_num = 1