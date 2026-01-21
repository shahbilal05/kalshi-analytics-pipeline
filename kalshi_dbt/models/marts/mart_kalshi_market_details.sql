WITH markets AS (
    SELECT * FROM {{ ref('stg_kalshi_markets') }}
),

events AS (
    SELECT * FROM {{ ref('stg_kalshi_events') }}
),

markets_enriched AS (
    SELECT
        markets.market_ticker,
        markets.event_ticker,
        markets.title,
        markets.status,
        markets.result,
        markets.close_time,
        markets.volume,
        
        COALESCE(
            events.category,
            CASE
                WHEN markets.event_ticker LIKE '%NBA%' OR markets.event_ticker LIKE 'KXMVENBASINGLEGAME%' THEN 'Sports'
                WHEN markets.event_ticker LIKE '%NFL%' OR markets.event_ticker LIKE '%NCAAFB%' THEN 'Sports'
                WHEN markets.event_ticker LIKE '%MLB%' OR markets.event_ticker LIKE '%NHL%' THEN 'Sports'
                WHEN markets.event_ticker LIKE '%SOCCER%' OR markets.event_ticker LIKE '%TENNIS%' THEN 'Sports'
    
                WHEN markets.event_ticker LIKE '%BTC%' OR markets.event_ticker LIKE '%ETH%' OR markets.event_ticker LIKE '%SOL%' THEN 'Crypto'
    
                WHEN markets.event_ticker LIKE '%WTI%' OR markets.event_ticker LIKE '%GOLD%' OR markets.event_ticker LIKE '%SP500%' THEN 'Financials'
                WHEN markets.event_ticker LIKE '%USDJPY%' OR markets.event_ticker LIKE '%EURUSD%' THEN 'Financials'
    
                WHEN markets.event_ticker LIKE '%CPI%' OR markets.event_ticker LIKE '%GDP%' OR markets.event_ticker LIKE '%UNEMPLOYMENT%' THEN 'Economics'

                WHEN markets.event_ticker LIKE '%ELECTION%' OR markets.event_ticker LIKE '%PRES%' OR markets.event_ticker LIKE '%SENATE%' THEN 'Politics'
    
                WHEN markets.event_ticker LIKE '%SPOTIFY%' OR markets.event_ticker LIKE '%OSCAR%' OR markets.event_ticker LIKE '%EMMY%' THEN 'Entertainment'
                WHEN markets.event_ticker LIKE '%SAG%' OR markets.event_ticker LIKE '%GRAMMY%' THEN 'Entertainment'
    
                ELSE 'Other'
            END
        ) AS category,
        
        (markets.yes_bid + markets.yes_ask) / 200.0 AS predicted_prob,
        
        CASE
            WHEN markets.result = 'yes' THEN 1.0
            WHEN markets.result = 'no' THEN 0.0
            ELSE NULL
        END AS actual_outcome
        
    FROM markets
    LEFT JOIN events ON markets.event_ticker = events.event_ticker
    WHERE markets.status IN ('determined', 'settled', 'finalized')
      AND markets.result IS NOT NULL
),

with_buckets AS (
    SELECT
        market_ticker,
        event_ticker,
        title,
        category,
        predicted_prob,
        actual_outcome,
        POWER(predicted_prob - actual_outcome, 2) AS brier_score,
        close_time,
        volume,
        
        CASE
            WHEN predicted_prob < 0.1 THEN '00-10%'
            WHEN predicted_prob < 0.2 THEN '10-20%'
            WHEN predicted_prob < 0.3 THEN '20-30%'
            WHEN predicted_prob < 0.4 THEN '30-40%'
            WHEN predicted_prob < 0.5 THEN '40-50%'
            WHEN predicted_prob < 0.6 THEN '50-60%'
            WHEN predicted_prob < 0.7 THEN '60-70%'
            WHEN predicted_prob < 0.8 THEN '70-80%'
            WHEN predicted_prob < 0.9 THEN '80-90%'
            ELSE '90-100%'
        END AS probability_bucket
        
    FROM markets_enriched
    WHERE predicted_prob IS NOT NULL
      AND actual_outcome IS NOT NULL
      AND category IS NOT NULL
),

final AS (
    SELECT
        market_ticker,
        event_ticker,
        title,
        category,
        probability_bucket,
        predicted_prob,
        actual_outcome,
        brier_score,
        close_time,
        volume,
        DATE(close_time) AS close_date,
        EXTRACT(YEAR FROM close_time) AS close_year,
        EXTRACT(MONTH FROM close_time) AS close_month,
        EXTRACT(DAY FROM close_time) AS close_day
    FROM with_buckets
)

SELECT * FROM final