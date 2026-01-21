WITH markets AS (
    SELECT * FROM {{ ref('stg_kalshi_markets') }}
),

events AS (
    SELECT * FROM {{ ref('stg_kalshi_events') }}
),

markets_with_category AS (
    SELECT
        markets.market_ticker,
        markets.event_ticker,
        markets.status,
        markets.result,
        markets.yes_bid,
        markets.yes_ask,
        markets.close_time,
        markets.volume,
        
        -- get category from events table, or infer from event_ticker pattern
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
        
        markets.last_price / 100.0 AS predicted_prob,
        
        -- convert result to numeric
        CASE
            WHEN markets.result = 'yes' THEN 1
            WHEN markets.result = 'no' THEN 0
            ELSE NULL
        END AS actual_outcome
        
    FROM markets
    LEFT JOIN events ON markets.event_ticker = events.event_ticker
    WHERE markets.status IN ('determined', 'settled', 'finalized')
),

with_brier_scores AS (
    SELECT
        category,
        predicted_prob,
        actual_outcome,
        close_time,
        volume,
        POWER(predicted_prob - actual_outcome, 2) AS brier_score
    FROM markets_with_category
    WHERE actual_outcome IS NOT NULL
      AND predicted_prob IS NOT NULL
      AND category IS NOT NULL
),

final AS (
    SELECT
        category,
        COUNT(*) AS total_markets,
        ROUND(AVG(predicted_prob), 4) AS avg_predicted_prob,
        ROUND(AVG(actual_outcome), 4) AS avg_actual_outcome,
        ROUND(AVG(brier_score), 4) AS avg_brier_score,
        ROUND(STDDEV(brier_score), 4) AS stddev_brier_score,
        MIN(close_time) AS earliest_market,
        MAX(close_time) AS latest_market,
        SUM(volume) AS total_volume
    FROM with_brier_scores
    GROUP BY category
)

SELECT * FROM final
ORDER BY total_markets DESC