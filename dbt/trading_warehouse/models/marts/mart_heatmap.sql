{{ config(materialized='table', schema='marts') }}

SELECT
    hour_utc,
    day_of_week,
    day_name,
    ROUND(AVG(pnl_usd)::numeric, 6) AS avg_pnl_usd,
    COUNT(*)                         AS trade_count
FROM {{ ref('fct_trades') }}
GROUP BY hour_utc, day_of_week, day_name
ORDER BY day_of_week, hour_utc
