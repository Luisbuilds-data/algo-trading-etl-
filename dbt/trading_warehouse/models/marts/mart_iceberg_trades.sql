{{ config(materialized='table', schema='marts') }}

SELECT
    symbol,
    side,
    outcome,
    COUNT(*)                            AS trade_count,
    SUM(pnl_usd)::numeric(12,4)        AS total_pnl_usd,
    AVG(pnl_usd)::numeric(12,4)        AS avg_pnl_usd,
    MIN(entry_ts)                       AS first_trade_ts,
    MAX(entry_ts)                       AS last_trade_ts
FROM {{ ref('stg_trades') }}
GROUP BY symbol, side, outcome
ORDER BY total_pnl_usd DESC
