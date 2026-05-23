{{ config(materialized='table', schema='marts') }}

SELECT
    (entry_ts AT TIME ZONE 'UTC')::date                                                     AS date,
    source,
    SUM(pnl_usd)                                                                            AS total_pnl_usd,
    COUNT(*)                                                                                AS trade_count,
    COUNT(*) FILTER (WHERE is_win)                                                          AS win_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_win)::numeric / NULLIF(COUNT(*), 0)::numeric, 2
    )                                                                                       AS win_rate_pct,
    MIN(pnl_usd)                                                                            AS max_single_loss,
    MAX(pnl_usd)                                                                            AS max_single_win
FROM {{ ref('fct_trades') }}
GROUP BY 1, 2
ORDER BY 1, 2
