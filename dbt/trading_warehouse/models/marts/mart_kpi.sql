{{ config(materialized='table', schema='marts') }}

WITH daily AS (
    SELECT
        (entry_ts AT TIME ZONE 'UTC')::date AS d,
        SUM(pnl_usd)                         AS dpnl
    FROM {{ ref('fct_trades') }}
    GROUP BY 1
),
cum AS (
    SELECT
        entry_ts,
        SUM(pnl_usd) OVER (ORDER BY entry_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cpnl
    FROM {{ ref('fct_trades') }}
),
drawdown_raw AS (
    SELECT
        cpnl,
        MAX(cpnl) OVER (ORDER BY entry_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS peak
    FROM cum
),
best_sym AS (
    SELECT symbol
    FROM {{ ref('fct_trades') }}
    GROUP BY symbol
    ORDER BY SUM(pnl_usd) DESC
    LIMIT 1
),
base AS (
    SELECT
        COUNT(*)                                                                                   AS total_trades,
        ROUND(100.0 * COUNT(*) FILTER (WHERE is_win)::numeric / NULLIF(COUNT(*), 0)::numeric, 2)  AS win_rate_pct,
        SUM(pnl_usd)                                                                               AS total_pnl_usd,
        ROUND(AVG(trade_duration_minutes)::numeric, 2)                                             AS avg_duration_minutes
    FROM {{ ref('fct_trades') }}
),
sharpe AS (
    SELECT ROUND(
        (AVG(dpnl) / NULLIF(STDDEV(dpnl), 0) * SQRT(252))::numeric, 4
    ) AS sharpe_ratio
    FROM daily
),
mdd AS (
    SELECT ROUND(MAX(
        CASE WHEN peak > 0 THEN (peak - cpnl) / peak * 100.0 ELSE 0 END
    ), 4) AS max_drawdown_pct
    FROM drawdown_raw
)

SELECT
    b.total_trades,
    b.win_rate_pct,
    b.total_pnl_usd,
    s.sharpe_ratio,
    m.max_drawdown_pct,
    (SELECT symbol FROM best_sym) AS best_symbol,
    b.avg_duration_minutes
FROM base b
CROSS JOIN sharpe s
CROSS JOIN mdd m
