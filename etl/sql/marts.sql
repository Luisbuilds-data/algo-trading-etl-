-- ============================================================
-- marts.sql  —  analytics views on top of raw.trades + raw.wazuh_alerts
-- ============================================================

CREATE SCHEMA IF NOT EXISTS marts;

-- ── 1. trading_summary ────────────────────────────────────────
CREATE OR REPLACE VIEW marts.trading_summary AS
SELECT
    *,
    SUM(pnl_usd) OVER (
        ORDER BY entry_ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                         AS cumulative_pnl,
    EXTRACT(EPOCH FROM (exit_ts - entry_ts)) / 60.0          AS trade_duration_minutes,
    (pnl_usd > 0)                                            AS is_win
FROM raw.trades;

-- ── 2. daily_pnl ─────────────────────────────────────────────
CREATE OR REPLACE VIEW marts.daily_pnl AS
SELECT
    (entry_ts AT TIME ZONE 'UTC')::date       AS date,
    source,
    SUM(pnl_usd)                              AS total_pnl_usd,
    COUNT(*)                                  AS trade_count,
    COUNT(*) FILTER (WHERE pnl_usd > 0)       AS win_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE pnl_usd > 0)
              / NULLIF(COUNT(*), 0),
        2
    )                                         AS win_rate_pct,
    MIN(pnl_usd)                              AS max_single_loss,
    MAX(pnl_usd)                              AS max_single_win
FROM raw.trades
GROUP BY 1, 2;

-- ── 3. kpi_summary ────────────────────────────────────────────
CREATE OR REPLACE VIEW marts.kpi_summary AS
WITH
daily AS (
    SELECT (entry_ts AT TIME ZONE 'UTC')::date AS d,
           SUM(pnl_usd)                         AS dpnl
    FROM raw.trades
    GROUP BY 1
),
cum AS (
    SELECT entry_ts,
           SUM(pnl_usd) OVER (ORDER BY entry_ts
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cpnl
    FROM raw.trades
),
drawdown_raw AS (
    SELECT cpnl,
           MAX(cpnl) OVER (ORDER BY entry_ts
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS peak
    FROM cum
),
best_sym AS (
    SELECT symbol
    FROM raw.trades
    GROUP BY symbol
    ORDER BY SUM(pnl_usd) DESC
    LIMIT 1
),
base AS (
    SELECT COUNT(*)                                                 AS total_trades,
           ROUND(100.0 * COUNT(*) FILTER (WHERE pnl_usd > 0)
                       / NULLIF(COUNT(*), 0), 2)                   AS overall_win_rate_pct,
           SUM(pnl_usd)                                            AS total_pnl_usd,
           ROUND(AVG(EXTRACT(EPOCH FROM (exit_ts - entry_ts)) / 60.0)::NUMERIC, 2)
                                                                   AS avg_trade_duration_minutes
    FROM raw.trades
),
sharpe AS (
    SELECT ROUND(
        (AVG(dpnl) / NULLIF(STDDEV(dpnl), 0) * SQRT(252))::NUMERIC, 4
    ) AS sharpe_ratio
    FROM daily
),
mdd AS (
    SELECT ROUND(
        MAX(CASE WHEN peak > 0 THEN (peak - cpnl) / peak * 100.0 ELSE 0 END)::NUMERIC, 4
    ) AS max_drawdown_pct
    FROM drawdown_raw
)
SELECT
    b.total_trades,
    b.overall_win_rate_pct,
    b.total_pnl_usd,
    s.sharpe_ratio,
    m.max_drawdown_pct,
    (SELECT symbol FROM best_sym) AS best_symbol,
    b.avg_trade_duration_minutes
FROM base b
CROSS JOIN sharpe s
CROSS JOIN mdd m;

-- ── 4. hourly_pnl_heatmap ─────────────────────────────────────
CREATE OR REPLACE VIEW marts.hourly_pnl_heatmap AS
SELECT
    hour_utc,
    EXTRACT(DOW FROM entry_ts AT TIME ZONE 'UTC')::int   AS day_of_week,
    TO_CHAR(entry_ts AT TIME ZONE 'UTC', 'Dy')           AS day_name,
    ROUND(AVG(pnl_usd)::NUMERIC, 6)                      AS avg_pnl_usd,
    COUNT(*)                                              AS trade_count
FROM raw.trades
GROUP BY 1, 2, 3
ORDER BY 2, 1;

-- ── 5a. wazuh_daily ───────────────────────────────────────────
CREATE OR REPLACE VIEW marts.wazuh_daily AS
SELECT
    (timestamp AT TIME ZONE 'UTC')::date   AS date,
    rule_level,
    COUNT(*)                               AS alert_count
FROM raw.wazuh_alerts
GROUP BY 1, 2
ORDER BY 1, 2;

-- ── 5b. wazuh_top_sources ─────────────────────────────────────
CREATE OR REPLACE VIEW marts.wazuh_top_sources AS
SELECT
    (timestamp AT TIME ZONE 'UTC')::date   AS date,
    src_ip,
    COUNT(*)                               AS alert_count,
    MIN(rule_level)                        AS min_level,
    MAX(rule_level)                        AS max_level
FROM raw.wazuh_alerts
WHERE src_ip IS NOT NULL AND src_ip <> ''
GROUP BY 1, 2
ORDER BY 1, alert_count DESC;

-- ── 6. trading_vs_security ────────────────────────────────────
CREATE OR REPLACE VIEW marts.trading_vs_security AS
SELECT
    t.id                                                         AS trade_pk,
    t.trade_id,
    t.symbol,
    t.source,
    t.side,
    t.entry_ts,
    t.exit_ts,
    t.pnl_usd,
    t.signal_score,
    COUNT(w.id)                                                  AS alerts_within_30min,
    COUNT(w.id) FILTER (WHERE w.rule_level >= 10)                AS high_severity_alerts,
    STRING_AGG(DISTINCT w.rule_description, '; ' ORDER BY w.rule_description)
        FILTER (WHERE w.id IS NOT NULL)                          AS alert_descriptions
FROM raw.trades t
LEFT JOIN raw.wazuh_alerts w
    ON w.timestamp BETWEEN t.entry_ts - INTERVAL '30 minutes'
                       AND t.entry_ts + INTERVAL '30 minutes'
GROUP BY t.id, t.trade_id, t.symbol, t.source, t.side,
         t.entry_ts, t.exit_ts, t.pnl_usd, t.signal_score;

-- ── Grant SELECT on all mart views to metabase ─────────────────
GRANT USAGE ON SCHEMA marts TO metabase;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO metabase;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO metabase;
