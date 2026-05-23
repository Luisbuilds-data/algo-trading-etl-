{{ config(materialized='table', schema='marts') }}

SELECT
    t.trade_id,
    t.symbol,
    t.source,
    t.side,
    t.entry_ts,
    t.exit_ts,
    t.pnl_usd,
    t.signal_score,
    t.is_win,
    COUNT(a.alert_id)                                                                   AS alerts_within_30min,
    COUNT(a.alert_id) FILTER (WHERE a.rule_level >= 10)                                 AS high_severity_alerts,
    STRING_AGG(DISTINCT a.rule_description, '; ' ORDER BY a.rule_description)
        FILTER (WHERE a.alert_id IS NOT NULL)                                           AS alert_descriptions
FROM {{ ref('fct_trades') }} t
LEFT JOIN {{ ref('fct_alerts') }} a
    ON  a.alert_ts >= t.entry_ts - INTERVAL '30 minutes'
    AND a.alert_ts <= t.entry_ts + INTERVAL '30 minutes'
GROUP BY
    t.trade_id, t.symbol, t.source, t.side,
    t.entry_ts, t.exit_ts, t.pnl_usd, t.signal_score, t.is_win
