{{ config(materialized='table', schema='marts') }}

SELECT
    t.id,
    t.trade_id,
    t.source,
    t.symbol,
    t.side,
    t.outcome,
    t.reason,
    t.entry_ts,
    t.exit_ts,
    t.hour_utc,
    t.entry_price,
    t.exit_price,
    t.pnl_usd,
    t.pnl_pct,
    t.signal_score,
    t.position_size_usd,
    t.balance_at_entry,
    t.bot_indicators,
    t.loaded_at,
    t.trade_duration_minutes,
    t.is_win,
    d.day_of_week,
    d.day_name,
    d.month,
    d.quarter,
    d.is_weekend,
    s.asset_class,
    src.display_name AS source_display_name
FROM {{ ref('stg_trades') }} t
LEFT JOIN {{ ref('dim_date') }}   d   ON d.date_day  = (t.entry_ts AT TIME ZONE 'UTC')::date
LEFT JOIN {{ ref('dim_symbol') }} s   ON s.symbol    = t.symbol
LEFT JOIN {{ ref('dim_source') }} src ON src.source  = t.source
