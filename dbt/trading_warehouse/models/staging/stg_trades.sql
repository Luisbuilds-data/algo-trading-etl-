{{ config(materialized='view', schema='staging') }}

SELECT
    id,
    trade_id,
    source,
    symbol,
    side,
    outcome,
    reason,
    entry_ts AT TIME ZONE 'UTC'         AS entry_ts,
    exit_ts  AT TIME ZONE 'UTC'         AS exit_ts,
    hour_utc,
    entry_price::numeric                AS entry_price,
    exit_price::numeric                 AS exit_price,
    pnl_usd::numeric                    AS pnl_usd,
    pnl_pct::numeric                    AS pnl_pct,
    signal_score::numeric               AS signal_score,
    position_size_usd::numeric          AS position_size_usd,
    balance_at_entry::numeric           AS balance_at_entry,
    indicators                          AS bot_indicators,
    loaded_at,
    EXTRACT(EPOCH FROM (exit_ts - entry_ts)) / 60.0  AS trade_duration_minutes,
    pnl_usd > 0                         AS is_win
FROM {{ source('raw', 'trades') }}
