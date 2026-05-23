# Database Schema

## `raw.trades`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | `bigint` | NO | Serial primary key |
| `trade_id` | `text` | NO | Unique trade ID from source broker |
| `source` | `text` | NO | Broker source: oanda or kraken |
| `symbol` | `text` | YES | Trading instrument (e.g. EUR_USD, BTC/USD) |
| `side` | `text` | YES | Trade direction: buy or sell |
| `outcome` | `text` | YES |  |
| `reason` | `text` | YES |  |
| `entry_ts` | `timestamp with time zone` | YES | Entry timestamp (UTC) |
| `exit_ts` | `timestamp with time zone` | YES | Exit timestamp (UTC) |
| `hour_utc` | `integer` | YES |  |
| `entry_price` | `numeric` | YES | Entry price |
| `exit_price` | `numeric` | YES | Exit price |
| `pnl_usd` | `numeric` | YES | Realized profit/loss in USD |
| `pnl_pct` | `numeric` | YES |  |
| `signal_score` | `numeric` | YES | Bot signal confidence score |
| `position_size_usd` | `numeric` | YES | Notional position size in USD |
| `balance_at_entry` | `numeric` | YES | Account balance at trade entry |
| `indicators` | `jsonb` | YES | JSONB blob of source-specific technical indicators |
| `loaded_at` | `timestamp with time zone` | YES | Timestamp when row was inserted by ETL |

## `raw.wazuh_alerts`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | `bigint` | NO | Serial primary key |
| `alert_id` | `text` | NO | MD5 hash of (timestamp + rule_id + full_log) — dedup key |
| `timestamp` | `timestamp with time zone` | YES | Alert timestamp (UTC) |
| `rule_id` | `integer` | YES | Wazuh rule ID |
| `rule_level` | `integer` | YES | Severity level (1-15; ≥10 = critical) |
| `rule_description` | `text` | YES | Human-readable rule description |
| `agent_name` | `text` | YES | Wazuh agent hostname |
| `src_ip` | `text` | YES | Source IP of suspicious activity (nullable) |
| `full_log` | `text` | YES | Raw log line that triggered the alert |
| `raw` | `jsonb` | YES |  |
| `loaded_at` | `timestamp with time zone` | YES | Timestamp when row was inserted by ETL |

