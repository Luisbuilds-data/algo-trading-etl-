# trading_warehouse — dbt Dimensional Model

A star-schema data warehouse built on `trading_db` using dbt-postgres. Replaces eight
hand-written SQL views with a tested, documented three-layer pipeline.

## Architecture

```
raw.*  (source tables — never touched by dbt)
  │
  ▼
staging/        views — 1:1 with source, light casting & renaming only
  │
  ▼
dimensions/     tables — conformed dims (date, symbol, source, Wazuh rule)
facts/          tables — grain-level facts joined to dims (fct_trades, fct_alerts)
  │
  ▼
marts/          tables — business aggregates consumed by dashboards & reports
```

### Staging layer (`schema: staging`, `materialized: view`)

| Model | Source | Key transforms |
|---|---|---|
| `stg_trades` | `raw.trades` | Explicit numeric casts, `indicators→bot_indicators`, adds `trade_duration_minutes`, `is_win` |
| `stg_wazuh_alerts` | `raw.wazuh_alerts` | `timestamp→alert_ts`, `COALESCE(src_ip, 'unknown')` |
| `stg_benchmark_prices` | `raw.benchmark_prices` | Explicit casts, `date→price_date` |

### Dimension layer (`schema: marts`, `materialized: table`)

| Model | Grain | Notes |
|---|---|---|
| `dim_date` | One row per calendar day | 2026-01-01 → 2026-12-31; day_of_week (0=Mon), quarter, is_weekend |
| `dim_symbol` | One row per trading symbol | `asset_class`: forex (contains `/`) or crypto |
| `dim_source` | One row per data source | `display_name`: OANDA Forex, Kraken Crypto |
| `dim_rule` | One row per Wazuh rule_id | `severity_label`: low (1-5), medium (6-9), high (10+) |

### Fact layer (`schema: marts`, `materialized: table`)

| Model | Grain | Joins |
|---|---|---|
| `fct_trades` | One row per trade | dim_date (entry date), dim_symbol, dim_source |
| `fct_alerts` | One row per Wazuh alert | dim_date (alert date), dim_rule |

### Mart layer (`schema: marts`, `materialized: table`)

| Model | Description |
|---|---|
| `mart_daily_pnl` | Daily P&L by source: total, win rate, max win/loss |
| `mart_kpi` | Single-row KPI summary: Sharpe ratio, max drawdown, best symbol |
| `mart_heatmap` | Avg P&L by hour × day-of-week (heatmap source) |
| `mart_security` | Alert count + top source IP by date and severity |
| `mart_cross_domain` | Trades with Wazuh alert count within ±30 min window |

## Setup

### 1. Install dbt-postgres

```bash
source /path/to/venv/bin/activate
pip install dbt-postgres
```

### 2. Configure connection

Copy `profiles.yml.example` to `~/.dbt/profiles.yml` and fill in your values:

```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
```

### 3. Verify connection

```bash
dbt debug --profiles-dir ~/.dbt --project-dir dbt/trading_warehouse
```

### 4. Run all models

```bash
dbt run --profiles-dir ~/.dbt --project-dir dbt/trading_warehouse
```

### 5. Run tests

```bash
dbt test --profiles-dir ~/.dbt --project-dir dbt/trading_warehouse
```

## Tests

| Model | Column | Test |
|---|---|---|
| `fct_trades` | `id` | unique, not_null |
| `fct_trades` | `trade_id` | not_null |
| `fct_trades` | `side` | accepted_values: BUY, SELL |
| `fct_trades` | `pnl_usd` | not_null |
| `fct_alerts` | `alert_id` | unique, not_null |
| `dim_date` | `date_day` | unique, not_null |

## dbt version

Developed against `dbt-core==1.12.0-b1` and `dbt-postgres==1.10.0`.
