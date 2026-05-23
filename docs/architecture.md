# Architecture Overview

## Infrastructure

```
┌─────────────────────────────────────────────┐
│  WireGuard VPN  (<wireguard-ip>/24)             │
│                                             │
│  Hub EC2 (<wireguard-ip> / <ec2-public-ip>)     │
│  ├── PostgreSQL 16  :5432                   │
│  │   └── trading_db                        │
│  │       ├── raw.trades                    │
│  │       ├── raw.wazuh_alerts              │
│  │       ├── raw.benchmark_prices          │
│  │       └── marts.*  (7 views)            │
│  ├── Metabase (Docker) :3000               │
│  ├── Web Application (PM2 + Nginx)       │
│  └── Prefect ETL  (systemd daily timer)    │
│                                             │
│  Wazuh EC2 (<wireguard-ip>)                    │
│  ├── Wazuh Manager + Dashboard             │
│  └── rsync → Hub EC2 every 15 min          │
└─────────────────────────────────────────────┘
                  │
            AWS S3 (us-west-1)
            ├── <your-s3-bucket>/raw/trades/
            └── <your-s3-bucket>/raw/wazuh/
```

## ETL Flow (daily at 06:00 UTC)

1. `extract_trades` — reads OANDA + Kraken CSVs
2. `validate_trades` — schema check, drop bad rows
3. `load_postgres` — upsert into raw.trades (conflict on trade_id)
4. `export_parquet` — write daily parquet snapshot
5. `upload_s3` — push parquet to S3
6. `extract_wazuh_alerts` — parse /home/ubuntu/etl/wazuh_alerts.json
7. `load_wazuh_alerts` — upsert into raw.wazuh_alerts (conflict on alert_id)
8. `fetch_benchmark_prices` — yfinance BTC/ETH daily OHLCV upsert

## Mart Views

| View | Description |
|------|-------------|
| `marts.trading_summary` | All trades + cumulative PnL window function |
| `marts.daily_pnl` | Daily PnL, win rate, trade count by source |
| `marts.kpi_summary` | Global KPIs: Sharpe, max drawdown, win rate |
| `marts.hourly_pnl_heatmap` | Avg PnL by hour × day of week |
| `marts.wazuh_daily` | Alert counts by date and rule level |
| `marts.wazuh_top_sources` | Top attack source IPs (non-null) |
| `marts.trading_vs_security` | Trades joined with ±30-min Wazuh alerts |
| `marts.equity_vs_benchmark` | Bot cumulative PnL vs BTC/ETH % returns |
