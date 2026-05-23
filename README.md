# CLE Portfolio — Trading Bot & Security Analytics Stack

Live OANDA + Kraken algorithmic trading bot data pipeline joined with
Wazuh IDS security alerts, visualised in Metabase.

## Stack
| Layer | Technology |
|-------|-----------|
| Ingestion | Python · Prefect · yfinance |
| Storage | PostgreSQL 16 (trading_db) · AWS S3 |
| Transform | SQL mart views (marts schema) |
| Visualisation | Metabase v0.61 |
| Infrastructure | AWS EC2 · Ubuntu 24.04 · systemd |
| Security monitoring | Wazuh IDS |

## Repository layout
```
etl/
  etl_pipeline.py     Prefect flow — trades + Wazuh alerts + benchmark prices
  sql/marts.sql       Postgres mart view definitions
systemd/
  etl-pipeline.*      Daily ETL timer (Hub EC2)
  wazuh-rsync.*       15-min rsync timer (Wazuh EC2 → Hub EC2)
docs/
  architecture.md     System architecture overview
```

## Quick start
```bash
# Run ETL manually
cd /home/ubuntu/etl
source venv/bin/activate
python etl_pipeline.py

# Apply mart views
sudo -u postgres psql -d trading_db -f etl/sql/marts.sql
```

## Data sources
- **OANDA** — mean-reversion FX/crypto trades (CSV export)
- **Kraken** — spot crypto trades (CSV export)
- **Wazuh** — IDS alerts from Ubuntu host agent
- **yfinance** — BTC-USD / ETH-USD benchmark prices
