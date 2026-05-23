# Project Progress

## Key References

### Infrastructure
| Resource | Value |
|----------|-------|
| Hub EC2 instance | `i-0daa27a77ea6c24b0` (us-west-1) |
| Hub public IP | `54.193.109.101` |
| Hub WireGuard IP | `10.66.66.1` |
| Wazuh EC2 instance | `i-0ac7c3966a1530557` |
| Wazuh WireGuard IP | `10.66.66.3` |
| Access method | AWS SSM (no public SSH) |

### PostgreSQL
| Resource | Value |
|----------|-------|
| Host | `localhost:5432` |
| PgBouncer | `localhost:6432` |
| Database | `trading_db` |
| Peer auth user | `ubuntu` (ETL writes) |
| Read-only user | `metabase` / `metabase_secure_2026` |
| Schema | `raw` (tables) · `marts` (views) |

### Services & Ports
| Service | Location | Port |
|---------|----------|------|
| Metabase | Hub EC2, Docker host-network | `10.66.66.1:3000` |
| PromptTemplate.ca | Hub EC2, PM2 + Nginx | `443` (Cloudflare) |
| Wazuh dashboard | Wazuh EC2 | `https://10.66.66.3:443` |
| PostgreSQL | Hub EC2 | `5432` |

### Paths
| Resource | Path |
|----------|------|
| ETL pipeline | `/home/ubuntu/etl/etl_pipeline.py` |
| ETL venv | `/home/ubuntu/etl/venv/` |
| Wazuh alerts (synced) | `/home/ubuntu/etl/wazuh_alerts.json` |
| Trade CSVs | `/home/ubuntu/reversion_trades_mixed.csv` · `/home/ubuntu/trades_v65_month1.csv` |
| Mart SQL | `/home/ubuntu/etl/sql/marts.sql` |
| Portfolio repo | `/home/ubuntu/cle-portfolio/` |
| PromptTemplate app | `/home/ubuntu/prompttemplate/` |

### S3
| Bucket | Region | Contents |
|--------|--------|----------|
| `cle-portfolio-etl` | us-west-1 | Daily Parquet exports: `raw/trades/` · `raw/wazuh/` |
| `prompttemplate-assets` | us-west-1 | Deploy scripts relay |

### Metabase
| Resource | Value |
|----------|-------|
| URL | `http://10.66.66.1:3000` |
| Admin | `Luis.a.sanchezz@hotmail.com` / `Metabase2026!` |
| Dashboard | id=2 — "Trading Bot Performance & Security Operations" |
| Database connection | id=2 — "Trading & Security Analytics" → `trading_db` |
| Version | `v0.61.2.5` |

---

## Accomplished

### PromptTemplate.ca (Express app)
- [x] Deployed Node.js/Express 5 app to Hub EC2 via S3 relay + SSM
- [x] PM2 process manager configured and running
- [x] Nginx reverse proxy serving HTTP on port 80
- [x] AWS security group opened for ports 80/443
- [x] SES SMTP credentials regenerated and deployed (IAM key `AKIA6ODU2EOMHUAMFYFG`)
- [x] Cloudflare SSL — site live at `https://prompttemplate.ca`

### ETL Pipeline
- [x] `trading_db` database and `raw` schema created on Hub EC2
- [x] `raw.trades` table with UNIQUE `(trade_id, source)` constraint
- [x] `raw.wazuh_alerts` table with MD5 dedup `alert_id`
- [x] `raw.benchmark_prices` table (BTC-USD, ETH-USD daily OHLCV)
- [x] Prefect flow `daily_trades_etl` with 8 tasks:
  - `extract_trades` (OANDA + Kraken CSV)
  - `validate_trades`
  - `load_postgres` (upsert)
  - `export_parquet`
  - `upload_s3`
  - `extract_wazuh_alerts`
  - `load_wazuh_alerts`
  - `fetch_benchmark_prices` (yfinance)
- [x] 154 trades loaded (138 unique after dedup) · 436 Wazuh alerts · 104 benchmark rows
- [x] systemd timer: `etl-pipeline.timer` fires daily at 06:00 UTC
- [x] Parquet exports to `s3://cle-portfolio-etl/`

### Wazuh Rsync
- [x] SSH key pair created for Hub EC2 ↔ Wazuh EC2 WireGuard auth
- [x] `wazuh-rsync.service` + `wazuh-rsync.timer` on Wazuh EC2 (rsync every 15 min)
- [x] Alerts file landing at `/home/ubuntu/etl/wazuh_alerts.json`

### Mart Views (marts schema)
- [x] `marts.trading_summary` — all trades + cumulative PnL window
- [x] `marts.daily_pnl` — daily PnL, win rate, trade count by source
- [x] `marts.kpi_summary` — Sharpe ratio, max drawdown, win rate, best symbol
- [x] `marts.hourly_pnl_heatmap` — avg PnL by hour × day of week
- [x] `marts.wazuh_daily` — alert counts by date and rule level
- [x] `marts.wazuh_top_sources` — top attack IPs (null/NaN filtered)
- [x] `marts.trading_vs_security` — trades LEFT JOIN alerts ±30 min window
- [x] `marts.equity_vs_benchmark` — bot cumulative PnL + BTC/ETH % returns
- [x] All views GRANT SELECT to `metabase`

### Metabase Dashboard (id=2)
- [x] Metabase v0.61.2.5 deployed via Docker (`--network host`, 2GB swap)
- [x] Admin account created via setup API
- [x] PostgreSQL data source connected ("Trading & Security Analytics")
- [x] 17 cards on dashboard:
  - 7 KPI scalars (with color coding: green/red/amber)
  - Equity Curve (Cumulative P&L) — line
  - Daily P&L by Source — bar (oanda=green, kraken=blue)
  - Win Rate Trend — line
  - Security During Trading — bar
  - Top Attack Sources — bar (red, null-filtered)
  - Alerts Over Time — line (level colors: 3-5=green, 6-7=amber, 8-10=red)
  - P&L by Hour × Day of Week — bar grouped by day
  - Trade Duration Distribution — bar
  - Last ETL Run — scalar
  - Markdown header card
- [x] Benchmark infrastructure (`equity_vs_benchmark` view) in Postgres — removed from dashboard pending real position sizes

### Portfolio Repo (`/home/ubuntu/cle-portfolio/`)
- [x] Git repo initialized with 2 commits
- [x] `etl/etl_pipeline.py` + `etl/sql/marts.sql`
- [x] `systemd/` — 4 unit files (2 Hub EC2, 2 Wazuh EC2 placeholders)
- [x] `requirements.txt` — 7 pinned packages
- [x] `.env.example` · `.gitignore` · `LICENSE` (MIT 2026)
- [x] `docs/architecture.md` · `docs/schema.md` · `docs/runbook.md` (FR)
- [x] `docs/screenshots/` — awaiting dashboard screenshot

---

## Remaining / Next Steps

### High Priority
- [ ] **Wazuh rsync unit files** — SSH into Wazuh EC2, copy real service/timer content into `systemd/wazuh-rsync.*` and commit
- [ ] **Dashboard screenshot** — capture `http://10.66.66.1:3000/dashboard/2` over WireGuard, save to `docs/screenshots/dashboard-overview.png`
- [ ] **GitHub remote** — `git remote add origin <repo>` and push `cle-portfolio` repo
- [ ] **HTTPS on PromptTemplate.ca** — Nginx currently HTTP only; run certbot for Let's Encrypt TLS

### Medium Priority
- [ ] **Test email end-to-end** — SES SMTP credentials deployed but send path untested
- [ ] **Equity curve benchmark** — `marts.equity_vs_benchmark` built and tested; re-add to dashboard once trading with full position sizes
- [ ] **Security During Trading** — chart shows all zeros (trade dates Apr, Wazuh dates May); will auto-populate as datasets align over time
- [ ] **Equity Curve source split** — single series renders; needs `graph.series_breakout_column: "source"` viz setting

### Low Priority / Future
- [ ] pytest suite for ETL transformations
- [ ] dbt for marts layer (Project 2)
- [ ] MaxMind GeoLite2 IP enrichment for Wazuh sources
- [ ] MITRE ATT&CK mapping for Wazuh rule IDs
- [ ] Vercel vs EC2 frontend decision for PromptTemplate.ca

---

## Data Summary

| Table | Rows | Date range |
|-------|------|------------|
| `raw.trades` | 138 | 2026-04-04 → 2026-04-16 |
| `raw.wazuh_alerts` | 436 | 2026-05-22 (first sync day) |
| `raw.benchmark_prices` | 104 | 2026-04-01 → 2026-05-23 (BTC+ETH) |
