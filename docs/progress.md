# Project Progress

## Project 1 — ETL Pipeline ✅
Real-time ETL pipeline ingesting OANDA forex trades, Wazuh security alerts, and benchmark
prices into a PostgreSQL data warehouse. Runs on a scheduled basis via systemd, with S3
staging and structured logging.

**Completed:** May 2026

---

## Project 2 — dbt Dimensional Model ✅
Star-schema rebuild of the `marts` layer using dbt-postgres. Replaces eight plain-SQL views
with a tested, documented three-layer pipeline (staging → dimensions/facts → marts).

**Models:** 14 (3 staging views, 4 dimension tables, 2 fact tables, 5 mart tables)  
**Tests:** 9 passing (unique, not_null, accepted_values)  
**Schemas:** `staging.*`, `marts.*`

**Completed:** May 2026

---

## Project 3 — Upcoming
_TBD_
