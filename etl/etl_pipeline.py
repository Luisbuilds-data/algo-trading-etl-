#!/usr/bin/env python3
"""daily_trades_etl.py - Prefect ETL pipeline for trading + Wazuh data."""

import hashlib
import json
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import flow, task, get_run_logger

DB_DSN = "dbname=trading_db"  # peer auth as ubuntu via Unix socket
S3_BUCKET = "<your-s3-bucket>"
S3_REGION = "us-west-1"

SHARED_COLS = [
    "trade_id", "symbol", "side", "outcome", "reason",
    "entry_ts", "exit_ts", "hour_utc",
    "entry_price", "exit_price", "pnl_usd", "pnl_pct",
    "signal_score", "position_size_usd", "balance_at_entry",
]

SOURCES = [
    ("/home/ubuntu/reversion_trades_mixed.csv", "oanda"),
    ("/home/ubuntu/trades_v65_month1.csv", "kraken"),
]

WAZUH_ALERTS_FILE = "/home/ubuntu/etl/wazuh_alerts.json"


# ── Trades tasks ─────────────────────────────────────────────────────────────

@task
def extract_trades() -> pd.DataFrame:
    logger = get_run_logger()
    frames = []
    for path, source in SOURCES:
        df = pd.read_csv(path)
        df["source"] = source
        indicator_cols = [c for c in df.columns if c not in SHARED_COLS + ["source"]]
        df["indicators"] = df[indicator_cols].apply(
            lambda row: {k: v for k, v in row.items() if pd.notna(v)}, axis=1
        )
        df = df[SHARED_COLS + ["source", "indicators"]]
        frames.append(df)
        logger.info(f"Extracted {len(df)} rows from source='{source}' ({path})")
    return pd.concat(frames, ignore_index=True)


@task
def validate_trades(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    initial = len(df)

    df["pnl_usd"] = pd.to_numeric(df["pnl_usd"], errors="coerce")
    bad_pnl = df["pnl_usd"].isna()
    if bad_pnl.any():
        logger.warning(f"Dropping {bad_pnl.sum()} rows with non-numeric pnl_usd")
    df = df[~bad_pnl]

    df["entry_ts"] = pd.to_datetime(df["entry_ts"], utc=True, errors="coerce")
    df["exit_ts"] = pd.to_datetime(df["exit_ts"], utc=True, errors="coerce")
    bad_ts = df["entry_ts"].isna() | df["exit_ts"].isna() | (df["entry_ts"] >= df["exit_ts"])
    if bad_ts.any():
        logger.warning(f"Dropping {bad_ts.sum()} rows with invalid/reversed timestamps")
    df = df[~bad_ts]

    bad_id = df["trade_id"].isna() | (df["trade_id"].astype(str).str.strip() == "")
    if bad_id.any():
        logger.warning(f"Dropping {bad_id.sum()} rows with null/empty trade_id")
    df = df[~bad_id]

    dropped = initial - len(df)
    logger.info(f"Validation: {initial} → {len(df)} rows ({dropped} dropped)")
    return df.reset_index(drop=True)


@task
def load_postgres(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM raw.trades")
    before = cur.fetchone()[0]

    def _safe(val, cast=None):
        if pd.isna(val) if not isinstance(val, dict) else False:
            return None
        return cast(val) if cast else val

    rows = [
        (
            str(row["trade_id"]),
            row["source"],
            _safe(row.get("symbol"), str),
            _safe(row.get("side"), str),
            _safe(row.get("outcome"), str),
            _safe(row.get("reason"), str),
            row["entry_ts"].isoformat(),
            row["exit_ts"].isoformat(),
            _safe(row.get("hour_utc"), int),
            _safe(row.get("entry_price"), float),
            _safe(row.get("exit_price"), float),
            float(row["pnl_usd"]),
            _safe(row.get("pnl_pct"), float),
            _safe(row.get("signal_score"), float),
            _safe(row.get("position_size_usd"), float),
            _safe(row.get("balance_at_entry"), float),
            json.dumps(row["indicators"]) if isinstance(row["indicators"], dict) else "{}",
        )
        for _, row in df.iterrows()
    ]

    execute_values(cur, """
        INSERT INTO raw.trades (
            trade_id, source, symbol, side, outcome, reason,
            entry_ts, exit_ts, hour_utc,
            entry_price, exit_price, pnl_usd, pnl_pct, signal_score,
            position_size_usd, balance_at_entry, indicators
        ) VALUES %s
        ON CONFLICT (trade_id, source) DO NOTHING
    """, rows)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM raw.trades")
    after = cur.fetchone()[0]
    inserted = after - before
    skipped = len(rows) - inserted

    for src, grp in df.groupby("source"):
        logger.info(f"  source='{src}': {len(grp)} rows attempted")
    logger.info(f"Trades inserted: {inserted} | Skipped (duplicates): {skipped}")

    cur.close()
    conn.close()
    return df


@task
def export_parquet(df: pd.DataFrame, prefix: str = "trades") -> str:
    logger = get_run_logger()
    export_dir = Path("/home/ubuntu/etl/exports")
    export_dir.mkdir(parents=True, exist_ok=True)
    path = export_dir / f"{prefix}_{date.today().isoformat()}.parquet"
    df.to_parquet(path, index=False)
    size = path.stat().st_size
    logger.info(f"Parquet written: {path} ({size:,} bytes)")
    return str(path)


@task
def upload_s3(parquet_path: str, s3_prefix: str) -> str:
    logger = get_run_logger()
    s3 = boto3.client("s3", region_name=S3_REGION)
    key = f"{s3_prefix}/{Path(parquet_path).name}"
    s3.upload_file(parquet_path, S3_BUCKET, key)
    logger.info(f"Uploaded → s3://{S3_BUCKET}/{key}")
    return f"s3://{S3_BUCKET}/{key}"


# ── Wazuh tasks ───────────────────────────────────────────────────────────────

@task
def extract_wazuh_alerts() -> pd.DataFrame:
    logger = get_run_logger()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    records = []
    parse_errors = 0

    try:
        with open(WAZUH_ALERTS_FILE, "r", errors="replace") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    parse_errors += 1
                    if parse_errors <= 5:
                        logger.warning(f"JSON parse error line {lineno}: {e}")
                    continue

                ts_raw = obj.get("timestamp", "")
                try:
                    ts = pd.to_datetime(ts_raw, utc=True)
                except Exception:
                    parse_errors += 1
                    continue

                if ts < pd.Timestamp(cutoff):
                    continue

                rule = obj.get("rule", {})
                rule_id = rule.get("id")
                rule_level = rule.get("level")
                rule_desc = rule.get("description", "")
                agent_name = obj.get("agent", {}).get("name", "")
                src_ip = obj.get("data", {}).get("srcip") or obj.get("data", {}).get("src_ip")
                full_log = obj.get("full_log", "")

                alert_id = hashlib.md5(
                    f"{ts_raw}{rule_id}{full_log}".encode()
                ).hexdigest()

                records.append({
                    "alert_id": alert_id,
                    "timestamp": ts,
                    "rule_id": int(rule_id) if rule_id is not None else None,
                    "rule_level": int(rule_level) if rule_level is not None else None,
                    "rule_description": rule_desc,
                    "agent_name": agent_name,
                    "src_ip": src_ip,
                    "full_log": full_log,
                    "raw": json.dumps(obj),
                })

    except PermissionError:
        logger.error(f"Permission denied reading {WAZUH_ALERTS_FILE} — add ubuntu to wazuh group")
        return pd.DataFrame(columns=["alert_id","timestamp","rule_id","rule_level",
                                     "rule_description","agent_name","src_ip","full_log","raw"])
    except FileNotFoundError:
        logger.warning(f"{WAZUH_ALERTS_FILE} not found — skipping Wazuh extract")
        return pd.DataFrame(columns=["alert_id","timestamp","rule_id","rule_level",
                                     "rule_description","agent_name","src_ip","full_log","raw"])

    if parse_errors > 5:
        logger.warning(f"Total parse errors: {parse_errors}")

    df = pd.DataFrame(records)
    logger.info(f"Extracted {len(df)} Wazuh alerts from last 24h ({parse_errors} parse errors)")
    return df


@task
def load_wazuh_alerts(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    if df.empty:
        logger.info("No Wazuh alerts to load")
        return df

    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM raw.wazuh_alerts")
    before = cur.fetchone()[0]

    rows = [
        (
            row["alert_id"],
            row["timestamp"].isoformat() if pd.notna(row["timestamp"]) else None,
            row.get("rule_id"),
            row.get("rule_level"),
            row.get("rule_description"),
            row.get("agent_name"),
            row.get("src_ip"),
            row.get("full_log"),
            row["raw"],
        )
        for _, row in df.iterrows()
    ]

    execute_values(cur, """
        INSERT INTO raw.wazuh_alerts (
            alert_id, timestamp, rule_id, rule_level, rule_description,
            agent_name, src_ip, full_log, raw
        ) VALUES %s
        ON CONFLICT (alert_id) DO NOTHING
    """, rows)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM raw.wazuh_alerts")
    after = cur.fetchone()[0]
    inserted = after - before
    skipped = len(rows) - inserted

    logger.info(f"Wazuh inserted: {inserted} | Skipped (duplicates): {skipped}")

    cur.close()
    conn.close()
    return df


# ── Flow ──────────────────────────────────────────────────────────────────────


@task(name="fetch_benchmark_prices")
def fetch_benchmark_prices():
    import yfinance as yf
    from datetime import date, timedelta
    start = date(2026, 4, 1)
    end   = date.today() + timedelta(days=1)
    inserted = 0
    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            for symbol in ["BTC-USD", "ETH-USD"]:
                data = yf.download(symbol, start=str(start), end=str(end),
                                   auto_adjust=True, progress=False)
                if data.empty:
                    continue
                for idx, row in data.iterrows():
                    close = float(row["Close"].iloc[0]) if hasattr(row["Close"], "iloc") else float(row["Close"])
                    cur.execute(
                        "INSERT INTO raw.benchmark_prices (date, symbol, close_price) "
                        "VALUES (%s,%s,%s) ON CONFLICT (date, symbol) DO NOTHING",
                        (idx.date(), symbol, close)
                    )
                    inserted += cur.rowcount
        conn.commit()
    logger.info(f"Benchmark prices upserted: {inserted} rows")
    return inserted

@flow(name="daily_trades_etl")
def daily_trades_etl():
    # Trades
    raw = extract_trades()
    clean = validate_trades(raw)
    load_postgres(clean)
    trades_parquet = export_parquet(clean, prefix="trades")
    upload_s3(trades_parquet, s3_prefix="raw/trades")

    # Wazuh
    alerts = extract_wazuh_alerts()
    load_wazuh_alerts(alerts)
    if not alerts.empty:
        wazuh_parquet = export_parquet(alerts, prefix="wazuh")
        upload_s3(wazuh_parquet, s3_prefix="raw/wazuh")

    # Benchmark prices
    fetch_benchmark_prices()


if __name__ == "__main__":
    daily_trades_etl()
