import os
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
from prefect.artifacts import create_markdown_artifact

DB_DSN = "dbname=trading_db"  # peer auth as ubuntu via Unix socket
S3_BUCKET = os.getenv("S3_BUCKET", "cle-portfolio-etl")
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

GX_DIR = "/home/ubuntu/etl/gx"
ROW_COUNT_BASELINE_FILE = "/home/ubuntu/etl/gx/row_count_baseline.json"

TRADES_EXPECTED_COLS = {
    "trade_id", "symbol", "side", "outcome", "reason",
    "entry_ts", "exit_ts", "hour_utc", "entry_price", "exit_price",
    "pnl_usd", "pnl_pct", "signal_score", "position_size_usd",
    "balance_at_entry", "source", "indicators",
}
WAZUH_EXPECTED_COLS = {
    "alert_id", "timestamp", "rule_id", "rule_level",
    "rule_description", "agent_name", "src_ip", "full_log", "raw",
}


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


# ── Benchmark task ────────────────────────────────────────────────────────────

@task(name="fetch_benchmark_prices")
def fetch_benchmark_prices() -> pd.DataFrame:
    import yfinance as yf
    from datetime import date, timedelta
    logger = get_run_logger()
    start = date(2026, 4, 1)
    end   = date.today() + timedelta(days=1)
    inserted = 0
    records = []
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
                    records.append({"date": idx.date(), "symbol": symbol, "close_price": close})
        conn.commit()
    logger.info(f"Benchmark prices upserted: {inserted} rows")
    return pd.DataFrame(records)


# ── Iceberg task ──────────────────────────────────────────────────────────────

@task(name="write_iceberg_tables")
def write_iceberg_tables(
    trades: pd.DataFrame,
    alerts: pd.DataFrame,
    benchmark: pd.DataFrame,
) -> dict:
    import os
    import pyarrow as pa
    from pyiceberg.catalog.glue import GlueCatalog
    from pyiceberg.exceptions import NoSuchTableError
    from pyiceberg.transforms import DayTransform

    logger = get_run_logger()

    os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-1")

    COW_PROPERTIES = {
        "write.delete.mode": "copy-on-write",
        "write.update.mode": "copy-on-write",
    }

    catalog = GlueCatalog(
        "trading_lakehouse",
        **{
            "warehouse": "s3://cle-portfolio-etl/iceberg",
            "region": "us-west-1",
        },
    )

    NAMESPACE = "trading_lakehouse"
    s3_client = boto3.client("s3", region_name=S3_REGION)
    snapshot_ids: dict = {}

    def _update_current_metadata(name, tbl):
        """Copy the latest metadata file to current.metadata.json for DuckDB iceberg_scan."""
        loc = tbl.metadata_location  # e.g. s3://bucket/iceberg/trades/metadata/00003-uuid.metadata.json
        src_key = loc.replace(f"s3://{S3_BUCKET}/", "")
        dst_key = f"iceberg/{name}/metadata/current.metadata.json"
        s3_client.copy_object(
            Bucket=S3_BUCKET,
            CopySource={"Bucket": S3_BUCKET, "Key": src_key},
            Key=dst_key,
        )

    def _load_or_create(name, arrow_tbl, partition_col):
        identifier = (NAMESPACE, name)
        location = f"s3://cle-portfolio-etl/iceberg/{name}"
        try:
            return catalog.load_table(identifier)
        except NoSuchTableError:
            # Pass PyArrow schema directly — PyIceberg assigns field IDs internally
            tbl = catalog.create_table(
                identifier,
                schema=arrow_tbl.schema,
                location=location,
                properties=COW_PROPERTIES,
            )
            # Add partition spec after creation, referencing field by name
            with tbl.update_spec() as update:
                update.add_field(partition_col, DayTransform(), f"{partition_col}_day")
            return tbl

    # ── trades ────────────────────────────────────────────────────────────────
    if not trades.empty:
        t = trades.copy()
        # Serialize indicator dicts to JSON strings (PyArrow can't infer mixed-key maps)
        t["indicators"] = t["indicators"].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else str(x or "{}")
        )
        t["entry_ts"] = pd.to_datetime(t["entry_ts"], utc=True)
        t["exit_ts"] = pd.to_datetime(t["exit_ts"], utc=True)
        arrow_tbl = pa.Table.from_pandas(t, preserve_index=False)
        tbl = _load_or_create("trades", arrow_tbl, "entry_ts")
        tbl.append(arrow_tbl)
        _update_current_metadata("trades", tbl)
        snap = tbl.current_snapshot()
        snapshot_ids["trades"] = snap.snapshot_id if snap else None
        logger.info(f"Iceberg trades: appended {len(t)} rows")
    else:
        logger.info("Iceberg trades: no rows to append")

    # ── wazuh_alerts ──────────────────────────────────────────────────────────
    if not alerts.empty:
        a = alerts.copy()
        a["timestamp"] = pd.to_datetime(a["timestamp"], utc=True)
        arrow_tbl = pa.Table.from_pandas(a, preserve_index=False)
        tbl = _load_or_create("wazuh_alerts", arrow_tbl, "timestamp")
        tbl.append(arrow_tbl)
        _update_current_metadata("wazuh_alerts", tbl)
        snap = tbl.current_snapshot()
        snapshot_ids["wazuh_alerts"] = snap.snapshot_id if snap else None
        logger.info(f"Iceberg wazuh_alerts: appended {len(a)} rows")
    else:
        logger.info("Iceberg wazuh_alerts: no rows to append")

    # ── benchmark_prices ──────────────────────────────────────────────────────
    if not benchmark.empty:
        b = benchmark.copy()
        # Ensure date column is a proper date type for PyArrow
        b["date"] = pd.to_datetime(b["date"]).dt.date
        arrow_tbl = pa.Table.from_pandas(b, preserve_index=False)
        tbl = _load_or_create("benchmark_prices", arrow_tbl, "date")
        tbl.append(arrow_tbl)
        _update_current_metadata("benchmark_prices", tbl)
        snap = tbl.current_snapshot()
        snapshot_ids["benchmark_prices"] = snap.snapshot_id if snap else None
        logger.info(f"Iceberg benchmark_prices: appended {len(b)} rows")
    else:
        logger.info("Iceberg benchmark_prices: no rows to append")

    return snapshot_ids


# ── GX checkpoint task ────────────────────────────────────────────────────────

@task(name="run_gx_checkpoint")
def run_gx_checkpoint(
    trades: pd.DataFrame,
    alerts: pd.DataFrame,
) -> dict:
    import great_expectations as gx

    logger = get_run_logger()

    gx_path = Path(GX_DIR)
    gx_path.mkdir(parents=True, exist_ok=True)

    # Load row-count baseline from previous run
    baseline_file = Path(ROW_COUNT_BASELINE_FILE)
    baseline: dict = json.loads(baseline_file.read_text()) if baseline_file.exists() else {}

    def _row_range(key: str, current: int):
        prev = baseline.get(key)
        if not prev:
            return 0, max(current * 10, 1)  # first run: accept anything
        return int(prev * 0.5), int(prev * 1.5)

    # ── Build expectation suites (code is source of truth) ────────────────
    def _make_trades_suite(min_rows: int, max_rows: int) -> "gx.ExpectationSuite":
        suite = gx.ExpectationSuite(name="stg_trades")
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="trade_id"))
        # trade_id is unique per-source only; composite key (trade_id, source) is enforced by Postgres
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="side", value_set=["BUY", "SELL"]))
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="entry_price", min_value=0, strict_min=True))
        suite.add_expectation(
            gx.expectations.ExpectTableRowCountToBeBetween(
                min_value=min_rows, max_value=max_rows))
        suite.add_expectation(
            gx.expectations.ExpectTableColumnsToMatchSet(
                column_set=sorted(TRADES_EXPECTED_COLS), exact_match=False))
        return suite

    def _make_wazuh_suite(min_rows: int, max_rows: int) -> "gx.ExpectationSuite":
        suite = gx.ExpectationSuite(name="stg_wazuh_alerts")
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="alert_id"))
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeUnique(column="alert_id"))
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="timestamp"))
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="rule_level", min_value=0, max_value=15))
        suite.add_expectation(
            gx.expectations.ExpectTableRowCountToBeBetween(
                min_value=min_rows, max_value=max_rows))
        suite.add_expectation(
            gx.expectations.ExpectTableColumnsToMatchSet(
                column_set=sorted(WAZUH_EXPECTED_COLS), exact_match=False))
        return suite

    # ── Initialise file-based GX context ──────────────────────────────────
    context = gx.get_context(mode="file", project_root_dir=str(gx_path))

    # Pandas datasource — get or create
    DS_NAME = "etl_pandas"
    try:
        ds = context.data_sources.get(DS_NAME)
    except Exception:
        ds = context.data_sources.add_pandas(name=DS_NAME)

    def _get_or_add_batch_def(ds, asset_name: str):
        try:
            asset = ds.get_asset(asset_name)
        except Exception:
            asset = ds.add_dataframe_asset(name=asset_name)
        try:
            return asset.get_batch_definition("whole_df")
        except Exception:
            return asset.add_batch_definition_whole_dataframe("whole_df")

    trades_batch_def = _get_or_add_batch_def(ds, "trades_asset")
    wazuh_batch_def = _get_or_add_batch_def(ds, "wazuh_asset")

    # Suites — always rebuild so code stays source of truth
    trades_min, trades_max = _row_range("stg_trades", len(trades))
    wazuh_min, wazuh_max = _row_range("stg_wazuh_alerts", len(alerts))

    for suite_name in ["stg_trades", "stg_wazuh_alerts"]:
        try:
            context.suites.delete(suite_name)
        except Exception:
            pass
    trades_suite = context.suites.add(_make_trades_suite(trades_min, trades_max))
    wazuh_suite = context.suites.add(_make_wazuh_suite(wazuh_min, wazuh_max))

    # Validation definitions — rebuild alongside suites
    for val_name in ["trades_val", "wazuh_val"]:
        try:
            context.validation_definitions.delete(val_name)
        except Exception:
            pass
    trades_val_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="trades_val", data=trades_batch_def, suite=trades_suite))
    wazuh_val_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="wazuh_val", data=wazuh_batch_def, suite=wazuh_suite))

    # ── Run validations ───────────────────────────────────────────────────
    def _parse_result(vr) -> dict:
        failed = []
        for er in vr.results:
            if not er.success:
                exp_type = (
                    getattr(er.expectation_config, "type", None)
                    or getattr(er.expectation_config, "expectation_type", None)
                    or str(er.expectation_config)
                )
                failed.append(exp_type)
        return {"success": bool(vr.success), "failed_expectations": failed}

    results: dict = {}

    trades_vr = trades_val_def.run(batch_parameters={"dataframe": trades})
    results["stg_trades"] = _parse_result(trades_vr)

    if not alerts.empty:
        wazuh_vr = wazuh_val_def.run(batch_parameters={"dataframe": alerts})
        results["stg_wazuh_alerts"] = _parse_result(wazuh_vr)
    else:
        results["stg_wazuh_alerts"] = {"success": True, "failed_expectations": [], "skipped": True}

    # ── Update baseline with this run's counts ────────────────────────────
    baseline_file.write_text(json.dumps({
        "stg_trades": len(trades),
        "stg_wazuh_alerts": len(alerts),
    }))

    # ── Log results and raise on any failure ──────────────────────────────
    any_failed = False
    for suite_name, res in results.items():
        if res.get("skipped"):
            logger.info(f"GX '{suite_name}': SKIPPED (empty DataFrame)")
        elif res["success"]:
            logger.info(f"GX '{suite_name}': PASS")
        else:
            logger.error(f"GX '{suite_name}': FAIL — {res['failed_expectations']}")
            any_failed = True

    if any_failed:
        raise ValueError("GX validation failed — see failed expectations in logs above")

    return results


# ── Flow ──────────────────────────────────────────────────────────────────────

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
    benchmark = fetch_benchmark_prices()

    # Iceberg tables (additive — Postgres load path unchanged above)
    iceberg_info = write_iceberg_tables(clean, alerts, benchmark)

    # GX data quality checkpoint
    gx_results = run_gx_checkpoint(clean, alerts)

    # ── Prefect artifact: stable run summary ──────────────────────────────
    run_date = date.today().isoformat()

    def _snap(key):
        v = iceberg_info.get(key)
        return str(v) if v is not None else "n/a"

    def _gx_row(suite_name):
        res = gx_results.get(suite_name, {})
        if res.get("skipped"):
            return f"| {suite_name} | ⏭ SKIPPED | — |"
        status = "✅ PASS" if res["success"] else "❌ FAIL"
        failed = ", ".join(res.get("failed_expectations", [])) or "—"
        return f"| {suite_name} | {status} | {failed} |"

    md = f"""# Lakehouse Run Summary — {run_date}

## Rows Written (Iceberg append)
| Table | Rows |
|---|---|
| trades | {len(clean)} |
| wazuh_alerts | {len(alerts)} |
| benchmark_prices | {len(benchmark)} |

## Iceberg Snapshot IDs
| Table | Snapshot ID |
|---|---|
| trades | {_snap("trades")} |
| wazuh_alerts | {_snap("wazuh_alerts")} |
| benchmark_prices | {_snap("benchmark_prices")} |

## GX Validation
| Suite | Result | Failed Expectations |
|---|---|---|
{_gx_row("stg_trades")}
{_gx_row("stg_wazuh_alerts")}
"""

    create_markdown_artifact(key="lakehouse-run-summary", markdown=md)


if __name__ == "__main__":
    daily_trades_etl()
