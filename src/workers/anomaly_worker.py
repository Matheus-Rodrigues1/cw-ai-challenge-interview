"""
Anomaly Detection Worker

Reads transactions from Postgres, runs HybridAnomalyDetector,
and writes anomaly results back to the anomaly_results table
so Metabase can visualize them.
"""

import os
import sys
import time
import json
import logging
from datetime import datetime
from decimal import Decimal

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from models.anomaly_detector import HybridAnomalyDetector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("anomaly-worker")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "cloudwalk_transactions"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "admin"),
}

RUN_INTERVAL = int(os.getenv("RUN_INTERVAL_SECONDS", "60"))


def _native_float(x):
    if x is None:
        return None
    if isinstance(x, (float, int)):
        return float(x)
    if isinstance(x, (np.floating, np.integer)):
        return float(x.item())
    return float(x)


def _native_bool(x):
    if isinstance(x, (np.bool_,)):
        return bool(x.item())
    return bool(x)


def _to_timestamp(val):
    if val is None:
        return None
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    if isinstance(val, datetime):
        return val
    return pd.Timestamp(val).to_pydatetime()


def _json_safe(obj):
    """Recursively convert numpy scalars so json.dumps works."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]
    if isinstance(obj, (np.floating, np.integer, np.bool_)):
        return obj.item()
    if isinstance(obj, (float, int, bool, str)):
        return obj
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_transactions(conn, lookback_minutes: int = 120) -> pd.DataFrame:
    """Load recent transactions from Postgres."""
    query = """
        SELECT timestamp, status, count
        FROM transactions
        ORDER BY timestamp
    """
    return pd.read_sql(query, conn, parse_dates=["timestamp"])


def get_last_evaluated_timestamp(conn) -> datetime | None:
    """Get the latest timestamp already evaluated."""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(timestamp) FROM anomaly_results")
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def write_results(conn, results: list):
    """Insert anomaly results into Postgres."""
    if not results:
        return

    rows = []
    for r in results:
        z_scores = r.details.get("z_scores", {}) or {}
        counts = r.details.get("counts", {}) or {}
        iso = r.details.get("iso_score", 0)
        rows.append((
            _to_timestamp(r.timestamp),
            _native_bool(r.is_anomaly),
            _native_float(r.anomaly_score),
            str(r.alert_level),
            ", ".join(r.triggered_rules) if r.triggered_rules else None,
            json.dumps(_json_safe(z_scores)),
            _native_float(iso),
            json.dumps(_json_safe(counts)),
        ))

    insert_sql = """
        INSERT INTO anomaly_results
            (timestamp, is_anomaly, anomaly_score, alert_level,
             triggered_rules, z_scores, iso_score, counts)
        VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows)
    conn.commit()


def run_detection_cycle():
    """Single detection cycle: load data, evaluate, persist."""
    conn = get_connection()
    try:
        df = load_transactions(conn)
        if df.empty:
            logger.warning("No transaction data found. Skipping cycle.")
            return

        last_ts = get_last_evaluated_timestamp(conn)

        detector = HybridAnomalyDetector(
            z_threshold=2.5,
            iso_contamination=0.05,
            rolling_window=30,
            warning_score=0.5,
            critical_score=0.75,
            z_weight=0.6,
        )
        detector.fit(df)

        if last_ts:
            new_data = df[df["timestamp"] > last_ts]
            if new_data.empty:
                logger.info("No new data since last evaluation.")
                return
            results = detector.evaluate(new_data)
        else:
            results = detector.evaluate(df)

        anomalies = [r for r in results if r.is_anomaly]
        logger.info(
            f"Evaluated {len(results)} minutes | "
            f"Anomalies: {len(anomalies)} | "
            f"Critical: {sum(1 for r in results if r.alert_level == 'CRITICAL')} | "
            f"Warning: {sum(1 for r in results if r.alert_level == 'WARNING')}"
        )

        write_results(conn, results)
        logger.info(f"Wrote {len(results)} results to anomaly_results table.")

    except Exception:
        logger.exception("Error during detection cycle")
    finally:
        conn.close()


def main():
    logger.info("Anomaly Detection Worker started")
    logger.info(f"DB: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    logger.info(f"Interval: {RUN_INTERVAL}s")

    while True:
        logger.info("--- Starting detection cycle ---")
        run_detection_cycle()
        logger.info(f"Sleeping {RUN_INTERVAL}s until next cycle...")
        time.sleep(RUN_INTERVAL)


if __name__ == "__main__":
    main()
