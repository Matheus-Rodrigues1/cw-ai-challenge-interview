#!/usr/bin/env python3
"""
Insert a synthetic spike minute into `transactions` so the anomaly worker flags
CRITICAL/WARNING on the next cycle, then the monitoring worker can notify (email, etc.).

Prerequisites
  - Python deps: py -3 -m pip install -r requirements.txt  (needs psycopg2-binary)
  - Postgres reachable (e.g. port 5432 exposed: DB_HOST=localhost)
  - Optional: API for --wait (default http://localhost:8000)

Examples
  # Windows PowerShell (from cloudwalk-monitoring/)
  $env:DB_HOST="localhost"
  py -3 scripts/inject_spike_anomaly.py --wait

  # Same variables as docker-compose for DB_*; API URL if different port:
  py -3 scripts/inject_spike_anomaly.py --wait --api-url http://localhost:8000
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta

try:
    import psycopg2
except ImportError:
    print(
        "Missing dependency: psycopg2. Install with:\n"
        "  py -3 -m pip install psycopg2-binary\n"
        "or: py -3 -m pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise SystemExit(1) from None


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "cloudwalk_transactions"),
        user=os.getenv("DB_USER", "admin"),
        password=os.getenv("DB_PASSWORD", "admin"),
    )


def next_spike_minute(conn) -> datetime:
    """Minute strictly after all existing data and past anomaly evaluations."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT GREATEST(
                COALESCE((SELECT MAX(timestamp) FROM anomaly_results), TIMESTAMP '1970-01-01'),
                COALESCE((SELECT MAX(timestamp) FROM transactions), TIMESTAMP '1970-01-01')
            )
            """
        )
        row = cur.fetchone()
        base = row[0]
    if base is None:
        base = datetime.now().replace(second=0, microsecond=0)
    nxt = base + timedelta(minutes=1)
    if getattr(nxt, "tzinfo", None) is not None:
        nxt = nxt.replace(tzinfo=None)
    return nxt


def insert_spike(conn, ts: datetime) -> int:
    """One row per status for the same minute — matches CSV shape (minute granularity)."""
    rows = [
        (ts, "approved", 40),
        (ts, "denied", 120_000),
        (ts, "failed", 35_000),
        (ts, "reversed", 12_000),
        (ts, "backend_reversed", 8_000),
        (ts, "refunded", 3),
    ]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO transactions (timestamp, status, count) VALUES (%s, %s, %s)",
            rows,
        )
    conn.commit()
    return len(rows)


def poll_unprocessed(api_url: str, timeout_s: int) -> bool:
    try:
        import requests
    except ImportError:
        print("Install requests (`pip install requests`) or omit --wait.", file=sys.stderr)
        return False

    url = f"{api_url.rstrip('/')}/api/v1/anomalies/unprocessed"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = requests.get(url, params={"min_level": "WARNING", "limit": 10}, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data.get("count", 0) > 0:
                print(json.dumps(data, indent=2, default=str))
                return True
        except requests.RequestException as e:
            print(f"Polling API: {e}", file=sys.stderr)
        time.sleep(3)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--api-url",
        default=os.getenv("API_BASE_URL", "http://localhost:8000"),
        help="Monitoring API base URL (for --wait)",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Poll GET /api/v1/anomalies/unprocessed until rows appear or timeout",
    )
    parser.add_argument("--timeout", type=int, default=180, help="Seconds for --wait (default 180)")
    args = parser.parse_args()

    conn = get_conn()
    try:
        ts = next_spike_minute(conn)
        n = insert_spike(conn, ts)
        print(f"Inserted {n} transaction rows for minute: {ts}")
        print(
            "Next: anomaly_worker (~60s by default) evaluates new minutes → writes anomaly_results; "
            "monitoring_worker (~30s) picks unprocessed rows → POST /notifications/send (email, etc.)."
        )
        print(f"Logs: docker compose logs -f anomaly_worker monitoring_worker monitoring_api")

        if args.wait:
            print(f"Waiting up to {args.timeout}s for unprocessed anomalies via API...")
            if poll_unprocessed(args.api_url, args.timeout):
                return 0
            print(
                "Timeout: no unprocessed anomaly yet. "
                "Confirm workers are running and check anomaly_worker logs.",
                file=sys.stderr,
            )
            return 1
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
