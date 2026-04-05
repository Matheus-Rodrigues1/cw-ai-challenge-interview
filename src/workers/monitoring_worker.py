"""
Monitoring Worker

Polls the API for unprocessed anomalies, validates severity
patterns, and triggers notifications through the notification API.

Flow:
  1. GET  /api/v1/anomalies/unprocessed   → fetch pending alerts
  2. Validate each anomaly (dedup, escalation logic)
  3. POST /api/v1/notifications/send       → dispatch per channel
  4. Sleep and repeat
"""

import os
import time
import logging
from datetime import datetime

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("monitoring-worker")

API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
CRITICAL_CHANNELS = os.getenv("CRITICAL_CHANNELS", "console,email").split(",")
WARNING_CHANNELS = os.getenv("WARNING_CHANNELS", "console,email").split(",")
MIN_SCORE_TO_NOTIFY = float(os.getenv("MIN_SCORE_TO_NOTIFY", "0.5"))


def fetch_unprocessed() -> list[dict]:
    """Get anomalies that haven't been notified yet."""
    try:
        resp = requests.get(
            f"{API_BASE}/api/v1/anomalies/unprocessed",
            params={"min_level": "WARNING", "limit": 50},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("anomalies", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch unprocessed anomalies: {e}")
        return []


def should_notify(anomaly: dict) -> bool:
    """Decide whether this anomaly warrants a notification."""
    score = anomaly.get("anomaly_score", 0)
    if score < MIN_SCORE_TO_NOTIFY:
        return False

    level = anomaly.get("alert_level", "NORMAL")
    if level not in ("CRITICAL", "WARNING"):
        return False

    return True


def get_channels(anomaly: dict) -> list[str]:
    """Pick notification channels based on alert severity."""
    level = anomaly.get("alert_level", "WARNING")
    if level == "CRITICAL":
        return [ch.strip() for ch in CRITICAL_CHANNELS]
    return [ch.strip() for ch in WARNING_CHANNELS]


def send_notification(anomaly_id: int, channels: list[str]) -> dict:
    """Call the notification API endpoint."""
    try:
        resp = requests.post(
            f"{API_BASE}/api/v1/notifications/send",
            json={"anomaly_id": anomaly_id, "channels": channels},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error(f"Notification request failed for anomaly {anomaly_id}: {e}")
        return {"error": str(e)}


def acknowledge(anomaly_id: int):
    """Mark anomaly as acknowledged if notification send failed partially."""
    try:
        requests.post(
            f"{API_BASE}/api/v1/anomalies/{anomaly_id}/acknowledge",
            timeout=10,
        )
    except requests.RequestException:
        pass


def process_anomalies(anomalies: list[dict]):
    """Validate and notify for each unprocessed anomaly."""
    critical_count = 0
    warning_count = 0
    notified = 0

    for anomaly in anomalies:
        aid = anomaly.get("id")
        level = anomaly.get("alert_level", "?")
        score = anomaly.get("anomaly_score", 0)
        ts = anomaly.get("timestamp", "?")

        if not should_notify(anomaly):
            acknowledge(aid)
            logger.info(f"Skipped anomaly {aid} (score={score} below threshold)")
            continue

        channels = get_channels(anomaly)
        logger.info(
            f"Notifying anomaly {aid} | {level} | score={score} | "
            f"ts={ts} | channels={channels}"
        )

        result = send_notification(aid, channels)

        if "error" not in result:
            notified += 1
            if level == "CRITICAL":
                critical_count += 1
            else:
                warning_count += 1
        else:
            acknowledge(aid)

    return notified, critical_count, warning_count


def check_api_health() -> bool:
    """Verify the API is reachable before starting the poll loop."""
    try:
        resp = requests.get(f"{API_BASE}/api/v1/health", timeout=5)
        data = resp.json()
        return data.get("database") == "connected"
    except Exception:
        return False


def main():
    logger.info("Monitoring Worker started")
    logger.info(f"API: {API_BASE}")
    logger.info(f"Poll interval: {POLL_INTERVAL}s")
    logger.info(f"Min score: {MIN_SCORE_TO_NOTIFY}")
    logger.info(f"Critical channels: {CRITICAL_CHANNELS}")
    logger.info(f"Warning channels: {WARNING_CHANNELS}")

    while not check_api_health():
        logger.warning("API not ready, retrying in 10s...")
        time.sleep(10)

    logger.info("API is healthy — starting poll loop")

    while True:
        anomalies = fetch_unprocessed()

        if anomalies:
            logger.info(f"Found {len(anomalies)} unprocessed anomalies")
            notified, crit, warn = process_anomalies(anomalies)
            logger.info(
                f"Cycle complete | Notified: {notified} | "
                f"Critical: {crit} | Warning: {warn}"
            )
        else:
            logger.info("No unprocessed anomalies")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
