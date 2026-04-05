"""
CloudWalk Monitoring — FastAPI

Postgres-backed API plus a hybrid anomaly model (Z-score + isolation forest)
and mandatory rules for denied / failed / reversal volumes above normal.

Endpoints
─────────
Health
  GET  /api/v1/health

Evaluate (model + rules + recommendation) — challenge requirement
  POST /api/v1/transactions/evaluate

Transactions (read from Postgres)
  GET  /api/v1/transactions
  GET  /api/v1/transactions/summary

Anomalies (read from anomaly_results)
  GET  /api/v1/anomalies
  GET  /api/v1/anomalies/unprocessed
  POST /api/v1/anomalies/{id}/acknowledge

Notifications
  POST /api/v1/notifications/send
  GET  /api/v1/notifications/history

Model
  GET  /api/v1/stats
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from api.database import get_conn, get_cursor, init_pool, close_pool
from api.notifications import notifier
from models.anomaly_detector import HybridAnomalyDetector
from models.monitoring_rules import MANDATORY_RULE_IDS, build_recommendation

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("monitoring-api")

ALERT_ON_EVALUATE = os.getenv("ALERT_ON_EVALUATE", "").lower() in ("1", "true", "yes")

detector: HybridAnomalyDetector | None = None


def _load_and_fit_detector() -> HybridAnomalyDetector | None:
    """Train hybrid detector on all rows in ``transactions`` (same data as CSV pipeline)."""
    try:
        with get_conn() as conn:
            df = pd.read_sql(
                'SELECT "timestamp", status, count FROM transactions ORDER BY "timestamp"',
                conn,
                parse_dates=["timestamp"],
            )
    except Exception:
        logger.exception("Could not load transactions for model training")
        return None

    if df.empty:
        logger.warning("transactions table is empty — /transactions/evaluate will be unavailable")
        return None

    det = HybridAnomalyDetector(
        z_threshold=float(os.getenv("Z_THRESHOLD", "2.5")),
        iso_contamination=float(os.getenv("ISO_CONTAMINATION", "0.05")),
        rolling_window=int(os.getenv("ROLLING_WINDOW", "30")),
        warning_score=float(os.getenv("WARNING_SCORE", "0.5")),
        critical_score=float(os.getenv("CRITICAL_SCORE", "0.75")),
        z_weight=float(os.getenv("Z_WEIGHT", "0.6")),
    )
    det.fit(df)
    logger.info("HybridAnomalyDetector fitted on %s transaction rows", len(df))
    return det


app = FastAPI(
    title="CloudWalk Transaction Monitoring API",
    description="Database-backed monitoring API with notification support",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup():
    global detector
    init_pool()
    detector = _load_and_fit_detector()
    logger.info("API started — DB pool ready")
    logger.info(
        "Notifier: available channels=%s | email SMTP host=%s (password set=%s)",
        notifier.available_channels(),
        notifier.smtp_host or "(none)",
        bool(notifier.smtp_pass),
    )


@app.on_event("shutdown")
def on_shutdown():
    close_pool()


# ── Schemas ──────────────────────────────────────────────────────────────

class NotificationRequest(BaseModel):
    anomaly_id: int = Field(..., description="ID from anomaly_results")
    channels: list[str] = Field(
        default=["console"],
        description="Channels to notify: console, slack, email",
    )

class NotificationResponse(BaseModel):
    anomaly_id: int
    channels_attempted: list[str]
    results: dict
    notified_at: str

class AnomalyOut(BaseModel):
    id: int
    evaluated_at: Optional[str]
    timestamp: str
    is_anomaly: bool
    anomaly_score: float
    alert_level: str
    triggered_rules: Optional[str]
    z_scores: Optional[dict]
    iso_score: Optional[float]
    counts: Optional[dict]
    notified_at: Optional[str]


class TransactionMinuteInput(BaseModel):
    """One minute of per-status transaction counts (ingest for evaluation)."""

    timestamp: Optional[str] = Field(None, description="ISO timestamp for this minute")
    approved: int = Field(0, ge=0)
    denied: int = Field(0, ge=0)
    failed: int = Field(0, ge=0)
    reversed: int = Field(0, ge=0)
    backend_reversed: int = Field(0, ge=0)
    refunded: int = Field(0, ge=0)


class EvaluateResponse(BaseModel):
    timestamp: str
    is_anomaly: bool
    anomaly_score: float
    alert_level: str
    recommendation: str
    mandatory_rule_alerts: list[str]
    triggered_rules: list[str]
    details: dict
    notification_sent: bool = False


# ── Health ───────────────────────────────────────────────────────────────

@app.get("/api/v1/health")
def health_check():
    db_ok = False
    try:
        with get_cursor() as cur:
            cur.execute("SELECT 1")
            db_ok = True
    except Exception:
        pass

    return {
        "status": "healthy" if db_ok else "degraded",
        "database": "connected" if db_ok else "unreachable",
        "model_ready": detector is not None and getattr(detector, "_is_fitted", False),
        "timestamp": datetime.utcnow().isoformat(),
    }


# ── Evaluate (hybrid model + mandatory rules + recommendation) ──────────

@app.post("/api/v1/transactions/evaluate", response_model=EvaluateResponse)
def evaluate_transactions_minute(tx: TransactionMinuteInput):
    """
    Ingest one minute of transaction counts, run the hybrid anomaly model plus
    mandatory rules (denied / failed / reversed above normal), and return an
    operator **recommendation**. Optionally sends a notification when
    ``ALERT_ON_EVALUATE`` is enabled and level is WARNING/CRITICAL.
    """
    if detector is None or not detector._is_fitted:
        raise HTTPException(
            status_code=503,
            detail="Model not ready — load transactions into Postgres and restart the API.",
        )

    data = {
        "timestamp": tx.timestamp or datetime.utcnow().isoformat(),
        "approved": tx.approved,
        "denied": tx.denied,
        "failed": tx.failed,
        "reversed": tx.reversed,
        "backend_reversed": tx.backend_reversed,
        "refunded": tx.refunded,
    }

    result = detector.evaluate_single(data)
    mandatory = [r for r in result.triggered_rules if r in MANDATORY_RULE_IDS]
    other = [r for r in result.triggered_rules if r not in MANDATORY_RULE_IDS]

    recommendation = build_recommendation(
        result.alert_level, mandatory, other,
    )

    notified = False
    if ALERT_ON_EVALUATE and result.is_anomaly:
        try:
            notifier.notify(
                result.alert_level,
                result.timestamp,
                float(result.anomaly_score),
                result.triggered_rules,
                result.details,
            )
            notified = True
        except Exception:
            logger.exception("notify on evaluate failed")

    return EvaluateResponse(
        timestamp=result.timestamp,
        is_anomaly=result.is_anomaly,
        anomaly_score=float(result.anomaly_score),
        alert_level=result.alert_level,
        recommendation=recommendation,
        mandatory_rule_alerts=mandatory,
        triggered_rules=result.triggered_rules,
        details=result.details,
        notification_sent=notified,
    )


# ── Transactions ─────────────────────────────────────────────────────────

@app.get("/api/v1/transactions")
def get_transactions(
    status: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = Query(500, le=5000),
):
    """Query transactions from Postgres with optional filters."""
    clauses, params = [], []

    if status:
        clauses.append("status = %s")
        params.append(status)
    if start:
        clauses.append("timestamp >= %s")
        params.append(start)
    if end:
        clauses.append("timestamp <= %s")
        params.append(end)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"SELECT * FROM transactions {where} ORDER BY timestamp DESC LIMIT %s"
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

    return {"count": len(rows), "transactions": _serialize_rows(rows)}


@app.get("/api/v1/transactions/summary")
def transactions_summary():
    """Aggregated transaction statistics."""
    query = """
        SELECT
            status,
            COUNT(*)            AS total_records,
            SUM(count)          AS total_count,
            ROUND(AVG(count),2) AS avg_per_minute,
            MAX(count)          AS max_per_minute,
            MIN(timestamp)      AS first_seen,
            MAX(timestamp)      AS last_seen
        FROM transactions
        GROUP BY status
        ORDER BY total_count DESC
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return {"statuses": _serialize_rows(rows)}


# ── Anomalies ────────────────────────────────────────────────────────────

@app.get("/api/v1/anomalies")
def get_anomalies(
    level: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    only_anomalies: bool = True,
    limit: int = Query(100, le=2000),
):
    """Query anomaly results with filters."""
    clauses, params = [], []

    if only_anomalies:
        clauses.append("is_anomaly = TRUE")
    if level:
        clauses.append("alert_level = %s")
        params.append(level.upper())
    if start:
        clauses.append("timestamp >= %s")
        params.append(start)
    if end:
        clauses.append("timestamp <= %s")
        params.append(end)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
        SELECT * FROM anomaly_results
        {where}
        ORDER BY anomaly_score DESC, timestamp DESC
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

    return {"count": len(rows), "anomalies": _serialize_rows(rows)}


@app.get("/api/v1/anomalies/unprocessed")
def get_unprocessed_anomalies(
    min_level: str = Query("WARNING", description="Minimum alert level: WARNING or CRITICAL"),
    limit: int = Query(50, le=500),
):
    """Return anomalies that have not been notified yet."""
    levels = ["CRITICAL", "WARNING"] if min_level.upper() == "WARNING" else ["CRITICAL"]
    placeholders = ",".join(["%s"] * len(levels))

    query = f"""
        SELECT * FROM anomaly_results
        WHERE is_anomaly = TRUE
          AND notified_at IS NULL
          AND alert_level IN ({placeholders})
        ORDER BY
            CASE alert_level WHEN 'CRITICAL' THEN 0 ELSE 1 END,
            anomaly_score DESC
        LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, [*levels, limit])
            rows = cur.fetchall()

    return {"count": len(rows), "anomalies": _serialize_rows(rows)}


@app.post("/api/v1/anomalies/{anomaly_id}/acknowledge")
def acknowledge_anomaly(anomaly_id: int):
    """Mark an anomaly as notified (without sending a notification)."""
    with get_cursor() as cur:
        cur.execute(
            "UPDATE anomaly_results SET notified_at = NOW() WHERE id = %s RETURNING id",
            (anomaly_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, f"Anomaly {anomaly_id} not found")

    return {"acknowledged": anomaly_id}


# ── Notifications ────────────────────────────────────────────────────────

@app.post("/api/v1/notifications/send", response_model=NotificationResponse)
def send_notification(req: NotificationRequest):
    """
    Send a notification for a specific anomaly through the
    requested channels (console, slack, email).
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM anomaly_results WHERE id = %s", (req.anomaly_id,))
            anomaly = cur.fetchone()
            if not anomaly:
                raise HTTPException(404, f"Anomaly {req.anomaly_id} not found")

    results = {}
    for channel in req.channels:
        try:
            notifier.send_channel(
                channel=channel,
                alert_level=anomaly["alert_level"],
                timestamp=str(anomaly["timestamp"]),
                score=float(anomaly["anomaly_score"]),
                triggered_rules=(anomaly["triggered_rules"] or "").split(", "),
                details={"counts": anomaly.get("counts"), "z_scores": anomaly.get("z_scores")},
            )
            results[channel] = "sent"
            _log_notification(req.anomaly_id, channel, "sent")
        except Exception as e:
            results[channel] = f"error: {e}"
            _log_notification(req.anomaly_id, channel, "error", str(e))

    logger.info(
        "POST /notifications/send anomaly_id=%s results=%s",
        req.anomaly_id,
        results,
    )

    with get_cursor() as cur:
        cur.execute(
            "UPDATE anomaly_results SET notified_at = NOW() WHERE id = %s",
            (req.anomaly_id,),
        )

    return NotificationResponse(
        anomaly_id=req.anomaly_id,
        channels_attempted=req.channels,
        results=results,
        notified_at=datetime.utcnow().isoformat(),
    )


@app.get("/api/v1/notifications/history")
def notification_history(limit: int = Query(100, le=500)):
    """View past notifications."""
    query = """
        SELECT nl.*, ar.alert_level, ar.anomaly_score, ar.timestamp AS anomaly_timestamp
        FROM notification_log nl
        JOIN anomaly_results ar ON ar.id = nl.anomaly_id
        ORDER BY nl.sent_at DESC
        LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

    return {"count": len(rows), "notifications": _serialize_rows(rows)}


# ── Stats ────────────────────────────────────────────────────────────────

@app.get("/api/v1/stats")
def get_stats():
    """Aggregated anomaly detection statistics from the database."""
    query = """
        SELECT
            COUNT(*)                                            AS total_evaluated,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)        AS total_anomalies,
            SUM(CASE WHEN alert_level='CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
            SUM(CASE WHEN alert_level='WARNING'  THEN 1 ELSE 0 END) AS warning_count,
            ROUND(AVG(anomaly_score)::numeric, 4)               AS avg_score,
            MAX(anomaly_score)                                  AS max_score,
            MIN(timestamp)                                      AS first_evaluation,
            MAX(timestamp)                                      AS last_evaluation,
            SUM(CASE WHEN notified_at IS NOT NULL THEN 1 ELSE 0 END) AS notified_count,
            SUM(CASE WHEN is_anomaly AND notified_at IS NULL THEN 1 ELSE 0 END) AS pending_notifications
        FROM anomaly_results
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            row = cur.fetchone()

    return _serialize_row(row) if row else {}


# ── Helpers ──────────────────────────────────────────────────────────────

def _log_notification(anomaly_id: int, channel: str, status: str, error: str = None):
    try:
        with get_cursor() as cur:
            cur.execute(
                "INSERT INTO notification_log (anomaly_id, channel, status, error_message) VALUES (%s,%s,%s,%s)",
                (anomaly_id, channel, status, error),
            )
    except Exception:
        logger.exception("Failed to log notification")


def _serialize_row(row: dict) -> dict:
    """Convert non-JSON-serializable types (datetime, Decimal) to strings."""
    return {k: _safe(v) for k, v in row.items()}


def _serialize_rows(rows: list[dict]) -> list[dict]:
    return [_serialize_row(r) for r in rows]


def _safe(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    if hasattr(v, "as_integer_ratio"):  # Decimal / float
        return float(v)
    return v


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
