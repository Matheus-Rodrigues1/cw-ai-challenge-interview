"""
AI/ML Anomaly Detection Worker  (worker-ai-ml)
===============================================
Pure machine-learning anomaly detection — zero hardcoded thresholds or business rules.

Architecture
------------
  • Feature engineering   : per-minute pivot from transactions + auth-code diversity
                            + cyclical time encoding (hour/day-of-week sin/cos)
  • Models                : Isolation Forest  +  Local Outlier Factor (novelty=True)
  • Ensemble              : equal-weight mean of min-max-normalised scores
  • Adaptive thresholds   : WARNING = P75, CRITICAL = P90 of training-set ensemble scores
                            (recalculated on every retrain — fully data-driven)
  • Retraining            : every RETRAIN_EVERY_N_CYCLES cycles (default 5)
  • Persistence           : results → ai_anomaly_results table in Postgres

Deliberately independent of the rule-based anomaly_worker.
"""

import json
import logging
import math
import os
import sys
import time
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import StandardScaler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("ai-ml-worker")

# ── Configuration ──────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "cloudwalk_transactions"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "admin"),
}

RUN_INTERVAL = int(os.getenv("RUN_INTERVAL_SECONDS", "120"))
RETRAIN_EVERY_N_CYCLES = int(os.getenv("RETRAIN_EVERY_N_CYCLES", "5"))
# Expected fraction of anomalies in the data — controls model sensitivity
CONTAMINATION = float(os.getenv("CONTAMINATION", "0.05"))


# ── Ensemble Model ─────────────────────────────────────────────────────────────

class EnsembleAnomalyDetector:
    """
    Isolation Forest + Local Outlier Factor ensemble.

    No business rules and no hardcoded score thresholds.
    WARNING / CRITICAL boundaries are derived from the P75 / P90 percentiles of
    the ensemble scores computed on the training set itself, so they adapt to
    whatever pattern the data shows.
    """

    def __init__(self, contamination: float = 0.05):
        self.contamination = contamination
        self.if_model = IsolationForest(
            n_estimators=200,
            contamination=contamination,
            random_state=42,
            n_jobs=-1,
        )
        self.lof_model = LocalOutlierFactor(
            n_neighbors=20,
            contamination=contamination,
            novelty=True,       # required to call predict / score_samples on new data
            n_jobs=-1,
        )
        self.scaler = StandardScaler()
        self.fitted = False
        self.model_version: Optional[str] = None
        self.num_training_samples: int = 0

        # Saved from training for deterministic normalisation at predict time
        self._if_neg_min: float = 0.0
        self._if_neg_max: float = 1.0
        self._lof_neg_min: float = 0.0
        self._lof_neg_max: float = 1.0

        # Adaptive thresholds — updated on every retrain
        self.warning_threshold: float = 0.50
        self.critical_threshold: float = 0.75

    # ── Fitting ────────────────────────────────────────────────────────────────

    def fit(self, X: np.ndarray) -> None:
        X_scaled = self.scaler.fit_transform(X)
        self.if_model.fit(X_scaled)
        self.lof_model.fit(X_scaled)

        # Raw anomaly scores on training set (higher = more normal for both models)
        if_raw = self.if_model.score_samples(X_scaled)
        lof_raw = self.lof_model.score_samples(X_scaled)

        # Negate so that "higher value = more anomalous"
        if_neg = -if_raw
        lof_neg = -lof_raw

        # Persist normalisation bounds so predict() uses the same scale
        self._if_neg_min, self._if_neg_max = float(if_neg.min()), float(if_neg.max())
        self._lof_neg_min, self._lof_neg_max = float(lof_neg.min()), float(lof_neg.max())

        # Normalised scores on training data → ensemble
        if_norm = self._norm(if_neg, self._if_neg_min, self._if_neg_max)
        lof_norm = self._norm(lof_neg, self._lof_neg_min, self._lof_neg_max)
        ensemble = 0.5 * if_norm + 0.5 * lof_norm

        # Adaptive thresholds from score distribution on training data
        self.warning_threshold = float(np.percentile(ensemble, 75))
        self.critical_threshold = float(np.percentile(ensemble, 90))

        self.fitted = True
        self.model_version = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        self.num_training_samples = len(X)

        logger.info(
            f"Model fitted | version={self.model_version} | samples={self.num_training_samples} "
            f"| WARNING≥{self.warning_threshold:.3f} | CRITICAL≥{self.critical_threshold:.3f}"
        )

    # ── Prediction ─────────────────────────────────────────────────────────────

    def predict(self, X: np.ndarray) -> list[dict]:
        if not self.fitted:
            raise RuntimeError("Model not fitted — call fit() first.")

        X_scaled = self.scaler.transform(X)
        if_norm = self._norm(
            -self.if_model.score_samples(X_scaled),
            self._if_neg_min, self._if_neg_max,
        )
        lof_norm = self._norm(
            -self.lof_model.score_samples(X_scaled),
            self._lof_neg_min, self._lof_neg_max,
        )
        ensemble = 0.5 * if_norm + 0.5 * lof_norm

        results = []
        for i in range(len(X)):
            score = float(ensemble[i])
            if score >= self.critical_threshold:
                alert_level = "CRITICAL"
                is_anomaly = True
            elif score >= self.warning_threshold:
                alert_level = "WARNING"
                is_anomaly = True
            else:
                alert_level = "NORMAL"
                is_anomaly = False

            results.append(
                {
                    "if_score": float(if_norm[i]),
                    "lof_score": float(lof_norm[i]),
                    "ensemble_score": score,
                    "is_anomaly": is_anomaly,
                    "alert_level": alert_level,
                }
            )
        return results

    @staticmethod
    def _norm(x: np.ndarray, min_val: float, max_val: float) -> np.ndarray:
        rng = max_val - min_val
        if rng < 1e-10:
            return np.zeros_like(x, dtype=float)
        return np.clip((x - min_val) / rng, 0.0, 1.0)


# ── Feature Engineering ────────────────────────────────────────────────────────

FEATURE_COLS = [
    # Raw counts
    "approved", "denied", "failed", "reversed", "backend_reversed", "refunded", "total_count",
    # Rate features (ML-friendly ratios)
    "denial_rate", "failure_rate", "reversal_rate", "approval_rate",
    # Cyclical time encoding — lets models know 23:00 ≈ 00:00
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    # Rolling 30-minute window statistics
    "rolling_denied_30m", "rolling_failed_30m", "rolling_total_30m",
    "stddev_denied_30m", "stddev_total_30m",
    # Auth-code diversity features
    "distinct_auth_codes", "auth_51_rate", "auth_59_rate",
]

PIVOT_SQL = """
    SELECT
        minute_ts,
        approved, denied, failed, reversed, backend_reversed, refunded, total_count,
        COALESCE(denied::float        / NULLIF(total_count, 0), 0) AS denial_rate,
        COALESCE(failed::float        / NULLIF(total_count, 0), 0) AS failure_rate,
        COALESCE((reversed + backend_reversed)::float
                                      / NULLIF(total_count, 0), 0) AS reversal_rate,
        COALESCE(approved::float      / NULLIF(total_count, 0), 0) AS approval_rate,
        AVG(denied::float)       OVER w30 AS rolling_denied_30m,
        AVG(failed::float)       OVER w30 AS rolling_failed_30m,
        AVG(total_count::float)  OVER w30 AS rolling_total_30m,
        COALESCE(STDDEV(denied::float)      OVER w30, 0) AS stddev_denied_30m,
        COALESCE(STDDEV(total_count::float) OVER w30, 0) AS stddev_total_30m
    FROM monitoring_minute_pivot
    WINDOW w30 AS (ORDER BY minute_ts ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    ORDER BY minute_ts
"""

AUTH_SQL = """
    SELECT
        date_trunc('minute', timestamp)                                AS minute_ts,
        COUNT(DISTINCT auth_code)                                      AS distinct_auth_codes,
        SUM(count)                                                     AS total_auth,
        SUM(CASE WHEN auth_code = '51' THEN count ELSE 0 END)         AS auth_51,
        SUM(CASE WHEN auth_code = '59' THEN count ELSE 0 END)         AS auth_59
    FROM transactions_auth_codes
    GROUP BY 1
    ORDER BY 1
"""


def build_feature_dataframe(conn) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (feature_df[FEATURE_COLS], meta_df['minute_ts']) with aligned row indices.
    """
    pivot_df = pd.read_sql(PIVOT_SQL, conn, parse_dates=["minute_ts"])
    auth_df  = pd.read_sql(AUTH_SQL,  conn, parse_dates=["minute_ts"])

    auth_df["auth_51_rate"] = (
        auth_df["auth_51"] / auth_df["total_auth"].replace(0, np.nan)
    ).fillna(0.0)
    auth_df["auth_59_rate"] = (
        auth_df["auth_59"] / auth_df["total_auth"].replace(0, np.nan)
    ).fillna(0.0)

    df = pivot_df.merge(
        auth_df[["minute_ts", "distinct_auth_codes", "auth_51_rate", "auth_59_rate"]],
        on="minute_ts",
        how="left",
    )

    # Cyclical encoding — hour and day-of-week as sin/cos pairs
    h = df["minute_ts"].dt.hour
    d = df["minute_ts"].dt.dayofweek
    df["hour_sin"] = np.sin(2 * math.pi * h / 24)
    df["hour_cos"] = np.cos(2 * math.pi * h / 24)
    df["dow_sin"]  = np.sin(2 * math.pi * d / 7)
    df["dow_cos"]  = np.cos(2 * math.pi * d / 7)

    feature_df = df[FEATURE_COLS].fillna(0.0).reset_index(drop=True)
    meta_df    = df[["minute_ts"]].reset_index(drop=True)
    return feature_df, meta_df


# ── Database Helpers ───────────────────────────────────────────────────────────

ENSURE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ai_anomaly_results (
    id                   SERIAL PRIMARY KEY,
    detected_at          TIMESTAMP DEFAULT NOW(),
    data_timestamp       TIMESTAMP,
    source               VARCHAR(50) DEFAULT 'transactions',
    feature_snapshot     JSONB,
    if_score             DOUBLE PRECISION,
    lof_score            DOUBLE PRECISION,
    ensemble_score       DOUBLE PRECISION,
    is_anomaly           BOOLEAN     NOT NULL,
    alert_level          VARCHAR(20) NOT NULL,
    model_version        VARCHAR(50),
    num_training_samples INTEGER
);
CREATE INDEX IF NOT EXISTS idx_ai_ts    ON ai_anomaly_results(data_timestamp);
CREATE INDEX IF NOT EXISTS idx_ai_level ON ai_anomaly_results(alert_level);
CREATE INDEX IF NOT EXISTS idx_ai_det   ON ai_anomaly_results(detected_at);
"""

INSERT_SQL = """
    INSERT INTO ai_anomaly_results
        (data_timestamp, source, feature_snapshot,
         if_score, lof_score, ensemble_score,
         is_anomaly, alert_level, model_version, num_training_samples)
    VALUES %s
"""

SNAPSHOT_COLS = [
    "denial_rate", "failure_rate", "reversal_rate",
    "total_count", "rolling_denied_30m", "auth_51_rate",
]


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(ENSURE_TABLE_SQL)
    conn.commit()


def get_last_evaluated_ts(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(data_timestamp) FROM ai_anomaly_results")
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def write_results(conn, rows: list[tuple]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows)
    conn.commit()


# ── Detection Cycle ────────────────────────────────────────────────────────────

def run_cycle(
    detector: EnsembleAnomalyDetector,
    cycle_num: int,
    *,
    retrain: bool,
) -> None:
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        ensure_table(conn)

        feature_df, meta_df = build_feature_dataframe(conn)
        if feature_df.empty:
            logger.warning("No feature data available — skipping cycle.")
            return

        X = feature_df.values

        if retrain or not detector.fitted:
            logger.info(f"Retraining on {len(X)} rows…")
            detector.fit(X)

        # Only evaluate rows newer than the last persisted timestamp
        last_ts = get_last_evaluated_ts(conn)
        if last_ts is not None:
            mask = meta_df["minute_ts"] > pd.Timestamp(last_ts)
            new_idx = meta_df.index[mask].tolist()
        else:
            new_idx = list(meta_df.index)

        if not new_idx:
            logger.info("No new rows since last run.")
            return

        new_X    = feature_df.iloc[new_idx].values
        new_meta = meta_df.iloc[new_idx]
        preds    = detector.predict(new_X)

        rows: list[tuple] = []
        for i, (df_idx, pred) in enumerate(zip(new_idx, preds)):
            ts = new_meta.iloc[i]["minute_ts"]
            if isinstance(ts, pd.Timestamp):
                ts = ts.to_pydatetime()

            snapshot = {
                col: round(float(feature_df.iloc[df_idx][col]), 4)
                for col in SNAPSHOT_COLS
            }

            rows.append((
                ts,
                "transactions",
                json.dumps(snapshot),
                pred["if_score"],
                pred["lof_score"],
                pred["ensemble_score"],
                pred["is_anomaly"],
                pred["alert_level"],
                detector.model_version,
                detector.num_training_samples,
            ))

        write_results(conn, rows)

        n_warn = sum(1 for r in rows if r[7] == "WARNING")
        n_crit = sum(1 for r in rows if r[7] == "CRITICAL")
        logger.info(
            f"Cycle {cycle_num} | Rows={len(rows)} "
            f"| Anomalies={n_warn + n_crit} "
            f"| WARNING={n_warn} | CRITICAL={n_crit}"
        )

    except Exception:
        logger.exception("Error during AI-ML detection cycle")
    finally:
        conn.close()


# ── Entry Point ────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("AI/ML Anomaly Detection Worker starting…")
    logger.info(
        f"DB={DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} | "
        f"interval={RUN_INTERVAL}s | retrain_every={RETRAIN_EVERY_N_CYCLES} cycles | "
        f"contamination={CONTAMINATION}"
    )

    detector = EnsembleAnomalyDetector(contamination=CONTAMINATION)
    cycle = 0

    while True:
        cycle += 1
        retrain = (cycle % RETRAIN_EVERY_N_CYCLES == 1)   # cycles 1, 6, 11, …
        logger.info(f"─── Cycle {cycle} {'[RETRAIN]' if retrain else ''} ───")
        run_cycle(detector, cycle, retrain=retrain)
        logger.info(f"Sleeping {RUN_INTERVAL}s…")
        time.sleep(RUN_INTERVAL)


if __name__ == "__main__":
    main()
