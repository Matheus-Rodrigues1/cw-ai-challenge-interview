"""
CloudWalk Monitoring — Hybrid Anomaly Detection Model

Combines two approaches:
  1. Z-Score (rule-based) — flags individual status counts that deviate
     more than N standard deviations from the rolling mean.
  2. Isolation Forest (ML-based) — unsupervised model trained on
     multi-dimensional transaction features per minute.

The final anomaly_score is a weighted blend of both methods.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from dataclasses import dataclass
from typing import Optional

from models.monitoring_rules import mandatory_status_rules


@dataclass
class AnomalyResult:
    """Result returned for each evaluated minute."""
    timestamp: str
    is_anomaly: bool
    anomaly_score: float          # 0.0 (normal) → 1.0 (severe anomaly)
    alert_level: str              # NORMAL | WARNING | CRITICAL
    triggered_rules: list         # which rules flagged it
    details: dict                 # per-status breakdown


class HybridAnomalyDetector:
    """
    Hybrid anomaly detector combining Z-Score rules with Isolation Forest.

    Parameters
    ----------
    z_threshold : float
        Z-score above which a single status is considered anomalous.
    iso_contamination : float
        Expected proportion of anomalies in the dataset (Isolation Forest param).
    rolling_window : int
        Minutes of history to compute rolling statistics.
    warning_score : float
        Score threshold to trigger WARNING level.
    critical_score : float
        Score threshold to trigger CRITICAL level.
    z_weight : float
        Weight of Z-Score component in final score (0-1).
    """

    def __init__(
        self,
        z_threshold: float = 2.5,
        iso_contamination: float = 0.05,
        rolling_window: int = 30,
        warning_score: float = 0.5,
        critical_score: float = 0.75,
        z_weight: float = 0.6,
    ):
        self.z_threshold = z_threshold
        self.iso_contamination = iso_contamination
        self.rolling_window = rolling_window
        self.warning_score = warning_score
        self.critical_score = critical_score
        self.z_weight = z_weight
        self.iso_weight = 1 - z_weight

        self.scaler = StandardScaler()
        self.iso_forest = IsolationForest(
            contamination=iso_contamination,
            n_estimators=200,
            random_state=42,
            n_jobs=-1,
        )

        # learned statistics
        self._status_stats: dict = {}
        self._is_fitted: bool = False

    # ── Training ────────────────────────────────────────────────────────
    def fit(self, transactions_df: pd.DataFrame) -> "HybridAnomalyDetector":
        """
        Fit the model on historical transaction data.

        Parameters
        ----------
        transactions_df : DataFrame
            Columns: timestamp, status, count
        """
        pivot = transactions_df.pivot_table(
            index="timestamp", columns="status", values="count", fill_value=0
        ).sort_index()

        # Learn per-status statistics
        for col in pivot.columns:
            self._status_stats[col] = {
                "mean": pivot[col].mean(),
                "std": pivot[col].std(),
                "q99": pivot[col].quantile(0.99),
            }

        # Build feature matrix for Isolation Forest
        features = self._build_features(pivot)
        scaled = self.scaler.fit_transform(features)
        self.iso_forest.fit(scaled)
        self._is_fitted = True
        self._feature_columns = features.columns.tolist()

        return self

    def _build_features(self, pivot: pd.DataFrame) -> pd.DataFrame:
        """Create feature matrix from pivoted transaction data."""
        features = pivot.copy()

        # Add ratio features
        total = features.sum(axis=1).replace(0, 1)
        for col in pivot.columns:
            features[f"{col}_ratio"] = features[col] / total

        # Add rolling statistics
        for col in ["denied", "failed", "reversed", "backend_reversed"]:
            if col in pivot.columns:
                features[f"{col}_rolling_mean"] = (
                    pivot[col].rolling(self.rolling_window, min_periods=1).mean()
                )
                features[f"{col}_rolling_std"] = (
                    pivot[col].rolling(self.rolling_window, min_periods=1).std().fillna(0)
                )

        # Add approval rate
        if "approved" in pivot.columns and "denied" in pivot.columns:
            denom = pivot["approved"] + pivot["denied"]
            features["approval_rate"] = (
                pivot["approved"] / denom.replace(0, 1)
            )

        return features.fillna(0)

    # ── Prediction ──────────────────────────────────────────────────────
    def evaluate(self, transactions_df: pd.DataFrame) -> list[AnomalyResult]:
        """
        Evaluate transactions and return anomaly results per minute.

        Parameters
        ----------
        transactions_df : DataFrame
            Same format as fit() input.
        """
        if not self._is_fitted:
            raise RuntimeError("Model not fitted. Call fit() first.")

        pivot = transactions_df.pivot_table(
            index="timestamp", columns="status", values="count", fill_value=0
        ).sort_index()

        features = self._build_features(pivot)

        # Ensure same columns as training
        for col in self._feature_columns:
            if col not in features.columns:
                features[col] = 0
        features = features[self._feature_columns]

        scaled = self.scaler.transform(features)

        # Isolation Forest scores (-1 = anomaly, 1 = normal)
        iso_raw = self.iso_forest.decision_function(scaled)
        # Normalize to 0-1 (lower decision = more anomalous)
        iso_scores = 1 - (iso_raw - iso_raw.min()) / (iso_raw.max() - iso_raw.min() + 1e-10)

        results = []
        for i, (ts, row) in enumerate(pivot.iterrows()):
            z_scores = {}
            triggered = []

            mandatory, _mand_z = mandatory_status_rules(
                row, self._status_stats, self.z_threshold
            )
            triggered.extend(mandatory)

            for status in ["denied", "failed", "reversed", "backend_reversed"]:
                if status in row.index and status in self._status_stats:
                    stats = self._status_stats[status]
                    if stats["std"] > 0:
                        z = (row[status] - stats["mean"]) / stats["std"]
                        z_scores[status] = round(z, 2)

            # Check approved drops (negative z-score)
            if "approved" in row.index and "approved" in self._status_stats:
                stats = self._status_stats["approved"]
                if stats["std"] > 0:
                    z = (row["approved"] - stats["mean"]) / stats["std"]
                    z_scores["approved"] = round(z, 2)
                    if z < -self.z_threshold:
                        triggered.append(f"approved_drop_zscore_{z:.1f}")

            # Combine scores
            max_z = max([abs(v) for v in z_scores.values()], default=0)
            z_component = min(max_z / 10.0, 1.0)  # normalize to 0-1
            iso_component = iso_scores[i]

            final_score = round(
                self.z_weight * z_component + self.iso_weight * iso_component,
                3,
            )

            # Mandatory business rules: denied / failed / reversed above normal
            if mandatory:
                final_score = max(final_score, self.warning_score)
                if len(mandatory) >= 2:
                    final_score = max(final_score, min(1.0, self.critical_score))

            final_score = round(final_score, 3)

            # Determine alert level
            if final_score >= self.critical_score:
                level = "CRITICAL"
            elif final_score >= self.warning_score:
                level = "WARNING"
            else:
                level = "NORMAL"

            results.append(AnomalyResult(
                timestamp=str(ts),
                is_anomaly=level != "NORMAL",
                anomaly_score=final_score,
                alert_level=level,
                triggered_rules=triggered,
                details={
                    "z_scores": z_scores,
                    "iso_score": round(iso_component, 3),
                    "z_component": round(z_component, 3),
                    "counts": {k: int(v) for k, v in row.items()},
                    "mandatory_rules": mandatory,
                },
            ))

        return results

    # ── Single-point evaluation (for API endpoint) ──────────────────────
    def evaluate_single(self, transaction_data: dict) -> AnomalyResult:
        """
        Evaluate a single minute of transaction data.

        Parameters
        ----------
        transaction_data : dict
            Keys are statuses, values are counts.
            Example: {"approved": 110, "denied": 45, "failed": 3, ...}
        """
        if not self._is_fitted:
            raise RuntimeError("Model not fitted. Call fit() first.")

        ts = str(transaction_data.get("timestamp", pd.Timestamp.now()))
        # Align to known statuses from training
        row_data: dict[str, float] = {}
        for st in self._status_stats:
            row_data[st] = float(transaction_data.get(st, 0))
        for k, v in transaction_data.items():
            if k == "timestamp":
                continue
            if k not in row_data:
                row_data[k] = float(v)

        row = pd.Series(row_data)
        z_scores: dict[str, float] = {}
        triggered: list[str] = []

        mandatory, _ = mandatory_status_rules(row, self._status_stats, self.z_threshold)
        triggered.extend(mandatory)

        for status in ["denied", "failed", "reversed", "backend_reversed", "approved"]:
            if status not in self._status_stats:
                continue
            stats = self._status_stats[status]
            cnt = float(row.get(status, 0))
            if stats["std"] and stats["std"] > 0:
                z = (cnt - stats["mean"]) / stats["std"]
                z_scores[status] = round(z, 2)
                if status == "approved" and z < -self.z_threshold:
                    triggered.append(f"approved_drop_zscore_{z:.1f}")

        max_z = max([abs(v) for v in z_scores.values()], default=0)
        z_component = min(max_z / 10.0, 1.0)
        # Single-minute evaluation: no time window for isolation forest — use neutral blend
        iso_component = 0.5
        final_score = round(
            self.z_weight * z_component + self.iso_weight * iso_component,
            3,
        )
        if mandatory:
            final_score = max(final_score, self.warning_score)
            if len(mandatory) >= 2:
                final_score = max(final_score, min(1.0, self.critical_score))
        final_score = round(final_score, 3)

        if final_score >= self.critical_score:
            level = "CRITICAL"
        elif final_score >= self.warning_score:
            level = "WARNING"
        else:
            level = "NORMAL"

        counts_int = {k: int(v) for k, v in row_data.items()}
        return AnomalyResult(
            timestamp=ts,
            is_anomaly=level != "NORMAL",
            anomaly_score=final_score,
            alert_level=level,
            triggered_rules=triggered,
            details={
                "z_scores": z_scores,
                "iso_score": iso_component,
                "z_component": round(z_component, 3),
                "counts": counts_int,
                "mandatory_rules": mandatory,
                "evaluation_mode": "single_minute",
            },
        )


# ── Convenience: Train from CSV ─────────────────────────────────────────
def train_from_csv(csv_path: str) -> HybridAnomalyDetector:
    """Load CSV and return a fitted detector."""
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    model = HybridAnomalyDetector()
    model.fit(df)
    return model


if __name__ == "__main__":
    import os

    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "transactions.csv")
    df = pd.read_csv(data_path, parse_dates=["timestamp"])

    print("Training hybrid anomaly detector...")
    detector = HybridAnomalyDetector()
    detector.fit(df)

    print("Evaluating full dataset...")
    results = detector.evaluate(df)

    anomalies = [r for r in results if r.is_anomaly]
    critical = [r for r in results if r.alert_level == "CRITICAL"]
    warning = [r for r in results if r.alert_level == "WARNING"]

    print(f"\nTotal minutes evaluated: {len(results)}")
    print(f"Anomalies detected:     {len(anomalies)} ({len(anomalies)/len(results)*100:.1f}%)")
    print(f"  CRITICAL:             {len(critical)}")
    print(f"  WARNING:              {len(warning)}")

    print("\n── Top 10 CRITICAL anomalies ──")
    critical_sorted = sorted(critical, key=lambda r: r.anomaly_score, reverse=True)
    for r in critical_sorted[:10]:
        print(f"  {r.timestamp} | score={r.anomaly_score:.3f} | rules={r.triggered_rules}")
