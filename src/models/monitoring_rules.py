"""
Mandatory monitoring rules (challenge requirements).

Alerts when per-minute counts are **above normal** for:
  - denied transactions
  - failed transactions
  - reversed transactions (reversed and/or backend_reversed)

These are evaluated with Z-scores against historical means from training data,
combined with the hybrid score-based model in HybridAnomalyDetector.
"""

from __future__ import annotations

from typing import Any

# Rule ids returned in API / model output
RULE_DENIED_ABOVE_NORMAL = "ALERT_DENIED_ABOVE_NORMAL"
RULE_FAILED_ABOVE_NORMAL = "ALERT_FAILED_ABOVE_NORMAL"
RULE_REVERSED_ABOVE_NORMAL = "ALERT_REVERSED_ABOVE_NORMAL"
RULE_BACKEND_REVERSED_ABOVE_NORMAL = "ALERT_BACKEND_REVERSED_ABOVE_NORMAL"

MANDATORY_RULE_IDS = (
    RULE_DENIED_ABOVE_NORMAL,
    RULE_FAILED_ABOVE_NORMAL,
    RULE_REVERSED_ABOVE_NORMAL,
    RULE_BACKEND_REVERSED_ABOVE_NORMAL,
)


def z_score(value: float, mean: float, std: float) -> float:
    if std is None or std <= 0:
        return 0.0
    return (value - mean) / std


def mandatory_status_rules(
    row: Any,
    status_stats: dict[str, dict[str, float]],
    z_threshold: float,
) -> tuple[list[str], dict[str, float]]:
    """
    For one minute's status counts (pivot row), return mandatory rule ids that fired
    and z-scores for denied, failed, reversed, backend_reversed.
    """
    triggered: list[str] = []
    z_map: dict[str, float] = {}

    def get_count(name: str) -> float:
        if name in row.index:
            return float(row[name])
        return 0.0

    for status, rule_id in (
        ("denied", RULE_DENIED_ABOVE_NORMAL),
        ("failed", RULE_FAILED_ABOVE_NORMAL),
        ("reversed", RULE_REVERSED_ABOVE_NORMAL),
        ("backend_reversed", RULE_BACKEND_REVERSED_ABOVE_NORMAL),
    ):
        if status not in status_stats:
            continue
        stats = status_stats[status]
        std = float(stats["std"])
        mean = float(stats["mean"])
        cnt = get_count(status)
        z = z_score(cnt, mean, std)
        z_map[status] = round(z, 3)
        if std > 0 and z > z_threshold:
            triggered.append(rule_id)

    return triggered, z_map


def build_recommendation(
    alert_level: str,
    mandatory_triggered: list[str],
    other_rules: list[str],
) -> str:
    """Human-readable recommendation for operators."""
    if alert_level == "NORMAL":
        return (
            "No action required. Denied, failed, and reversal volumes are within "
            "expected ranges relative to historical baselines."
        )

    parts: list[str] = []

    if RULE_DENIED_ABOVE_NORMAL in mandatory_triggered:
        parts.append(
            "Denied transactions are above normal — review auth codes (e.g. 51 vs 59), "
            "issuer behaviour, and fraud filters."
        )
    if RULE_FAILED_ABOVE_NORMAL in mandatory_triggered:
        parts.append(
            "Failed transactions are above normal — check connectivity to processors, "
            "timeouts, and gateway error rates."
        )
    if RULE_REVERSED_ABOVE_NORMAL in mandatory_triggered or RULE_BACKEND_REVERSED_ABOVE_NORMAL in mandatory_triggered:
        parts.append(
            "Reversal-related counts are above normal — review chargebacks, disputes, "
            "and settlement/backend reversal flows."
        )

    if not parts:
        parts.append(
            "Anomaly score elevated by the hybrid model (Z-score + isolation forest); "
            "review triggered technical rules and minute-level volumes."
        )

    if alert_level == "CRITICAL":
        parts.append("Severity CRITICAL: escalate on-call and open an incident if sustained.")
    elif alert_level == "WARNING":
        parts.append("Severity WARNING: monitor closely and prepare escalation.")

    if other_rules:
        parts.append(f"Additional signals: {', '.join(other_rules[:5])}.")

    return " ".join(parts)
