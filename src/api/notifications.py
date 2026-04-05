"""
CloudWalk Monitoring — Alert Notification System

Channels:
  - console  (always available)
  - slack    (requires SLACK_WEBHOOK_URL)
  - email    (requires SMTP_HOST + ALERT_EMAIL_TO)

Configure via environment variables:
  SLACK_WEBHOOK_URL  — Slack incoming webhook URL
  SMTP_HOST          — Hosted SMTP hostname (e.g. SendGrid, Brevo, Resend, Gmail — no self-hosted server)
  SMTP_PORT          — SMTP port (587 with STARTTLS, or 465 with SMTP_SSL)
  SMTP_SSL           — If "true"/"1", use implicit TLS on SMTP_PORT (typical for port 465)
  SMTP_USER          — SMTP username
  SMTP_PASS          — SMTP password or API key (provider-specific)
  ALERT_EMAIL_TO     — Recipient email address
  ALERT_EMAIL_FROM   — Sender address (must be allowed by your provider)
"""

import os
import json
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logger = logging.getLogger("monitoring-alerts")


class AlertNotifier:
    """Multi-channel alert notification dispatcher."""

    VALID_CHANNELS = {"console", "slack", "email"}

    def __init__(self):
        self.slack_webhook = (os.getenv("SLACK_WEBHOOK_URL") or "").strip() or None
        self.smtp_host = (os.getenv("SMTP_HOST") or "").strip() or None
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_ssl = os.getenv("SMTP_SSL", "").strip().lower() in ("1", "true", "yes")
        self.smtp_user = (os.getenv("SMTP_USER") or "").strip() or None
        self.smtp_pass = (os.getenv("SMTP_PASS") or "").strip() or None
        self.email_to = (os.getenv("ALERT_EMAIL_TO") or "").strip() or None
        self.email_from = (os.getenv("ALERT_EMAIL_FROM") or "monitoring@cloudwalk.io").strip()
        self.smtp_debug = os.getenv("SMTP_DEBUG", "").strip().lower() in ("1", "true", "yes")

    def available_channels(self) -> list[str]:
        channels = ["console"]
        if self.slack_webhook:
            channels.append("slack")
        if self.smtp_host and self.email_to and self.smtp_pass:
            channels.append("email")
        return channels

    def notify(
        self,
        alert_level: str,
        timestamp: str,
        score: float,
        triggered_rules: list[str],
        details: Optional[dict] = None,
    ):
        """Send alert through ALL configured channels."""
        for ch in self.available_channels():
            self.send_channel(ch, alert_level, timestamp, score, triggered_rules, details)

    def send_channel(
        self,
        channel: str,
        alert_level: str,
        timestamp: str,
        score: float,
        triggered_rules: list[str],
        details: Optional[dict] = None,
    ):
        """Send alert through a SPECIFIC channel. Raises on failure."""
        channel = channel.lower()
        if channel not in self.VALID_CHANNELS:
            raise ValueError(f"Unknown channel '{channel}'. Valid: {self.VALID_CHANNELS}")

        message = self._format_message(alert_level, timestamp, score, triggered_rules, details)

        if channel == "console":
            self._log_alert(alert_level, message)
        elif channel == "slack":
            if not self.slack_webhook:
                raise RuntimeError("SLACK_WEBHOOK_URL not configured")
            self._send_slack(alert_level, timestamp, score, triggered_rules, details)
        elif channel == "email":
            if not self.smtp_host or not self.email_to:
                raise RuntimeError("SMTP_HOST / ALERT_EMAIL_TO not configured")
            if not self.smtp_pass:
                raise RuntimeError(
                    "SMTP_PASS is empty — SendGrid needs the API key as password "
                    "(username must be the literal: apikey)"
                )
            self._send_email(alert_level, timestamp, message)

    def _format_message(self, level, timestamp, score, rules, details):
        rules_clean = [r for r in rules if r]
        lines = [
            f"[{level}] Transaction Anomaly Detected",
            f"Timestamp: {timestamp}",
            f"Anomaly Score: {score:.3f}",
            f"Triggered Rules: {', '.join(rules_clean) if rules_clean else 'N/A'}",
        ]
        if details:
            counts = details.get("counts")
            if counts and isinstance(counts, dict):
                lines.append(f"Counts: {json.dumps(counts)}")
        return "\n".join(lines)

    def _log_alert(self, level, message):
        if level == "CRITICAL":
            logger.critical(message)
        elif level == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)

    def _send_slack(self, level, timestamp, score, rules, details):
        if not HAS_REQUESTS:
            raise RuntimeError("requests library not installed")

        color = "#e74c3c" if level == "CRITICAL" else "#f39c12"
        emoji = "CRITICAL" if level == "CRITICAL" else "WARNING"
        rules_clean = [r for r in rules if r]

        payload = {
            "attachments": [{
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"[{emoji}] Transaction Anomaly",
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Timestamp:*\n{timestamp}"},
                            {"type": "mrkdwn", "text": f"*Score:*\n{score:.3f}"},
                            {"type": "mrkdwn", "text": f"*Rules:*\n{', '.join(rules_clean) or 'N/A'}"},
                        ]
                    },
                ]
            }]
        }

        if details and details.get("counts") and isinstance(details["counts"], dict):
            count_str = " | ".join(f"{k}: {v}" for k, v in details["counts"].items())
            payload["attachments"][0]["blocks"].append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Counts:* {count_str}"}
            })

        resp = requests.post(self.slack_webhook, json=payload, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(f"Slack returned {resp.status_code}: {resp.text}")

    def _send_email(self, level, timestamp, message):
        msg = MIMEMultipart()
        msg["From"] = self.email_from
        msg["To"] = self.email_to
        # ASCII subject avoids encoding issues with some SMTP relays
        msg["Subject"] = f"[{level}] CloudWalk Transaction Alert - {timestamp}"
        msg.attach(MIMEText(message, "plain"))

        logger.info(
            "Sending email alert via %s:%s (SSL=%s) to %s",
            self.smtp_host,
            self.smtp_port,
            self.smtp_ssl,
            self.email_to,
        )

        if self.smtp_ssl:
            with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port) as server:
                if self.smtp_debug:
                    server.set_debuglevel(1)
                server.login(self.smtp_user, self.smtp_pass)
                server.send_message(msg)
        else:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.smtp_debug:
                    server.set_debuglevel(1)
                server.starttls()
                server.login(self.smtp_user, self.smtp_pass)
                server.send_message(msg)
        logger.info(f"Email alert sent to {self.email_to}")


notifier = AlertNotifier()
