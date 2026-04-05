#!/usr/bin/env python3
"""
Generate docs/architecture_overview.pdf -- a well-formatted architecture
reference for the CloudWalk Transaction Monitoring System.

Usage:
    py -3 docs/generate_architecture_pdf.py

Requires: fpdf2 >= 2.7   (pip install fpdf2)
"""

import os
from fpdf import FPDF

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_FILE = os.path.join(OUT_DIR, "architecture_overview.pdf")

# Colors
DARK_BLUE      = (25,  55,  95)
MID_BLUE       = (41,  98, 168)
LIGHT_BLUE_BG  = (232, 240, 254)
TEAL           = (0,  105,  92)
LIGHT_TEAL_BG  = (224, 242, 241)
WHITE          = (255, 255, 255)
BLACK          = (30,  30,  30)
GRAY           = (100, 100, 100)
LIGHT_GRAY     = (245, 245, 245)
TABLE_HEADER_BG = (41, 98, 168)
TABLE_ALT_BG   = (240, 245, 255)
TEAL_HEADER_BG = (0,  137, 123)


class ArchPDF(FPDF):

    def header(self):
        if self.page_no() == 1:
            return
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*MID_BLUE)
        self.cell(0, 8, "CloudWalk Transaction Monitoring - Architecture Overview", align="L")
        self.set_draw_color(*LIGHT_BLUE_BG)
        self.set_line_width(0.5)
        self.line(self.l_margin, self.get_y() + 8, self.w - self.r_margin, self.get_y() + 8)
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(*GRAY)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def ensure_space(self, min_h=50):
        """Add a new page if remaining vertical space is less than min_h mm."""
        if self.get_y() + min_h > self.h - self.b_margin:
            self.add_page()

    def section_heading(self, number, title, teal=False):
        self.ensure_space(50)
        self.ln(6)
        self.set_font("Helvetica", "B", 14)
        self.set_fill_color(*(TEAL if teal else DARK_BLUE))
        self.set_text_color(*WHITE)
        w = self.w - self.l_margin - self.r_margin
        self.cell(w, 10, f"  {number}. {title}", fill=True, new_x="LMARGIN", new_y="NEXT", border=0, align="L")
        self.ln(4)
        self.set_text_color(*BLACK)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10.5)
        self.set_text_color(*BLACK)
        w = self.w - self.l_margin - self.r_margin
        self.multi_cell(w, 6, text, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def sub_heading(self, text, teal=False):
        self.ln(2)
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(*(TEAL if teal else MID_BLUE))
        w = self.w - self.l_margin - self.r_margin
        self.cell(w, 8, text, new_x="LMARGIN", new_y="NEXT", align="L")
        self.set_draw_color(*(TEAL if teal else MID_BLUE))
        self.set_line_width(0.3)
        self.line(self.l_margin, self.get_y(), self.l_margin + w, self.get_y())
        self.ln(2)
        self.set_text_color(*BLACK)

    def badge(self, text, teal=False):
        """Small coloured label for inline callouts."""
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(*(LIGHT_TEAL_BG if teal else LIGHT_BLUE_BG))
        self.set_text_color(*(TEAL if teal else MID_BLUE))
        self.cell(0, 7, f"  {text}  ", fill=True, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)
        self.set_text_color(*BLACK)

    def service_block(self, name, port, description, teal=False):
        self.ln(2)
        x_start = self.get_x()
        y_start = self.get_y()
        w = self.w - self.l_margin - self.r_margin
        
        label = f"{name}  (port {port})" if port else f"{name}  (no exposed port)"
        
        self.set_font("Helvetica", "", 10)
        lines = self.multi_cell(w - 8, 5.5, description, dry_run=True, output="LINES")
        h = len(lines) * 5.5 + 10
        
        if y_start + h > self.h - 20:
            self.add_page()
            x_start = self.get_x()
            y_start = self.get_y()

        self.set_fill_color(*(LIGHT_TEAL_BG if teal else LIGHT_BLUE_BG))
        self.rect(x_start, y_start, w, h, "F")
        self.set_fill_color(*(TEAL if teal else MID_BLUE))
        self.rect(x_start, y_start, 2, h, "F")
        
        self.set_xy(x_start + 6, y_start + 2)
        self.set_font("Helvetica", "B", 10.5)
        self.set_text_color(*(TEAL if teal else MID_BLUE))
        self.cell(w - 6, 6, label, new_x="LMARGIN", new_y="NEXT")
        
        self.set_x(x_start + 6)
        self.set_font("Helvetica", "", 10)
        self.set_text_color(*BLACK)
        self.multi_cell(w - 8, 5.5, description, new_x="LMARGIN", new_y="NEXT")
        self.set_y(y_start + h + 2)

    def table(self, headers, rows, col_widths=None, teal_header=False):
        w = self.w - self.l_margin - self.r_margin
        if col_widths is None:
            col_widths = [w / len(headers)] * len(headers)

        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(*(TEAL_HEADER_BG if teal_header else TABLE_HEADER_BG))
        self.set_text_color(*WHITE)
        self.set_draw_color(*WHITE)
        self.set_line_width(0.2)
        
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 8, f"  {h}", border=1, fill=True, align="L")
        self.ln()

        self.set_font("Helvetica", "", 9.5)
        self.set_text_color(*BLACK)
        self.set_draw_color(230, 230, 230)
        
        for row_idx, row in enumerate(rows):
            max_lines = 1
            for i, cell in enumerate(row):
                lines = self.multi_cell(col_widths[i], 6, f"  {cell}", border=0, dry_run=True, output="LINES")
                max_lines = max(max_lines, len(lines))

            row_h = max_lines * 6 + 4
            x_start = self.get_x()
            y_start = self.get_y()

            if y_start + row_h > self.h - 20:
                self.add_page()
                y_start = self.get_y()
                x_start = self.get_x()

            for i, cell in enumerate(row):
                self.set_xy(x_start + sum(col_widths[:i]), y_start)
                self.set_fill_color(*(TABLE_ALT_BG if row_idx % 2 == 1 else WHITE))
                self.rect(x_start + sum(col_widths[:i]), y_start, col_widths[i], row_h, "DF")
                
                self.set_xy(x_start + sum(col_widths[:i]), y_start + 2)
                self.multi_cell(col_widths[i], 6, f"  {cell}", border=0)

            self.set_xy(x_start, y_start + row_h)
        self.ln(4)

    def bullet(self, text, indent=6):
        self.set_font("Helvetica", "", 10.5)
        self.set_text_color(*BLACK)
        x = self.get_x()
        self.set_x(x + indent)
        w = self.w - self.l_margin - self.r_margin - indent
        self.multi_cell(w, 6, text, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def code_block(self, text):
        self.ln(2)
        self.set_font("Courier", "", 9.5)
        self.set_fill_color(*LIGHT_GRAY)
        self.set_draw_color(210, 210, 210)
        self.set_text_color(50, 50, 50)
        w = self.w - self.l_margin - self.r_margin
        
        lines = text.split("\n")
        h = len(lines) * 5.5 + 6
        
        x_start = self.get_x()
        y_start = self.get_y()
        if y_start + h > self.h - 20:
            self.add_page()
            y_start = self.get_y()
            x_start = self.get_x()
            
        self.rect(x_start, y_start, w, h, "DF")
        self.set_xy(x_start + 4, y_start + 3)
        self.multi_cell(w - 8, 5.5, text, border=0, align="L", new_x="LMARGIN", new_y="NEXT")
        self.set_y(y_start + h + 4)
        self.set_text_color(*BLACK)


def build_pdf() -> str:
    pdf = ArchPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.set_left_margin(18)
    pdf.set_right_margin(18)
    w = 210 - 18 - 18   # A4 width minus margins

    # ── Cover / Title ────────────────────────────────────────────────────
    pdf.add_page()
    pdf.set_fill_color(*LIGHT_BLUE_BG)
    pdf.rect(0, 0, 210, 100, "F")

    pdf.ln(35)
    pdf.set_font("Helvetica", "B", 26)
    pdf.set_text_color(*DARK_BLUE)
    pdf.multi_cell(w, 14, "CloudWalk Transaction\nMonitoring System", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)

    pdf.set_draw_color(*MID_BLUE)
    pdf.set_line_width(1)
    pdf.line(60, pdf.get_y(), 150, pdf.get_y())
    pdf.ln(8)

    pdf.set_font("Helvetica", "I", 16)
    pdf.set_text_color(*MID_BLUE)
    pdf.cell(w, 8, "Architecture Overview", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(35)

    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(*BLACK)
    pdf.multi_cell(w, 6.5, (
        "This document describes the architecture, data flow, "
        "environment configuration, and API surface of the "
        "CloudWalk Transaction Monitoring System -- a near-real-time "
        "anomaly detection platform for payment transactions using both "
        "rule-based hybrid detection and a pure ML ensemble."
    ), align="C", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(35)
    pdf.set_font("Helvetica", "I", 9.5)
    pdf.set_text_color(*GRAY)
    pdf.cell(w, 5, "Generated by docs/generate_architecture_pdf.py", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(w, 5, "Source of truth: code, docker-compose.yaml, .env.example", align="C", new_x="LMARGIN", new_y="NEXT")

    # ── 1. System Summary ────────────────────────────────────────────────
    pdf.add_page()
    pdf.section_heading("1", "System Summary")
    pdf.body_text(
        "The system monitors payment-transaction volumes in near real time using two "
        "independent anomaly-detection pipelines. It ingests per-minute transaction "
        "counts from CSV files into PostgreSQL and runs:"
    )
    pdf.bullet("- A rule-based hybrid detector (Z-Score + Isolation Forest + mandatory "
               "business rules) on a 60-second loop, writing results to anomaly_results.")
    pdf.bullet("- A pure ML ensemble (Isolation Forest + LOF + One-Class SVM + Autoencoder) on a "
               "120-second loop with data-driven thresholds, writing results to "
               "ai_anomaly_results. No hardcoded rules or fixed score cutoffs.")
    pdf.body_text(
        "Alerts are dispatched through console and SMTP email (SendGrid) when denied, "
        "failed, or reversed transactions exceed normal levels. "
        "Everything is containerized with Docker Compose (7 services) and "
        "requires only 'docker compose up -d --build' to start."
    )

    # ── 2. Services ──────────────────────────────────────────────────────
    pdf.section_heading("2", "Services (Docker Compose -- 7 containers)")
    pdf.service_block("postgres_db", "5432",
        "PostgreSQL 15. Schema + data loaded on first boot via init.sql "
        "(COPY from /csv_data CSVs). Tables: transactions, transactions_auth_codes, "
        "checkout, transactions_db, anomaly_results, ai_anomaly_results, "
        "notification_log. Views: monitoring_minute_pivot, monitoring_minute_with_rollups.")
    pdf.service_block("pgadmin", "8080",
        "pgAdmin 4 web UI. Login: admin@admin.com / admin.")
    pdf.service_block("metabase", "3000",
        "Metabase BI dashboards. Hosts 4 dashboards querying cloudwalk_transactions "
        "directly. Upload scripts are idempotent (delete-then-recreate on each run).")
    pdf.service_block("monitoring_api", "8000",
        "FastAPI. 11 REST endpoints: health, transactions (query + evaluate), "
        "anomalies (list + unprocessed + acknowledge), notifications "
        "(send + history), stats. Trains HybridAnomalyDetector on startup.")
    pdf.service_block("anomaly_worker", None,
        "Rule-based detection loop (default 60s). Loads all transactions, fits the "
        "HybridAnomalyDetector, evaluates only minutes newer than the last run, "
        "and inserts results into anomaly_results.")
    pdf.service_block("monitoring_worker", None,
        "Notification loop (default 30s). Polls GET /anomalies/unprocessed, "
        "selects channels (CRITICAL_CHANNELS or WARNING_CHANNELS), and calls "
        "POST /notifications/send per anomaly.")
    pdf.service_block("worker_ai_ml", None,
        "Pure ML detection loop (default 120s). Runs an Isolation Forest + "
        "LOF ensemble on all available data. Retrains every "
        "RETRAIN_EVERY_N_CYCLES cycles. Thresholds (WARNING/CRITICAL) are "
        "computed as P75/P90 of training-set ensemble scores -- no hardcoded values. "
        "Results go to ai_anomaly_results. Auto-sync: every cycle cross-references "
        "anomaly_results to detect and fill gaps, ensuring the AI dashboard stays "
        "in lock-step with rule-based detection and manual inserts.",
        teal=True)

    # ── 3. Rule-Based Detection Model ────────────────────────────────────
    pdf.section_heading("3", "Rule-Based Detection Model (anomaly_worker)")
    pdf.sub_heading("Scoring formula  (src/models/anomaly_detector.py)")
    pdf.body_text(
        "Every minute receives a final_score from 0.0 (normal) to 1.0 (severe):"
    )
    pdf.code_block(
        "final_score = 0.6 x z_component  +  0.4 x iso_component\n\n"
        "  z_component   = min(max_abs_z / 10.0, 1.0)\n"
        "                  Largest absolute Z-score across all statuses, capped at 1.\n\n"
        "  iso_component = 1 - (raw_IF_score - min) / (max - min + 1e-10)\n"
        "                  Isolation Forest decision_function, inverted and\n"
        "                  min-max normalised: 1 = most anomalous."
    )
    pdf.body_text(
        "Z-scores are computed against the global mean and std learned from the full "
        "training set (all rows in the transactions table). Weights are configurable "
        "via Z_WEIGHT (default 0.6) and ISO_CONTAMINATION (default 0.05)."
    )

    pdf.sub_heading("Mandatory business rules  (src/models/monitoring_rules.py)")
    pdf.body_text(
        "Each rule fires independently when the per-minute Z-score exceeds Z_THRESHOLD "
        "(default 2.5). Rules are checked for: denied, failed, reversed, backend_reversed."
    )
    pdf.table(
        ["Rule ID", "Status monitored", "Fires when"],
        [
            ["ALERT_DENIED_ABOVE_NORMAL",           "denied",           "denied Z-score > 2.5"],
            ["ALERT_FAILED_ABOVE_NORMAL",            "failed",           "failed Z-score > 2.5"],
            ["ALERT_REVERSED_ABOVE_NORMAL",          "reversed",         "reversed Z-score > 2.5"],
            ["ALERT_BACKEND_REVERSED_ABOVE_NORMAL",  "backend_reversed", "backend_reversed Z-score > 2.5"],
        ],
        col_widths=[w * 0.47, w * 0.23, w * 0.30],
    )
    pdf.body_text(
        "Bonus signal (not a mandatory rule): if approved Z-score < -2.5, an "
        "approved_drop_zscore_<value> flag is appended to triggered_rules."
    )

    pdf.sub_heading("Score bump from mandatory rules")
    pdf.table(
        ["Mandatory rules fired", "Score floor applied", "Guaranteed level"],
        [
            ["0",   "None -- weighted formula stands",   "Depends on score"],
            ["1",   "score = max(score, 0.50)",          "At least WARNING"],
            [">= 2", "score = max(score, 0.75)",         "At least CRITICAL"],
        ],
        col_widths=[w * 0.27, w * 0.43, w * 0.30],
    )

    pdf.sub_heading("Alert levels and actions")
    pdf.table(
        ["Level", "Score range", "is_anomaly", "Action"],
        [
            ["NORMAL",   "< 0.50",              "false", "No notification dispatched"],
            ["WARNING",  ">= 0.50 and < 0.75",  "true",  "Alert sent to WARNING channels"],
            ["CRITICAL", ">= 0.75",             "true",  "Alert sent to CRITICAL channels"],
        ],
        col_widths=[w * 0.16, w * 0.26, w * 0.16, w * 0.42],
    )

    pdf.sub_heading("Operator recommendations  (build_recommendation)")
    pdf.table(
        ["Triggered rule", "Guidance generated"],
        [
            ["ALERT_DENIED_ABOVE_NORMAL",
             "Review auth codes (51 vs 59), issuer behaviour, fraud filters"],
            ["ALERT_FAILED_ABOVE_NORMAL",
             "Check processor connectivity, gateway timeouts, error rates"],
            ["ALERT_REVERSED_ABOVE_NORMAL / BACKEND",
             "Review chargebacks, disputes, settlement and backend reversal flows"],
            ["No mandatory rule (IF-only elevation)",
             "Review technical rules and minute-level volumes"],
            ["Any CRITICAL",
             "Escalate on-call and open incident if sustained"],
        ],
        col_widths=[w * 0.38, w * 0.62],
    )

    # ── 4. Alert Notifications via SendGrid ──────────────────────────────
    pdf.section_heading("4", "Alert Notifications (monitoring_worker + SendGrid)")

    pdf.sub_heading("Gate: when a notification is sent")
    pdf.body_text(
        "The monitoring_worker polls GET /api/v1/anomalies/unprocessed every "
        "POLL_INTERVAL_SECONDS (default 30s, min_level=WARNING, limit=50). "
        "An anomaly is dispatched only when BOTH conditions hold:"
    )
    pdf.table(
        ["Condition", "Default value", "Configurable via"],
        [
            ["anomaly_score >= MIN_SCORE_TO_NOTIFY",   "0.5",           "MIN_SCORE_TO_NOTIFY env var"],
            ["alert_level in (WARNING, CRITICAL)",      "-- (fixed)",    "Not overridable (NORMAL is never sent)"],
        ],
        col_widths=[w * 0.48, w * 0.18, w * 0.34],
    )

    pdf.sub_heading("Channel routing by severity")
    pdf.table(
        ["Alert level", "Env var read", "Default channels"],
        [
            ["CRITICAL", "CRITICAL_CHANNELS", "console, email"],
            ["WARNING",  "WARNING_CHANNELS",  "console, email"],
        ],
        col_widths=[w * 0.20, w * 0.35, w * 0.45],
    )
    pdf.body_text(
        "Available channel values: console (always works), email (requires SMTP config), "
        "slack (requires SLACK_WEBHOOK_URL). Multiple channels are comma-separated."
    )

    pdf.sub_heading("Email via SendGrid -- prerequisites")
    pdf.body_text(
        "Email is dispatched only when ALL THREE of these are set and non-empty:"
    )
    pdf.table(
        ["Env var", "Required value for SendGrid"],
        [
            ["SMTP_HOST", "smtp.sendgrid.net"],
            ["SMTP_USER", "apikey  (literal string -- not your account email)"],
            ["SMTP_PASS", "SG.xxxxx  (SendGrid API key)"],
            ["ALERT_EMAIL_TO", "Recipient address (e.g. team@company.com)"],
        ],
        col_widths=[w * 0.28, w * 0.72],
    )

    pdf.sub_heading("Email content")
    pdf.code_block(
        "Subject: [CRITICAL] CloudWalk Transaction Alert - 2025-07-13 14:32:00\n\n"
        "Body:\n"
        "  [CRITICAL] Transaction Anomaly Detected\n"
        "  Timestamp    : 2025-07-13 14:32:00\n"
        "  Anomaly Score: 0.831\n"
        "  Triggered Rules: ALERT_DENIED_ABOVE_NORMAL, ALERT_FAILED_ABOVE_NORMAL\n"
        "  Counts: {\"approved\": 40, \"denied\": 120000, \"failed\": 35000, ...}"
    )

    pdf.sub_heading("SMTP protocol selection")
    pdf.table(
        ["SMTP_PORT", "SMTP_SSL env var", "Protocol used"],
        [
            ["587 (default)", "unset or false", "STARTTLS  (SMTP + starttls())"],
            ["465",           "true",           "Implicit TLS  (SMTP_SSL)"],
        ],
        col_widths=[w * 0.22, w * 0.28, w * 0.50],
    )

    pdf.sub_heading("Slack (optional)")
    pdf.body_text(
        "Set SLACK_WEBHOOK_URL to enable. Each message is a colour-coded attachment: "
        "red (#e74c3c) for CRITICAL, orange (#f39c12) for WARNING. "
        "Fields: timestamp, anomaly score, triggered rules, per-status counts."
    )

    # ── 5. AI/ML Detection Model ─────────────────────────────────────────
    pdf.section_heading("5", "AI/ML Detection Model (worker_ai_ml)", teal=True)
    pdf.sub_heading("EnsembleAnomalyDetector  (src/workers/ai_ml_worker.py)", teal=True)
    pdf.body_text(
        "Runs entirely independently of the rule-based pipeline. No business rules "
        "and no fixed score thresholds -- all boundaries are derived from the data."
    )
    pdf.ln(1)

    pdf.table(
        ["Component", "How it works"],
        [
            ["Isolation Forest",
             "Tree-based; each tree isolates samples by random splits. "
             "Anomalies are isolated in fewer splits (shorter path length)."],
            ["Local Outlier Factor",
             "Density-based; compares a point's local density to its k nearest "
             "neighbours. Points in sparse regions score higher."],
            ["Ensemble score",
             "Equal-weight mean of min-max-normalised IF and LOF scores. "
             "Normalisation bounds are saved from the training set so predictions "
             "are always on the same scale."],
        ],
        col_widths=[w * 0.28, w * 0.72],
        teal_header=True,
    )

    pdf.sub_heading("Feature engineering (21 features)", teal=True)
    pdf.table(
        ["Feature group", "Features"],
        [
            ["Raw counts",        "approved, denied, failed, reversed, backend_reversed, refunded, total_count"],
            ["Rate features",     "denial_rate, failure_rate, reversal_rate, approval_rate"],
            ["Cyclical time",     "hour_sin, hour_cos (hour/24), dow_sin, dow_cos (day-of-week/7)"],
            ["Rolling 30-min",    "rolling_denied_30m, rolling_failed_30m, rolling_total_30m, stddev_denied_30m, stddev_total_30m"],
            ["Auth-code diversity", "distinct_auth_codes, auth_51_rate, auth_59_rate"],
        ],
        col_widths=[w * 0.28, w * 0.72],
        teal_header=True,
    )

    pdf.sub_heading("Adaptive thresholds", teal=True)
    pdf.body_text(
        "After fitting, the worker computes ensemble scores on the training set itself "
        "and sets WARNING = P75 and CRITICAL = P90 of that distribution. Thresholds "
        "are recalculated on every retrain cycle (default: every 5 cycles / ~10 min), "
        "so the model continuously adapts to the observed data pattern."
    )

    pdf.sub_heading("Auto-sync with rule-based pipeline", teal=True)
    pdf.body_text(
        "Every cycle the AI worker cross-references anomaly_results with "
        "ai_anomaly_results via a LEFT JOIN to detect timestamps present in one "
        "but missing from the other. For each gap: if feature data is available in "
        "monitoring_minute_pivot, the full AI ensemble runs; otherwise the rule-based "
        "result is mirrored as a fallback. This guarantees the AI dashboard stays "
        "perfectly synchronised with the rule-based pipeline and manual inserts "
        "without operator intervention."
    )

    pdf.sub_heading("Retraining schedule", teal=True)
    pdf.table(
        ["Variable", "Default", "Effect"],
        [
            ["RUN_INTERVAL_SECONDS",   "120",  "Seconds between evaluation cycles"],
            ["RETRAIN_EVERY_N_CYCLES", "5",    "Full retrain on cycles 1, 6, 11, ... (~10 min cadence)"],
            ["CONTAMINATION",          "0.05", "Expected anomaly fraction passed to IF and LOF constructors"],
        ],
        col_widths=[w * 0.35, w * 0.15, w * 0.50],
        teal_header=True,
    )

    # ── 6. Data Flow ─────────────────────────────────────────────────────
    pdf.section_heading("6", "Data Flow")
    pdf.body_text(
        "1)  Postgres boots -> docker-init creates metabase_appdb -> "
        "init.sql creates all tables (including ai_anomaly_results), loads CSVs "
        "via COPY, creates monitoring views."
    )
    pdf.body_text(
        "2a) anomaly_worker starts -> reads ALL transactions from Postgres -> "
        "fits HybridAnomalyDetector -> evaluates only new minutes -> "
        "INSERTs into anomaly_results -> sleeps 60s -> repeats."
    )
    pdf.body_text(
        "2b) worker_ai_ml starts -> reads transactions + auth_codes, builds 21-feature "
        "matrix -> fits/retrains EnsembleAnomalyDetector (IF + LOF + OCSVM + Autoencoder) -> "
        "evaluates new minutes -> INSERTs into ai_anomaly_results -> "
        "runs auto-sync (LEFT JOIN anomaly_results vs ai_anomaly_results to fill "
        "any gaps from manual inserts or timing differences) -> "
        "sleeps 120s -> repeats (retrains every 5 cycles)."
    )
    pdf.body_text(
        "3)  monitoring_worker starts -> waits for API health -> "
        "GETs /anomalies/unprocessed (score >= 0.5 from anomaly_results) -> "
        "POSTs /notifications/send -> API dispatches (console + email) and "
        "UPDATEs notified_at -> sleeps 30s -> repeats."
    )
    pdf.body_text(
        "4)  Metabase queries Postgres directly. Rule-based dashboards read "
        "anomaly_results; the AI dashboard reads ai_anomaly_results exclusively."
    )

    # ── 7. Environment Variables ──────────────────────────────────────────
    pdf.section_heading("7", "Environment Variables")

    pdf.sub_heading("Email via SendGrid (in .env, consumed by monitoring_api)")
    pdf.table(
        ["Variable", "Required", "Example / Default"],
        [
            ["SMTP_HOST",         "Yes", "smtp.sendgrid.net"],
            ["SMTP_PORT",         "No",  "587"],
            ["SMTP_SSL",          "No",  "true (for port 465)"],
            ["SMTP_USER",         "Yes", "apikey (literal for SendGrid)"],
            ["SMTP_PASS",         "Yes", "SG.xxxxx (API key)"],
            ["ALERT_EMAIL_TO",    "Yes", "you@gmail.com"],
            ["ALERT_EMAIL_FROM",  "No",  "monitoring@cloudwalk.io"],
            ["ALERT_ON_EVALUATE", "No",  "false"],
            ["SMTP_DEBUG",        "No",  "false"],
        ],
        col_widths=[w * 0.30, w * 0.14, w * 0.56],
    )

    pdf.sub_heading("Rule-based worker tuning (docker-compose.yaml)")
    pdf.table(
        ["Variable", "Service", "Default"],
        [
            ["RUN_INTERVAL_SECONDS",  "anomaly_worker",    "60"],
            ["POLL_INTERVAL_SECONDS", "monitoring_worker", "30"],
            ["MIN_SCORE_TO_NOTIFY",   "monitoring_worker", "0.5"],
            ["CRITICAL_CHANNELS",     "monitoring_worker", "console,email"],
            ["WARNING_CHANNELS",      "monitoring_worker", "console,email"],
        ],
        col_widths=[w * 0.36, w * 0.32, w * 0.32],
    )

    pdf.sub_heading("AI/ML worker tuning (docker-compose.yaml)", teal=True)
    pdf.table(
        ["Variable", "Service", "Default"],
        [
            ["RUN_INTERVAL_SECONDS",    "worker_ai_ml", "120"],
            ["RETRAIN_EVERY_N_CYCLES",  "worker_ai_ml", "5"],
            ["CONTAMINATION",           "worker_ai_ml", "0.05"],
        ],
        col_widths=[w * 0.36, w * 0.32, w * 0.32],
        teal_header=True,
    )

    pdf.sub_heading("Rule-based model tuning (optional, pass to monitoring_api)")
    pdf.table(
        ["Variable", "Default", "Description"],
        [
            ["Z_THRESHOLD",       "2.5",  "Z-score cutoff for flagging"],
            ["ISO_CONTAMINATION", "0.05", "Expected anomaly rate (Isolation Forest)"],
            ["ROLLING_WINDOW",    "30",   "Minutes for rolling statistics"],
            ["WARNING_SCORE",     "0.5",  "Score threshold for WARNING"],
            ["CRITICAL_SCORE",    "0.75", "Score threshold for CRITICAL"],
            ["Z_WEIGHT",          "0.6",  "Z-Score weight (IF gets 1 - Z_WEIGHT)"],
        ],
        col_widths=[w * 0.28, w * 0.14, w * 0.58],
    )

    pdf.sub_heading("Database (defaults in docker-compose.yaml)")
    pdf.table(
        ["Variable", "Default"],
        [
            ["DB_HOST",     "postgres_db (Docker) / localhost"],
            ["DB_PORT",     "5432"],
            ["DB_NAME",     "cloudwalk_transactions"],
            ["DB_USER",     "admin"],
            ["DB_PASSWORD", "admin"],
        ],
        col_widths=[w * 0.35, w * 0.65],
    )

    # ── 8. API Endpoints ──────────────────────────────────────────────────
    pdf.section_heading("8", "API Endpoints")
    pdf.body_text("Base URL: http://localhost:8000  --  Swagger UI at /docs")
    pdf.table(
        ["Method", "Endpoint", "Description"],
        [
            ["GET",  "/api/v1/health",                      "DB + model status"],
            ["POST", "/api/v1/transactions/evaluate",        "Hybrid model evaluation"],
            ["GET",  "/api/v1/transactions",                 "Query with filters"],
            ["GET",  "/api/v1/transactions/summary",         "Per-status aggregation"],
            ["GET",  "/api/v1/anomalies",                    "Anomaly results with filters"],
            ["GET",  "/api/v1/anomalies/unprocessed",        "Pending notifications"],
            ["POST", "/api/v1/anomalies/{id}/acknowledge",   "Mark as notified"],
            ["POST", "/api/v1/notifications/send",           "Dispatch alert (console + email)"],
            ["GET",  "/api/v1/notifications/history",        "Notification log"],
            ["GET",  "/api/v1/stats",                        "Aggregated detection stats"],
        ],
        col_widths=[w * 0.10, w * 0.45, w * 0.45],
    )

    # ── 9. Metabase Dashboards ─────────────────────────────────────────────
    pdf.section_heading("9", "Metabase Dashboards (4 total)")
    pdf.body_text(
        "All dashboards are defined in metabase/Dashboards/manifest.json. "
        "The upload script (upload_dashboards.py) is idempotent: it deletes any existing "
        "dashboard with the same name (and its cards) before recreating, preventing duplicates. "
        "Use --dashboard to update a single dashboard in isolation."
    )
    pdf.table(
        ["Dashboard", "Source table", "Contents"],
        [
            ["CloudWalk -- Anomaly monitoring",
             "anomaly_results",
             "Score timeline, alert level distribution, CRITICAL table, pending count"],
            ["CloudWalk -- Transactions & operational data",
             "transactions / auth_codes / checkout / transactions_db",
             "Approved/denied per hour, auth codes 51/59, checkout comparison, OpInt volume"],
            ["CloudWalk -- Minute monitoring",
             "monitoring_minute_with_rollups",
             "Denied/failed/reversed per minute + 60-min rolling averages"],
            ["CloudWalk -- Anomalies with AI Detection",
             "ai_anomaly_results + anomaly_results",
             "AI score timeline, per-model breakdowns (IF/LOF/OCSVM/Autoencoder), "
             "AI vs rule-based benchmarking, alert distribution, CRITICAL table, "
             "hourly anomaly counts, worker health check (data lag, pending CRITICALs)"],
        ],
        col_widths=[w * 0.28, w * 0.27, w * 0.45],
    )

    # ── 10. Running Locally ───────────────────────────────────────────────
    pdf.section_heading("10", "Running Locally (without Docker)")
    pdf.table(
        ["Step", "Command / Action"],
        [
            ["1", "Install Python 3.11+ and PostgreSQL 15"],
            ["2", "Create database cloudwalk_transactions, run sql/init.sql"],
            ["3", "pip install -r requirements.txt"],
            ["4", "Export DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD"],
            ["5", "PYTHONPATH=src uvicorn api.main:app --host 0.0.0.0 --port 8000"],
            ["6", "PYTHONPATH=src python src/workers/anomaly_worker.py"],
            ["7", "API_BASE_URL=http://localhost:8000 PYTHONPATH=src python src/workers/monitoring_worker.py"],
            ["8", "python src/workers/ai_ml_worker.py  (uses DB_* env vars, no API dependency)"],
        ],
        col_widths=[w * 0.08, w * 0.92],
    )

    pdf.output(OUT_FILE)
    return OUT_FILE


if __name__ == "__main__":
    path = build_pdf()
    print(f"PDF generated: {path}")
