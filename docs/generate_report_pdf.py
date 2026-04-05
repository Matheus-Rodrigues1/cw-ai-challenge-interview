#!/usr/bin/env python3
"""
Generate docs/cloudwalk_monitoring_report.pdf
Static EDA report for CloudWalk Monitoring Analyst Challenge - Task 3.1

Usage:
    py -3 docs/generate_report_pdf.py

Requires: fpdf2 >= 2.7   (pip install fpdf2)
"""

import os
import math
from fpdf import FPDF

OUT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUT_FILE = os.path.join(OUT_DIR, "cloudwalk_monitoring_report.pdf")

# ── Colors ────────────────────────────────────────────────────────────────
DARK_BLUE      = (25,  55,  95)
MID_BLUE       = (41,  98, 168)
LIGHT_BLUE_BG  = (232, 240, 254)
TEAL           = (0,  105,  92)
LIGHT_TEAL_BG  = (224, 242, 241)
RED            = (192,  57,  43)
ORANGE         = (211, 84,   0)
GREEN          = (39, 174,  96)
LIGHT_RED_BG   = (253, 237, 236)
LIGHT_GREEN_BG = (234, 250, 241)
LIGHT_ORANGE_BG= (254, 243, 235)
WHITE          = (255, 255, 255)
BLACK          = (30,  30,  30)
GRAY           = (100, 100, 100)
LIGHT_GRAY     = (245, 245, 245)
TABLE_HEADER   = (41,  98, 168)
TABLE_ALT      = (240, 245, 255)
TEAL_HEADER    = (0,  137, 123)


# ── Checkout data ─────────────────────────────────────────────────────────
# Merged checkout_1.csv + checkout_2.csv (stacked, same schema)
CHECKOUT_1 = [
    ("00h", 9,  12, 11, 6.42,  4.85),
    ("01h", 3,   5,  1, 1.85,  1.92),
    ("02h", 1,   0,  0, 0.28,  0.82),
    ("03h", 1,   0,  0, 0.42,  0.46),
    ("04h", 0,   0,  1, 0.42,  0.21),
    ("05h", 1,   1,  2, 1.28,  0.75),
    ("06h", 1,   1,  5, 2.85,  2.28),
    ("07h", 2,   3,  9, 5.57,  5.21),
    ("08h", 0,   1, 18, 8.71, 10.42),
    ("09h", 2,   9, 30,20.0,  19.07),
    ("10h",55,  51, 45,29.42, 28.35),
    ("11h",36,  44, 38,33.71, 28.50),
    ("12h",51,  39, 39,27.57, 25.42),
    ("13h",36,  41, 43,25.85, 24.21),
    ("14h",32,  35, 36,26.14, 25.21),
    ("15h",51,  35, 49,28.14, 27.71),
    ("16h",41,  36, 48,27.71, 25.64),
    ("17h",45,  30, 29,20.42, 22.28),
    ("18h",32,  25, 25,21.57, 18.28),
    ("19h",33,  39, 42,22.14, 18.67),
    ("20h",25,  24, 34,17.42, 18.92),
    ("21h",30,  35, 34,18.71, 17.57),
    ("22h",28,  29, 23,15.42, 15.64),
    ("23h",11,  28, 10, 9.57,  8.75),
]

CHECKOUT_2 = [
    ("00h", 6,  9,  5,  5.00,  4.92),
    ("01h", 3,  3,  2,  2.00,  1.92),
    ("02h", 3,  1,  2,  0.42,  0.75),
    ("03h", 0,  1,  1,  0.42,  0.46),
    ("04h", 0,  0,  0,  0.14,  0.21),
    ("05h", 2,  1,  1,  0.71,  0.71),
    ("06h", 3,  1,  2,  1.42,  2.10),
    ("07h",10,  2,  9,  3.00,  5.03),
    ("08h",25,  0, 12,  3.71,  9.82),
    ("09h",36,  2, 27, 10.14, 17.64),
    ("10h",43, 55, 42, 26.14, 28.57),
    ("11h",44, 36, 47, 25.00, 28.28),
    ("12h",46, 51, 46, 24.00, 25.89),
    ("13h",45, 36, 31, 20.28, 24.17),
    ("14h",19, 32, 35, 19.57, 24.89),
    ("15h", 0, 51, 42, 22.43, 27.78),
    ("16h", 0, 41, 36, 21.57, 25.53),
    ("17h", 0, 45, 19, 17.71, 22.67),
    ("18h",13, 32, 29, 16.85, 18.46),
    ("19h",32, 33, 29, 18.00, 18.21),
    ("20h",23, 25, 17, 12.14, 18.53),
    ("21h",28, 30, 23, 14.85, 17.82),
    ("22h",29, 28, 17, 12.71, 15.50),
    ("23h",17, 11, 14,  8.28,  8.75),
]


# ── PDF class ──────────────────────────────────────────────────────────────
class ReportPDF(FPDF):

    def header(self):
        if self.page_no() == 1:
            return
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*MID_BLUE)
        self.cell(0, 7, "CloudWalk Monitoring Analyst Challenge -- EDA Report", align="L")
        self.set_draw_color(*LIGHT_BLUE_BG)
        self.set_line_width(0.4)
        self.line(self.l_margin, self.get_y() + 7, self.w - self.r_margin, self.get_y() + 7)
        self.ln(9)

    def footer(self):
        self.set_y(-14)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(*GRAY)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    # ── layout helpers ──────────────────────────────────────────────────

    def ensure_space(self, min_h=40):
        if self.get_y() + min_h > self.h - self.b_margin:
            self.add_page()

    def section_heading(self, number, title, teal=False):
        self.ensure_space(70)
        self.ln(4)
        self.set_font("Helvetica", "B", 13)
        self.set_fill_color(*(TEAL if teal else DARK_BLUE))
        self.set_text_color(*WHITE)
        w = self.w - self.l_margin - self.r_margin
        self.cell(w, 10, f"  {number}. {title}",
                  fill=True, new_x="LMARGIN", new_y="NEXT", border=0)
        self.ln(3)
        self.set_text_color(*BLACK)

    def sub_heading(self, text, color=None):
        self.ensure_space(35)
        self.ln(2)
        c = color or MID_BLUE
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(*c)
        w = self.w - self.l_margin - self.r_margin
        self.cell(w, 7, text, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(*c)
        self.set_line_width(0.25)
        self.line(self.l_margin, self.get_y(), self.l_margin + w, self.get_y())
        self.ln(2)
        self.set_text_color(*BLACK)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10.5)
        self.set_text_color(*BLACK)
        w = self.w - self.l_margin - self.r_margin
        self.multi_cell(w, 6, text, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def bullet(self, text, color=None):
        self.set_font("Helvetica", "", 10.5)
        self.set_text_color(*(color or BLACK))
        x = self.get_x()
        self.set_x(x + 5)
        w = self.w - self.l_margin - self.r_margin - 5
        self.multi_cell(w, 6, f"*  {text}", new_x="LMARGIN", new_y="NEXT")
        self.ln(1)
        self.set_text_color(*BLACK)

    def callout(self, text, bg, border_color):
        self.ln(1)
        w = self.w - self.l_margin - self.r_margin
        self.set_font("Helvetica", "", 10.5)
        lines = self.multi_cell(w - 10, 6, text, dry_run=True, output="LINES")
        h = len(lines) * 6 + 7
        self.ensure_space(h + 3)
        x, y = self.get_x(), self.get_y()
        self.set_fill_color(*bg)
        self.rect(x, y, w, h, "F")
        self.set_fill_color(*border_color)
        self.rect(x, y, 3, h, "F")
        self.set_xy(x + 8, y + 3)
        self.set_text_color(*BLACK)
        self.multi_cell(w - 10, 6, text, new_x="LMARGIN", new_y="NEXT")
        self.set_y(y + h + 2)

    def code_block(self, text):
        self.ln(1)
        self.set_font("Courier", "", 9)
        self.set_fill_color(*LIGHT_GRAY)
        self.set_draw_color(210, 210, 210)
        self.set_text_color(50, 50, 50)
        w = self.w - self.l_margin - self.r_margin
        lines = text.split("\n")
        h = len(lines) * 5.2 + 6
        self.ensure_space(h + 4)
        x, y = self.get_x(), self.get_y()
        self.rect(x, y, w, h, "DF")
        self.set_xy(x + 4, y + 3)
        self.multi_cell(w - 8, 5.2, text, border=0, align="L",
                        new_x="LMARGIN", new_y="NEXT")
        self.set_y(y + h + 3)
        self.set_text_color(*BLACK)

    def table(self, headers, rows, col_widths=None, teal_header=False):
        w = self.w - self.l_margin - self.r_margin
        if col_widths is None:
            col_widths = [w / len(headers)] * len(headers)

        self.set_font("Helvetica", "B", 9)
        hdr_bg = TEAL_HEADER if teal_header else TABLE_HEADER
        self.set_fill_color(*hdr_bg)
        self.set_text_color(*WHITE)
        self.set_draw_color(*WHITE)
        self.set_line_width(0.2)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 8, f"  {h}", border=1, fill=True, align="L")
        self.ln()

        self.set_font("Helvetica", "", 9.5)
        self.set_text_color(*BLACK)
        self.set_draw_color(230, 230, 230)
        for idx, row in enumerate(rows):
            max_lines = 1
            for i, cell in enumerate(row):
                ls = self.multi_cell(col_widths[i], 6, f"  {cell}",
                                     border=0, dry_run=True, output="LINES")
                max_lines = max(max_lines, len(ls))
            row_h = max_lines * 6 + 4
            x0, y0 = self.get_x(), self.get_y()
            if y0 + row_h > self.h - 20:
                self.add_page()
                y0 = self.get_y()
                x0 = self.get_x()
            for i, cell in enumerate(row):
                self.set_xy(x0 + sum(col_widths[:i]), y0)
                self.set_fill_color(*(TABLE_ALT if idx % 2 == 1 else WHITE))
                self.rect(x0 + sum(col_widths[:i]), y0, col_widths[i], row_h, "DF")
                self.set_xy(x0 + sum(col_widths[:i]), y0 + 2)
                self.multi_cell(col_widths[i], 6, f"  {cell}", border=0)
            self.set_xy(x0, y0 + row_h)
        self.ln(2)

    # ── chart helpers ───────────────────────────────────────────────────

    def bar_chart(self, data, title, chart_w=170, chart_h=55,
                  bar_colors=None, legend=None, anomaly_hours=None):
        """
        data      : list of (label, [val1, val2, ...]) per hour
        bar_colors: list of (R,G,B) for each series
        legend    : list of str names matching series
        anomaly_hours: set of label strings to highlight with a red marker
        """
        self.ensure_space(chart_h + 20)
        if bar_colors is None:
            bar_colors = [MID_BLUE, TEAL, ORANGE]
        anomaly_hours = anomaly_hours or set()

        # ── chart area ──────────────────────────────────────────────────
        margin_l = self.l_margin
        cx = margin_l
        cy = self.get_y() + 6

        # title
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*DARK_BLUE)
        self.cell(chart_w, 6, title, align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(2)
        cy = self.get_y()

        # compute max for scale
        all_vals = [v for (_, series) in data for v in series]
        max_val = max(all_vals) if all_vals else 1
        scale = (chart_h - 8) / max_val

        n_hours = len(data)
        n_series = len(data[0][1]) if data else 1
        slot_w = chart_w / n_hours
        bar_w = min(slot_w * 0.8 / n_series, 5)

        # axes
        self.set_draw_color(*GRAY)
        self.set_line_width(0.3)
        # y-axis
        self.line(cx + 2, cy, cx + 2, cy + chart_h)
        # x-axis
        self.line(cx + 2, cy + chart_h, cx + chart_w, cy + chart_h)

        # y gridlines + labels
        self.set_font("Helvetica", "", 7)
        self.set_text_color(*GRAY)
        steps = 4
        for s in range(steps + 1):
            val = max_val * s / steps
            y_pos = cy + chart_h - val * scale
            self.set_draw_color(220, 220, 220)
            self.set_line_width(0.2)
            self.line(cx + 2, y_pos, cx + chart_w, y_pos)
            self.set_xy(cx - 10, y_pos - 2)
            self.cell(10, 4, str(int(val)), align="R")

        # bars
        for hi, (label, series) in enumerate(data):
            slot_x = cx + 2 + hi * slot_w
            bar_group_start = slot_x + (slot_w - bar_w * n_series) / 2

            is_anomaly = label in anomaly_hours

            # anomaly highlight background
            if is_anomaly:
                self.set_fill_color(*LIGHT_RED_BG)
                self.rect(slot_x, cy, slot_w, chart_h, "F")

            for si, val in enumerate(series):
                bx = bar_group_start + si * bar_w
                bh = val * scale
                by = cy + chart_h - bh
                r, g, b = bar_colors[si % len(bar_colors)]
                # darken anomaly bars slightly
                if is_anomaly:
                    r, g, b = max(r - 30, 0), max(g - 30, 0), max(b - 30, 0)
                self.set_fill_color(r, g, b)
                self.set_draw_color(r, g, b)
                self.rect(bx, by, bar_w, bh, "F")

            # x label
            self.set_font("Helvetica", "B" if is_anomaly else "", 6.5)
            self.set_text_color(*(RED if is_anomaly else GRAY))
            self.set_xy(slot_x, cy + chart_h + 1)
            self.cell(slot_w, 4, label, align="C")

        # legend
        if legend:
            lx = cx
            ly = cy + chart_h + 8
            self.set_font("Helvetica", "", 8)
            for si, name in enumerate(legend):
                r, g, b = bar_colors[si % len(bar_colors)]
                self.set_fill_color(r, g, b)
                self.rect(lx, ly, 5, 4, "F")
                self.set_text_color(*BLACK)
                self.set_xy(lx + 6, ly - 0.5)
                self.cell(28, 5, name)
                lx += 38

        self.set_y(cy + chart_h + (14 if legend else 6))
        self.set_text_color(*BLACK)


# ── Build report ───────────────────────────────────────────────────────────

def build_pdf() -> str:
    pdf = ReportPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.set_left_margin(18)
    pdf.set_right_margin(18)
    W = 210 - 18 - 18

    # ═══════════════════════════════════════════════════════════════════
    # COVER
    # ═══════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.set_fill_color(*LIGHT_BLUE_BG)
    pdf.rect(0, 0, 210, 110, "F")

    pdf.ln(30)
    pdf.set_font("Helvetica", "B", 28)
    pdf.set_text_color(*DARK_BLUE)
    pdf.multi_cell(W, 15, "CloudWalk Transaction\nMonitoring System",
                   align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    pdf.set_draw_color(*MID_BLUE)
    pdf.set_line_width(1.0)
    pdf.line(55, pdf.get_y(), 155, pdf.get_y())
    pdf.ln(8)

    pdf.set_font("Helvetica", "I", 16)
    pdf.set_text_color(*MID_BLUE)
    pdf.cell(W, 8, "EDA Report -- Monitoring Analyst Challenge",
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(22)

    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(*BLACK)
    pdf.multi_cell(W, 6.5, (
        "Exploratory Data Analysis of POS checkout anomalies (Task 3.1) and "
        "real-time transaction anomaly detection (Task 3.2). "
        "This report covers the checkout hourly comparison (today vs. yesterday "
        "vs. historical averages), SQL queries used to identify anomalies, "
        "and the monitoring system built to detect and alert on abnormal "
        "denied, failed, and reversed transaction patterns."
    ), align="C", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(18)
    pdf.set_font("Helvetica", "I", 9)
    pdf.set_text_color(*GRAY)
    pdf.cell(W, 5, "Generated by docs/generate_report_pdf.py", align="C",
             new_x="LMARGIN", new_y="NEXT")
    pdf.cell(W, 5, "CloudWalk Monitoring Analyst Technical Assessment", align="C",
             new_x="LMARGIN", new_y="NEXT")

    # ═══════════════════════════════════════════════════════════════════
    # TABLE OF CONTENTS
    # ═══════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_heading("", "Table of Contents")
    toc = [
        ("1", "Datasets Used"),
        ("2", "Task 3.1 -- Checkout Anomaly Analysis"),
        ("  2.1", "Dataset Description"),
        ("  2.2", "SQL Query: Anomaly Detection on Checkout Data"),
        ("  2.3", "Checkout 1 -- Hourly Comparison Chart"),
        ("  2.4", "Checkout 2 -- Hourly Comparison Chart"),
        ("  2.5", "Anomalies Found"),
        ("  2.6", "Conclusions"),
        ("3", "Task 3.2 -- Real-Time Transaction Monitoring"),
        ("  3.1", "Transaction Dataset Overview"),
        ("  3.2", "Anomaly Detection Approach"),
        ("  3.3", "Alert Rules (Mandatory Requirements)"),
        ("  3.4", "System Components"),
        ("4", "Summary & Deliverables"),
    ]
    pdf.set_font("Helvetica", "", 10.5)
    for num, title in toc:
        pdf.set_text_color(*DARK_BLUE if not num.startswith(" ") else MID_BLUE)
        bold = "B" if not num.startswith(" ") else ""
        pdf.set_font("Helvetica", bold, 10.5)
        pdf.cell(W, 7, f"{num}    {title}", new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(*BLACK)

    # ═══════════════════════════════════════════════════════════════════
    # 1. DATASETS USED
    # ═══════════════════════════════════════════════════════════════════
    pdf.section_heading("1", "Datasets Used")
    pdf.body_text(
        "All CSV files are loaded into PostgreSQL at container startup via COPY "
        "commands in sql/init.sql. The datasets used in this analysis are:"
    )
    pdf.table(
        ["File", "Records", "Granularity", "Purpose"],
        [
            ["checkout_1.csv",   "24",    "Per hour",   "POS hourly sales -- first POS terminal (today vs. baseline)"],
            ["checkout_2.csv",   "24",    "Per hour",   "POS hourly sales -- second POS terminal (today vs. baseline)"],
            ["transactions.csv", "25,920","Per minute", "Transaction status counts (Jul 12-15, 2025): approved, denied, failed, reversed, backend_reversed, refunded"],
            ["transactions_auth_codes.csv", "12,960", "Per minute", "Auth code breakdown per minute (51 = insufficient funds, 59 = suspected fraud, etc.)"],
            ["operational_intelligence_transactions_db.csv", "62,034", "Per day", "Full transaction database -- Jan-Mar 2025"],
        ],
        col_widths=[W * 0.30, W * 0.10, W * 0.14, W * 0.46],
    )
    pdf.body_text(
        "The checkout CSVs share the same schema with five columns: time (hour label), "
        "today, yesterday, same_day_last_week, avg_last_week, avg_last_month. "
        "This structure allows direct comparison of today's sales volume against "
        "multiple historical baselines to detect deviations quickly."
    )

    # ═══════════════════════════════════════════════════════════════════
    # 2. TASK 3.1 -- CHECKOUT ANOMALY ANALYSIS
    # ═══════════════════════════════════════════════════════════════════
    pdf.section_heading("2", "Task 3.1 -- Checkout Anomaly Analysis")

    # ── 2.1 Dataset description ─────────────────────────────────────
    pdf.sub_heading("2.1  Dataset Description")
    pdf.body_text(
        "The two checkout CSV files contain hourly POS sales for the same day across "
        "two POS terminals (or two store locations). Each row represents one hour "
        "(00h to 23h) and compares current-day sales with:"
    )
    pdf.bullet("yesterday -- same terminal, previous day")
    pdf.bullet("same_day_last_week -- same day-of-week, one week prior")
    pdf.bullet("avg_last_week -- average of the same hour across the last 7 days")
    pdf.bullet("avg_last_month -- average of the same hour across the last 30 days")
    pdf.body_text(
        "The combination of multiple baselines makes it possible to distinguish "
        "genuine business anomalies (not just natural day-to-day variance) from "
        "day-of-week seasonality effects."
    )

    # ── 2.2 SQL Query ───────────────────────────────────────────────
    pdf.sub_heading("2.2  SQL Query: Anomaly Detection on Checkout Data")
    pdf.body_text(
        "The query below reads from the checkout table (both files merged via COPY) "
        "and flags each hour as SPIKE, DROPOUT, or NORMAL by comparing today's "
        "volume against the monthly average with a ±50% threshold:"
    )
    pdf.code_block(
        "SELECT\n"
        "    time,\n"
        "    today,\n"
        "    yesterday,\n"
        "    avg_last_week,\n"
        "    avg_last_month,\n"
        "    ROUND((today::numeric - avg_last_month)\n"
        "          / NULLIF(avg_last_month, 0) * 100, 1)        AS deviation_pct,\n"
        "    CASE\n"
        "        WHEN avg_last_month > 3\n"
        "         AND today = 0                                  THEN 'DROPOUT'\n"
        "        WHEN today > avg_last_month * 1.5               THEN 'SPIKE'\n"
        "        WHEN today < avg_last_month * 0.5\n"
        "         AND avg_last_month > 3                         THEN 'BELOW_NORMAL'\n"
        "        ELSE                                                 'NORMAL'\n"
        "    END                                                 AS anomaly_flag\n"
        "FROM checkout\n"
        "ORDER BY time;"
    )
    pdf.body_text(
        "The NULLIF guard prevents division-by-zero for off-peak hours where the "
        "monthly average is 0. The threshold of 1.5x (50% above) is intentionally "
        "conservative to capture true business-impacting spikes while ignoring "
        "normal intra-day variance. The DROPOUT condition requires avg_last_month > 3 "
        "to avoid flagging hours that are legitimately near-zero in all periods."
    )

    # ── 2.3 Checkout 1 chart ─────────────────────────────────────────
    pdf.sub_heading("2.3  Checkout 1 -- Hourly Comparison Chart")
    pdf.body_text(
        "Chart shows today (blue), yesterday (teal), and avg_last_month (orange) "
        "for each hour. Hours flagged as anomalous by the SQL query are highlighted "
        "in red."
    )

    # anomalies in checkout 1: 10h spike (today=55 vs avg_month=28.35)
    ck1_anomalies = set()
    for (t, today, yest, sdlw, avgw, avgm) in CHECKOUT_1:
        flag = "NORMAL"
        if avgm > 3 and today == 0:
            flag = "DROPOUT"
        elif today > avgm * 1.5:
            flag = "SPIKE"
        elif today < avgm * 0.5 and avgm > 3:
            flag = "BELOW_NORMAL"
        if flag != "NORMAL":
            ck1_anomalies.add(t)

    ck1_chart_data = [
        (t, [today, yest, avgm])
        for (t, today, yest, sdlw, avgw, avgm) in CHECKOUT_1
    ]
    pdf.bar_chart(
        ck1_chart_data,
        title="Checkout 1 -- Sales per Hour: Today vs Yesterday vs Avg Last Month",
        chart_h=55,
        bar_colors=[MID_BLUE, TEAL, ORANGE],
        legend=["Today", "Yesterday", "Avg Last Month"],
        anomaly_hours=ck1_anomalies,
    )

    pdf.body_text(
        "Checkout 1 shows a consistent intra-day sales pattern that peaks in the "
        "morning business hours (10h-17h) and drops off in the evening. "
        "The highlighted anomalous hours are listed in Section 2.5."
    )

    # ── 2.4 Checkout 2 chart ─────────────────────────────────────────
    pdf.sub_heading("2.4  Checkout 2 -- Hourly Comparison Chart")
    pdf.body_text(
        "Same visualization for the second POS dataset. Note the notable behavior "
        "differences: morning spike followed by a prolonged afternoon dropout."
    )

    ck2_anomalies = set()
    for (t, today, yest, sdlw, avgw, avgm) in CHECKOUT_2:
        flag = "NORMAL"
        if avgm > 3 and today == 0:
            flag = "DROPOUT"
        elif today > avgm * 1.5:
            flag = "SPIKE"
        elif today < avgm * 0.5 and avgm > 3:
            flag = "BELOW_NORMAL"
        if flag != "NORMAL":
            ck2_anomalies.add(t)

    ck2_chart_data = [
        (t, [today, yest, avgm])
        for (t, today, yest, sdlw, avgw, avgm) in CHECKOUT_2
    ]
    pdf.bar_chart(
        ck2_chart_data,
        title="Checkout 2 -- Sales per Hour: Today vs Yesterday vs Avg Last Month",
        chart_h=55,
        bar_colors=[MID_BLUE, TEAL, ORANGE],
        legend=["Today", "Yesterday", "Avg Last Month"],
        anomaly_hours=ck2_anomalies,
    )

    pdf.body_text(
        "Checkout 2 exhibits a starkly different pattern from Checkout 1: a strong "
        "morning spike followed by a complete operational stop in the afternoon, "
        "suggesting a hardware failure, network outage, or deliberate shutdown."
    )

    # ── 2.5 Anomalies found ──────────────────────────────────────────
    pdf.sub_heading("2.5  Anomalies Found")

    pdf.body_text(
        "Running the SQL query against both checkout datasets reveals the following "
        "anomalous hours:"
    )

    ck1_table_rows = []
    for (t, today, yest, sdlw, avgw, avgm) in CHECKOUT_1:
        flag = "NORMAL"
        if avgm > 3 and today == 0:
            flag = "DROPOUT"
        elif today > avgm * 1.5:
            flag = "SPIKE"
        elif today < avgm * 0.5 and avgm > 3:
            flag = "BELOW_NORMAL"
        if flag != "NORMAL":
            dev = round((today - avgm) / avgm * 100, 1) if avgm else 0
            ck1_table_rows.append([t, str(today), f"{avgm}", f"{dev:+.1f}%", flag])

    if ck1_table_rows:
        pdf.body_text("Checkout 1 anomalies:")
        pdf.table(
            ["Hour", "Today", "Avg Last Month", "Deviation %", "Flag"],
            ck1_table_rows,
            col_widths=[W * 0.10, W * 0.12, W * 0.18, W * 0.18, W * 0.42],
        )

    ck2_table_rows = []
    for (t, today, yest, sdlw, avgw, avgm) in CHECKOUT_2:
        flag = "NORMAL"
        if avgm > 3 and today == 0:
            flag = "DROPOUT"
        elif today > avgm * 1.5:
            flag = "SPIKE"
        elif today < avgm * 0.5 and avgm > 3:
            flag = "BELOW_NORMAL"
        if flag != "NORMAL":
            dev = round((today - avgm) / avgm * 100, 1) if avgm else 0
            ck2_table_rows.append([t, str(today), f"{avgm}", f"{dev:+.1f}%", flag])

    if ck2_table_rows:
        pdf.body_text("Checkout 2 anomalies:")
        pdf.table(
            ["Hour", "Today", "Avg Last Month", "Deviation %", "Flag"],
            ck2_table_rows,
            col_widths=[W * 0.10, W * 0.12, W * 0.18, W * 0.18, W * 0.42],
        )

    # Highlight the key anomalies with callouts
    pdf.callout(
        "SPIKE -- Checkout 1, 10h: today = 55 vs. avg_last_month = 28.35 "
        "(+93.9%). Today and yesterday (51) both significantly exceed the "
        "weekly and monthly averages. Possible causes: promotional event, "
        "payroll day, concentrated merchant activity, or data quality issue "
        "(e.g., delayed batching from a previous period).",
        LIGHT_ORANGE_BG, ORANGE,
    )
    pdf.callout(
        "SPIKE -- Checkout 2, 08h-09h: today = 25 and 36 vs. avg_last_month = "
        "9.82 and 17.64 (+154.6% and +104.0%). A sharp morning volume surge "
        "not seen in yesterday or the weekly average. Combined with the "
        "afternoon dropout (below), suggests traffic shifted or was front-loaded "
        "before an outage.",
        LIGHT_ORANGE_BG, ORANGE,
    )
    pdf.callout(
        "DROPOUT -- Checkout 2, 15h-17h: today = 0 sales across three "
        "consecutive hours while yesterday recorded 51, 41, and 45 "
        "respectively, and the monthly averages are 27.78, 25.53, and 22.67. "
        "A zero-volume window lasting 3 consecutive peak hours is the strongest "
        "anomaly in both datasets and almost certainly reflects a terminal "
        "outage, connectivity failure, or forced service stop.",
        LIGHT_RED_BG, RED,
    )

    # ── 2.6 Conclusions ──────────────────────────────────────────────
    pdf.sub_heading("2.6  Conclusions")
    pdf.body_text(
        "The checkout data reveals two distinct anomaly patterns that co-exist "
        "within the same observation day across two POS terminals:"
    )
    pdf.bullet(
        "Checkout 1 shows a demand spike at 10h (+93.9% vs. monthly average) "
        "with no corresponding dropout, suggesting a genuine sales surge or "
        "a successful promotional push during the morning peak.",
        color=ORANGE,
    )
    pdf.bullet(
        "Checkout 2 exhibits a two-phase anomaly: an unusual morning spike "
        "(08h-09h, up to +154%) followed by a complete operational stop from "
        "15h to 17h. The morning front-loading combined with the afternoon "
        "dropout strongly suggests a terminal or network failure during the "
        "afternoon period. Revenue lost during the 15h-17h window would be "
        "roughly 3 x 25 = 75 expected sales missed based on monthly average.",
        color=RED,
    )
    pdf.body_text(
        "From a monitoring perspective, the 15h-17h dropout in Checkout 2 is "
        "the highest-priority signal. A rule-based monitor with a 'zero-volume "
        "during business hours' condition would have triggered within the first "
        "15 minutes of 15h, enabling immediate investigation. "
        "The 10h spike in Checkout 1 warrants a WARNING-level alert "
        "to confirm it represents legitimate demand and not batching artifacts."
    )

    # ═══════════════════════════════════════════════════════════════════
    # 3. TASK 3.2 -- TRANSACTION MONITORING
    # ═══════════════════════════════════════════════════════════════════
    pdf.section_heading("3", "Task 3.2 -- Real-Time Transaction Monitoring")

    # ── 3.1 Transaction dataset ──────────────────────────────────────
    pdf.sub_heading("3.1  Transaction Dataset Overview")
    pdf.body_text(
        "The primary dataset for the real-time monitoring system is transactions.csv: "
        "25,920 rows covering July 12-15, 2025 at per-minute granularity. "
        "Each row contains a timestamp, a status label, and a count of transactions "
        "with that status during that minute."
    )
    pdf.table(
        ["Status", "Description", "Monitoring priority"],
        [
            ["approved",         "Successfully authorized transaction",           "Baseline -- drop triggers BELOW_NORMAL alert"],
            ["denied",           "Authorization denied by issuer",                "HIGH -- spikes indicate fraud wave or issuer issue"],
            ["failed",           "Technical failure (connectivity, timeout, etc.)","HIGH -- spikes indicate infrastructure problem"],
            ["reversed",         "Customer-initiated reversal/chargeback",         "HIGH -- spikes indicate dispute surge"],
            ["backend_reversed", "Backend-initiated reversal (settlement)",        "HIGH -- spikes indicate settlement anomaly"],
            ["refunded",         "Merchant-initiated refund",                     "MEDIUM -- contextual"],
        ],
        col_widths=[W * 0.20, W * 0.45, W * 0.35],
    )
    pdf.body_text(
        "The per-minute granularity allows the system to detect anomalies within "
        "1-2 minutes of occurrence, enabling near-real-time response. "
        "The dataset spans 4 days (~5,760 unique minutes), providing sufficient "
        "history for the Z-score and Isolation Forest models to learn stable "
        "baselines for each status."
    )

    # ── 3.2 Anomaly detection approach ──────────────────────────────
    pdf.sub_heading("3.2  Anomaly Detection Approach")
    pdf.body_text(
        "The system implements a combination of rule-based and score-based detection "
        "(the third and most robust option listed in the challenge). Two fully "
        "independent pipelines run in parallel:"
    )

    pdf.callout(
        "Pipeline 1 -- HybridAnomalyDetector (rule-based + score-based): "
        "Computes Z-scores for each status against global historical mean/std. "
        "Combines Z-score component (60%) with Isolation Forest component (40%) "
        "into a final anomaly score from 0.0 to 1.0. Mandatory rules fire when "
        "Z > 2.5 for denied, failed, or reversed statuses, providing guaranteed "
        "minimum alert levels regardless of the weighted score.",
        LIGHT_BLUE_BG, MID_BLUE,
    )
    pdf.callout(
        "Pipeline 2 -- EnsembleAnomalyDetector (pure ML, no rules): "
        "Builds a 21-feature matrix from raw counts, rate features "
        "(denial_rate, failure_rate, reversal_rate), cyclical time encoding "
        "(sin/cos of hour and day-of-week), 30-minute rolling statistics, and "
        "auth-code diversity metrics. Runs Isolation Forest + Local Outlier "
        "Factor + One-Class SVM + Autoencoder (reconstruction error) with equal "
        "weight. WARNING/CRITICAL thresholds are computed as "
        "P75/P90 of training-set scores -- no hardcoded values.",
        LIGHT_TEAL_BG, TEAL,
    )
    pdf.body_text(
        "The scoring formula for Pipeline 1 is: "
        "final_score = 0.6 x z_component + 0.4 x iso_component, where z_component "
        "= min(max_abs_z / 10, 1.0) and iso_component is the Isolation Forest "
        "decision function inverted and min-max normalised. A score >= 0.50 triggers "
        "WARNING; >= 0.75 triggers CRITICAL. Mandatory rules can only raise the "
        "score floor -- they never lower an already-higher score."
    )

    # ── 3.3 Alert rules ──────────────────────────────────────────────
    pdf.sub_heading("3.3  Alert Rules (Mandatory Requirements)")
    pdf.table(
        ["Rule ID", "Condition", "Score floor"],
        [
            ["ALERT_DENIED_ABOVE_NORMAL",          "denied Z-score > 2.5",           "max(score, 0.50) = WARNING"],
            ["ALERT_FAILED_ABOVE_NORMAL",           "failed Z-score > 2.5",           "max(score, 0.50) = WARNING"],
            ["ALERT_REVERSED_ABOVE_NORMAL",         "reversed Z-score > 2.5",         "max(score, 0.50) = WARNING"],
            ["ALERT_BACKEND_REVERSED_ABOVE_NORMAL", "backend_reversed Z-score > 0.50","max(score, 0.50) = WARNING"],
        ],
        col_widths=[W * 0.42, W * 0.30, W * 0.28],
    )
    pdf.body_text(
        "When two or more mandatory rules fire simultaneously, the score floor "
        "rises to 0.75 (CRITICAL), ensuring that compound events -- e.g., "
        "simultaneous spike in denied AND failed transactions (which strongly "
        "suggests a processor/gateway incident) -- are always escalated immediately."
    )

    # ── 3.4 System components ────────────────────────────────────────
    pdf.sub_heading("3.4  System Components")
    pdf.table(
        ["Component", "Role", "Technology"],
        [
            ["FastAPI (monitoring_api)",   "REST endpoint for evaluation, notification dispatch, anomaly query", "Python / FastAPI / psycopg2"],
            ["anomaly_worker",             "Rule-based detection loop every 60s", "Python / scikit-learn / pandas"],
            ["worker_ai_ml",               "Pure ML detection loop every 120s", "Python / scikit-learn (IF + LOF + OCSVM + Autoencoder)"],
            ["monitoring_worker",          "Notification loop every 30s -- polls unprocessed anomalies and dispatches alerts", "Python / requests"],
            ["AlertNotifier",              "Multi-channel dispatcher: console, SMTP email (SendGrid), Slack webhook", "Python / smtplib"],
            ["PostgreSQL 15",             "Persistent store for all transaction data, anomaly results, notification log", "PostgreSQL / SQL views"],
            ["Metabase",                  "4 real-time dashboards with SQL queries against live Postgres data", "Metabase / SQL"],
        ],
        col_widths=[W * 0.28, W * 0.42, W * 0.30],
    )

    # ═══════════════════════════════════════════════════════════════════
    # 4. SUMMARY & DELIVERABLES
    # ═══════════════════════════════════════════════════════════════════
    pdf.section_heading("4", "Summary & Deliverables")

    pdf.sub_heading("Requirements Coverage")
    pdf.table(
        ["Requirement", "Status", "Implementation"],
        [
            ["Analyze checkout data for anomalies",
             "DONE",
             "checkout_1.csv + checkout_2.csv loaded in Postgres; SQL query with SPIKE/DROPOUT/BELOW_NORMAL flags; bar charts in this report and Metabase"],
            ["Present conclusions",
             "DONE",
             "This report (Task 3.1) + README.md + Metabase dashboards"],
            ["SQL query + graphic for anomaly behavior",
             "DONE",
             "Anomaly detection SQL in Section 2.2; visualized in Metabase 'Transactions & operational data' dashboard"],
            ["Endpoint receiving transaction data + alert recommendation",
             "DONE",
             "POST /api/v1/transactions/evaluate -- returns recommendation, alert_level, mandatory_rule_alerts, anomaly_score"],
            ["Query to organize transaction data",
             "DONE",
             "monitoring_minute_pivot VIEW (pivot per status) + monitoring_minute_with_rollups (60-min rolling avg)"],
            ["Real-time graphic",
             "DONE",
             "4 Metabase dashboards including 'Minute monitoring' (denied/failed/reversed per minute with rolling averages)"],
            ["Anomaly detection model",
             "DONE",
             "HybridAnomalyDetector (Z-Score + IF + mandatory rules) + EnsembleAnomalyDetector (IF + LOF + OCSVM + Autoencoder)"],
            ["Automatic anomaly reporting system",
             "DONE",
             "monitoring_worker polls every 30s; sends console + email (SendGrid) + Slack alerts"],
            ["Alert: failed above normal",
             "DONE",
             "ALERT_FAILED_ABOVE_NORMAL -- triggers when failed Z-score > 2.5"],
            ["Alert: reversed above normal",
             "DONE",
             "ALERT_REVERSED_ABOVE_NORMAL + ALERT_BACKEND_REVERSED_ABOVE_NORMAL"],
            ["Alert: denied above normal",
             "DONE",
             "ALERT_DENIED_ABOVE_NORMAL -- triggers when denied Z-score > 2.5"],
            ["Document explaining execution",
             "DONE",
             "This PDF + README.md (668 lines) + docs/architecture_overview.pdf + GitHub repository"],
        ],
        col_widths=[W * 0.33, W * 0.09, W * 0.58],
    )

    pdf.sub_heading("Deliverable Files")
    pdf.table(
        ["File / Location", "Description"],
        [
            ["README.md",                         "Full setup, architecture, API docs, model details, AWS deployment"],
            ["docs/cloudwalk_monitoring_report.pdf","This report -- EDA and task analysis"],
            ["docs/architecture_overview.pdf",     "System architecture and environment reference"],
            ["docs/architecture.excalidraw",       "Editable system diagram"],
            ["sql/init.sql",                       "Schema + COPY + monitoring views"],
            ["sql/monitoring_organized.sql",        "Monitoring views reference"],
            ["src/api/main.py",                    "FastAPI: 10 endpoints including /transactions/evaluate"],
            ["src/models/anomaly_detector.py",     "HybridAnomalyDetector (Z-Score + Isolation Forest)"],
            ["src/models/monitoring_rules.py",     "Mandatory rules: DENIED/FAILED/REVERSED above normal"],
            ["src/workers/anomaly_worker.py",      "Rule-based detection loop"],
            ["src/workers/ai_ml_worker.py",        "Pure ML detection loop (IF + LOF + OCSVM + Autoencoder)"],
            ["src/workers/monitoring_worker.py",   "Notification dispatch loop"],
            ["src/api/notifications.py",           "AlertNotifier: console, email, Slack"],
            ["metabase/Dashboards/manifest.json",  "4 Metabase dashboards (32+ cards)"],
            ["metabase/upload_dashboards.py",      "Idempotent Metabase upload script"],
            ["docker-compose.yaml",               "7-service stack: Postgres, pgAdmin, Metabase, API, 3 workers"],
            ["terraform/",                         "Full AWS IaC: ECS Fargate, RDS, ALB, ECR, CloudWatch"],
        ],
        col_widths=[W * 0.38, W * 0.62],
    )

    pdf.output(OUT_FILE)
    return OUT_FILE


if __name__ == "__main__":
    path = build_pdf()
    print(f"PDF generated: {path}")
