#!/usr/bin/env python3
"""
Generate docs/architecture.excalidraw — the system architecture diagram
for the CloudWalk Transaction Monitoring project.

Usage:
    py -3 docs/build_architecture_diagram.py
    # or: python docs/build_architecture_diagram.py

Open the output in https://excalidraw.com or the VS Code Excalidraw extension.
"""

import json
import os
import random
import string

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_FILE = os.path.join(OUT_DIR, "architecture.excalidraw")

# ── Helpers ──────────────────────────────────────────────────────────────

_id_counter = 0


def _uid() -> str:
    global _id_counter
    _id_counter += 1
    return f"el_{_id_counter:04d}_{''.join(random.choices(string.ascii_lowercase, k=6))}"


def _rect(
    x: int, y: int, w: int, h: int,
    label: str,
    bg: str = "#ffffff",
    stroke: str = "#1e1e1e",
    font_size: int = 16,
    bold: bool = False,
    rounded: bool = True,
) -> tuple:
    eid = _uid()
    num_lines = label.count("\n") + 1 if label else 1
    line_h = font_size + 6
    text_h = num_lines * line_h
    text_y = y + (h // 2) - (text_h // 2)
    return {
        "id": eid,
        "type": "rectangle",
        "x": x, "y": y,
        "width": w, "height": h,
        "angle": 0,
        "strokeColor": stroke,
        "backgroundColor": bg,
        "fillStyle": "solid",
        "strokeWidth": 2,
        "roughness": 0,
        "opacity": 100,
        "roundness": {"type": 3} if rounded else None,
        "boundElements": [],
        "locked": False,
        "isDeleted": False,
    }, {
        "id": _uid(),
        "type": "text",
        "x": x + 10, "y": text_y,
        "width": w - 20, "height": text_h,
        "angle": 0,
        "strokeColor": "#1e1e1e",
        "backgroundColor": "transparent",
        "fillStyle": "solid",
        "strokeWidth": 1,
        "roughness": 0,
        "opacity": 100,
        "text": label,
        "fontSize": font_size,
        "fontFamily": 1,
        "textAlign": "center",
        "verticalAlign": "middle",
        "containerId": eid,
        "originalText": label,
        "autoResize": True,
        "roundness": None,
        "boundElements": [],
        "locked": False,
        "isDeleted": False,
    }


def _arrow(
    x1: int, y1: int, x2: int, y2: int,
    label: str = "",
    color: str = "#1e1e1e",
) -> list:
    aid = _uid()
    elements = [{
        "id": aid,
        "type": "arrow",
        "x": x1, "y": y1,
        "width": x2 - x1, "height": y2 - y1,
        "angle": 0,
        "strokeColor": color,
        "backgroundColor": "transparent",
        "fillStyle": "solid",
        "strokeWidth": 2,
        "roughness": 0,
        "opacity": 100,
        "points": [[0, 0], [x2 - x1, y2 - y1]],
        "startArrowhead": None,
        "endArrowhead": "arrow",
        "roundness": {"type": 2},
        "boundElements": [],
        "locked": False,
        "isDeleted": False,
    }]
    if label:
        mid_x = (x1 + x2) // 2
        mid_y = (y1 + y2) // 2
        
        text_x = mid_x - 40
        text_y = mid_y - 14
        
        if x1 == x2:
            text_x = mid_x + 8
            text_y = mid_y - 8
        elif y1 == y2:
            text_y = mid_y - 20
        else:
            text_y = mid_y - 22

        elements.append({
            "id": _uid(),
            "type": "text",
            "x": text_x, "y": text_y,
            "width": 80, "height": 16,
            "angle": 0,
            "strokeColor": color,
            "backgroundColor": "#ffffff",
            "fillStyle": "solid",
            "strokeWidth": 1,
            "roughness": 0,
            "opacity": 100,
            "text": label,
            "fontSize": 12,
            "fontFamily": 1,
            "textAlign": "center",
            "verticalAlign": "middle",
            "containerId": None,
            "originalText": label,
            "roundness": None,
            "boundElements": [],
            "locked": False,
            "isDeleted": False,
        })
    return elements


def _group_label(x: int, y: int, text: str, font_size: int = 14, color: str = "#868e96") -> dict:
    return {
        "id": _uid(),
        "type": "text",
        "x": x, "y": y,
        "width": len(text) * (font_size * 0.6),
        "height": font_size + 4,
        "angle": 0,
        "strokeColor": color,
        "backgroundColor": "transparent",
        "fillStyle": "solid",
        "strokeWidth": 1,
        "roughness": 0,
        "opacity": 100,
        "text": text,
        "fontSize": font_size,
        "fontFamily": 1,
        "textAlign": "left",
        "verticalAlign": "top",
        "containerId": None,
        "originalText": text,
        "roundness": None,
        "boundElements": [],
        "locked": False,
        "isDeleted": False,
    }


# ── Build diagram ────────────────────────────────────────────────────────

def build() -> list:
    elements: list = []

    def add(*args, **kwargs):
        r, t = _rect(*args, **kwargs)
        elements.extend([r, t])
        return r

    # ─────────────────────────────────────────────────────────────────────
    # Title
    # ─────────────────────────────────────────────────────────────────────
    elements.append({
        "id": _uid(), "type": "text",
        "x": 200, "y": -40, "width": 600, "height": 30,
        "angle": 0, "strokeColor": "#1e1e1e", "backgroundColor": "transparent",
        "fillStyle": "solid", "strokeWidth": 1, "roughness": 0, "opacity": 100,
        "text": "CloudWalk Transaction Monitoring — Architecture",
        "fontSize": 22, "fontFamily": 1, "textAlign": "center", "verticalAlign": "top",
        "containerId": None, "originalText": "CloudWalk Transaction Monitoring — Architecture",
        "roundness": None, "boundElements": [], "locked": False, "isDeleted": False,
    })

    # ─────────────────────────────────────────────────────────────────────
    # Layer 1 — Data ingestion  (y 10-95)
    # ─────────────────────────────────────────────────────────────────────
    elements.append(_group_label(20, 10, "DATA INGESTION", 12))

    add(30,   35, 160, 55, "data/*.csv\n(5 CSV files)",         bg="#e8f5e9", font_size=13)
    add(230,  35, 180, 55, "init.sql\nCOPY + views",            bg="#e8f5e9", font_size=13)
    add(450,  35, 210, 55, "docker-init/\ncreate metabase_appdb", bg="#e8f5e9", font_size=13)

    # ─────────────────────────────────────────────────────────────────────
    # Layer 2 — PostgreSQL  (y 120-315)
    # ─────────────────────────────────────────────────────────────────────
    elements.append(_group_label(20, 120, "DATABASE", 12))

    add(30, 145, 640, 165, "", bg="#e3f2fd", font_size=14)          # outer box
    elements.append(_group_label(40, 148, "PostgreSQL 15  :5432", 14, "#1565c0"))

    add( 45, 175, 130, 35, "transactions",       bg="#bbdefb", font_size=11)
    add(185, 175, 135, 35, "auth_codes",          bg="#bbdefb", font_size=11)
    add(330, 175, 100, 35, "checkout",            bg="#bbdefb", font_size=11)
    add(440, 175, 215, 35, "transactions_db",     bg="#bbdefb", font_size=11)

    add( 45, 220, 180, 35, "anomaly_results",     bg="#90caf9", font_size=12, bold=True)
    add(240, 220, 180, 35, "notification_log",    bg="#90caf9", font_size=12)
    add(440, 220, 215, 35, "views: pivot+rollups",bg="#e1f5fe", font_size=11)

    # NEW: ai_anomaly_results
    add( 45, 265, 240, 35, "ai_anomaly_results",  bg="#b2dfdb", stroke="#00695c", font_size=12, bold=True)
    elements.append(_group_label(295, 270, "← written by worker-ai-ml", 10, "#00695c"))

    # Arrows: ingest → DB
    elements.extend(_arrow(110,  90, 110, 175, "COPY"))
    elements.extend(_arrow(320,  90, 320, 145, "schema"))
    elements.extend(_arrow(555,  90, 555, 145))

    # ─────────────────────────────────────────────────────────────────────
    # Layer 3 — Workers  (x 700-980, y 120-430)
    # ─────────────────────────────────────────────────────────────────────
    elements.append(_group_label(700, 120, "WORKERS", 12))

    # anomaly_worker (rule-based)
    aw = add(700, 145, 270, 75,
             "anomaly_worker  (every 60s)\nHybridAnomalyDetector\nZ-Score 60% + IF 40% + rules",
             bg="#fff3e0", font_size=12)

    # monitoring_worker
    mw = add(700, 235, 270, 70,
             "monitoring_worker  (every 30s)\npoll unprocessed → notify\nconsole + email",
             bg="#fff3e0", font_size=12)

    # worker-ai-ml (NEW — teal)
    ai = add(700, 320, 270, 85,
             "worker-ai-ml  (every 120s)\nEnsembleAnomalyDetector\nIsolation Forest + LOF\ndata-driven thresholds",
             bg="#e0f2f1", stroke="#00695c", font_size=12)

    # anomaly_worker reads transactions, writes anomaly_results
    elements.extend(_arrow(700, 185, 175, 200, "SELECT"))
    elements.extend(_arrow(700, 200, 225, 255, "INSERT"))

    # worker-ai-ml reads transactions+auth_codes, writes ai_anomaly_results
    elements.extend(_arrow(700, 360, 290, 195, "SELECT", color="#00695c"))
    elements.extend(_arrow(700, 375, 285, 283, "INSERT", color="#00695c"))

    # ─────────────────────────────────────────────────────────────────────
    # Layer 4 — FastAPI  (y 450-570)
    # ─────────────────────────────────────────────────────────────────────
    elements.append(_group_label(20, 440, "API", 12))

    add(30, 460, 640, 110, "", bg="#fce4ec", font_size=14)
    elements.append(_group_label(40, 463, "FastAPI  monitoring_api  :8000", 14, "#c62828"))

    add( 45, 490, 180, 30, "POST /evaluate",           bg="#ffcdd2", font_size=11)
    add(235, 490, 190, 30, "GET /anomalies/*",          bg="#ffcdd2", font_size=11)
    add(435, 490, 220, 30, "POST /notifications/send",  bg="#ef9a9a", font_size=11)

    add( 45, 528, 130, 28, "GET /health",               bg="#ffcdd2", font_size=10)
    add(185, 528, 130, 28, "GET /stats",                bg="#ffcdd2", font_size=10)
    add(325, 528, 200, 28, "GET /notifications/history",bg="#ffcdd2", font_size=10)
    add(535, 528, 125, 28, "GET /transactions",         bg="#ffcdd2", font_size=10)

    # monitoring_worker → API
    elements.extend(_arrow(835, 420, 835, 460, ""))
    elements.extend(_arrow(700, 420, 640, 505, "HTTP"))

    # API reads/writes DB
    elements.extend(_arrow(135, 460, 135, 310, "query"))
    elements.extend(_arrow(535, 460, 420, 310, "write"))

    # ─────────────────────────────────────────────────────────────────────
    # Layer 5 — Alert channels  (x 700, y 450-545)
    # ─────────────────────────────────────────────────────────────────────
    elements.append(_group_label(700, 440, "ALERT CHANNELS", 12))

    add(700, 460, 130, 40, "Console log",       bg="#f5f5f5", font_size=12)
    add(700, 510, 230, 42, "SMTP Email\n(SendGrid)", bg="#fce4ec", font_size=12)

    elements.extend(_arrow(660, 500, 700, 480, ""))
    elements.extend(_arrow(660, 520, 700, 528, ""))

    # ─────────────────────────────────────────────────────────────────────
    # Layer 6 — Dashboards  (y 600-665)
    # ─────────────────────────────────────────────────────────────────────
    elements.append(_group_label(20, 595, "DASHBOARDS", 12))

    add( 30, 618, 220, 52, "Metabase  :3000\n4 dashboards",  bg="#ede7f6", font_size=13)
    add(270, 618, 170, 52, "pgAdmin  :8080",                 bg="#ede7f6", font_size=13)

    elements.extend(_arrow(130, 618, 130, 310))
    elements.extend(_arrow(355, 618, 355, 310))

    # ─────────────────────────────────────────────────────────────────────
    # Layer 7a — Rule-based detection model callout  (x 700, y 580)
    # ─────────────────────────────────────────────────────────────────────
    elements.append(_group_label(700, 575, "DETECTION MODELS", 12))

    add(700, 595, 280, 75,
        "HybridAnomalyDetector\n"
        "Z-Score (60%) + Isolation Forest (40%)\n"
        "+ mandatory rules:\n"
        "denied / failed / reversed above normal",
        bg="#f1f8e9", font_size=11)

    # ─────────────────────────────────────────────────────────────────────
    # Layer 7b — AI/ML model callout  (x 700, y 685)
    # ─────────────────────────────────────────────────────────────────────
    add(700, 685, 280, 80,
        "EnsembleAnomalyDetector  (NEW)\n"
        "Isolation Forest + Local Outlier Factor\n"
        "Adaptive thresholds: P75/P90 of\n"
        "training-set scores — no hardcoded rules",
        bg="#e0f2f1", stroke="#00695c", font_size=11)

    # Model callout connects to workers
    elements.extend(_arrow(840, 595, 840, 405))
    elements.extend(_arrow(840, 685, 840, 405, color="#00695c"))

    return elements


def main():
    random.seed(42)
    elements = build()

    doc = {
        "type": "excalidraw",
        "version": 2,
        "source": "build_architecture_diagram.py",
        "elements": elements,
        "appState": {
            "gridSize": None,
            "viewBackgroundColor": "#ffffff",
        },
        "files": {},
    }

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)

    print(f"Excalidraw diagram generated: {OUT_FILE}")
    print(f"  {len(elements)} elements")
    print("Open in https://excalidraw.com or VS Code Excalidraw extension.")


if __name__ == "__main__":
    main()
