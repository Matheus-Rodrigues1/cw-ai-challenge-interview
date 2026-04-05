#!/usr/bin/env python3
"""
Upload "Anomalies with AI Detection" dashboard to Metabase.

This script is a standalone entry-point for the AI dashboard and is independent
of the existing rule-based dashboards defined in Dashboards/manifest.json.

Prerequisites
-------------
1. Metabase running and initial setup completed (http://localhost:3000).
2. PostgreSQL data source added in Metabase pointing at cloudwalk_transactions.
3. worker-ai-ml container has run at least once (ai_anomaly_results table exists).

Environment variables
---------------------
  METABASE_URL        default http://localhost:3000
  METABASE_EMAIL      admin email used at Metabase first login
  METABASE_PASSWORD   that user's password
  METABASE_DB_NAME    Postgres db name (default cloudwalk_transactions)

  Or, with Metabase 43+ API key:
  METABASE_API_KEY    API key instead of email/password

Usage
-----
    cd cloudwalk-monitoring
    METABASE_EMAIL=admin@admin.com METABASE_PASSWORD=metabase123 \\
        python metabase/upload_ai_dashboard.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Point to the AI-specific manifest before loading the shared upload logic
AI_MANIFEST = str(
    Path(__file__).resolve().parent / "Dashboards" / "ai_manifest.json"
)
os.environ.setdefault("METABASE_MANIFEST", AI_MANIFEST)

# Reuse the generic upload logic — no duplication
sys.path.insert(0, str(Path(__file__).resolve().parent))
from upload_dashboards import main  # noqa: E402

if __name__ == "__main__":
    main()
