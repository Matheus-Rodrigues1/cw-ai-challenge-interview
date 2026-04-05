#!/usr/bin/env python3
"""
Upload Metabase dashboards from JSON definitions (see Dashboards/manifest.json).

Prerequisites
-------------
1. Metabase running (e.g. http://localhost:3000) and initial setup completed.
2. PostgreSQL data source added in Metabase pointing at database ``cloudwalk_transactions``
   (Admin > Databases > Add database).
3. Environment variables (or a ``.env`` file in this folder -- optional):

   METABASE_URL          default http://localhost:3000
   METABASE_EMAIL        admin email used at Metabase first login
   METABASE_PASSWORD     that user's password
   METABASE_DB_NAME      Postgres db name to match (default cloudwalk_transactions)
   METABASE_MANIFEST     path to manifest JSON (default: Dashboards/manifest.json)

API key (Metabase 43+): set METABASE_API_KEY instead of email/password if preferred.

Usage
-----
    cd cloudwalk-monitoring

    # Upload all dashboards
    python metabase/upload_dashboards.py

    # Upload only a specific dashboard (isolation -- other dashboards are untouched)
    python metabase/upload_dashboards.py --dashboard "CloudWalk -- Anomalies with AI Detection"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests

DEFAULT_MANIFEST = Path(__file__).resolve().parent / "Dashboards" / "manifest.json"


def _session(base_url: str) -> requests.Session:
    s = requests.Session()
    s.headers["Content-Type"] = "application/json"
    api_key = os.getenv("METABASE_API_KEY", "").strip()
    if api_key:
        s.headers["X-API-KEY"] = api_key
        return s

    email = os.getenv("METABASE_EMAIL", "").strip()
    password = os.getenv("METABASE_PASSWORD", "").strip()
    if not email or not password:
        print(
            "Set METABASE_EMAIL and METABASE_PASSWORD, or METABASE_API_KEY.",
            file=sys.stderr,
        )
        sys.exit(1)

    r = s.post(
        f"{base_url.rstrip('/')}/api/session",
        json={"username": email, "password": password},
        timeout=30,
    )
    if r.status_code != 200:
        print(f"Login failed: {r.status_code} {r.text}", file=sys.stderr)
        sys.exit(1)
    return s


def resolve_database_id(
    session: requests.Session, base_url: str, target_dbname: str
) -> int:
    r = session.get(f"{base_url.rstrip('/')}/api/database", timeout=30)
    r.raise_for_status()
    payload = r.json()
    rows = payload.get("data", payload) if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        rows = []

    manual = os.getenv("METABASE_DATABASE_ID")
    if manual:
        return int(manual)

    for db in rows:
        details = db.get("details") or {}
        dbname = (details.get("dbname") or details.get("db") or "").strip()
        name = (db.get("name") or "").strip()
        if dbname == target_dbname or name == target_dbname:
            return int(db["id"])

    print(
        "Could not find a Metabase database connection for "
        f"'{target_dbname}'. Add Postgres in Metabase UI or set METABASE_DATABASE_ID.",
        file=sys.stderr,
    )
    sys.exit(1)


def find_existing_dashboards(
    session: requests.Session, base_url: str, name: str
) -> list[int]:
    """Return IDs of every dashboard whose name exactly matches *name*."""
    r = session.get(f"{base_url.rstrip('/')}/api/dashboard", timeout=30)
    r.raise_for_status()
    payload = r.json()
    rows = payload.get("data", payload) if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        rows = []
    return [int(d["id"]) for d in rows if d.get("name") == name]


def delete_dashboard_and_cards(
    session: requests.Session, base_url: str, dashboard_id: int
) -> None:
    """Delete a dashboard and every card (question) that belonged to it."""
    base = base_url.rstrip("/")

    # Collect card IDs before deleting the dashboard
    r = session.get(f"{base}/api/dashboard/{dashboard_id}", timeout=30)
    card_ids: list[int] = []
    if r.status_code == 200:
        dashcards = r.json().get("dashcards") or []
        card_ids = [
            int(dc["card_id"])
            for dc in dashcards
            if dc.get("card_id") is not None
        ]

    # Delete the dashboard
    rd = session.delete(f"{base}/api/dashboard/{dashboard_id}", timeout=30)
    if rd.status_code in (200, 204):
        print(f"  Deleted dashboard id={dashboard_id}")
    else:
        print(
            f"  Warning: could not delete dashboard {dashboard_id}: "
            f"{rd.status_code} {rd.text[:120]}"
        )

    # Delete its cards so they don't pile up as orphans
    for cid in card_ids:
        rc = session.delete(f"{base}/api/card/{cid}", timeout=30)
        if rc.status_code not in (200, 204):
            print(f"  Warning: could not delete card {cid}: {rc.status_code}")


def delete_orphan_cards(
    session: requests.Session, base_url: str, card_names: set[str]
) -> None:
    """Delete every saved question whose name is in *card_names*."""
    base = base_url.rstrip("/")
    r = session.get(f"{base}/api/card", timeout=60)
    if r.status_code != 200:
        print(f"  Warning: could not list cards: {r.status_code}", file=sys.stderr)
        return

    all_cards = r.json()
    if not isinstance(all_cards, list):
        all_cards = all_cards.get("data", [])

    to_delete = [c for c in all_cards if c.get("name") in card_names]
    if to_delete:
        print(f"  Cleaning {len(to_delete)} orphan card(s)...")
    for card in to_delete:
        rc = session.delete(f"{base}/api/card/{card['id']}", timeout=30)
        if rc.status_code in (200, 204):
            print(f"    Deleted card '{card['name']}' (id={card['id']})")
        else:
            print(f"    Warning: could not delete card {card['id']}: {rc.status_code}")


def create_card(
    session: requests.Session,
    base_url: str,
    database_id: int,
    name: str,
    sql: str,
    display: str,
    visualization_settings: dict,
    cache_ttl: int | None = None,
) -> int:
    body = {
        "name": name,
        "dataset_query": {
            "type": "native",
            "native": {"query": sql.strip(), "template-tags": {}},
            "database": database_id,
        },
        "display": display,
        "visualization_settings": visualization_settings or {},
    }
    if cache_ttl is not None:
        body["cache_ttl"] = cache_ttl
    r = session.post(f"{base_url.rstrip('/')}/api/card", json=body, timeout=60)
    if r.status_code not in (200, 202):
        print(f"Card creation failed ({name}): {r.status_code} {r.text}", file=sys.stderr)
        r.raise_for_status()
    return int(r.json()["id"])


def create_dashboard(session: requests.Session, base_url: str, name: str, description: str) -> int:
    body = {"name": name, "description": description or None}
    r = session.post(f"{base_url.rstrip('/')}/api/dashboard", json=body, timeout=30)
    r.raise_for_status()
    return int(r.json()["id"])


def attach_cards(
    session: requests.Session,
    base_url: str,
    dashboard_id: int,
    cards: list[dict],
) -> None:
    r = session.get(f"{base_url.rstrip('/')}/api/dashboard/{dashboard_id}", timeout=30)
    r.raise_for_status()
    d = r.json()

    dashcards = []
    for i, c in enumerate(cards):
        layout = c["layout"]
        dashcards.append(
            {
                "id": -(i + 1),
                "dashboard_id": dashboard_id,
                "card_id": c["card_id"],
                "row": layout["row"],
                "col": layout["col"],
                "size_x": layout["size_x"],
                "size_y": layout["size_y"],
                "parameter_mappings": [],
                "visualization_settings": {},
            }
        )

    d["dashcards"] = dashcards
    d["parameters"] = d.get("parameters") or []
    if not d.get("tabs"):
        d["tabs"] = []

    for key in (
        "last-edit-info",
        "moderation_reviews",
        "collection",
        "entity_id",
    ):
        d.pop(key, None)

    r2 = session.put(
        f"{base_url.rstrip('/')}/api/dashboard/{dashboard_id}",
        json=d,
        timeout=60,
    )
    if r2.status_code != 200:
        print(f"Dashboard update failed: {r2.status_code} {r2.text}", file=sys.stderr)
        r2.raise_for_status()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload Metabase dashboards from a JSON manifest.",
    )
    parser.add_argument(
        "--manifest",
        default=os.getenv("METABASE_MANIFEST", str(DEFAULT_MANIFEST)),
        help="Path to the manifest JSON (default: Dashboards/manifest.json)",
    )
    parser.add_argument(
        "--dashboard",
        default=None,
        help=(
            "Upload only the dashboard whose name matches this value. "
            "Other dashboards in the manifest are left untouched."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    base_url = os.getenv("METABASE_URL", "http://localhost:3000").rstrip("/")
    target_db = os.getenv("METABASE_DB_NAME", "cloudwalk_transactions")
    manifest_path = Path(args.manifest)

    if not manifest_path.is_file():
        print(f"Manifest not found: {manifest_path}", file=sys.stderr)
        sys.exit(1)

    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    session = _session(base_url)
    db_id = resolve_database_id(session, base_url, target_db)
    print(f"Using Metabase database id={db_id} ({target_db})")

    dashboards = manifest.get("dashboards", [])

    if args.dashboard:
        dashboards = [d for d in dashboards if d["name"] == args.dashboard]
        if not dashboards:
            available = [d["name"] for d in manifest.get("dashboards", [])]
            print(
                f"Dashboard '{args.dashboard}' not found in manifest.\n"
                f"Available: {available}",
                file=sys.stderr,
            )
            sys.exit(1)
        print(f"Filter active -- only processing: {args.dashboard}")

    for dash_spec in dashboards:
        name = dash_spec["name"]
        desc = dash_spec.get("description") or ""

        names_to_clean = [name] + dash_spec.get("legacy_names", [])
        for clean_name in names_to_clean:
            existing_ids = find_existing_dashboards(session, base_url, clean_name)
            if existing_ids:
                print(f"Found {len(existing_ids)} existing dashboard(s) named '{clean_name}' -- removing...")
                for eid in existing_ids:
                    delete_dashboard_and_cards(session, base_url, eid)

        card_names = {c["name"] for c in dash_spec.get("cards", [])}
        delete_orphan_cards(session, base_url, card_names)

        print(f"Creating dashboard: {name}")
        dash_id = create_dashboard(session, base_url, name, desc)

        dash_cache_ttl = dash_spec.get("cache_ttl")

        built: list[dict] = []
        for card_spec in dash_spec.get("cards", []):
            card_cache = card_spec.get("cache_ttl", dash_cache_ttl)
            cid = create_card(
                session,
                base_url,
                db_id,
                card_spec["name"],
                card_spec["sql"],
                card_spec.get("display", "table"),
                card_spec.get("visualization_settings") or {},
                cache_ttl=card_cache,
            )
            built.append(
                {
                    "card_id": cid,
                    "layout": card_spec["layout"],
                }
            )
            print(f"  Card: {card_spec['name']} (id={cid})")

        attach_cards(session, base_url, dash_id, built)
        print(f"  Dashboard id={dash_id} -> {base_url}/dashboard/{dash_id}")

    print("Done.")


if __name__ == "__main__":
    main()
