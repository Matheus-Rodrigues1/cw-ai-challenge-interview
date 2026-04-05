"""
Centralized database connection pool for the API layer.

Uses psycopg2 with a simple connection-pool pattern so every
request does not pay the cost of a new TCP handshake.
"""

import os
import logging
from contextlib import contextmanager

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger("monitoring-api")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "cloudwalk_transactions"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "admin"),
}

_pool: ThreadedConnectionPool | None = None


def init_pool(minconn: int = 2, maxconn: int = 10):
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(minconn, maxconn, **DB_CONFIG)
        logger.info(f"DB pool created → {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")


def close_pool():
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None


@contextmanager
def get_conn():
    """Yield a connection from the pool; auto-return on exit."""
    init_pool()
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


@contextmanager
def get_cursor():
    """Yield a cursor (auto-commit on success, rollback on error)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            yield cur
