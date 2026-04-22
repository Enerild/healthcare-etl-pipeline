"""PostgreSQL connection factory."""

from __future__ import annotations

import logging
import os

import psycopg2

logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "etl_db"),
        user=os.getenv("DB_USER", "etl_user"),
        password=os.getenv("DB_PASSWORD", "etl_pass"),
    )
