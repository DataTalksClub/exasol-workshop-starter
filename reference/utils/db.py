"""
Shared database utilities for NHS Prescribing Data ingestion.

Provides connection management and import helpers.
"""

import json
import ssl

import pyexasol

from utils.connection_info import get_config
from utils.detect_format import CsvFormat


STAGING_SCHEMA = "PRESCRIPTIONS_UK_STAGING"
WAREHOUSE_SCHEMA = "PRESCRIPTIONS_UK"
URLS_FILE = "data/prescription_urls.json"


def connect() -> pyexasol.ExaConnection:
    cfg = get_config()
    conn = pyexasol.connect(
        dsn=f"{cfg['host']}:{cfg['port']}",
        user=cfg["user"],
        password=cfg["password"],
        encryption=True,
        websocket_sslopt={"cert_reqs": ssl.CERT_NONE, "check_hostname": False},
    )
    return conn


def ensure_schemas(conn: pyexasol.ExaConnection) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}")
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {WAREHOUSE_SCHEMA}")
    conn.execute(f"OPEN SCHEMA {STAGING_SCHEMA}")


def import_csv(
    conn: pyexasol.ExaConnection,
    table_name: str,
    csv_url: str,
    columns_def: str,
    fmt: CsvFormat,
) -> int:
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} ({columns_def})")

    parts = csv_url.rsplit("/", 1)
    base_url = parts[0]
    filename = parts[1]

    conn.execute(f"""
        IMPORT INTO {table_name}
        FROM CSV AT '{base_url}'
        FILE '{filename}'
        COLUMN SEPARATOR = ','
        ROW SEPARATOR = '{fmt.row_separator}'
        SKIP = {fmt.skip}
        ENCODING = 'UTF8'
    """)

    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    return count


def newer(col: str, tgt_col: str | None = None) -> str:
    if tgt_col is None:
        tgt_col = col
    return f"""CASE
                WHEN src.PERIOD >= tgt.PERIOD THEN src.{col}
                ELSE tgt.{tgt_col}
            END"""


def get_url(period: str, file_type: str) -> str:
    with open(URLS_FILE) as f:
        data = json.load(f)
    
    matches = [m for m in data["months"] if m["period"] == period]

    if not matches:
        raise ValueError(f"Period {period} not found in {URLS_FILE}")

    return matches[0][file_type]
