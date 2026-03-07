"""
Shared database utilities for NHS Prescribing Data ingestion.

Provides connection management, CSV format detection, and import helpers.
"""

import json
import ssl
from dataclasses import dataclass

import pyexasol
import requests

from connection_info import get_config

STAGING_SCHEMA = "PRESCRIPTIONS_UK_STAGING"
WAREHOUSE_SCHEMA = "PRESCRIPTIONS_UK"
URLS_FILE = "data/prescription_urls.json"


def connect():
    cfg = get_config()
    conn = pyexasol.connect(
        dsn="{}:{}".format(cfg["host"], cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        encryption=True,
        websocket_sslopt={"cert_reqs": ssl.CERT_NONE, "check_hostname": False},
    )
    conn.execute("CREATE SCHEMA IF NOT EXISTS {}".format(STAGING_SCHEMA))
    conn.execute("CREATE SCHEMA IF NOT EXISTS {}".format(WAREHOUSE_SCHEMA))
    conn.execute("OPEN SCHEMA {}".format(STAGING_SCHEMA))
    return conn


@dataclass
class CsvFormat:
    row_separator: str
    num_columns: int
    has_header: bool

    @property
    def skip(self):
        return 1 if self.has_header else 0


def detect_csv_format(url, sample_size=4096):
    try:
        resp = requests.get(url, headers={"Range": "bytes=0-{}".format(sample_size)}, timeout=30)
        resp.raise_for_status()
        sample = resp.content

        row_sep = "CRLF" if b"\r\n" in sample[:100] else "LF"

        lines = sample.split(b"\n")
        first_line = lines[0].decode("utf-8", errors="ignore")
        num_columns = len(first_line.split(","))

        if len(lines) >= 2 and lines[1].strip():
            second_line = lines[1].decode("utf-8", errors="ignore")
            data_cols = len(second_line.split(","))
            if data_cols != num_columns:
                num_columns = data_cols

        has_header = any(name in first_line.upper() for name in
                         ["SHA", "PCT", "PRACTICE", "BNF CODE", "BNF NAME",
                          "ITEMS", "NIC", "CHEM SUB", "ADDRESS"])

        return CsvFormat(row_separator=row_sep, num_columns=num_columns, has_header=has_header)
    except Exception as e:
        print("  Format detection error: {}, using defaults".format(e))
        return CsvFormat(row_separator="CRLF", num_columns=10, has_header=True)


def import_csv(conn, table_name, csv_url, columns_def, fmt):
    conn.execute("DROP TABLE IF EXISTS {}".format(table_name))
    conn.execute("CREATE TABLE {} ({})".format(table_name, columns_def))

    parts = csv_url.rsplit("/", 1)
    base_url = parts[0]
    filename = parts[1]

    conn.execute("""
        IMPORT INTO {}
        FROM CSV AT '{}'
        FILE '{}'
        COLUMN SEPARATOR = ','
        ROW SEPARATOR = '{}'
        SKIP = {}
        ENCODING = 'UTF8'
    """.format(table_name, base_url, filename, fmt.row_separator, fmt.skip))

    count = conn.execute("SELECT COUNT(*) FROM {}".format(table_name)).fetchone()[0]
    return count


def get_url(period, file_type):
    with open(URLS_FILE) as f:
        data = json.load(f)
    matches = [m for m in data["months"] if m["period"] == period]
    if not matches:
        raise ValueError("Period {} not found in {}".format(period, URLS_FILE))
    return matches[0][file_type]
