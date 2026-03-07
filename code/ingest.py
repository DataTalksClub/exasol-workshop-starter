"""
Ingest NHS Prescribing Data (2010-2018) into Exasol.

Imports CSV files directly from web URLs into Exasol using IMPORT FROM CSV AT.
Each month has 3 files: PDPI (prescriptions), ADDR (addresses), CHEM (chemicals).

Usage:
    uv run python ingest.py stage --all
    uv run python ingest.py stage --year 2015
    uv run python ingest.py stage --period 201506
    uv run python ingest.py finalize
    uv run python ingest.py cleanup
    uv run python ingest.py summary
    uv run python ingest.py query
"""

import argparse
import json
import ssl
import time
from pathlib import Path
from dataclasses import dataclass

import pyexasol
import requests

SCHEMA = "PRESCRIPTIONS_UK"
URLS_FILE = "data/prescription_urls.json"


def get_config():
    deployment_dir = Path(__file__).parent.parent / "deployment"
    dep_files = list(deployment_dir.glob("deployment-exasol-*.json"))
    sec_files = list(deployment_dir.glob("secrets-exasol-*.json"))
    if not dep_files or not sec_files:
        raise FileNotFoundError("No deployment files found in {}".format(deployment_dir))
    with open(dep_files[0]) as f:
        deploy = json.load(f)
    with open(sec_files[0]) as f:
        secrets = json.load(f)
    node = next(iter(deploy["nodes"].values()))
    return {
        "host": node["dnsName"],
        "port": int(node["database"]["dbPort"]),
        "user": secrets["dbUsername"],
        "password": secrets["dbPassword"],
    }


def connect():
    cfg = get_config()
    conn = pyexasol.connect(
        dsn="{}:{}".format(cfg["host"], cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        encryption=True,
        websocket_sslopt={"cert_reqs": ssl.CERT_NONE, "check_hostname": False},
    )
    conn.execute("CREATE SCHEMA IF NOT EXISTS {}".format(SCHEMA))
    conn.execute("OPEN SCHEMA {}".format(SCHEMA))
    return conn


# --- CSV format detection ---

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


# --- Raw schemas (match CSV exactly) ---

def get_raw_pdpi_schema(num_columns):
    base = """
    SHA VARCHAR(100), PCT VARCHAR(100), PRACTICE VARCHAR(100),
    BNF_CODE VARCHAR(50), BNF_NAME VARCHAR(2000),
    ITEMS DECIMAL(18,0), NIC DECIMAL(18,2), ACT_COST DECIMAL(18,2),
    QUANTITY DECIMAL(18,0), PERIOD VARCHAR(100)
    """
    if num_columns > 10:
        return base + ", EXTRA_PADDING VARCHAR(2000)"
    return base


def get_raw_addr_schema(num_columns):
    base = """
    PERIOD VARCHAR(100), PRACTICE_CODE VARCHAR(100),
    PRACTICE_NAME VARCHAR(2000), ADDRESS_1 VARCHAR(2000),
    ADDRESS_2 VARCHAR(2000), ADDRESS_3 VARCHAR(2000),
    COUNTY VARCHAR(2000), POSTCODE VARCHAR(200)
    """
    if num_columns > 8:
        return base + ", EXTRA_PADDING VARCHAR(2000)"
    return base


def get_raw_chem_schema(num_columns):
    if num_columns >= 3:
        return "CHEM_SUB VARCHAR(50), NAME VARCHAR(2000), PERIOD VARCHAR(200)"
    return "CHEM_SUB VARCHAR(50), NAME VARCHAR(2000)"


# --- Import helpers ---

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


def load_pdpi(conn, period, url):
    fmt = detect_csv_format(url)
    raw_table = "STG_RAW_PDPI_{}".format(period)
    count = import_csv(conn, raw_table, url, get_raw_pdpi_schema(fmt.num_columns), fmt)
    if count == 0:
        return

    proc_table = "STG_PDPI_{}".format(period)
    conn.execute("DROP TABLE IF EXISTS {}".format(proc_table))
    conn.execute("""CREATE TABLE {} (
        SHA VARCHAR(10), PCT VARCHAR(10), PRACTICE VARCHAR(20),
        BNF_CODE VARCHAR(15), BNF_NAME VARCHAR(200),
        ITEMS DECIMAL(18,0), NIC DECIMAL(18,2), ACT_COST DECIMAL(18,2),
        QUANTITY DECIMAL(18,0), PERIOD VARCHAR(6)
    )""".format(proc_table))

    conn.execute("""
        INSERT INTO {} (SHA, PCT, PRACTICE, BNF_CODE, BNF_NAME, ITEMS, NIC, ACT_COST, QUANTITY, PERIOD)
        SELECT TRIM(SHA), TRIM(PCT), TRIM(PRACTICE), TRIM(BNF_CODE), TRIM(BNF_NAME),
               ITEMS, NIC, ACT_COST, QUANTITY, '{}'
        FROM {}
    """.format(proc_table, period, raw_table))

    conn.execute("DROP TABLE IF EXISTS {}".format(raw_table))
    proc_count = conn.execute("SELECT COUNT(*) FROM {}".format(proc_table)).fetchone()[0]
    print("  PDPI: {:,} rows".format(proc_count))


def load_addr(conn, period, url):
    fmt = detect_csv_format(url)
    raw_table = "STG_RAW_ADDR_{}".format(period)
    count = import_csv(conn, raw_table, url, get_raw_addr_schema(fmt.num_columns), fmt)
    if count == 0:
        return

    proc_table = "STG_ADDR_{}".format(period)
    conn.execute("DROP TABLE IF EXISTS {}".format(proc_table))
    conn.execute("""CREATE TABLE {} (
        PERIOD VARCHAR(6), PRACTICE_CODE VARCHAR(20), PRACTICE_NAME VARCHAR(200),
        ADDRESS_1 VARCHAR(200), ADDRESS_2 VARCHAR(200), ADDRESS_3 VARCHAR(200),
        COUNTY VARCHAR(200), POSTCODE VARCHAR(20)
    )""".format(proc_table))

    conn.execute("""
        INSERT INTO {} (PERIOD, PRACTICE_CODE, PRACTICE_NAME, ADDRESS_1, ADDRESS_2, ADDRESS_3, COUNTY, POSTCODE)
        SELECT '{}', TRIM(PRACTICE_CODE), TRIM(PRACTICE_NAME), TRIM(ADDRESS_1),
               TRIM(ADDRESS_2), TRIM(ADDRESS_3), TRIM(COUNTY), TRIM(POSTCODE)
        FROM {}
    """.format(proc_table, period, raw_table))

    conn.execute("DROP TABLE IF EXISTS {}".format(raw_table))
    proc_count = conn.execute("SELECT COUNT(*) FROM {}".format(proc_table)).fetchone()[0]
    print("  ADDR: {:,} rows".format(proc_count))


def load_chem(conn, period, url):
    fmt = detect_csv_format(url)
    raw_table = "STG_RAW_CHEM_{}".format(period)
    count = import_csv(conn, raw_table, url, get_raw_chem_schema(fmt.num_columns), fmt)
    if count == 0:
        return

    proc_table = "STG_CHEM_{}".format(period)
    conn.execute("DROP TABLE IF EXISTS {}".format(proc_table))
    conn.execute("""CREATE TABLE {} (
        CHEM_SUB VARCHAR(15), NAME VARCHAR(200), PERIOD VARCHAR(6)
    )""".format(proc_table))

    conn.execute("""
        INSERT INTO {} (CHEM_SUB, NAME, PERIOD)
        SELECT TRIM(CHEM_SUB), TRIM(NAME), '{}'
        FROM {}
    """.format(proc_table, period, raw_table))

    conn.execute("DROP TABLE IF EXISTS {}".format(raw_table))
    proc_count = conn.execute("SELECT COUNT(*) FROM {}".format(proc_table)).fetchone()[0]
    print("  CHEM: {:,} rows".format(proc_count))


# --- Commands ---

def cmd_stage(args):
    with open(URLS_FILE) as f:
        data = json.load(f)
    periods = data["months"]

    if args.period:
        periods = [p for p in periods if p["period"] == args.period]
    elif args.year:
        periods = [p for p in periods if p["period"].startswith(args.year)]
    elif not args.all:
        print("Specify --period, --year, or --all")
        return

    conn = connect()

    existing = {row[0] for row in conn.execute(
        "SELECT table_name FROM EXA_ALL_TABLES "
        "WHERE table_schema = '{}' AND table_name LIKE 'STG_PDPI_%'".format(SCHEMA)
    ).fetchall()}

    skipped = 0
    overall_start = time.time()

    for period_data in periods:
        period = period_data["period"]

        if "STG_PDPI_{}".format(period) in existing and not args.force:
            print("[{}] Skipping (already loaded)".format(period))
            skipped += 1
            continue

        print("[{}] Loading...".format(period))
        start = time.time()

        if period_data["pdpi"]:
            load_pdpi(conn, period, period_data["pdpi"])
        if period_data["addr"]:
            load_addr(conn, period, period_data["addr"])
        if period_data["chem"]:
            load_chem(conn, period, period_data["chem"])

        elapsed = time.time() - start
        print("[{}] Done in {:.1f}s".format(period, elapsed))

    total = time.time() - overall_start
    print("Total: {:.1f}s ({:.1f} min), {} skipped".format(total, total / 60, skipped))
    conn.close()


def cmd_finalize(args):
    print("Creating final tables from staging...")
    conn = connect()

    # PRACTICE dimension
    addr_tables = [row[0] for row in conn.execute(
        "SELECT table_name FROM EXA_ALL_TABLES "
        "WHERE table_schema = '{}' AND table_name LIKE 'STG_ADDR_%' "
        "ORDER BY table_name DESC".format(SCHEMA)
    ).fetchall()]

    if addr_tables:
        conn.execute("DROP TABLE IF EXISTS PRACTICE")
        conn.execute("""CREATE TABLE PRACTICE (
            PRACTICE_CODE VARCHAR(20) PRIMARY KEY, PRACTICE_NAME VARCHAR(200),
            ADDRESS_1 VARCHAR(200), ADDRESS_2 VARCHAR(200), ADDRESS_3 VARCHAR(200),
            COUNTY VARCHAR(200), POSTCODE VARCHAR(20)
        )""")

        union = " UNION ALL ".join(
            "SELECT PERIOD, PRACTICE_CODE, PRACTICE_NAME, ADDRESS_1, ADDRESS_2, ADDRESS_3, COUNTY, POSTCODE FROM {}".format(t)
            for t in addr_tables
        )
        conn.execute("""
            INSERT INTO PRACTICE
            SELECT PRACTICE_CODE, PRACTICE_NAME, ADDRESS_1, ADDRESS_2, ADDRESS_3, COUNTY, POSTCODE
            FROM (
                SELECT PERIOD, PRACTICE_CODE, PRACTICE_NAME, ADDRESS_1, ADDRESS_2, ADDRESS_3, COUNTY, POSTCODE,
                       ROW_NUMBER() OVER (PARTITION BY PRACTICE_CODE ORDER BY PERIOD DESC) AS rn
                FROM ({})
            ) WHERE rn = 1
        """.format(union))

        count = conn.execute("SELECT COUNT(*) FROM PRACTICE").fetchone()[0]
        print("PRACTICE: {:,} rows".format(count))

    # CHEMICAL dimension
    chem_tables = [row[0] for row in conn.execute(
        "SELECT table_name FROM EXA_ALL_TABLES "
        "WHERE table_schema = '{}' AND table_name LIKE 'STG_CHEM_%' "
        "ORDER BY table_name DESC".format(SCHEMA)
    ).fetchall()]

    if chem_tables:
        conn.execute("DROP TABLE IF EXISTS CHEMICAL")
        conn.execute("""CREATE TABLE CHEMICAL (
            CHEM_SUB VARCHAR(15) PRIMARY KEY, NAME VARCHAR(200)
        )""")

        union = " UNION ALL ".join(
            "SELECT CHEM_SUB, NAME, PERIOD FROM {}".format(t) for t in chem_tables
        )
        conn.execute("""
            INSERT INTO CHEMICAL
            SELECT CHEM_SUB, NAME
            FROM (
                SELECT CHEM_SUB, NAME, PERIOD,
                       ROW_NUMBER() OVER (PARTITION BY CHEM_SUB ORDER BY PERIOD DESC) AS rn
                FROM ({})
            ) WHERE rn = 1
        """.format(union))

        count = conn.execute("SELECT COUNT(*) FROM CHEMICAL").fetchone()[0]
        print("CHEMICAL: {:,} rows".format(count))

    # PRESCRIPTIONS fact
    pdpi_tables = [row[0] for row in conn.execute(
        "SELECT table_name FROM EXA_ALL_TABLES "
        "WHERE table_schema = '{}' AND table_name LIKE 'STG_PDPI_%' "
        "ORDER BY table_name".format(SCHEMA)
    ).fetchall()]

    if pdpi_tables:
        conn.execute("DROP TABLE IF EXISTS PRESCRIPTIONS")
        conn.execute("""CREATE TABLE PRESCRIPTIONS (
            SHA VARCHAR(10), PCT VARCHAR(10), PRACTICE VARCHAR(20),
            BNF_CODE VARCHAR(15), BNF_NAME VARCHAR(200),
            ITEMS DECIMAL(18,0), NIC DECIMAL(18,2), ACT_COST DECIMAL(18,2),
            QUANTITY DECIMAL(18,0), PERIOD VARCHAR(6)
        )""")

        total = 0
        for i, table in enumerate(pdpi_tables, 1):
            period = table.replace("STG_PDPI_", "")
            start = time.time()
            conn.execute("""
                INSERT INTO PRESCRIPTIONS
                SELECT SHA, PCT, PRACTICE, BNF_CODE, BNF_NAME, ITEMS, NIC, ACT_COST, QUANTITY, PERIOD
                FROM {}
            """.format(table))
            count = conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
            elapsed = time.time() - start
            total += count
            print("  {}/{}: {} - {:,} rows ({:.1f}s)".format(i, len(pdpi_tables), period, count, elapsed))

        conn.execute("ALTER TABLE PRESCRIPTIONS DISTRIBUTE BY PRACTICE")
        print("PRESCRIPTIONS: {:,} total rows".format(total))

    conn.close()
    print("Done!")


def cmd_cleanup(args):
    print("Dropping staging tables...")
    conn = connect()
    for prefix in ["STG_PDPI_", "STG_ADDR_", "STG_CHEM_", "STG_RAW_"]:
        tables = conn.execute(
            "SELECT table_name FROM EXA_ALL_TABLES "
            "WHERE table_schema = '{}' AND table_name LIKE '{}%'".format(SCHEMA, prefix)
        ).fetchall()
        for row in tables:
            conn.execute("DROP TABLE IF EXISTS {}".format(row[0]))
            print("  Dropped: {}".format(row[0]))
    conn.close()
    print("Done!")


def cmd_summary(args):
    conn = connect()
    tables = conn.execute(
        "SELECT table_name FROM EXA_ALL_TABLES "
        "WHERE table_schema = '{}' ORDER BY table_name".format(SCHEMA)
    ).fetchall()
    if not tables:
        print("No tables found.")
    else:
        for row in tables:
            count = conn.execute("SELECT COUNT(*) FROM {}".format(row[0])).fetchone()[0]
            print("  {}: {:,}".format(row[0], count))
    conn.close()


def cmd_query(args):
    conn = connect()

    print("Top 3 most prescribed chemicals in East Central London (EC postcodes):")
    rows = conn.execute("""
        SELECT p.BNF_NAME, SUM(p.ITEMS) AS total
        FROM PRESCRIPTIONS p
        JOIN PRACTICE pr ON p.PRACTICE = pr.PRACTICE_CODE
        WHERE pr.POSTCODE LIKE 'EC%'
        GROUP BY p.BNF_NAME
        ORDER BY total DESC
        LIMIT 3
    """).fetchall()
    for i, row in enumerate(rows, 1):
        print("  {}. {} - {}".format(i, row[0], int(row[1])))

    if rows:
        top = rows[0][0]
        print("\nYear with most prescriptions of '{}':".format(top))
        yr = conn.execute("""
            SELECT FLOOR(p.PERIOD / 100) AS yr, SUM(p.ITEMS) AS total
            FROM PRESCRIPTIONS p
            JOIN PRACTICE pr ON p.PRACTICE = pr.PRACTICE_CODE
            WHERE pr.POSTCODE LIKE 'EC%%' AND p.BNF_NAME = '{}'
            GROUP BY FLOOR(p.PERIOD / 100)
            ORDER BY total DESC
            LIMIT 1
        """.format(top)).fetchone()
        if yr:
            print("  Year: {}, Prescriptions: {}".format(int(yr[0]), int(yr[1])))
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="NHS Prescribing Data Ingestion (2010-2018)")
    parser.add_argument("command", choices=["stage", "finalize", "cleanup", "summary", "query"])
    parser.add_argument("-p", "--period", type=str, default=None, help="Specific period (e.g. 201506)")
    parser.add_argument("-y", "--year", type=str, default=None, help="Specific year (e.g. 2015)")
    parser.add_argument("--all", action="store_true", help="Process all periods")
    parser.add_argument("-f", "--force", action="store_true", help="Re-load even if already loaded")
    args = parser.parse_args()

    cmds = {
        "stage": cmd_stage,
        "finalize": cmd_finalize,
        "cleanup": cmd_cleanup,
        "summary": cmd_summary,
        "query": cmd_query,
    }
    cmds[args.command](args)


if __name__ == "__main__":
    main()
