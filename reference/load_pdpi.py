"""
Load PDPI (prescriptions) for a given period.

Pipeline: STG_RAW_PDPI → STG_PDPI (trim) → DELETE + INSERT into warehouse.

Usage:
    uv run python load_pdpi.py --period 201008
"""

import argparse
import time

import pyexasol

import utils.db as db
from utils.detect_format import detect_csv_format


def get_raw_schema(num_columns: int) -> str:
    base = """
    SHA VARCHAR(100),
    PCT VARCHAR(100),
    PRACTICE VARCHAR(100),
    BNF_CODE VARCHAR(50),
    BNF_NAME VARCHAR(2000),
    ITEMS DECIMAL(18,0),
    NIC DECIMAL(18,2),
    ACT_COST DECIMAL(18,2),
    QUANTITY DECIMAL(18,0),
    PERIOD VARCHAR(100)
    """
    if num_columns > 10:
        return base + ", EXTRA_PADDING VARCHAR(2000)"
    return base


def load_raw(conn: pyexasol.ExaConnection, period: str, url: str) -> int:
    fmt = detect_csv_format(url)
    raw_table = f"STG_RAW_PDPI_{period}"
    count = db.import_csv(conn, raw_table, url, get_raw_schema(fmt.num_columns), fmt)
    return count


def trim(conn: pyexasol.ExaConnection, period: str) -> None:
    raw_table = f"STG_RAW_PDPI_{period}"
    stg_table = f"STG_PDPI_{period}"

    conn.execute(f"DROP TABLE IF EXISTS {stg_table}")
    conn.execute(f"""CREATE TABLE {stg_table} (
        SHA VARCHAR(10),
        PCT VARCHAR(10),
        PRACTICE VARCHAR(20),
        BNF_CODE VARCHAR(15),
        BNF_NAME VARCHAR(200),
        ITEMS DECIMAL(18,0),
        NIC DECIMAL(18,2),
        ACT_COST DECIMAL(18,2),
        QUANTITY DECIMAL(18,0),
        PERIOD VARCHAR(6)
    )""")

    conn.execute(f"""
        INSERT INTO {stg_table}
        SELECT
            TRIM(SHA),
            TRIM(PCT),
            TRIM(PRACTICE),
            TRIM(BNF_CODE),
            TRIM(BNF_NAME),
            ITEMS,
            NIC,
            ACT_COST,
            QUANTITY,
            '{period}'
        FROM {raw_table}
    """)

    conn.execute(f"DROP TABLE IF EXISTS {raw_table}")

    stg_count = conn.execute(f"SELECT COUNT(*) FROM {stg_table}").fetchone()[0]
    print(f"  STG_PDPI: {stg_count:,} rows")


def insert_into_warehouse(conn: pyexasol.ExaConnection, period: str) -> None:
    stg_table = f"STG_PDPI_{period}"

    conn.execute(f"DELETE FROM {db.WAREHOUSE_SCHEMA}.PRESCRIPTION WHERE PERIOD = '{period}'")

    conn.execute(f"""
        INSERT INTO {db.WAREHOUSE_SCHEMA}.PRESCRIPTION
        SELECT
            PRACTICE,
            BNF_CODE,
            SUBSTR(BNF_CODE, 1, 9),
            BNF_NAME,
            ITEMS,
            NIC,
            ACT_COST,
            QUANTITY,
            PERIOD
        FROM {db.STAGING_SCHEMA}.{stg_table}
    """)

    wh_count = conn.execute(
        f"SELECT TO_CHAR(COUNT(*)) FROM {db.WAREHOUSE_SCHEMA}.PRESCRIPTION"
    ).fetchone()[0]
    print(f"  PRESCRIPTION: {wh_count} rows in warehouse")


def load(conn: pyexasol.ExaConnection, period: str, url: str) -> None:
    count = load_raw(conn, period, url)
    if count == 0:
        print("  No rows loaded")
        return

    trim(conn, period)
    insert_into_warehouse(conn, period)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load PDPI (prescriptions)")
    parser.add_argument("--period", required=True, help="Period to load (e.g. 201008)")
    parser.add_argument("--step", choices=["load_raw", "trim", "insert"],
                        help="Run a single step")
    args = parser.parse_args()

    url = db.get_url(args.period, "pdpi")
    if not url:
        print(f"No PDPI URL for period {args.period}")
        return

    conn = db.connect()
    db.ensure_schemas(conn)

    # Ensure warehouse table exists
    db.create_if_not_exists(conn, f"""
        CREATE TABLE IF NOT EXISTS {db.WAREHOUSE_SCHEMA}.PRESCRIPTION (
            PRACTICE_CODE VARCHAR(20),
            BNF_CODE VARCHAR(15),
            CHEMICAL_CODE VARCHAR(9),
            DRUG_NAME VARCHAR(200),
            ITEMS DECIMAL(18,0),
            NET_COST DECIMAL(18,2),
            ACTUAL_COST DECIMAL(18,2),
            QUANTITY DECIMAL(18,0),
            PERIOD VARCHAR(6)
        )
    """)

    print(f"[{args.period}] Loading PDPI...")
    start = time.time()

    if not args.step:
        load(conn, args.period, url)
    elif args.step == "load_raw":
        load_raw(conn, args.period, url)
    elif args.step == "trim":
        trim(conn, args.period)
    elif args.step == "insert":
        insert_into_warehouse(conn, args.period)

    print(f"[{args.period}] Done in {time.time() - start:.1f}s")

    conn.close()


if __name__ == "__main__":
    main()
