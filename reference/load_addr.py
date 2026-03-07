"""
Load ADDR (practice addresses) for a given period.

Pipeline: STG_RAW_ADDR → STG_ADDR (trim) → STG_PROCESSED_ADDR (address concat) → MERGE into warehouse.

Usage:
    uv run python load_addr.py --period 201008
"""

import argparse
import time

import pyexasol

import utils.db as db
from utils.detect_format import detect_csv_format


def get_raw_schema(num_columns: int) -> str:
    base = """
    PERIOD VARCHAR(100),
    PRACTICE_CODE VARCHAR(100),
    PRACTICE_NAME VARCHAR(2000),
    ADDRESS_1 VARCHAR(2000),
    ADDRESS_2 VARCHAR(2000),
    ADDRESS_3 VARCHAR(2000),
    COUNTY VARCHAR(2000),
    POSTCODE VARCHAR(200)
    """
    if num_columns > 8:
        return base + ", EXTRA_PADDING VARCHAR(2000)"
    return base


def load_raw(conn: pyexasol.ExaConnection, period: str, url: str) -> int:
    fmt = detect_csv_format(url)
    raw_table = f"STG_RAW_ADDR_{period}"
    schema = get_raw_schema(fmt.num_columns)
    count = db.import_csv(conn, raw_table, url, schema, fmt)
    return count


def trim(conn: pyexasol.ExaConnection, period: str) -> None:
    raw_table = f"STG_RAW_ADDR_{period}"
    stg_table = f"STG_ADDR_{period}"

    conn.execute(f"DROP TABLE IF EXISTS {stg_table}")
    conn.execute(f"""CREATE TABLE {stg_table} (
        PERIOD VARCHAR(6),
        PRACTICE_CODE VARCHAR(20),
        PRACTICE_NAME VARCHAR(200),
        ADDRESS_1 VARCHAR(200),
        ADDRESS_2 VARCHAR(200),
        ADDRESS_3 VARCHAR(200),
        COUNTY VARCHAR(200),
        POSTCODE VARCHAR(20)
    )""")

    conn.execute(f"""
        INSERT INTO {stg_table}
        SELECT
            '{period}',
            TRIM(PRACTICE_CODE),
            TRIM(PRACTICE_NAME),
            TRIM(ADDRESS_1),
            TRIM(ADDRESS_2),
            TRIM(ADDRESS_3),
            TRIM(COUNTY),
            TRIM(POSTCODE)
        FROM {raw_table}
    """)

    conn.execute(f"DROP TABLE IF EXISTS {raw_table}")


def combine_address(conn: pyexasol.ExaConnection, period: str) -> None:
    stg_table = f"STG_ADDR_{period}"
    processed_table = f"STG_PROCESSED_ADDR_{period}"

    conn.execute(f"DROP TABLE IF EXISTS {processed_table}")
    conn.execute(f"""CREATE TABLE {processed_table} (
        PERIOD VARCHAR(6),
        PRACTICE_CODE VARCHAR(20),
        PRACTICE_NAME VARCHAR(200),
        ADDRESS VARCHAR(600),
        COUNTY VARCHAR(200),
        POSTCODE VARCHAR(20)
    )""")

    conn.execute(f"""
        INSERT INTO {processed_table}
        SELECT
            PERIOD,
            PRACTICE_CODE,
            PRACTICE_NAME,
            TRIM(BOTH ', ' FROM REPLACE(
                COALESCE(ADDRESS_1, '') || ', ' ||
                COALESCE(ADDRESS_2, '') || ', ' ||
                COALESCE(ADDRESS_3, ''),
                ', , ', ', '
            )),
            COUNTY,
            POSTCODE
        FROM {stg_table}
    """)

    proc_count = conn.execute(f"SELECT COUNT(*) FROM {processed_table}").fetchone()[0]
    print(f"  STG_PROCESSED_ADDR: {proc_count:,} rows")


def merge_into_warehouse(conn: pyexasol.ExaConnection, period: str) -> None:
    processed_table = f"STG_PROCESSED_ADDR_{period}"

    conn.execute(f"""
        MERGE INTO {db.WAREHOUSE_SCHEMA}.PRACTICE tgt
        USING {db.STAGING_SCHEMA}.{processed_table} src
        ON tgt.PRACTICE_CODE = src.PRACTICE_CODE
        WHEN MATCHED THEN UPDATE SET
            tgt.PRACTICE_NAME = {db.newer('PRACTICE_NAME')},
            tgt.ADDRESS = {db.newer('ADDRESS')},
            tgt.COUNTY = {db.newer('COUNTY')},
            tgt.POSTCODE = {db.newer('POSTCODE')},
            tgt.PERIOD = {db.newer('PERIOD')}
        WHEN NOT MATCHED THEN INSERT VALUES (
            src.PRACTICE_CODE, src.PRACTICE_NAME, src.ADDRESS,
            src.COUNTY, src.POSTCODE, src.PERIOD
        )
    """)

    wh_count = conn.execute(f"SELECT COUNT(*) FROM {db.WAREHOUSE_SCHEMA}.PRACTICE").fetchone()[0]
    print(f"  PRACTICE: {wh_count:,} rows in warehouse")


def load(conn: pyexasol.ExaConnection, period: str, url: str) -> None:
    count = load_raw(conn, period, url)
    if count == 0:
        print("  No rows loaded")
        return

    trim(conn, period)
    combine_address(conn, period)
    merge_into_warehouse(conn, period)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load ADDR (practice addresses)")
    parser.add_argument("--period", required=True, help="Period to load (e.g. 201008)")
    parser.add_argument("--step", choices=["load_raw", "trim", "combine_address", "merge"],
                        help="Run a single step")
    args = parser.parse_args()

    url = db.get_url(args.period, "addr")
    if not url:
        print(f"No ADDR URL for period {args.period}")
        return

    conn = db.connect()
    db.ensure_schemas(conn)

    # Ensure warehouse table exists
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {db.WAREHOUSE_SCHEMA}.PRACTICE (
            PRACTICE_CODE VARCHAR(20),
            PRACTICE_NAME VARCHAR(200),
            ADDRESS VARCHAR(600),
            COUNTY VARCHAR(200),
            POSTCODE VARCHAR(20),
            PERIOD VARCHAR(6)
        )
    """)

    print(f"[{args.period}] Loading ADDR...")
    start = time.time()

    if not args.step:
        load(conn, args.period, url)
    elif args.step == "load_raw":
        load_raw(conn, args.period, url)
    elif args.step == "trim":
        trim(conn, args.period)
    elif args.step == "combine_address":
        combine_address(conn, args.period)
    elif args.step == "merge":
        merge_into_warehouse(conn, args.period)

    print(f"[{args.period}] Done in {time.time() - start:.1f}s")

    conn.close()


if __name__ == "__main__":
    main()
