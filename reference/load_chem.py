"""
Load CHEM (chemical substances) for a given period.

Pipeline: STG_RAW_CHEM → STG_CHEM (trim) → MERGE into warehouse.

Usage:
    uv run python load_chem.py --period 201008
"""

import argparse
import time

import pyexasol

import utils.db as db
from utils.detect_format import detect_csv_format


def get_raw_schema(num_columns: int) -> str:
    if num_columns >= 3:
        return "CHEM_SUB VARCHAR(50), NAME VARCHAR(2000), PERIOD VARCHAR(200)"
    return "CHEM_SUB VARCHAR(50), NAME VARCHAR(2000)"


def load_raw(conn: pyexasol.ExaConnection, period: str, url: str) -> int:
    fmt = detect_csv_format(url)
    raw_table = f"STG_RAW_CHEM_{period}"
    count = db.import_csv(conn, raw_table, url, get_raw_schema(fmt.num_columns), fmt)
    return count


def trim(conn: pyexasol.ExaConnection, period: str) -> None:
    raw_table = f"STG_RAW_CHEM_{period}"
    stg_table = f"STG_CHEM_{period}"

    conn.execute(f"DROP TABLE IF EXISTS {stg_table}")
    conn.execute(f"""CREATE TABLE {stg_table} (
        CHEM_SUB VARCHAR(15),
        NAME VARCHAR(200),
        PERIOD VARCHAR(6)
    )""")

    conn.execute(f"""
        INSERT INTO {stg_table}
        SELECT
            TRIM(CHEM_SUB),
            TRIM(NAME),
            '{period}'
        FROM {raw_table}
    """)

    conn.execute(f"DROP TABLE IF EXISTS {raw_table}")

    stg_count = conn.execute(f"SELECT COUNT(*) FROM {stg_table}").fetchone()[0]
    print(f"  STG_CHEM: {stg_count:,} rows")


def merge_into_warehouse(conn: pyexasol.ExaConnection, period: str) -> None:
    stg_table = f"STG_CHEM_{period}"

    conn.execute(f"""
        MERGE INTO {db.WAREHOUSE_SCHEMA}.CHEMICAL tgt
        USING {db.STAGING_SCHEMA}.{stg_table} src
        ON tgt.CHEMICAL_CODE = src.CHEM_SUB
        WHEN MATCHED THEN UPDATE SET
            tgt.CHEMICAL_NAME = {db.newer('NAME', 'CHEMICAL_NAME')},
            tgt.PERIOD = {db.newer('PERIOD')}
        WHEN NOT MATCHED THEN INSERT VALUES (
            src.CHEM_SUB, src.NAME, src.PERIOD
        )
    """)

    wh_count = conn.execute(f"SELECT COUNT(*) FROM {db.WAREHOUSE_SCHEMA}.CHEMICAL").fetchone()[0]
    print(f"  CHEMICAL: {wh_count:,} rows in warehouse")


def load(conn: pyexasol.ExaConnection, period: str, url: str) -> None:
    count = load_raw(conn, period, url)
    if count == 0:
        print("  No rows loaded")
        return

    trim(conn, period)
    merge_into_warehouse(conn, period)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load CHEM (chemical substances)")
    parser.add_argument("--period", required=True, help="Period to load (e.g. 201008)")
    parser.add_argument("--step", choices=["load_raw", "trim", "merge"],
                        help="Run a single step")
    args = parser.parse_args()

    url = db.get_url(args.period, "chem")
    if not url:
        print(f"No CHEM URL for period {args.period}")
        return

    conn = db.connect()
    db.ensure_schemas(conn)

    # Ensure warehouse table exists
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {db.WAREHOUSE_SCHEMA}.CHEMICAL (
            CHEMICAL_CODE VARCHAR(15),
            CHEMICAL_NAME VARCHAR(200),
            PERIOD VARCHAR(6)
        )
    """)

    print(f"[{args.period}] Loading CHEM...")
    start = time.time()

    if not args.step:
        load(conn, args.period, url)
    elif args.step == "load_raw":
        load_raw(conn, args.period, url)
    elif args.step == "trim":
        trim(conn, args.period)
    elif args.step == "merge":
        merge_into_warehouse(conn, args.period)

    print(f"[{args.period}] Done in {time.time() - start:.1f}s")

    conn.close()


if __name__ == "__main__":
    main()
