"""
Load CHEM (chemical substances) for a given period.

Pipeline: STG_RAW_CHEM → STG_CHEM (trim) → MERGE into warehouse.

Usage:
    uv run python load_chem.py --period 201008
"""

import argparse
import time

from db import (
    connect, detect_csv_format, import_csv, get_url,
    STAGING_SCHEMA, WAREHOUSE_SCHEMA,
)


def get_raw_schema(num_columns):
    if num_columns >= 3:
        return "CHEM_SUB VARCHAR(50), NAME VARCHAR(2000), PERIOD VARCHAR(200)"
    return "CHEM_SUB VARCHAR(50), NAME VARCHAR(2000)"


def load(conn, period, url):
    fmt = detect_csv_format(url)

    # Stage 1: load raw CSV
    raw_table = "STG_RAW_CHEM_{}".format(period)
    count = import_csv(conn, raw_table, url, get_raw_schema(fmt.num_columns), fmt)
    if count == 0:
        print("  No rows loaded")
        return

    # Stage 2: trim whitespace
    stg_table = "STG_CHEM_{}".format(period)
    conn.execute("DROP TABLE IF EXISTS {}".format(stg_table))
    conn.execute("""CREATE TABLE {} (
        CHEM_SUB VARCHAR(15),
        NAME VARCHAR(200),
        PERIOD VARCHAR(6)
    )""".format(stg_table))

    conn.execute("""
        INSERT INTO {}
        SELECT
            TRIM(CHEM_SUB),
            TRIM(NAME),
            '{}'
        FROM {}
    """.format(stg_table, period, raw_table))

    conn.execute("DROP TABLE IF EXISTS {}".format(raw_table))

    stg_count = conn.execute("SELECT COUNT(*) FROM {}".format(stg_table)).fetchone()[0]
    print("  STG_CHEM: {:,} rows".format(stg_count))

    # Stage 3: merge into warehouse
    conn.execute("""
        MERGE INTO {wh}.CHEMICAL tgt
        USING {stg}.{table} src
        ON tgt.CHEMICAL_CODE = src.CHEM_SUB
        WHEN MATCHED THEN UPDATE SET
            tgt.CHEMICAL_NAME = CASE WHEN src.PERIOD >= tgt.PERIOD THEN src.NAME ELSE tgt.CHEMICAL_NAME END,
            tgt.PERIOD = CASE WHEN src.PERIOD >= tgt.PERIOD THEN src.PERIOD ELSE tgt.PERIOD END
        WHEN NOT MATCHED THEN INSERT VALUES (
            src.CHEM_SUB, src.NAME, src.PERIOD
        )
    """.format(wh=WAREHOUSE_SCHEMA, stg=STAGING_SCHEMA, table=stg_table))

    wh_count = conn.execute("SELECT COUNT(*) FROM {}.CHEMICAL".format(WAREHOUSE_SCHEMA)).fetchone()[0]
    print("  CHEMICAL: {:,} rows in warehouse".format(wh_count))


def main():
    parser = argparse.ArgumentParser(description="Load CHEM (chemical substances)")
    parser.add_argument("--period", required=True, help="Period to load (e.g. 201008)")
    args = parser.parse_args()

    url = get_url(args.period, "chem")
    if not url:
        print("No CHEM URL for period {}".format(args.period))
        return

    conn = connect()

    # Ensure warehouse table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS {}.CHEMICAL (
            CHEMICAL_CODE VARCHAR(15),
            CHEMICAL_NAME VARCHAR(200),
            PERIOD VARCHAR(6)
        )
    """.format(WAREHOUSE_SCHEMA))

    print("[{}] Loading CHEM...".format(args.period))
    start = time.time()
    load(conn, args.period, url)
    print("[{}] Done in {:.1f}s".format(args.period, time.time() - start))

    conn.close()


if __name__ == "__main__":
    main()
