"""
Load ADDR (practice addresses) for a given period.

Pipeline: STG_RAW_ADDR → STG_ADDR (trim) → STG_PROCESSED_ADDR (address concat) → MERGE into warehouse.

Usage:
    uv run python load_addr.py --period 201008
"""

import argparse
import time

from db import (
    connect, detect_csv_format, import_csv, get_url,
    STAGING_SCHEMA, WAREHOUSE_SCHEMA,
)


def get_raw_schema(num_columns):
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


def load(conn, period, url):
    fmt = detect_csv_format(url)

    # Stage 1: load raw CSV
    raw_table = "STG_RAW_ADDR_{}".format(period)
    count = import_csv(conn, raw_table, url, get_raw_schema(fmt.num_columns), fmt)
    if count == 0:
        print("  No rows loaded")
        return

    # Stage 2: trim whitespace
    stg_table = "STG_ADDR_{}".format(period)
    conn.execute("DROP TABLE IF EXISTS {}".format(stg_table))
    conn.execute("""CREATE TABLE {} (
        PERIOD VARCHAR(6),
        PRACTICE_CODE VARCHAR(20),
        PRACTICE_NAME VARCHAR(200),
        ADDRESS_1 VARCHAR(200),
        ADDRESS_2 VARCHAR(200),
        ADDRESS_3 VARCHAR(200),
        COUNTY VARCHAR(200),
        POSTCODE VARCHAR(20)
    )""".format(stg_table))

    conn.execute("""
        INSERT INTO {}
        SELECT
            '{}',
            TRIM(PRACTICE_CODE),
            TRIM(PRACTICE_NAME),
            TRIM(ADDRESS_1),
            TRIM(ADDRESS_2),
            TRIM(ADDRESS_3),
            TRIM(COUNTY),
            TRIM(POSTCODE)
        FROM {}
    """.format(stg_table, period, raw_table))

    conn.execute("DROP TABLE IF EXISTS {}".format(raw_table))

    # Stage 3: combine address fields
    processed_table = "STG_PROCESSED_ADDR_{}".format(period)
    conn.execute("DROP TABLE IF EXISTS {}".format(processed_table))
    conn.execute("""CREATE TABLE {} (
        PERIOD VARCHAR(6),
        PRACTICE_CODE VARCHAR(20),
        PRACTICE_NAME VARCHAR(200),
        ADDRESS VARCHAR(600),
        COUNTY VARCHAR(200),
        POSTCODE VARCHAR(20)
    )""".format(processed_table))

    conn.execute("""
        INSERT INTO {}
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
        FROM {}
    """.format(processed_table, stg_table))

    proc_count = conn.execute("SELECT COUNT(*) FROM {}".format(processed_table)).fetchone()[0]
    print("  STG_PROCESSED_ADDR: {:,} rows".format(proc_count))

    # Stage 4: merge into warehouse
    conn.execute("""
        MERGE INTO {wh}.PRACTICE tgt
        USING {stg}.{processed} src
        ON tgt.PRACTICE_CODE = src.PRACTICE_CODE
        WHEN MATCHED THEN UPDATE SET
            tgt.PRACTICE_NAME = CASE WHEN src.PERIOD >= tgt.PERIOD THEN src.PRACTICE_NAME ELSE tgt.PRACTICE_NAME END,
            tgt.ADDRESS = CASE WHEN src.PERIOD >= tgt.PERIOD THEN src.ADDRESS ELSE tgt.ADDRESS END,
            tgt.COUNTY = CASE WHEN src.PERIOD >= tgt.PERIOD THEN src.COUNTY ELSE tgt.COUNTY END,
            tgt.POSTCODE = CASE WHEN src.PERIOD >= tgt.PERIOD THEN src.POSTCODE ELSE tgt.POSTCODE END,
            tgt.PERIOD = CASE WHEN src.PERIOD >= tgt.PERIOD THEN src.PERIOD ELSE tgt.PERIOD END
        WHEN NOT MATCHED THEN INSERT VALUES (
            src.PRACTICE_CODE, src.PRACTICE_NAME, src.ADDRESS,
            src.COUNTY, src.POSTCODE, src.PERIOD
        )
    """.format(wh=WAREHOUSE_SCHEMA, stg=STAGING_SCHEMA, processed=processed_table))

    wh_count = conn.execute("SELECT COUNT(*) FROM {}.PRACTICE".format(WAREHOUSE_SCHEMA)).fetchone()[0]
    print("  PRACTICE: {:,} rows in warehouse".format(wh_count))


def main():
    parser = argparse.ArgumentParser(description="Load ADDR (practice addresses)")
    parser.add_argument("--period", required=True, help="Period to load (e.g. 201008)")
    args = parser.parse_args()

    url = get_url(args.period, "addr")
    if not url:
        print("No ADDR URL for period {}".format(args.period))
        return

    conn = connect()

    # Ensure warehouse table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS {}.PRACTICE (
            PRACTICE_CODE VARCHAR(20),
            PRACTICE_NAME VARCHAR(200),
            ADDRESS VARCHAR(600),
            COUNTY VARCHAR(200),
            POSTCODE VARCHAR(20),
            PERIOD VARCHAR(6)
        )
    """.format(WAREHOUSE_SCHEMA))

    print("[{}] Loading ADDR...".format(args.period))
    start = time.time()
    load(conn, args.period, url)
    print("[{}] Done in {:.1f}s".format(args.period, time.time() - start))

    conn.close()


if __name__ == "__main__":
    main()
