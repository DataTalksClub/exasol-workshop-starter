"""
Load PDPI (prescriptions) for a given period.

Pipeline: STG_RAW_PDPI → STG_PDPI (trim) → DELETE + INSERT into warehouse.

Usage:
    uv run python load_pdpi.py --period 201008
"""

import argparse
import time

from db import (
    connect, detect_csv_format, import_csv, get_url,
    STAGING_SCHEMA, WAREHOUSE_SCHEMA,
)


def get_raw_schema(num_columns):
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


def load(conn, period, url):
    fmt = detect_csv_format(url)

    # Stage 1: load raw CSV
    raw_table = "STG_RAW_PDPI_{}".format(period)
    count = import_csv(conn, raw_table, url, get_raw_schema(fmt.num_columns), fmt)
    if count == 0:
        print("  No rows loaded")
        return

    # Stage 2: trim whitespace
    stg_table = "STG_PDPI_{}".format(period)
    conn.execute("DROP TABLE IF EXISTS {}".format(stg_table))
    conn.execute("""CREATE TABLE {} (
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
    )""".format(stg_table))

    conn.execute("""
        INSERT INTO {}
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
            '{}'
        FROM {}
    """.format(stg_table, period, raw_table))

    conn.execute("DROP TABLE IF EXISTS {}".format(raw_table))

    stg_count = conn.execute("SELECT COUNT(*) FROM {}".format(stg_table)).fetchone()[0]
    print("  STG_PDPI: {:,} rows".format(stg_count))

    # Stage 3: delete + insert into warehouse
    conn.execute("DELETE FROM {}.PRESCRIPTION WHERE PERIOD = '{}'".format(WAREHOUSE_SCHEMA, period))

    conn.execute("""
        INSERT INTO {wh}.PRESCRIPTION
        SELECT
            PRACTICE,
            BNF_CODE,
            BNF_NAME,
            ITEMS,
            NIC,
            ACT_COST,
            QUANTITY,
            PERIOD
        FROM {stg}.{table}
    """.format(wh=WAREHOUSE_SCHEMA, stg=STAGING_SCHEMA, table=stg_table))

    wh_count = conn.execute(
        "SELECT TO_CHAR(COUNT(*)) FROM {}.PRESCRIPTION".format(WAREHOUSE_SCHEMA)
    ).fetchone()[0]
    print("  PRESCRIPTION: {} rows in warehouse".format(wh_count))


def main():
    parser = argparse.ArgumentParser(description="Load PDPI (prescriptions)")
    parser.add_argument("--period", required=True, help="Period to load (e.g. 201008)")
    args = parser.parse_args()

    url = get_url(args.period, "pdpi")
    if not url:
        print("No PDPI URL for period {}".format(args.period))
        return

    conn = connect()

    # Ensure warehouse table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS {}.PRESCRIPTION (
            PRACTICE_CODE VARCHAR(20),
            BNF_CODE VARCHAR(15),
            DRUG_NAME VARCHAR(200),
            ITEMS DECIMAL(18,0),
            NET_COST DECIMAL(18,2),
            ACTUAL_COST DECIMAL(18,2),
            QUANTITY DECIMAL(18,0),
            PERIOD VARCHAR(6)
        )
    """.format(WAREHOUSE_SCHEMA))

    print("[{}] Loading PDPI...".format(args.period))
    start = time.time()
    load(conn, args.period, url)
    print("[{}] Done in {:.1f}s".format(args.period, time.time() - start))

    conn.close()


if __name__ == "__main__":
    main()
