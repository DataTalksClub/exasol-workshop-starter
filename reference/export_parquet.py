"""
Export warehouse tables to Parquet files.

PRESCRIPTION is exported per period (~10M rows each, ~700MB CSV).
PRACTICE and CHEMICAL are small and exported in one go.

Uses pyexasol HTTP transport (CSV) then PyArrow for Parquet conversion.

Usage:
    uv run python export_parquet.py [--output-dir data/parquet]
"""

import argparse
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

import utils.db as db


SCHEMAS = {
    "PRACTICE": pa.schema([
        ("PRACTICE_CODE", pa.string()),
        ("PRACTICE_NAME", pa.string()),
        ("ADDRESS", pa.string()),
        ("COUNTY", pa.string()),
        ("POSTCODE", pa.string()),
        ("PERIOD", pa.string()),
    ]),
    "CHEMICAL": pa.schema([
        ("CHEMICAL_CODE", pa.string()),
        ("CHEMICAL_NAME", pa.string()),
        ("PERIOD", pa.string()),
    ]),
    "PRESCRIPTION": pa.schema([
        ("PRACTICE_CODE", pa.string()),
        ("BNF_CODE", pa.string()),
        ("DRUG_NAME", pa.string()),
        ("ITEMS", pa.int64()),
        ("NET_COST", pa.decimal128(18, 2)),
        ("ACTUAL_COST", pa.decimal128(18, 2)),
        ("QUANTITY", pa.int64()),
        ("PERIOD", pa.string()),
    ]),
}


def csv_to_parquet(csv_path: str, parquet_path: str, schema: pa.Schema) -> None:
    convert_options = pcsv.ConvertOptions(column_types=schema)
    table = pcsv.read_csv(csv_path, convert_options=convert_options)
    pq.write_table(table, parquet_path)


def export_small_table(conn, table: str, output_dir: Path) -> None:
    full_name = f"{db.WAREHOUSE_SCHEMA}.{table}"
    schema = SCHEMAS[table]
    count = conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()[0]

    table_dir = output_dir / table.lower()
    table_dir.mkdir(parents=True, exist_ok=True)

    print(f"{table}: {count:,} rows")
    t0 = time.time()

    csv_path = str(table_dir / f"{table.lower()}.csv")
    parquet_path = str(table_dir / f"{table.lower()}.parquet")

    conn.export_to_file(csv_path, f"SELECT * FROM {full_name}",
                        export_params={"with_column_names": True})
    csv_to_parquet(csv_path, parquet_path, schema)
    os.remove(csv_path)

    size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
    print(f"  {size_mb:.1f} MB, {time.time() - t0:.1f}s")


def export_prescriptions(conn, output_dir: Path) -> None:
    table_dir = output_dir / "prescription"
    table_dir.mkdir(parents=True, exist_ok=True)
    schema = SCHEMAS["PRESCRIPTION"]

    periods = conn.execute(
        f"SELECT DISTINCT PERIOD FROM {db.WAREHOUSE_SCHEMA}.PRESCRIPTION ORDER BY PERIOD"
    ).fetchall()
    periods = [r[0] for r in periods]

    total = conn.execute(f"SELECT COUNT(*) FROM {db.WAREHOUSE_SCHEMA}.PRESCRIPTION").fetchone()[0]
    print(f"PRESCRIPTION: {total:,} rows, {len(periods)} periods")

    rows_done = 0
    t0 = time.time()

    for i, period in enumerate(periods):
        csv_path = str(table_dir / f"{period}.csv")
        parquet_path = str(table_dir / f"{period}.parquet")

        conn.export_to_file(csv_path,
            f"SELECT * FROM {db.WAREHOUSE_SCHEMA}.PRESCRIPTION WHERE PERIOD = '{period}'",
            export_params={"with_column_names": True})

        csv_to_parquet(csv_path, parquet_path, schema)
        os.remove(csv_path)

        size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
        rows_done += conn.execute(
            f"SELECT COUNT(*) FROM {db.WAREHOUSE_SCHEMA}.PRESCRIPTION WHERE PERIOD = '{period}'"
        ).fetchone()[0]
        elapsed = time.time() - t0
        pct = rows_done / total * 100
        print(f"  [{i+1}/{len(periods)}] {period}: {size_mb:.0f} MB, "
              f"{pct:.0f}% done, {elapsed:.0f}s elapsed")

    total_size = sum(f.stat().st_size for f in table_dir.glob("*.parquet")) / (1024 * 1024)
    print(f"  {total_size:.0f} MB total, {time.time() - t0:.0f}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/parquet")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    conn = db.connect()
    db.ensure_schemas(conn)

    start = time.time()
    export_small_table(conn, "PRACTICE", output_dir)
    export_small_table(conn, "CHEMICAL", output_dir)
    export_prescriptions(conn, output_dir)

    conn.close()
    print(f"\nTotal: {time.time() - start:.0f}s")


if __name__ == "__main__":
    main()
