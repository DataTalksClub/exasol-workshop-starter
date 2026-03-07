"""
Detect CSV format for NHS Prescribing Data files.

Downloads the first 4KB of a CSV file using an HTTP Range request
and determines the row separator (CRLF or LF), number of columns,
and whether the file has a header row.

Usage:
    uv run python detect_format.py <url>
    uv run python detect_format.py --period 201008
"""

import argparse
import json

import requests


def detect_csv_format(url, sample_size=4096):
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

    return {
        "row_separator": row_sep,
        "num_columns": num_columns,
        "has_header": has_header,
        "skip": 1 if has_header else 0,
        "first_line": first_line.strip()[:120],
    }


def main():
    parser = argparse.ArgumentParser(description="Detect CSV format for NHS data files")
    parser.add_argument("url", nargs="?", help="URL of a CSV file to check")
    parser.add_argument("--period", help="Check all files for a period (e.g. 201008)")
    args = parser.parse_args()

    if args.period:
        with open("data/prescription_urls.json") as f:
            data = json.load(f)
        matches = [m for m in data["months"] if m["period"] == args.period]
        if not matches:
            print("Period {} not found".format(args.period))
            return
        month = matches[0]
        for file_type in ["pdpi", "addr", "chem"]:
            url = month[file_type]
            if not url:
                print("{}: no URL".format(file_type.upper()))
                continue
            print("{}:".format(file_type.upper()))
            fmt = detect_csv_format(url)
            print("  URL: {}".format(url.split("/")[-1]))
            print("  Row separator: {}".format(fmt["row_separator"]))
            print("  Columns: {}".format(fmt["num_columns"]))
            print("  Has header: {}".format(fmt["has_header"]))
            print("  Skip: {}".format(fmt["skip"]))
            print("  First line: {}".format(fmt["first_line"]))
            print()
    elif args.url:
        fmt = detect_csv_format(args.url)
        for key, val in fmt.items():
            print("{}: {}".format(key, val))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
