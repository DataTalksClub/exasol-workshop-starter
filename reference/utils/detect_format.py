"""
Detect CSV format for NHS Prescribing Data files.

Downloads the first 4KB of a CSV file using an HTTP Range request
and determines the row separator (CRLF or LF), number of columns,
and whether the file has a header row.
"""

from dataclasses import dataclass

import requests

HEADER_NAMES = ["SHA", "PCT", "PRACTICE", "BNF CODE", "BNF NAME",
                "ITEMS", "NIC", "CHEM SUB", "ADDRESS"]


@dataclass
class CsvFormat:
    row_separator: str
    num_columns: int
    has_header: bool
    skip: int


def download_sample(url: str, sample_size: int = 4096) -> bytes:
    resp = requests.get(url, headers={"Range": f"bytes=0-{sample_size}"}, timeout=30)
    resp.raise_for_status()
    return resp.content


def detect_row_separator(sample: bytes) -> str:
    if b"\r\n" in sample[:100]:
        return "CRLF"
    else:
        return "LF"


def count_columns(lines: list[bytes]) -> int:
    first_line = lines[0].decode("utf-8", errors="ignore")
    num_columns = len(first_line.split(","))

    if len(lines) >= 2 and lines[1].strip():
        second_line = lines[1].decode("utf-8", errors="ignore")
        data_cols = len(second_line.split(","))
        if data_cols != num_columns:
            num_columns = data_cols

    return num_columns


def check_has_header(line: str) -> bool:
    upper_line = line.upper()
    for name in HEADER_NAMES:
        if name in upper_line:
            return True
    return False


def detect_csv_format(url: str, sample_size: int = 4096) -> CsvFormat:
    sample = download_sample(url, sample_size)

    lines = sample.split(b"\n")
    first_line = lines[0].decode("utf-8", errors="ignore")

    row_separator = detect_row_separator(sample)
    num_columns = count_columns(lines)
    has_header = check_has_header(first_line)

    if has_header:
        skip = 1
    else:
        skip = 0

    return CsvFormat(
        row_separator=row_separator,
        num_columns=num_columns,
        has_header=has_header,
        skip=skip,
    )
