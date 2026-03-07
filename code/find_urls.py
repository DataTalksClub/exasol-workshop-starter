"""
Find NHS Prescribing Data CSV URLs from data.gov.uk (2010-2018).

Scrapes the dataset page to find CSV file URLs grouped by month.
Each month has 3 files: PDPI (prescriptions), ADDR (addresses), CHEM (chemicals).

Source: https://www.data.gov.uk/dataset/176ae264-2484-4afe-a297-d51798eb8228/prescribing-by-gp-practice-presentation-level
"""

import os
import re
import json
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup


DATASET_URL = "https://www.data.gov.uk/dataset/176ae264-2484-4afe-a297-d51798eb8228/prescribing-by-gp-practice-presentation-level"
OUTPUT_FILE = "data/prescription_urls.json"


def extract_period(url):
    decoded = unquote(url)
    match = re.search(r"T(\d{6})", decoded)
    return match.group(1) if match else None


def get_file_type(url):
    upper = unquote(url).upper()
    if "PDPI" in upper:
        return "pdpi"
    elif "ADDR" in upper:
        return "addr"
    elif "CHEM" in upper and "SUBS" in upper:
        return "chem"
    return None


def main():
    print("Fetching {}...".format(DATASET_URL))
    response = requests.get(DATASET_URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    months = {}

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if not href.lower().endswith(".csv"):
            continue

        period = extract_period(href)
        if not period:
            continue

        file_type = get_file_type(href)
        if not file_type:
            continue

        if period not in months:
            months[period] = {"period": period, "pdpi": None, "addr": None, "chem": None}
        months[period][file_type] = href

    result = sorted(months.values(), key=lambda x: x["period"])
    print("Found {} months of data".format(len(result)))
    if result:
        print("Range: {} to {}".format(result[0]["period"], result[-1]["period"]))

    for m in result[:3]:
        print("  {}:".format(m["period"]))
        for key in ["pdpi", "addr", "chem"]:
            if m[key]:
                print("    {}: {}...".format(key, m[key].split("/")[-1][:50]))

    output = {
        "source_url": DATASET_URL,
        "total_months": len(result),
        "months": result
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print("Saved to {}".format(OUTPUT_FILE))


if __name__ == "__main__":
    main()
