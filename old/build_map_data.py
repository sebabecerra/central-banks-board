#!/usr/bin/env python3
"""Build country-grouped JSON for the governors map app."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path("/Users/sbc/projects/central-banks-board")
INPUT_CSV = PROJECT_DIR / "data" / "kof_governors_with_sources.csv"
OUTPUT_JSON = PROJECT_DIR / "map-app" / "public" / "governors_by_country.json"
OUTPUT_CSV = PROJECT_DIR / "map-app" / "public" / "kof_governors_with_sources.csv"

NAME_ALIASES = {
    "United States": "United States of America",
    "Russia": "Russian Federation",
    "Bosnia and Herzegovina": "Bosnia and Herz.",
    "Central African Republic": "Central African Rep.",
    "Democratic Republic of the Congo": "Dem. Rep. Congo",
    "Congo, Dem. Rep. (Zaire)": "Dem. Rep. Congo",
    "Congo, Republic of": "Congo",
    "Dominican Republic": "Dominican Rep.",
    "Equatorial Guinea": "Eq. Guinea",
    "Eswatini": "eSwatini",
    "Gambia": "Gambia",
    "Iran, Islamic Rep.": "Iran",
    "Korea, Rep.": "South Korea",
    "Korea, Dem. Rep.": "North Korea",
    "Kyrgyz Republic": "Kyrgyzstan",
    "Lao PDR": "Laos",
    "Moldova": "Moldova",
    "North Macedonia": "Macedonia",
    "Solomon Islands": "Solomon Is.",
    "South Sudan": "S. Sudan",
    "Sao Tome and Principe": "São Tomé and Principe",
    "Venezuela, RB": "Venezuela",
}


def main() -> None:
    df = pd.read_csv(INPUT_CSV)
    for column in ["iso3", "country", "name", "position", "start_year", "end_year", "status", "source_url"]:
        df[column] = df[column].fillna("").astype(str).str.strip()

    grouped = {}
    for iso3, group in df.groupby("iso3", sort=True):
        country = group["country"].iloc[0] if not group.empty else iso3
        records = (
            group.sort_values(["start_year", "end_year", "name"])
            [["name", "position", "start_year", "end_year", "status"]]
            .to_dict(orient="records")
        )
        current = group[group["status"] == "Actual"]
        current_name = current["name"].iloc[0] if not current.empty else ""
        source_url = next((value for value in group["source_url"] if value), "")
        grouped[iso3] = {
            "iso3": iso3,
            "country": country,
            "mapCountryName": NAME_ALIASES.get(country, country),
            "currentGovernor": current_name,
            "totalGovernors": len(group),
            "sourceUrl": source_url,
            "records": records,
        }

    summary = {
        "totalCountries": int(df["iso3"].nunique()),
        "totalRecords": int(len(df)),
        "currentGovernors": int((df["status"] == "Actual").sum()),
        "historicalGovernors": int((df["status"] == "Histórico").sum()),
        "withSourceUrl": int(df["source_url"].fillna("").astype(str).str.strip().ne("").sum()),
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps({"summary": summary, "countries": grouped}, ensure_ascii=False, indent=2))
    shutil.copyfile(INPUT_CSV, OUTPUT_CSV)
    print(f"Wrote {OUTPUT_JSON}")
    print(f"Copied {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
