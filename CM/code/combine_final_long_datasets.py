"""
Combine the two final long-format central-bank people datasets and remove duplicates.

Inputs
------
- CM/data/central_bank_people_from_banks_long.csv
- CM/data/central_bank_people_from_categories_long.csv

Output
------
- CM/data/central_bank_people_combined_long.csv

Method
------
1. Read both final long-format datasets.
2. Align them to the same expected schema.
3. Append the rows.
4. Drop exact and logical duplicates.
5. Export a single combined long-format dataset.
"""

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

BANKS_PATH = DATA_DIR / "central_bank_people_from_banks_long.csv"
CATEGORIES_PATH = DATA_DIR / "central_bank_people_from_categories_long.csv"
OUTPUT_PATH = DATA_DIR / "central_bank_people_combined_long.csv"

FINAL_COLUMNS = [
    "country",
    "central_bank_name",
    "name",
    "position",
    "start_year",
    "end_year",
    "source_dataset",
    "source_method",
    "source_page",
    "source_detail",
]


def read_final_dataset(path: Path, source_dataset: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";")
    required_columns = [col for col in FINAL_COLUMNS if col != "source_dataset"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"{path.name} is missing expected columns: {missing}")
    df = df[required_columns].copy()
    df["source_dataset"] = source_dataset
    return df[FINAL_COLUMNS]


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def build_combined_dataset() -> pd.DataFrame:
    banks_df = read_final_dataset(BANKS_PATH, "banks")
    categories_df = read_final_dataset(CATEGORIES_PATH, "categories")

    combined_df = pd.concat([banks_df, categories_df], ignore_index=True)

    for col in FINAL_COLUMNS:
        combined_df[col] = combined_df[col].map(normalize_text)

    combined_df = combined_df.drop_duplicates().reset_index(drop=True)

    combined_df["country_key"] = combined_df["country"].str.lower()
    combined_df["central_bank_key"] = combined_df["central_bank_name"].str.lower()
    combined_df["name_key"] = combined_df["name"].str.lower()
    combined_df["position_key"] = combined_df["position"].str.lower()
    combined_df["start_year_key"] = combined_df["start_year"].str.lower()
    combined_df["end_year_key"] = combined_df["end_year"].str.lower()

    combined_df = combined_df.drop_duplicates(
        subset=[
            "country_key",
            "central_bank_key",
            "name_key",
            "position_key",
            "start_year_key",
            "end_year_key",
        ]
    ).drop(
        columns=[
            "country_key",
            "central_bank_key",
            "name_key",
            "position_key",
            "start_year_key",
            "end_year_key",
        ]
    ).sort_values(
        ["country", "central_bank_name", "name", "position", "start_year", "end_year"]
    ).reset_index(drop=True)

    return combined_df


def main() -> None:
    combined_df = build_combined_dataset()
    combined_df.to_csv(OUTPUT_PATH, index=False, sep=";")
    print(f"Combined rows: {len(combined_df)}")
    print(combined_df.head(20).to_string(index=False))
    print(f"\nSaved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
