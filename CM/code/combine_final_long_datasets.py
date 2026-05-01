"""
Combine the three final long-format central-bank people datasets and remove duplicates.

Inputs
------
- CM/data/central_bank_people_from_banks_long.csv
- CM/data/central_bank_people_from_categories_long.csv
- CM/data/kof_governors_with_sources.csv

Output
------
- CM/data/central_bank_people_combined_long.csv

Method
------
1. Read the three final long-format datasets.
2. Align them to the same expected schema.
3. Append the rows.
4. Drop exact and logical duplicates.
5. Export a single combined long-format dataset.
"""

from pathlib import Path
import re
import unicodedata

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

BANKS_PATH = DATA_DIR / "central_bank_people_from_banks_long.csv"
CATEGORIES_PATH = DATA_DIR / "central_bank_people_from_categories_long.csv"
KOF_PATH = DATA_DIR / "kof_governors_with_sources.csv"
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


def read_final_dataset(path: Path, default_source_dataset: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", dtype=str).fillna("")
    missing = [col for col in FINAL_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"{path.name} is missing expected columns: {missing}")
    df = df[FINAL_COLUMNS].copy()
    df["source_dataset"] = df["source_dataset"].fillna("").astype(str).str.strip()
    df.loc[df["source_dataset"] == "", "source_dataset"] = default_source_dataset
    return df[FINAL_COLUMNS]


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_year(value: object) -> str:
    text = normalize_text(value)
    if re.fullmatch(r"\d{4}\.0", text):
        return text[:-2]
    return text


def normalize_name_key(value: object) -> str:
    text = normalize_text(value).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def canonical_position(value: object) -> str:
    text = normalize_text(value).lower()
    if "deputy governor" in text:
        return "deputy governor"
    if "governor" in text:
        return "governor"
    if "president" in text:
        return "president"
    if "chair" in text:
        return "chair"
    if "board" in text:
        return "board member"
    return text


def join_unique(values: pd.Series) -> str:
    cleaned = [normalize_text(v) for v in values if normalize_text(v)]
    seen = []
    for value in cleaned:
        if value not in seen:
            seen.append(value)
    return " | ".join(seen)


def first_non_empty(values: pd.Series) -> str:
    for value in values:
        value = normalize_text(value)
        if value:
            return value
    return ""


def build_combined_dataset() -> pd.DataFrame:
    banks_df = read_final_dataset(BANKS_PATH, "banks")
    categories_df = read_final_dataset(CATEGORIES_PATH, "categories")
    kof_df = read_final_dataset(KOF_PATH, "kof")

    combined_df = pd.concat([banks_df, categories_df, kof_df], ignore_index=True)

    for col in FINAL_COLUMNS:
        combined_df[col] = combined_df[col].map(normalize_text)

    combined_df["start_year"] = combined_df["start_year"].map(normalize_year)
    combined_df["end_year"] = combined_df["end_year"].map(normalize_year)

    combined_df = combined_df.drop_duplicates().reset_index(drop=True)

    combined_df["country_key"] = combined_df["country"].str.lower()
    combined_df["central_bank_key"] = combined_df["central_bank_name"].str.lower()
    combined_df["name_key"] = combined_df["name"].map(normalize_name_key)
    combined_df["position_key"] = combined_df["position"].map(canonical_position)
    combined_df["start_year_key"] = combined_df["start_year"].map(normalize_year).str.lower()
    combined_df["end_year_key"] = combined_df["end_year"].map(normalize_year).str.lower()

    key_columns = [
        "country_key",
        "central_bank_key",
        "name_key",
        "position_key",
        "start_year_key",
        "end_year_key",
    ]

    combined_df = combined_df.groupby(key_columns, dropna=False, as_index=False).agg(
        {
            "country": first_non_empty,
            "central_bank_name": first_non_empty,
            "name": first_non_empty,
            "position": first_non_empty,
            "start_year": first_non_empty,
            "end_year": first_non_empty,
            "source_dataset": join_unique,
            "source_method": join_unique,
            "source_page": join_unique,
            "source_detail": join_unique,
        }
    ).sort_values(
        ["country", "central_bank_name", "name", "position", "start_year", "end_year"]
    ).reset_index(drop=True)

    return combined_df[FINAL_COLUMNS]


def main() -> None:
    combined_df = build_combined_dataset()
    combined_df.to_csv(OUTPUT_PATH, index=False, sep=";")
    print(f"Combined rows: {len(combined_df)}")
    print(combined_df.head(20).to_string(index=False))
    print(f"\nSaved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
