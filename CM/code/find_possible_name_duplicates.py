"""
Detect likely duplicate person records in the combined long-format dataset
using name similarity within comparable groups.

Input
-----
- CM/data/central_bank_people_combined_long.csv

Output
------
- CM/data-aux/possible_name_duplicates.csv

Method
------
1. Read the combined final dataset.
2. Restrict comparisons to rows that share:
   - country
   - central_bank_name
   - position
3. Prefer comparisons where start/end years also match.
4. Compute string similarity on normalized names.
5. Export likely duplicate pairs for manual review.
"""

from difflib import SequenceMatcher
from itertools import combinations
from pathlib import Path
import re
import unicodedata

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
AUX_DIR = BASE_DIR / "data-aux"

INPUT_PATH = DATA_DIR / "central_bank_people_combined_long.csv"
OUTPUT_PATH = AUX_DIR / "possible_name_duplicates.csv"


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def remove_accents(value: str) -> str:
    return "".join(
        char for char in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(char)
    )


def normalize_name_for_similarity(name: str) -> str:
    name = normalize_text(name).lower()
    name = remove_accents(name)
    name = re.sub(r"\b[a-z]\.\b", " ", name)
    name = re.sub(r"\b[a-z]\b", " ", name)
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def years_compatible(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    return (
        normalize_text(a_start) == normalize_text(b_start)
        and normalize_text(a_end) == normalize_text(b_end)
    )


def build_duplicate_candidates(df: pd.DataFrame) -> pd.DataFrame:
    candidates = []

    grouped = df.groupby(
        ["country", "central_bank_name", "position"],
        dropna=False,
        sort=False,
    )

    for (country, central_bank_name, position), group in grouped:
        if len(group) < 2:
            continue

        rows = group.reset_index(drop=True)

        for i, j in combinations(range(len(rows)), 2):
            left = rows.iloc[i]
            right = rows.iloc[j]

            left_name = normalize_name_for_similarity(left["name"])
            right_name = normalize_name_for_similarity(right["name"])

            if not left_name or not right_name:
                continue

            score = similarity(left_name, right_name)
            years_match = years_compatible(
                left["start_year"], left["end_year"],
                right["start_year"], right["end_year"],
            )

            threshold = 0.88 if years_match else 0.94
            if score < threshold:
                continue

            candidates.append(
                {
                    "country": country,
                    "central_bank_name": central_bank_name,
                    "position": position,
                    "name_a": left["name"],
                    "name_b": right["name"],
                    "name_a_normalized": left_name,
                    "name_b_normalized": right_name,
                    "start_year_a": normalize_text(left["start_year"]),
                    "end_year_a": normalize_text(left["end_year"]),
                    "start_year_b": normalize_text(right["start_year"]),
                    "end_year_b": normalize_text(right["end_year"]),
                    "source_dataset_a": normalize_text(left["source_dataset"]),
                    "source_dataset_b": normalize_text(right["source_dataset"]),
                    "source_method_a": normalize_text(left["source_method"]),
                    "source_method_b": normalize_text(right["source_method"]),
                    "source_page_a": normalize_text(left["source_page"]),
                    "source_page_b": normalize_text(right["source_page"]),
                    "source_detail_a": normalize_text(left["source_detail"]),
                    "source_detail_b": normalize_text(right["source_detail"]),
                    "years_match": years_match,
                    "similarity_score": round(score, 4),
                }
            )

    candidates_df = pd.DataFrame(candidates)
    if candidates_df.empty:
        return candidates_df

    return candidates_df.sort_values(
        ["similarity_score", "country", "central_bank_name", "position", "name_a", "name_b"],
        ascending=[False, True, True, True, True, True],
    ).reset_index(drop=True)


def main() -> None:
    AUX_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(INPUT_PATH, sep=";")

    required = {
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
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns in combined dataset: {missing}")

    candidates_df = build_duplicate_candidates(df)
    candidates_df.to_csv(OUTPUT_PATH, index=False, sep=";")

    print(f"Possible duplicate pairs: {len(candidates_df)}")
    if not candidates_df.empty:
        print(candidates_df.head(20).to_string(index=False))
    print(f"\nSaved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
