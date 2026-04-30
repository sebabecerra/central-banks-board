#!/usr/bin/env python3
"""Clean and separate governor names from central_bank_governors.csv."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pandas as pd


INPUT_PATH = Path("/Users/sbc/projects/central-banks-board/data/central_bank_governors.csv")
OUTPUT_PATH = Path("/Users/sbc/projects/central-banks-board/data/central_bank_governors_clean.csv")


def normalize_key(key: str) -> str:
    key = str(key or "")
    key = re.sub(r"\[[^\]]*\]", "", key)
    key = key.lower().strip()
    key = key.replace("№", "no").replace("#", "no")
    key = re.sub(r"\s+", " ", key)
    return key


def clean_text(value: str) -> str:
    value = str(value or "")
    value = re.sub(r"\[[^\]]*\]", "", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" -–—,;")


def parse_row_data(raw: str) -> dict[str, str]:
    if pd.isna(raw):
        return {}
    try:
        data = ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): clean_text(v) for k, v in data.items() if clean_text(v)}


def first_matching_value(data: dict[str, str], patterns: list[str]) -> str:
    normalized = [(key, normalize_key(key), value) for key, value in data.items()]
    for pattern in patterns:
        for _key, normalized_key, value in normalized:
            if pattern == normalized_key:
                return value
    for pattern in patterns:
        for _key, normalized_key, value in normalized:
            if pattern in normalized_key:
                return value
    return ""


def infer_role(data: dict[str, str], source_label: str) -> str:
    label = clean_text(source_label).lower()
    if "chair" in label:
        return "Chair"
    if "president" in label:
        return "President"
    if "governor" in label:
        return "Governor"

    for key in data:
        normalized = normalize_key(key)
        if "chair" in normalized:
            return "Chair"
        if "president" in normalized:
            return "President"
        if "governor" in normalized:
            return "Governor"
    return ""


def split_term(term_value: str) -> tuple[str, str]:
    if not term_value:
        return "", ""
    pieces = re.split(r"\s*[–—-]\s*", term_value, maxsplit=1)
    if len(pieces) == 2:
        return clean_text(pieces[0]), clean_text(pieces[1])
    return clean_text(term_value), ""


def infer_name(data: dict[str, str]) -> str:
    name_patterns = [
        "name",
        "governor",
        "president",
        "chairman",
        "chair",
        "key people",
    ]
    name = first_matching_value(data, name_patterns)
    name = clean_text(name)
    if name.lower() in {
        "",
        "name",
        "governor",
        "president",
        "chairman",
        "chair",
        "key people",
        "board of directors",
    }:
        return ""
    return name


def looks_like_real_person(name: str) -> bool:
    lowered = clean_text(name).lower()
    if not lowered:
        return False
    blocked_prefixes = (
        "governors of ",
        "governor of ",
        "presidents of ",
        "president of ",
        "chairmen of ",
        "chairman of ",
        "chairs of ",
        "list of ",
        "board of ",
    )
    if lowered.startswith(blocked_prefixes):
        return False
    if lowered in {"governors", "governor", "presidents", "president", "chairmen", "chairman"}:
        return False
    return True


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cleaned_rows: list[dict[str, str]] = []

    for row in df.itertuples(index=False):
        data = parse_row_data(row.row_data)
        governor_name = infer_name(data)
        if not governor_name or not looks_like_real_person(governor_name):
            continue

        start_date = first_matching_value(
            data,
            [
                "took office",
                "entered office",
                "from",
                "start",
                "term of office | start of term",
                "term of office",
                "period",
                "term",
                "tenure",
                "time in office",
            ],
        )
        end_date = first_matching_value(
            data,
            [
                "left office",
                "exited office",
                "until",
                "end",
                "term of office | end of term",
            ],
        )

        if not end_date and start_date:
            split_start, split_end = split_term(start_date)
            if split_end:
                start_date, end_date = split_start, split_end

        cleaned_rows.append(
            {
                "country": clean_text(row.country),
                "central_bank": clean_text(row.central_bank),
                "wikipedia_bank_url": clean_text(row.wikipedia_bank_url),
                "source_type": clean_text(row.source_type),
                "source_label": clean_text(row.source_label),
                "role": infer_role(data, row.source_label),
                "governor_name": governor_name,
                "start_date": clean_text(start_date),
                "end_date": clean_text(end_date),
                "row_data": row.row_data,
            }
        )

    clean_df = pd.DataFrame(cleaned_rows)
    if clean_df.empty:
        return clean_df

    clean_df = clean_df.drop_duplicates(
        subset=["country", "central_bank", "governor_name", "start_date", "end_date"]
    ).reset_index(drop=True)
    return clean_df


def main() -> None:
    df = pd.read_csv(INPUT_PATH)
    clean_df = clean_dataframe(df)
    clean_df.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {OUTPUT_PATH}")
    print(f"Rows: {len(clean_df)}")


if __name__ == "__main__":
    main()
