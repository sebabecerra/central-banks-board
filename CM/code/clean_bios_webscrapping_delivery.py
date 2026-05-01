"""
Stage 2 for Bios_WebScrapping: clean the raw enrichment into a delivery file.

This script is the delivery stage for the Bios pipeline. It takes the raw
Wikipedia/Wikidata enrichment, keeps only the requested columns, normalizes the
codebook fields, and blanks enriched values when the underlying match looks
unreliable.

Preferred inputs
----------------
- CM/data/bios_webscrapping_wikipedia_enriched.xlsx
- CM/data-aux/bios_webscrapping_wikipedia_enriched_audit.csv
- CM/data/bios_webscrapping_wikipedia_enriched.csv

Outputs
-------
- CM/data/Bios_WebScrapping_enriched.csv
- CM/data/bios_webscrapping_wikipedia_enriched.csv
- CM/data-aux/bios_webscrapping_wikipedia_enriched_audit.csv

Method
------
1. Keep only the columns required by the delivery specification.
2. Normalize Position, Sex, Education, years, and months.
3. Clean university fields.
4. Reject suspicious Wikipedia matches conservatively.
5. Save a delivery file and a separate audit file.
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_AUX_DIR = BASE_DIR / "data-aux"

INPUT_PATH = DATA_DIR / "bios_webscrapping_wikipedia_enriched.csv"
INPUT_XLSX_PATH = DATA_DIR / "bios_webscrapping_wikipedia_enriched.xlsx"
DELIVERY_PATH = DATA_DIR / "Bios_WebScrapping_enriched.csv"
NORMALIZED_PATH = DATA_DIR / "bios_webscrapping_wikipedia_enriched.csv"
AUDIT_PATH = DATA_AUX_DIR / "bios_webscrapping_wikipedia_enriched_audit.csv"

DELIVERY_COLUMNS = [
    "PName_original",
    "PName",
    "iso3",
    "first",
    "last",
    "iso3Birth",
    "Start_year",
    "Start_month",
    "End_year",
    "End_month",
    "Position",
    "Sex",
    "Birth_year",
    "Birth_month",
    "Education",
    "BA_or_MA",
    "MBA",
    "PhD",
    "CountryBirth",
    "CityBirth",
]

ENRICHED_FIELDS = [
    "Start_year",
    "Start_month",
    "End_year",
    "End_month",
    "Birth_year",
    "Birth_month",
    "Education",
    "BA_or_MA",
    "MBA",
    "PhD",
    "CountryBirth",
    "CityBirth",
]

SUSPICIOUS_TITLE_TERMS = {
    "ballpark",
    "stadium",
    "airport",
    "school",
    "university",
    "bank",
    "park",
    "list",
    "category",
}

EDUCATION_ALLOWED = {"1", "2", "3", "4", "6"}
POSITION_ALLOWED = {"0", "1", "2"}
SEX_ALLOWED = {"0", "1"}


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_name(value: object) -> str:
    text = clean_text(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_numeric_code(value: object, allowed: set[str]) -> str:
    text = clean_text(value)
    if not text:
        return ""
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    if text in allowed:
        return text
    return ""


def normalize_year_or_month(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    if re.fullmatch(r"\d{1,4}", text):
        return text
    return ""


def first_name_compatible(file_first: str, title_first: str) -> bool:
    file_first = normalize_name(file_first)
    title_first = normalize_name(title_first)
    if not file_first or not title_first:
        return False
    if len(file_first) <= 1:
        return False
    if file_first == title_first:
        return True
    if len(file_first) >= 4 and (file_first in title_first or title_first in file_first):
        return True
    return False


def is_reliable_match(row: pd.Series) -> bool:
    if clean_text(row.get("Wikipedia_match_status", "")) != "matched":
        return False

    title = clean_text(row.get("Wikipedia_title", ""))
    qid = clean_text(row.get("Wikipedia_qid", ""))
    if not title or not qid:
        return False

    title_key = normalize_name(title)
    search_name = clean_text(row.get("PName", "")) or clean_text(row.get("PName_original", ""))
    search_key = normalize_name(search_name)
    file_first = clean_text(row.get("first", ""))
    file_last = normalize_name(row.get("last", ""))

    if not search_key or not file_last:
        return False

    if any(term in title_key.split() for term in SUSPICIOUS_TITLE_TERMS):
        return False

    title_tokens = title_key.split()
    if not title_tokens or title_tokens[-1] != file_last:
        return False

    title_first = title_tokens[0]
    if not first_name_compatible(file_first, title_first):
        return False

    return True


def extract_institutions(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""

    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"\b(?:B\.?A\.?|M\.?A\.?|M\.?B\.?A\.?|Ph\.?D\.?|MSc|BSc|MPA|Licentiate|Degree in Law|High school)\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)

    pattern = re.compile(
        r"([A-Z][A-Za-zÀ-ÿ'’&.\-]*(?:\s+(?:of|the|and|for|de|del|di|da|du|la|le|in|[A-Z][A-Za-zÀ-ÿ'’&.\-]*)){0,8}\s+"
        r"(?:University|College|School|Institute|Academy|Polytechnic)"
        r"(?:\s+of\s+(?:the\s+)?[A-Z][A-Za-zÀ-ÿ'’&.\-]*){0,4})"
    )
    matches = pattern.findall(text)

    special = []
    for token in ["INSEAD", "MIT", "Harvard Kennedy School", "London School of Economics"]:
        if token in text:
            special.append(token)

    institutions = []
    for item in matches + special:
        item = clean_text(item).strip(" ,;")
        subparts = [item]
        if ";" not in item:
            subparts = [
                clean_text(part)
                for part in re.split(
                    r"(?=(?:[A-Z][A-Za-zÀ-ÿ'’&.\-]*\s+){1,4}(?:University|College|School|Institute|Academy|Polytechnic)\b)",
                    item,
                )
                if clean_text(part)
            ]

        for part in subparts:
            anchor_count = len(re.findall(r"\b(?:University|College|School|Institute|Academy|Polytechnic|INSEAD|MIT)\b", part))
            if anchor_count == 0:
                continue
            if anchor_count > 1 and ";" not in part:
                continue
            if re.search(r"\b(?:degree|phd|mba|b\.?a\.?|m\.?a\.?|bsc|msc|mpa)\b", part, flags=re.IGNORECASE):
                continue
            if len(part.split()) > 8:
                continue
            if part and part not in institutions:
                institutions.append(part)

    return "; ".join(institutions)


def blank_enriched_fields(row: pd.Series) -> pd.Series:
    for field in ENRICHED_FIELDS:
        row[field] = ""
    row["Wikipedia_match_status"] = "rejected"
    return row


def main() -> None:
    if INPUT_XLSX_PATH.exists():
        source_path = INPUT_XLSX_PATH
    elif AUDIT_PATH.exists():
        source_path = AUDIT_PATH
    else:
        source_path = INPUT_PATH
    if not source_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {source_path}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DATA_AUX_DIR.mkdir(parents=True, exist_ok=True)

    if source_path.suffix.lower() == ".xlsx":
        df = pd.read_excel(source_path)
    else:
        df = pd.read_csv(source_path, sep=";")

    for col in DELIVERY_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    for col in ["Position", "Sex", "Education"]:
        allowed = POSITION_ALLOWED if col == "Position" else SEX_ALLOWED if col == "Sex" else EDUCATION_ALLOWED
        df[col] = df[col].map(lambda x: normalize_numeric_code(x, allowed))

    for col in ["Birth_year", "Birth_month", "Start_year", "Start_month", "End_year", "End_month"]:
        df[col] = df[col].map(normalize_year_or_month)

    for col in ["BA_or_MA", "MBA", "PhD"]:
        df[col] = df[col].map(extract_institutions)

    df["CountryBirth"] = df["CountryBirth"].map(clean_text)
    df["CityBirth"] = df["CityBirth"].map(clean_text)

    reliable_mask = df.apply(is_reliable_match, axis=1)
    df.loc[~reliable_mask] = df.loc[~reliable_mask].apply(blank_enriched_fields, axis=1)

    audit_df = df.copy()
    audit_df["Wikipedia_match_reliable"] = reliable_mask.map(lambda x: "1" if x else "0")

    delivery_df = df[DELIVERY_COLUMNS].copy()
    delivery_df.to_csv(DELIVERY_PATH, index=False, sep=";")
    delivery_df.to_csv(NORMALIZED_PATH, index=False, sep=";")
    audit_df.to_csv(AUDIT_PATH, index=False, sep=";")

    print(f"Rows cleaned: {len(delivery_df)}")
    print(f"Reliable matches kept: {int(reliable_mask.sum())}")
    print(f"Rows blanked due to unreliable match: {int((~reliable_mask).sum())}")
    print(f"Saved delivery file: {DELIVERY_PATH}")
    print(f"Saved normalized file: {NORMALIZED_PATH}")
    print(f"Saved audit file: {AUDIT_PATH}")


if __name__ == "__main__":
    main()
