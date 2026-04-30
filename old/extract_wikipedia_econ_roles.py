#!/usr/bin/env python3
"""Extract exact economy-related roles from Wikipedia for names in the Excel file."""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from bs4 import BeautifulSoup


PROJECT_DIR = Path("/Users/sbc/projects/central-banks-board")
DEFAULT_INPUT = PROJECT_DIR / "data" / "Bios_WebScrapping.xlsx"
DEFAULT_OUTPUT = PROJECT_DIR / "data" / "wikipedia_econ_roles.csv"
DEFAULT_ERRORS_OUTPUT = PROJECT_DIR / "data" / "wikipedia_econ_roles_errors.csv"
DEFAULT_SLEEP_SECONDS = 0.5

HEADERS = {
    "User-Agent": "CentralBanksBoardWikipediaRoles/1.0 (research script)"
}

INFOBOX_HEADERS = {
    "occupation",
    "profession",
    "title",
    "office",
    "known for",
    "discipline",
}

ECON_KEYWORDS = [
    "economist",
    "economics",
    "economic",
    "central bank",
    "central banker",
    "banker",
    "banking",
    "governor",
    "president of",
    "chairman",
    "finance minister",
    "minister of finance",
    "monetary",
    "macroeconom",
    "microeconom",
    "treasury",
    "federal reserve",
    "professor of economics",
]

LEAD_PATTERNS = [
    re.compile(
        r"\b(?:is|was)\s+(?:an?|the)\s+([^.;]{0,220})",
        flags=re.IGNORECASE,
    )
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Excel input path.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="CSV output path.")
    parser.add_argument(
        "--errors-output",
        default=str(DEFAULT_ERRORS_OUTPUT),
        help="CSV output path for unresolved names.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N unique names.")
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=DEFAULT_SLEEP_SECONDS,
        help="Delay between Wikipedia requests.",
    )
    parser.add_argument(
        "--search-wikipedia",
        dest="search_wikipedia",
        action="store_true",
        help="Actually query Wikipedia.",
    )
    parser.add_argument(
        "--no-search-wikipedia",
        dest="search_wikipedia",
        action="store_false",
        help="Do not query Wikipedia; only prepare the output structure.",
    )
    parser.set_defaults(search_wikipedia=True)
    args, _unknown = parser.parse_known_args()
    args.input = str(Path(args.input).expanduser().resolve())
    args.output = str(Path(args.output).expanduser().resolve())
    args.errors_output = str(Path(args.errors_output).expanduser().resolve())
    return args


def normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def contains_econ_keyword(text: str) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in ECON_KEYWORDS)


def fetch_search_results(session: requests.Session, query: str) -> list[dict]:
    response = session.get(
        "https://en.wikipedia.org/w/api.php",
        params={
            "action": "query",
            "list": "search",
            "srsearch": query,
            "srlimit": 5,
            "format": "json",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return payload.get("query", {}).get("search", [])


def fetch_page_html(session: requests.Session, title: str) -> str:
    response = session.get(
        "https://en.wikipedia.org/w/api.php",
        params={
            "action": "parse",
            "page": title,
            "prop": "text",
            "format": "json",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["parse"]["text"]["*"]


def extract_infobox_candidates(soup: BeautifulSoup) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    infobox = soup.select_one("table.infobox")
    if not infobox:
        return rows

    for tr in infobox.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        header = th.get_text(" ", strip=True)
        value = td.get_text(" ", strip=True)
        if not header or not value:
            continue
        rows.append((header, value))
    return rows


def extract_lead_paragraphs(soup: BeautifulSoup) -> list[str]:
    content = soup.select_one("div.mw-parser-output")
    if not content:
        return []

    paragraphs: list[str] = []
    for node in content.find_all("p", recursive=False):
        text = node.get_text(" ", strip=True)
        if text:
            paragraphs.append(text)
        if len(paragraphs) >= 3:
            break
    return paragraphs


def choose_exact_role(infobox_rows: Iterable[tuple[str, str]], lead_paragraphs: Iterable[str]) -> tuple[str, str]:
    for header, value in infobox_rows:
        if header.strip().lower() in INFOBOX_HEADERS and contains_econ_keyword(value):
            return value, f"infobox:{header}"

    for header, value in infobox_rows:
        if contains_econ_keyword(value):
            return value, f"infobox:{header}"

    for paragraph in lead_paragraphs:
        for pattern in LEAD_PATTERNS:
            match = pattern.search(paragraph)
            if not match:
                continue
            phrase = match.group(1).strip(" ,")
            if contains_econ_keyword(phrase):
                return phrase, "lead"

    for paragraph in lead_paragraphs:
        if contains_econ_keyword(paragraph):
            return paragraph, "lead_paragraph"

    return "", ""


def candidate_queries(name: str) -> list[str]:
    return [
        f'"{name}" economist',
        f'"{name}" central bank',
        f'"{name}" finance',
        f'"{name}"',
    ]


def find_wikipedia_role(session: requests.Session, name: str, sleep_seconds: float) -> dict:
    seen_titles: set[str] = set()

    for query in candidate_queries(name):
        results = fetch_search_results(session, query)
        time.sleep(sleep_seconds)
        for result in results:
            title = result.get("title", "").strip()
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)

            html = fetch_page_html(session, title)
            time.sleep(sleep_seconds)
            soup = BeautifulSoup(html, "html.parser")
            infobox_rows = extract_infobox_candidates(soup)
            lead_paragraphs = extract_lead_paragraphs(soup)
            exact_role, source_kind = choose_exact_role(infobox_rows, lead_paragraphs)
            if not exact_role:
                continue

            return {
                "matched_title": title,
                "wikipedia_role_exact": exact_role,
                "wikipedia_role_source": source_kind,
                "wikipedia_url": f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
            }

    return {
        "matched_title": "",
        "wikipedia_role_exact": "",
        "wikipedia_role_source": "",
        "wikipedia_url": "",
    }


def main() -> None:
    args = parse_args()
    df = pd.read_excel(args.input)
    work = df[["PName_original", "PName", "iso3", "Position"]].copy()
    work["PName_original"] = work["PName_original"].map(normalize_name)
    work["PName"] = work["PName"].map(normalize_name)

    unique_names = (
        work[["PName_original", "PName"]]
        .drop_duplicates()
        .assign(search_name=lambda x: x["PName"].where(x["PName"] != "", x["PName_original"]))
    )
    unique_names = unique_names[unique_names["search_name"] != ""]
    if args.limit > 0:
        unique_names = unique_names.head(args.limit)

    session = requests.Session()
    session.headers.update(HEADERS)

    results: list[dict] = []
    errors: list[dict] = []

    total = len(unique_names)
    for index, row in enumerate(unique_names.itertuples(index=False), start=1):
        search_name = row.search_name
        print(f"{index}/{total} {search_name}", flush=True)
        if not args.search_wikipedia:
            results.append(
                {
                    "PName_original": row.PName_original,
                    "PName": row.PName,
                    "search_name": search_name,
                    "matched_title": "",
                    "wikipedia_role_exact": "",
                    "wikipedia_role_source": "",
                    "wikipedia_url": "",
                }
            )
            continue
        try:
            role = find_wikipedia_role(session, search_name, args.sleep_seconds)
            results.append(
                {
                    "PName_original": row.PName_original,
                    "PName": row.PName,
                    "search_name": search_name,
                    **role,
                }
            )
        except Exception as exc:
            errors.append(
                {
                    "PName_original": row.PName_original,
                    "PName": row.PName,
                    "search_name": search_name,
                    "error": str(exc),
                }
            )

    result_df = pd.DataFrame(results)
    merged = work.merge(result_df, on=["PName_original", "PName"], how="left")
    merged.to_csv(args.output, index=False)
    pd.DataFrame(errors).to_csv(args.errors_output, index=False)

    print(json.dumps(
        {
            "output": str(args.output),
            "errors_output": str(args.errors_output),
            "rows": len(merged),
            "matched_unique_names": int(result_df["wikipedia_role_exact"].fillna("").ne("").sum()) if not result_df.empty else 0,
            "unresolved_unique_names": len(errors),
        },
        ensure_ascii=False,
    ))


if __name__ == "__main__":
    main()
