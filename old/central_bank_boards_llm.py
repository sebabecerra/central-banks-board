#!/usr/bin/env python3
"""LLM-driven central bank board extractor from official websites.

This script is the notebook flow converted into a reusable CLI:
- input: country + website (+ optional board_page_url)
- discovery: search likely board pages for the same domain
- extraction: fetch page text and ask Mistral for structured members
- output: CSV with source_url
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from ddgs import DDGS
from dotenv import load_dotenv


PROJECT_DIR = Path("/Users/sbc/projects/central-banks-board")
DEFAULT_ENV_PATH = PROJECT_DIR / ".env"
DEFAULT_OUTPUT = PROJECT_DIR / "central_bank_boards_from_websites.csv"
HEADERS = {"User-Agent": "Mozilla/5.0"}
BOARD_HINTS = [
    "board",
    "leadership",
    "management",
    "governors",
    "directors",
    "governance",
    "executive board",
    "consejo",
    "directorio",
    "junta",
]


@dataclass(frozen=True)
class InputRow:
    country: str
    website: str
    board_page_url: str = ""


@dataclass(frozen=True)
class OutputRow:
    country: str
    name: str
    role: str
    source_url: str


def load_api_key(env_path: Path) -> str:
    load_dotenv(env_path)
    api_key = os.environ.get("MISTRAL_API_KEY", "")
    if not api_key:
        raise ValueError(f"MISTRAL_API_KEY is missing. Check {env_path}.")
    return api_key


def same_site(base_url: str, candidate_url: str) -> bool:
    base_host = urlparse(base_url).netloc.lower().replace("www.", "")
    candidate_host = urlparse(candidate_url).netloc.lower().replace("www.", "")
    return base_host == candidate_host


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def looks_like_name(text: str) -> bool:
    text = clean_text(text)
    if len(text.split()) < 2 or len(text.split()) > 5:
        return False
    if any(ch.isdigit() for ch in text):
        return False
    blocked = {
        "the governing board",
        "board of directors",
        "executive board",
        "annual report",
        "the president",
        "the chairman",
    }
    if text.lower() in blocked:
        return False
    return bool(
        re.match(
            r"^[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ'`.-]+(?:\s+[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ'`.-]+){1,4}$",
            text,
        )
    )


def llm_extract(text: str, country: str, api_key: str) -> str:
    url = "https://api.mistral.ai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    prompt = f"""
Extract current central bank board or leadership members for {country}.

Rules:
- Only return real people.
- Exclude generic phrases like 'The Governing Board' or 'Annual Report'.
- Exclude commentary, notes, and explanations.
- Prefer current members shown on the page.
- If no people are clearly listed, return nothing.

Return one line per person in this exact format:
Name - Role

TEXT:
{text}
"""
    data = {
        "model": "mistral-small",
        "messages": [
            {
                "role": "system",
                "content": "You extract structured people data from official institutional webpages.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
    }
    response = requests.post(url, json=data, headers=headers, timeout=60)
    if response.status_code != 200:
        return ""
    return response.json()["choices"][0]["message"]["content"]


def search_board_pages(country: str, website: str, max_results: int = 8) -> List[str]:
    queries = [
        f"site:{urlparse(website).netloc} {country} central bank board",
        f"site:{urlparse(website).netloc} leadership",
        f"site:{urlparse(website).netloc} governors",
        f"site:{urlparse(website).netloc} directors",
        f"site:{urlparse(website).netloc} management",
    ]

    found: List[str] = []
    seen = set()
    with DDGS() as ddgs:
        for query in queries:
            for result in ddgs.text(query, max_results=max_results):
                href = result.get("href", "")
                if not href.startswith("http"):
                    continue
                if not same_site(website, href):
                    continue
                haystack = (href + " " + result.get("title", "") + " " + result.get("body", "")).lower()
                if not any(hint in haystack for hint in BOARD_HINTS):
                    continue
                if href in seen:
                    continue
                seen.add(href)
                found.append(href)
    return found[:8]


def fallback_search(country: str, website: str, max_results: int = 8) -> List[str]:
    domain = urlparse(website).netloc
    queries = [
        f"{country} central bank board members",
        f"{country} central bank leadership",
        f"{country} central bank governors",
    ]

    found = []
    seen = set()
    with DDGS() as ddgs:
        for query in queries:
            for result in ddgs.text(query, max_results=max_results):
                href = result.get("href", "")
                if not href.startswith("http"):
                    continue
                if href in seen:
                    continue
                seen.add(href)
                haystack = (href + " " + result.get("title", "") + " " + result.get("body", "")).lower()
                score = 0
                if same_site(website, href):
                    score += 5
                if domain and domain.replace("www.", "") in href.lower():
                    score += 3
                if any(hint in haystack for hint in BOARD_HINTS):
                    score += 3
                if any(bad in href.lower() for bad in ["wikipedia", "reuters", "bloomberg", "linkedin"]):
                    score -= 10
                if score > 0:
                    found.append((score, href))

    found.sort(key=lambda item: item[0], reverse=True)
    return [href for _, href in found[:8]]


def find_board_page(country: str, website: str) -> str:
    candidates = search_board_pages(country, website)
    if candidates:
        return candidates[0]

    try:
        response = requests.get(website, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(response.text, "lxml")
        for link in soup.find_all("a", href=True):
            href = urljoin(website, link["href"])
            text = clean_text(link.get_text(" ", strip=True)).lower()
            combined = (href + " " + text).lower()
            if same_site(website, href) and any(hint in combined for hint in BOARD_HINTS):
                return href
    except Exception:
        pass

    fallback_candidates = fallback_search(country, website)
    if fallback_candidates:
        return fallback_candidates[0]

    return website


def get_clean_text(url: str) -> str:
    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(response.text, "lxml")

        for tag in soup(["script", "style", "noscript", "svg", "footer", "nav", "header"]):
            tag.decompose()

        texts = []
        for tag in soup.find_all(["h1", "h2", "h3", "li", "p", "td", "span"]):
            text = clean_text(tag.get_text(" ", strip=True))
            lowered = text.lower()
            if len(text) < 4:
                continue
            if any(x in lowered for x in ["cookie", "privacy", "subscribe", "newsletter", "all rights reserved"]):
                continue
            texts.append(text)

        return "\n".join(texts[:700])
    except Exception:
        return ""


def parse_people(text: str, country: str, source_url: str) -> List[OutputRow]:
    rows: List[OutputRow] = []
    seen = set()
    for line in text.split("\n"):
        if "-" not in line:
            continue
        name, role = line.split("-", 1)
        name = clean_text(name).strip("*• ")
        role = clean_text(role)
        if not looks_like_name(name):
            continue
        if len(role) < 3:
            continue
        key = (country, name, role, source_url)
        if key in seen:
            continue
        seen.add(key)
        rows.append(OutputRow(country=country, name=name, role=role, source_url=source_url))
    return rows


def agent(country: str, website: str, api_key: str, board_page_url: str = "") -> List[OutputRow]:
    source_url = board_page_url or find_board_page(country, website)
    print(f"  source_url: {source_url}")
    text = get_clean_text(source_url)
    print(f"  downloaded_chars: {len(text)}")
    if len(text) < 200 and source_url != website:
        print("  fallback_to_website: yes")
        text = get_clean_text(website)
        source_url = website
        print(f"  website_chars: {len(text)}")
    extracted = llm_extract(text, country, api_key)
    print(f"  llm_chars: {len(extracted)}")
    rows = parse_people(extracted, country, source_url)
    print(f"  parsed_rows: {len(rows)}")
    return rows


def build_dataset(input_rows: List[InputRow], api_key: str) -> List[OutputRow]:
    all_rows: List[OutputRow] = []
    for row in input_rows:
        print(f"Processing {row.country} -> {row.website}")
        try:
            rows = agent(
                country=row.country,
                website=row.website,
                board_page_url=row.board_page_url,
                api_key=api_key,
            )
            all_rows.extend(rows)
        except Exception as exc:
            print("Error:", row.country, exc)
    # de-dupe while preserving order
    deduped = list({(r.country, r.name, r.role, r.source_url): r for r in all_rows}.values())
    return deduped


def write_csv(rows: Iterable[OutputRow], output_path: Path) -> None:
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["country", "name", "role", "source_url"])
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_PATH), help="Path to .env file.")
    parser.add_argument("--input-json", default="", help="Path to JSON file with input rows.")
    parser.add_argument("--country", default="", help="Single country run.")
    parser.add_argument("--website", default="", help="Single website run.")
    parser.add_argument("--board-page-url", default="", help="Optional known board page URL.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV path.")
    return parser.parse_args()


def load_input_rows(args: argparse.Namespace) -> List[InputRow]:
    if args.input_json:
        with open(args.input_json, encoding="utf-8") as handle:
            raw_rows = json.load(handle)
        return [InputRow(**row) for row in raw_rows]

    if args.country and args.website:
        return [InputRow(country=args.country, website=args.website, board_page_url=args.board_page_url)]

    return [
        InputRow(country="Chile", website="https://www.bcentral.cl"),
        InputRow(country="Argentina", website="https://www.bcra.gob.ar"),
        InputRow(country="Brazil", website="https://www.bcb.gov.br"),
        InputRow(country="United States", website="https://www.federalreserve.gov"),
        InputRow(country="European Central Bank", website="https://www.ecb.europa.eu"),
    ]


def main() -> None:
    args = parse_args()
    api_key = load_api_key(Path(args.env_file))
    input_rows = load_input_rows(args)
    rows = build_dataset(input_rows, api_key=api_key)
    output_path = Path(args.output)
    write_csv(rows, output_path)
    print(f"Wrote {output_path}")
    print(f"Rows: {len(rows)}")


if __name__ == "__main__":
    main()
