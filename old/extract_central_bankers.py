#!/usr/bin/env python3
"""Build a clean central-bank leadership dataset from Wikipedia.

This script replaces the exploratory notebook with a reproducible pipeline:

1. Download the Wikipedia table of central banks.
2. Discover relevant leadership categories under "Category:Central bankers".
3. Traverse category pagination to collect person names.
4. Match category bank names back to the central-bank table.
5. Write a flat CSV with one row per person.
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import socket
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError

try:
    from bs4 import BeautifulSoup
except ImportError as exc:  # pragma: no cover - exercised only in missing-dependency environments.
    raise SystemExit(
        "Missing dependency: beautifulsoup4. Install it with `python3 -m pip install beautifulsoup4`."
    ) from exc


BASE_URL = "https://en.wikipedia.org"
CENTRAL_BANKS_URL = f"{BASE_URL}/wiki/List_of_central_banks"
CENTRAL_BANKERS_CATEGORY_URL = f"{BASE_URL}/wiki/Category:Central_bankers"
DEFAULT_OUTPUT = "governors_clean_names.csv"
USER_AGENT = "Mozilla/5.0 (compatible; CentralBanksBoard/2.0; +https://github.com/sebabecerra/central-banks-board)"
HEADERS = {"User-Agent": USER_AGENT}

ROLE_PATTERNS: Sequence[Tuple[str, str]] = (
    ("Governor", r"\bgovernors?\b"),
    ("President", r"\bpresidents?\b"),
    ("Chair", r"\bchair(men|women|persons?)?\b|\bchairs?\b"),
)

CATEGORY_PREFIXES: Sequence[str] = (
    "Governors of ",
    "Governor of ",
    "Presidents of ",
    "President of ",
    "Chairmen of ",
    "Chairman of ",
    "Chairwomen of ",
    "Chairwoman of ",
    "Chairpersons of ",
    "Chairperson of ",
    "Chairs of ",
    "Chair of ",
)

STOPWORDS = {
    "a",
    "an",
    "and",
    "bank",
    "central",
    "de",
    "del",
    "for",
    "la",
    "national",
    "of",
    "reserve",
    "the",
}


@dataclass(frozen=True)
class CentralBank:
    country: str
    central_bank: str


@dataclass(frozen=True)
class CategoryRecord:
    category_name: str
    category_url: str


@dataclass(frozen=True)
class PersonRecord:
    country: str
    central_bank_name: str
    pname_original: str
    pname: str
    first: str
    last: str
    position: str
    category_name: str
    category_url: str


def clean_text(value: str) -> str:
    value = re.sub(r"\[[^\]]+\]", "", value or "")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_text(value: str) -> str:
    value = clean_text(value).lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9 ]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def simplify_bank_name(value: str) -> str:
    normalized = normalize_text(value)
    tokens = [token for token in normalized.split() if token not in STOPWORDS]
    return " ".join(tokens)


def split_name(value: str) -> Tuple[str, str]:
    pieces = clean_text(value).split()
    if len(pieces) < 2:
        return clean_text(value), ""
    return pieces[0], " ".join(pieces[1:])


def infer_position(category_name: str) -> str:
    text = normalize_text(category_name)
    if any(blocked in text.split() for blocked in ("deputy", "vice", "assistant", "acting")):
        return ""
    for label, pattern in ROLE_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return label
    return ""


def infer_bank_name(category_name: str) -> str:
    text = clean_text(category_name)
    for prefix in CATEGORY_PREFIXES:
        if text.startswith(prefix):
            text = text[len(prefix) :]
            break
    return text.strip()


def fetch_html(url: str, retries: int = 4, timeout: int = 45) -> BeautifulSoup:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(request, timeout=timeout) as response:
                html = response.read()
            return BeautifulSoup(html, "html.parser")
        except HTTPError as exc:
            last_error = exc
            if exc.code == 429 and attempt < retries:
                retry_after = exc.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else 2.0 * attempt
                logging.warning("Rate limited on %s; retrying in %.1fs", url, delay)
                time.sleep(delay)
                continue
            if attempt < retries:
                delay = 1.5 * attempt
                logging.warning("HTTP %s on %s; retrying in %.1fs", exc.code, url, delay)
                time.sleep(delay)
                continue
            break
        except (URLError, TimeoutError, socket.timeout) as exc:
            last_error = exc
            if attempt < retries:
                delay = 1.5 * attempt
                logging.warning("Network error on %s; retrying in %.1fs", url, delay)
                time.sleep(delay)
                continue
            break
    raise RuntimeError(f"Failed to fetch {url}: {last_error}") from last_error


class WikipediaCentralBankersPipeline:
    def __init__(self, pause_seconds: float = 0.1) -> None:
        self.pause_seconds = pause_seconds
        self.soup_cache: Dict[str, BeautifulSoup] = {}

    def get_soup(self, url: str) -> BeautifulSoup:
        if url not in self.soup_cache:
            self.soup_cache[url] = fetch_html(url)
            if self.pause_seconds > 0:
                time.sleep(self.pause_seconds)
        return self.soup_cache[url]

    def list_central_banks(self) -> List[CentralBank]:
        soup = self.get_soup(CENTRAL_BANKS_URL)
        table = soup.select_one("table.wikitable")
        if table is None:
            raise RuntimeError("Could not find the central banks table on Wikipedia.")

        banks: List[CentralBank] = []
        for row in table.select("tr")[1:]:
            cells = row.find_all(["th", "td"])
            if len(cells) < 3:
                continue

            country = clean_text(cells[0].get_text(" ", strip=True))
            central_bank = clean_text(cells[2].get_text(" ", strip=True))
            if not country or not central_bank:
                continue
            banks.append(CentralBank(country=country, central_bank=central_bank))

        if not banks:
            raise RuntimeError("No central banks were parsed from the Wikipedia table.")

        logging.info("Parsed %d central banks", len(banks))
        return banks

    def list_leadership_categories(self) -> List[CategoryRecord]:
        soup = self.get_soup(CENTRAL_BANKERS_CATEGORY_URL)
        subcategories = soup.select_one("#mw-subcategories")
        if subcategories is None:
            raise RuntimeError("Could not find subcategories in Category:Central bankers.")

        categories: List[CategoryRecord] = []
        seen_urls = set()
        for link in subcategories.select("a[href]"):
            href = link.get("href", "")
            if not href.startswith("/wiki/Category:"):
                continue

            category_name = clean_text(link.get_text(" ", strip=True))
            if not infer_position(category_name):
                continue

            category_url = urllib.parse.urljoin(BASE_URL, href)
            if category_url in seen_urls:
                continue

            seen_urls.add(category_url)
            categories.append(CategoryRecord(category_name=category_name, category_url=category_url))

        categories.sort(key=lambda item: normalize_text(item.category_name))
        logging.info("Discovered %d leadership categories", len(categories))
        return categories

    def extract_people_from_category(self, category: CategoryRecord) -> List[str]:
        names: List[str] = []
        seen_names = set()
        next_url: Optional[str] = category.category_url

        while next_url:
            soup = self.get_soup(next_url)
            pages_section = soup.select_one("#mw-pages")
            if pages_section:
                for item in pages_section.select("div.mw-category-group li"):
                    link = item.find("a", href=True)
                    if link is None:
                        continue
                    href = link.get("href", "")
                    if not href.startswith("/wiki/") or href.startswith("/wiki/Category:"):
                        continue

                    name = clean_text(link.get_text(" ", strip=True))
                    if not self.is_valid_person_name(name):
                        continue
                    if name in seen_names:
                        continue

                    seen_names.add(name)
                    names.append(name)

            next_url = self.find_next_category_page(pages_section)

        names.sort(key=normalize_text)
        return names

    @staticmethod
    def find_next_category_page(pages_section: Optional[BeautifulSoup]) -> Optional[str]:
        if pages_section is None:
            return None
        for link in pages_section.select("a[href]"):
            if clean_text(link.get_text(" ", strip=True)).lower() != "next page":
                continue
            href = link.get("href", "")
            return urllib.parse.urljoin(BASE_URL, href)
        return None

    @staticmethod
    def is_valid_person_name(name: str) -> bool:
        cleaned = clean_text(name)
        if len(cleaned.split()) < 2:
            return False
        if any(char.isdigit() for char in cleaned):
            return False
        return True


class CentralBankMatcher:
    def __init__(self, banks: Sequence[CentralBank]) -> None:
        self.banks = list(banks)
        self.bank_index = self._build_bank_index(self.banks)

    def match(self, inferred_bank_name: str) -> Tuple[str, str]:
        direct_key = simplify_bank_name(inferred_bank_name)
        if direct_key in self.bank_index:
            bank = self.bank_index[direct_key]
            return bank.country, bank.central_bank

        best_match: Optional[CentralBank] = None
        best_score = 0.0
        target = simplify_bank_name(inferred_bank_name)
        for candidate_key, bank in self.bank_index.items():
            score = SequenceMatcher(a=target, b=candidate_key).ratio()
            if score > best_score:
                best_score = score
                best_match = bank

        if best_match and best_score >= 0.72:
            logging.debug(
                "Fuzzy-matched category bank '%s' to '%s' (%.2f)",
                inferred_bank_name,
                best_match.central_bank,
                best_score,
            )
            return best_match.country, best_match.central_bank

        return "", clean_text(inferred_bank_name)

    @staticmethod
    def _build_bank_index(banks: Sequence[CentralBank]) -> Dict[str, CentralBank]:
        index: Dict[str, CentralBank] = {}
        for bank in banks:
            keys = {
                simplify_bank_name(bank.central_bank),
                simplify_bank_name(bank.country),
                simplify_bank_name(f"Bank of {bank.country}"),
                simplify_bank_name(f"Central Bank of {bank.country}"),
            }
            for key in keys:
                if key:
                    index.setdefault(key, bank)
        return index


def build_rows(
    pipeline: WikipediaCentralBankersPipeline,
    matcher: CentralBankMatcher,
    categories: Sequence[CategoryRecord],
) -> List[PersonRecord]:
    rows: List[PersonRecord] = []
    seen = set()

    for index, category in enumerate(categories, start=1):
        logging.info("Processing category %d/%d: %s", index, len(categories), category.category_name)
        central_bank_name = infer_bank_name(category.category_name)
        country, matched_bank_name = matcher.match(central_bank_name)
        position = infer_position(category.category_name)

        for person_name in pipeline.extract_people_from_category(category):
            first, last = split_name(person_name)
            record = PersonRecord(
                country=country,
                central_bank_name=matched_bank_name,
                pname_original=person_name,
                pname=person_name,
                first=first,
                last=last,
                position=position,
                category_name=category.category_name,
                category_url=category.category_url,
            )
            dedupe_key = (
                normalize_text(record.country),
                normalize_text(record.central_bank_name),
                normalize_text(record.pname_original),
                normalize_text(record.position),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append(record)

    rows.sort(
        key=lambda row: (
            normalize_text(row.country),
            normalize_text(row.central_bank_name),
            normalize_text(row.pname_original),
        )
    )
    logging.info("Built %d flat person rows", len(rows))
    return rows


def write_csv(rows: Iterable[PersonRecord], output_path: str) -> None:
    fieldnames = [
        "country",
        "central_bank_name",
        "PName_original",
        "PName",
        "first",
        "last",
        "Position",
        "category_name",
        "category_url",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "country": row.country,
                    "central_bank_name": row.central_bank_name,
                    "PName_original": row.pname_original,
                    "PName": row.pname,
                    "first": row.first,
                    "last": row.last,
                    "Position": row.position,
                    "category_name": row.category_name,
                    "category_url": row.category_url,
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="CSV output path.")
    parser.add_argument(
        "--limit-categories",
        type=int,
        default=0,
        help="Process only the first N matching categories for a faster dry run.",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.1,
        help="Delay between uncached requests to be polite to Wikipedia.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging verbosity.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s %(message)s",
    )

    pipeline = WikipediaCentralBankersPipeline(pause_seconds=args.pause_seconds)
    banks = pipeline.list_central_banks()
    categories = pipeline.list_leadership_categories()
    if args.limit_categories > 0:
        categories = categories[: args.limit_categories]
        logging.info("Limiting run to %d categories", len(categories))

    matcher = CentralBankMatcher(banks)
    rows = build_rows(pipeline, matcher, categories)
    write_csv(rows, args.output)

    matched_rows = sum(1 for row in rows if row.country)
    logging.info("Wrote %s", args.output)
    logging.info("Rows: %d", len(rows))
    logging.info("Rows with matched country: %d", matched_rows)


if __name__ == "__main__":
    main()
