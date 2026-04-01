#!/usr/bin/env python3
"""Discover central-bank board pages and extract leadership members.

Pipeline:
1. Parse the Wikipedia list of central banks.
2. Visit each bank's Wikipedia page and recover the official website.
3. Crawl the official website homepage to find a likely board/leadership page.
4. Extract member names and roles from that page with generic heuristics.
5. Write a flat CSV with source URLs for later manual review.
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
import warnings
from dataclasses import dataclass
from html import unescape
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError

try:
    from bs4 import BeautifulSoup, Tag, XMLParsedAsHTMLWarning
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: beautifulsoup4. Install it with `python3 -m pip install beautifulsoup4`."
    ) from exc


BASE_WIKI_URL = "https://en.wikipedia.org"
CENTRAL_BANKS_URL = f"{BASE_WIKI_URL}/wiki/List_of_central_banks"
DEFAULT_OUTPUT = "central_bank_boards.csv"
USER_AGENT = "Mozilla/5.0 (compatible; CentralBanksBoard/3.0; +https://github.com/sebabecerra/central-banks-board)"
HEADERS = {"User-Agent": USER_AGENT}
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

DISCOVERY_KEYWORDS = (
    "board",
    "boards",
    "leadership",
    "management",
    "governors",
    "executive",
    "directors",
    "governing",
    "about",
    "organization",
)
BLOCKED_URL_KEYWORDS = (
    "privacy",
    "cookie",
    "contact",
    "search",
    "careers",
    "jobs",
    "news",
    "press",
    "media",
    "publication",
    "speech",
    "report",
    "annual-report",
    "procurement",
)
LIKELY_BOARD_PATHS = (
    "/about/board",
    "/about-us/board",
    "/about/leadership",
    "/about-us/leadership",
    "/leadership",
    "/management",
    "/about/management",
    "/about-us/management",
    "/governors",
    "/board-of-directors",
    "/governing-board",
    "/about/organization",
)
NAME_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z'`.-]+(?:\s+[A-Z][A-Za-z'`.-]+){1,5})\b"
)


@dataclass(frozen=True)
class CentralBankRecord:
    country: str
    central_bank_name: str
    wikipedia_url: str


@dataclass(frozen=True)
class BoardPageCandidate:
    url: str
    title: str
    score: int
    discovery_method: str


@dataclass(frozen=True)
class BoardMemberRecord:
    country: str
    central_bank_name: str
    wikipedia_url: str
    official_website: str
    board_page_url: str
    discovery_method: str
    member_name: str
    member_role: str
    source_title: str
    source_snippet: str


def clean_text(value: str) -> str:
    value = unescape(value or "")
    value = re.sub(r"\[[^\]]+\]", "", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_text(value: str) -> str:
    value = clean_text(value).lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9 ]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def hostname(url: str) -> str:
    return urllib.parse.urlparse(url).netloc.lower()


def is_same_site(left: str, right: str) -> bool:
    return hostname(left).lstrip("www.") == hostname(right).lstrip("www.")


def looks_like_name(text: str) -> bool:
    cleaned = clean_text(text)
    if not cleaned or len(cleaned) > 80:
        return False
    if any(char.isdigit() for char in cleaned):
        return False
    words = cleaned.split()
    if len(words) < 2 or len(words) > 6:
        return False
    stop_phrases = {
        "board of directors",
        "board of governors",
        "monetary policy committee",
        "senior management",
        "executive management",
        "advisory board",
    }
    if normalize_text(cleaned) in stop_phrases:
        return False
    match = NAME_PATTERN.search(cleaned)
    return bool(match and clean_text(match.group(1)) == cleaned)


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
            if exc.code == 404:
                break
            if exc.code in {403, 429} and attempt < retries:
                delay = 2.0 * attempt
                logging.warning("HTTP %s on %s; retrying in %.1fs", exc.code, url, delay)
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


class BoardDiscoveryPipeline:
    def __init__(self, pause_seconds: float = 0.2) -> None:
        self.pause_seconds = pause_seconds
        self.soup_cache: Dict[str, BeautifulSoup] = {}

    def get_soup(self, url: str) -> BeautifulSoup:
        if url not in self.soup_cache:
            self.soup_cache[url] = fetch_html(url)
            if self.pause_seconds > 0:
                time.sleep(self.pause_seconds)
        return self.soup_cache[url]

    def list_central_banks(self) -> List[CentralBankRecord]:
        soup = self.get_soup(CENTRAL_BANKS_URL)
        table = soup.select_one("table.wikitable")
        if table is None:
            raise RuntimeError("Could not find the central bank table on Wikipedia.")

        rows: List[CentralBankRecord] = []
        for row in table.select("tr")[1:]:
            cells = row.find_all(["th", "td"])
            if len(cells) < 3:
                continue

            country = clean_text(cells[0].get_text(" ", strip=True))
            bank_cell = cells[2]
            central_bank_name = clean_text(bank_cell.get_text(" ", strip=True))
            if normalize_text(central_bank_name).startswith("no central bank"):
                continue

            wiki_link = bank_cell.find("a", href=re.compile(r"^/wiki/"))
            wikipedia_url = ""
            if wiki_link and wiki_link.get("href"):
                wikipedia_url = urllib.parse.urljoin(BASE_WIKI_URL, wiki_link["href"])

            if country and central_bank_name:
                rows.append(
                    CentralBankRecord(
                        country=country,
                        central_bank_name=central_bank_name,
                        wikipedia_url=wikipedia_url,
                    )
                )

        logging.info("Parsed %d banks from Wikipedia", len(rows))
        return rows

    def find_official_website(self, bank: CentralBankRecord) -> str:
        if not bank.wikipedia_url:
            return ""

        soup = self.get_soup(bank.wikipedia_url)

        for tr in soup.select("table.infobox tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th is None or td is None:
                continue
            if normalize_text(th.get_text(" ", strip=True)) != "website":
                continue
            for link in td.find_all("a", href=True):
                href = clean_text(link.get("href", ""))
                if href.startswith(("http://", "https://")):
                    return href

        for selector in (
            "table.infobox a.external[href]",
            "table.infobox td a[href]",
            "#External_links a.external[href]",
            "#External_links a[href]",
        ):
            for link in soup.select(selector):
                href = clean_text(link.get("href", ""))
                if self.is_probable_official_site(href, bank):
                    return href

        return ""

    def is_probable_official_site(self, url: str, bank: CentralBankRecord) -> bool:
        if not url.startswith(("http://", "https://")):
            return False
        host = hostname(url)
        if not host:
            return False
        bad_hosts = {"facebook.com", "linkedin.com", "x.com", "twitter.com", "youtube.com", "instagram.com"}
        if any(host.endswith(bad) for bad in bad_hosts):
            return False
        bank_tokens = [token for token in normalize_text(bank.central_bank_name).split() if len(token) > 2]
        host_text = normalize_text(host.replace(".", " "))
        return any(token in host_text for token in bank_tokens[:4]) or "bank" in host_text or "banco" in host_text

    def discover_board_page(self, official_website: str) -> Optional[BoardPageCandidate]:
        if not official_website:
            return None

        candidates: List[BoardPageCandidate] = []
        homepage = self.get_soup(official_website)
        homepage_title = clean_text(homepage.title.get_text(" ", strip=True)) if homepage.title else ""

        for candidate_url in self.generate_common_path_candidates(official_website):
            score = self.score_board_candidate(candidate_url, "", candidate_url, same_site=True, direct_path=True)
            candidates.append(
                BoardPageCandidate(
                    url=candidate_url,
                    title="",
                    score=score,
                    discovery_method="guessed_path",
                )
            )

        for link in homepage.select("a[href]"):
            href = clean_text(link.get("href", ""))
            absolute_url = urllib.parse.urljoin(official_website, href)
            if not absolute_url.startswith(("http://", "https://")):
                continue
            if not is_same_site(official_website, absolute_url):
                continue

            title = clean_text(link.get_text(" ", strip=True))
            score = self.score_board_candidate(absolute_url, title, homepage_title, same_site=True, direct_path=False)
            if score <= 0:
                continue

            candidates.append(
                BoardPageCandidate(
                    url=absolute_url,
                    title=title,
                    score=score,
                    discovery_method="homepage_link",
                )
            )

        for sitemap_candidate in self.discover_from_sitemap(official_website):
            candidates.append(sitemap_candidate)

        deduped: Dict[str, BoardPageCandidate] = {}
        for candidate in candidates:
            current = deduped.get(candidate.url)
            if current is None or candidate.score > current.score:
                deduped[candidate.url] = candidate

        ranked = sorted(deduped.values(), key=lambda item: item.score, reverse=True)
        for candidate in ranked[:10]:
            try:
                soup = self.get_soup(candidate.url)
            except Exception as exc:
                logging.debug("Skipping candidate %s: %s", candidate.url, exc)
                continue
            page_text = normalize_text(soup.get_text(" ", strip=True)[:4000])
            if "board" in page_text or "governor" in page_text or "director" in page_text or "management" in page_text:
                title = clean_text(soup.title.get_text(" ", strip=True)) if soup.title else candidate.title
                return BoardPageCandidate(
                    url=candidate.url,
                    title=title,
                    score=candidate.score,
                    discovery_method=candidate.discovery_method,
                )

        return None

    def discover_from_sitemap(self, official_website: str) -> Iterable[BoardPageCandidate]:
        parsed = urllib.parse.urlparse(official_website)
        base = f"{parsed.scheme}://{parsed.netloc}"
        sitemap_urls = (
            urllib.parse.urljoin(base, "/sitemap.xml"),
            urllib.parse.urljoin(base, "/sitemap_index.xml"),
        )

        for sitemap_url in sitemap_urls:
            try:
                soup = self.get_soup(sitemap_url)
            except Exception:
                continue

            for loc in soup.find_all("loc"):
                url = clean_text(loc.get_text(" ", strip=True))
                if not url.startswith(("http://", "https://")):
                    continue
                if not is_same_site(base, url):
                    continue
                score = self.score_board_candidate(url, "", "", same_site=True, direct_path=False)
                if score <= 0:
                    continue
                yield BoardPageCandidate(
                    url=url,
                    title="",
                    score=score + 2,
                    discovery_method="sitemap",
                )

    def generate_common_path_candidates(self, official_website: str) -> Iterable[str]:
        parsed = urllib.parse.urlparse(official_website)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for path in LIKELY_BOARD_PATHS:
            yield urllib.parse.urljoin(base, path)

    def score_board_candidate(
        self,
        url: str,
        link_text: str,
        page_title: str,
        *,
        same_site: bool,
        direct_path: bool,
    ) -> int:
        haystack = " ".join(
            [
                normalize_text(url.replace("/", " ")),
                normalize_text(link_text),
                normalize_text(page_title),
            ]
        )
        score = 0
        for keyword in DISCOVERY_KEYWORDS:
            if keyword in haystack:
                score += 4 if keyword in {"board", "governors", "directors", "leadership"} else 2
        if any(blocked in haystack for blocked in BLOCKED_URL_KEYWORDS):
            score -= 10
        if same_site:
            score += 3
        if direct_path:
            score += 1
        if url.count("/") <= 4:
            score += 1
        return score

    def extract_board_members(
        self,
        bank: CentralBankRecord,
        official_website: str,
        board_page: BoardPageCandidate,
    ) -> List[BoardMemberRecord]:
        soup = self.get_soup(board_page.url)
        page_title = clean_text(soup.title.get_text(" ", strip=True)) if soup.title else ""

        records: List[BoardMemberRecord] = []
        seen = set()

        for container in self.iter_candidate_containers(soup):
            for raw_name, raw_role, raw_snippet in self.extract_people_from_container(container):
                if not looks_like_name(raw_name):
                    continue

                key = (normalize_text(raw_name), normalize_text(raw_role))
                if key in seen:
                    continue
                seen.add(key)

                records.append(
                    BoardMemberRecord(
                        country=bank.country,
                        central_bank_name=bank.central_bank_name,
                        wikipedia_url=bank.wikipedia_url,
                        official_website=official_website,
                        board_page_url=board_page.url,
                        discovery_method=board_page.discovery_method,
                        member_name=clean_text(raw_name),
                        member_role=clean_text(raw_role),
                        source_title=page_title,
                        source_snippet=clean_text(raw_snippet)[:300],
                    )
                )

        return records

    def iter_candidate_containers(self, soup: BeautifulSoup) -> Iterable[Tag]:
        yielded = 0
        seen_ids = set()

        for heading in soup.find_all(re.compile(r"^h[1-4]$")):
            heading_text = normalize_text(heading.get_text(" ", strip=True))
            if not any(keyword in heading_text for keyword in ("board", "governor", "director", "leadership", "management")):
                continue

            current = heading.find_next_sibling()
            hops = 0
            while current is not None and hops < 6:
                if isinstance(current, Tag):
                    marker = id(current)
                    if marker not in seen_ids:
                        seen_ids.add(marker)
                        yielded += 1
                        yield current
                if getattr(current, "name", "") and re.fullmatch(r"h[1-4]", current.name):
                    break
                current = current.find_next_sibling()
                hops += 1

        for selector in (
            "[class*='board']",
            "[class*='leadership']",
            "[class*='management']",
            "[class*='director']",
            "[id*='board']",
            "[id*='leadership']",
            "[id*='management']",
        ):
            for node in soup.select(selector):
                if not isinstance(node, Tag):
                    continue
                marker = id(node)
                if marker in seen_ids:
                    continue
                seen_ids.add(marker)
                yielded += 1
                yield node

        if yielded == 0:
            main = soup.select_one("main") or soup.body
            if main and isinstance(main, Tag):
                yield main

    def extract_people_from_container(self, container: Tag) -> Iterable[Tuple[str, str, str]]:
        for item in container.select("li, tr, article, div, section, p"):
            text = clean_text(item.get_text(" ", strip=True))
            if not text or len(text) < 6:
                continue
            if not any(keyword in normalize_text(text) for keyword in ("chair", "governor", "director", "president", "member", "chief", "deputy", "secretary")):
                continue

            pairs = self.extract_name_role_pairs(text)
            for name, role in pairs:
                yield name, role, text

        text_block = clean_text(container.get_text(" ", strip=True))
        if text_block:
            for name, role in self.extract_name_role_pairs(text_block):
                yield name, role, text_block

    def extract_name_role_pairs(self, text: str) -> List[Tuple[str, str]]:
        pairs: List[Tuple[str, str]] = []
        text = clean_text(text)

        patterns = (
            r"^(?P<name>[A-Z][A-Za-z'`.-]+(?:\s+[A-Z][A-Za-z'`.-]+){1,5})\s+[–\-:,|]\s+(?P<role>.+)$",
            r"^(?P<role>Chair(?:man|person)?|Governor|Deputy Governor|President|Director|Board Member|Chief [A-Za-z ]+)\s*[–\-:,|]?\s+(?P<name>[A-Z][A-Za-z'`.-]+(?:\s+[A-Z][A-Za-z'`.-]+){1,5})$",
        )
        for pattern in patterns:
            match = re.match(pattern, text)
            if match:
                name = clean_text(match.group("name"))
                role = clean_text(match.group("role"))
                if looks_like_name(name):
                    pairs.append((name, role))
                    return pairs

        for match in NAME_PATTERN.finditer(text):
            name = clean_text(match.group(1))
            if not looks_like_name(name):
                continue
            remainder = clean_text(text.replace(name, "", 1)).strip(" -:|,")
            role = self.derive_role(remainder)
            pairs.append((name, role))
        return pairs[:5]

    @staticmethod
    def derive_role(text: str) -> str:
        text = clean_text(text)
        if not text:
            return ""
        keywords = (
            "chairman",
            "chairperson",
            "chair",
            "governor",
            "deputy governor",
            "president",
            "director",
            "member",
            "chief executive",
            "secretary",
        )
        lowered = normalize_text(text)
        for keyword in keywords:
            if keyword in lowered:
                return text[:120]
        return text[:120] if len(text.split()) <= 12 else ""


def write_csv(rows: Iterable[BoardMemberRecord], output_path: str) -> None:
    fieldnames = [
        "country",
        "central_bank_name",
        "wikipedia_url",
        "official_website",
        "board_page_url",
        "discovery_method",
        "member_name",
        "member_role",
        "source_title",
        "source_snippet",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "country": row.country,
                    "central_bank_name": row.central_bank_name,
                    "wikipedia_url": row.wikipedia_url,
                    "official_website": row.official_website,
                    "board_page_url": row.board_page_url,
                    "discovery_method": row.discovery_method,
                    "member_name": row.member_name,
                    "member_role": row.member_role,
                    "source_title": row.source_title,
                    "source_snippet": row.source_snippet,
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="CSV output path.")
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N banks.")
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.2,
        help="Delay between uncached requests to be polite to remote sites.",
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
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")

    pipeline = BoardDiscoveryPipeline(pause_seconds=args.pause_seconds)
    banks = pipeline.list_central_banks()
    if args.limit > 0:
        banks = banks[: args.limit]
        logging.info("Limiting run to %d banks", len(banks))

    rows: List[BoardMemberRecord] = []
    for index, bank in enumerate(banks, start=1):
        logging.info("Processing %d/%d: %s", index, len(banks), bank.central_bank_name)
        try:
            official_website = pipeline.find_official_website(bank)
            if not official_website:
                logging.warning("No official website found for %s", bank.central_bank_name)
                continue

            board_page = pipeline.discover_board_page(official_website)
            if board_page is None:
                logging.warning("No board page found for %s", bank.central_bank_name)
                continue

            board_rows = pipeline.extract_board_members(bank, official_website, board_page)
            if not board_rows:
                logging.warning("No members extracted for %s from %s", bank.central_bank_name, board_page.url)
                continue

            rows.extend(board_rows)
            logging.info("Extracted %d members from %s", len(board_rows), board_page.url)
        except Exception as exc:
            logging.warning("Failed on %s: %s", bank.central_bank_name, exc)

    write_csv(rows, args.output)
    logging.info("Wrote %s", args.output)
    logging.info("Rows: %d", len(rows))


if __name__ == "__main__":
    main()
