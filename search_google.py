#!/usr/bin/env python3
"""Simple Google search scraper for research workflows.

This script sends a Google search request and extracts the visible results
from the HTML response. It is useful for discovery tasks such as finding
official board pages for central banks.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import socket
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from html import unescape
from typing import Iterable, List, Optional
from urllib.error import HTTPError, URLError

try:
    from bs4 import BeautifulSoup
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: beautifulsoup4. Install it with `python3 -m pip install beautifulsoup4`."
    ) from exc


GOOGLE_SEARCH_URL = "https://www.google.com/search"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass(frozen=True)
class SearchResult:
    position: int
    title: str
    url: str
    snippet: str


def clean_text(value: str) -> str:
    value = unescape(value or "")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def fetch_html(url: str, retries: int = 3, timeout: int = 30) -> str:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * attempt)
                continue
            break
        except (URLError, TimeoutError, socket.timeout) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * attempt)
                continue
            break
    raise RuntimeError(f"Failed to fetch {url}: {last_error}") from last_error


def build_google_url(
    query: str,
    *,
    num_results: int,
    language: str,
    country: str,
    start: int,
) -> str:
    params = {
        "q": query,
        "num": str(num_results),
        "hl": language,
        "gl": country,
        "start": str(start),
        "pws": "0",
    }
    return f"{GOOGLE_SEARCH_URL}?{urllib.parse.urlencode(params)}"


def extract_target_url(raw_href: str) -> str:
    if raw_href.startswith("/url?"):
        parsed = urllib.parse.parse_qs(urllib.parse.urlparse(raw_href).query)
        target = parsed.get("q", [""])[0]
        return clean_text(target)
    if raw_href.startswith("http://") or raw_href.startswith("https://"):
        return clean_text(raw_href)
    return ""


def parse_results(html: str) -> List[SearchResult]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[SearchResult] = []
    seen_urls = set()

    html_lower = html.lower()
    if "enablejs" in html_lower or "httpservice/retry/enablejs" in html_lower:
        raise RuntimeError(
            "Google returned an anti-bot / enable-JavaScript page instead of search results. "
            "This HTML-scraping approach is blocked right now."
        )

    for block in soup.select("div.g, div.Gx5Zad"):
        link = block.select_one("a[href]")
        title_node = block.select_one("h3")
        if link is None or title_node is None:
            continue

        url = extract_target_url(link.get("href", ""))
        if not url or "google." in urllib.parse.urlparse(url).netloc:
            continue
        if url in seen_urls:
            continue

        snippet = ""
        for selector in ("div.VwiC3b", "div.yXK7lf", "span.aCOpRe", "div[data-sncf='1']"):
            node = block.select_one(selector)
            if node:
                snippet = clean_text(node.get_text(" ", strip=True))
                break

        seen_urls.add(url)
        results.append(
            SearchResult(
                position=len(results) + 1,
                title=clean_text(title_node.get_text(" ", strip=True)),
                url=url,
                snippet=snippet,
            )
        )

    return results


def search_google(
    query: str,
    *,
    limit: int,
    language: str,
    country: str,
    pause_seconds: float,
) -> List[SearchResult]:
    results: List[SearchResult] = []
    start = 0

    while len(results) < limit:
        batch_size = min(10, limit - len(results))
        url = build_google_url(
            query,
            num_results=batch_size,
            language=language,
            country=country,
            start=start,
        )
        html = fetch_html(url)
        page_results = parse_results(html)
        if not page_results:
            break

        for result in page_results:
            if len(results) >= limit:
                break
            results.append(
                SearchResult(
                    position=len(results) + 1,
                    title=result.title,
                    url=result.url,
                    snippet=result.snippet,
                )
            )

        start += batch_size
        if pause_seconds > 0 and len(results) < limit:
            time.sleep(pause_seconds)

    return results


def write_csv(results: Iterable[SearchResult], output_path: str) -> None:
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["position", "title", "url", "snippet"])
        writer.writeheader()
        for result in results:
            writer.writerow(asdict(result))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", help="Google query.")
    parser.add_argument("--limit", type=int, default=10, help="Maximum number of results to return.")
    parser.add_argument("--language", default="en", help="Google hl parameter.")
    parser.add_argument("--country", default="us", help="Google gl parameter.")
    parser.add_argument("--pause-seconds", type=float, default=1.0, help="Delay between result pages.")
    parser.add_argument("--site", default="", help="Optional site restriction, for example bankofengland.co.uk.")
    parser.add_argument(
        "--format",
        choices=("json", "csv", "text"),
        default="text",
        help="Output format.",
    )
    parser.add_argument("--output", default="", help="Optional file path for CSV or JSON output.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    query = args.query
    if args.site:
        query = f"site:{args.site} {query}"

    try:
        results = search_google(
            query,
            limit=args.limit,
            language=args.language,
            country=args.country,
            pause_seconds=args.pause_seconds,
        )
    except RuntimeError as exc:
        raise SystemExit(
            f"{exc}\n"
            "Tip: for reliable Google results, use the Google Custom Search JSON API instead of scraping HTML."
        ) from exc

    if args.output and args.format == "csv":
        write_csv(results, args.output)
        print(f"Wrote {args.output}")
        return

    if args.output and args.format == "json":
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump([asdict(result) for result in results], handle, ensure_ascii=False, indent=2)
        print(f"Wrote {args.output}")
        return

    if args.format == "json":
        print(json.dumps([asdict(result) for result in results], ensure_ascii=False, indent=2))
        return

    if args.format == "csv":
        print("position,title,url,snippet")
        for result in results:
            row = [str(result.position), result.title, result.url, result.snippet]
            print(",".join(json.dumps(cell, ensure_ascii=False) for cell in row))
        return

    for result in results:
        print(f"{result.position}. {result.title}")
        print(result.url)
        if result.snippet:
            print(result.snippet)
        print()


if __name__ == "__main__":
    main()
