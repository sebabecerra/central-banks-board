#!/usr/bin/env python3
"""Google Custom Search JSON API client.

Usage requires:
- GOOGLE_API_KEY
- GOOGLE_CSE_ID

These can be exported in the shell or passed as arguments.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import socket
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from typing import Iterable, List, Optional
from urllib.error import HTTPError, URLError


GOOGLE_CSE_URL = "https://www.googleapis.com/customsearch/v1"


@dataclass(frozen=True)
class SearchResult:
    position: int
    title: str
    url: str
    snippet: str


def fetch_json(url: str, retries: int = 3, timeout: int = 30) -> dict:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8", errors="replace"))
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


def build_url(query: str, *, api_key: str, cse_id: str, start: int, num: int) -> str:
    params = {
        "key": api_key,
        "cx": cse_id,
        "q": query,
        "start": str(start),
        "num": str(num),
    }
    return f"{GOOGLE_CSE_URL}?{urllib.parse.urlencode(params)}"


def search_google_api(
    query: str,
    *,
    api_key: str,
    cse_id: str,
    limit: int,
    pause_seconds: float,
) -> List[SearchResult]:
    results: List[SearchResult] = []
    start = 1

    while len(results) < limit:
        batch_size = min(10, limit - len(results))
        url = build_url(query, api_key=api_key, cse_id=cse_id, start=start, num=batch_size)
        payload = fetch_json(url)
        items = payload.get("items", [])
        if not items:
            break

        for item in items:
            results.append(
                SearchResult(
                    position=len(results) + 1,
                    title=item.get("title", ""),
                    url=item.get("link", ""),
                    snippet=item.get("snippet", ""),
                )
            )
            if len(results) >= limit:
                break

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
    parser.add_argument("--pause-seconds", type=float, default=0.5, help="Delay between API pages.")
    parser.add_argument("--api-key", default=os.getenv("GOOGLE_API_KEY", ""), help="Google API key.")
    parser.add_argument("--cse-id", default=os.getenv("GOOGLE_CSE_ID", ""), help="Google Custom Search Engine ID.")
    parser.add_argument("--format", choices=("json", "csv", "text"), default="text", help="Output format.")
    parser.add_argument("--output", default="", help="Optional file path for CSV or JSON output.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.api_key or not args.cse_id:
        raise SystemExit(
            "Missing credentials. Set GOOGLE_API_KEY and GOOGLE_CSE_ID, "
            "or pass --api-key and --cse-id."
        )

    results = search_google_api(
        args.query,
        api_key=args.api_key,
        cse_id=args.cse_id,
        limit=args.limit,
        pause_seconds=args.pause_seconds,
    )

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
