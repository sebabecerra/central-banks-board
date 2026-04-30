#!/usr/bin/env python3
"""Build a source table for central-bank official websites and board pages."""

from __future__ import annotations

import argparse
import csv
import logging
from dataclasses import asdict, dataclass
from typing import Iterable, List

from extract_central_bank_boards import BoardDiscoveryPipeline


DEFAULT_OUTPUT = "central_bank_sources.csv"


@dataclass(frozen=True)
class CentralBankSourceRow:
    country: str
    central_bank_name: str
    wikipedia_url: str
    official_website: str
    board_page_url: str
    board_page_title: str
    board_page_discovery_method: str


def build_rows(limit: int, pause_seconds: float) -> List[CentralBankSourceRow]:
    pipeline = BoardDiscoveryPipeline(pause_seconds=pause_seconds)
    banks = pipeline.list_central_banks()
    if limit > 0:
        banks = banks[:limit]
        logging.info("Limiting run to %d banks", len(banks))

    rows: List[CentralBankSourceRow] = []
    for index, bank in enumerate(banks, start=1):
        logging.info("Processing %d/%d: %s", index, len(banks), bank.central_bank_name)
        try:
            official_website = pipeline.find_official_website(bank)
        except Exception as exc:
            logging.warning("Official website lookup failed for %s: %s", bank.central_bank_name, exc)
            official_website = ""

        board_page_url = ""
        board_page_title = ""
        board_page_discovery_method = ""
        if official_website:
            try:
                board_page = pipeline.discover_board_page(official_website)
            except Exception as exc:
                logging.warning("Board-page discovery failed for %s: %s", bank.central_bank_name, exc)
                board_page = None
            if board_page is not None:
                board_page_url = board_page.url
                board_page_title = board_page.title
                board_page_discovery_method = board_page.discovery_method

        rows.append(
            CentralBankSourceRow(
                country=bank.country,
                central_bank_name=bank.central_bank_name,
                wikipedia_url=bank.wikipedia_url,
                official_website=official_website,
                board_page_url=board_page_url,
                board_page_title=board_page_title,
                board_page_discovery_method=board_page_discovery_method,
            )
        )

    return rows


def write_csv(rows: Iterable[CentralBankSourceRow], output_path: str) -> None:
    fieldnames = [
        "country",
        "central_bank_name",
        "wikipedia_url",
        "official_website",
        "board_page_url",
        "board_page_title",
        "board_page_discovery_method",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="CSV output path.")
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N banks.")
    parser.add_argument("--pause-seconds", type=float, default=0.2, help="Delay between uncached requests.")
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
    rows = build_rows(limit=args.limit, pause_seconds=args.pause_seconds)
    write_csv(rows, args.output)
    official_hits = sum(1 for row in rows if row.official_website)
    board_hits = sum(1 for row in rows if row.board_page_url)
    logging.info("Wrote %s", args.output)
    logging.info("Rows: %d", len(rows))
    logging.info("Official websites found: %d", official_hits)
    logging.info("Board pages found: %d", board_hits)


if __name__ == "__main__":
    main()
