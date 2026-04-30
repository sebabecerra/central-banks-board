#!/usr/bin/env python3
"""Minimal Gemini test script for central-bank board questions."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import List

import pandas as pd
import requests
from dotenv import load_dotenv


PROJECT_DIR = Path("/Users/sbc/projects/central-banks-board")
DEFAULT_ENV_PATH = PROJECT_DIR / ".env"
DEFAULT_MODEL = "gemini-2.5-flash"
DEFAULT_OUTPUT = PROJECT_DIR / "gemini_chile_board.csv"
DEFAULT_PROMPT = """Return ONLY valid JSON.
No markdown.
No commentary.

Task:
Identify the current and historical board/council members of the Central Bank of Chile.

Rules:
- Include only people you are reasonably confident about.
- Do not invent names.
- If uncertain, omit the person.
- Use exactly this schema:
{
  "current": [
    {"name": "", "role": "", "period": ""}
  ],
  "historical": [
    {"name": "", "role": "", "period": ""}
  ]
}
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_PATH), help="Path to .env file.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Gemini model, for example gemini-2.5-flash or gemini-2.5-pro.")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT, help="Prompt to send.")
    parser.add_argument("--country", default="Chile", help="Country label for the output table.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="CSV output path.")
    return parser.parse_args()


def extract_text(payload: dict) -> str:
    parts = payload.get("candidates", [{}])[0].get("content", {}).get("parts", [])
    return "\n".join(part.get("text", "") for part in parts)


def extract_json_block(text: str) -> str:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return ""
    return text[start : end + 1]


def rows_from_payload(payload: dict, country: str) -> List[dict]:
    rows: List[dict] = []
    for status in ("current", "historical"):
        members = payload.get(status, [])
        if not isinstance(members, list):
            continue
        for item in members:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "country": country,
                    "status": status,
                    "name": item.get("name", ""),
                    "role": item.get("role", ""),
                    "period": item.get("period", ""),
                }
            )
    return rows


def main() -> None:
    args = parse_args()
    load_dotenv(args.env_file)

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise ValueError(f"GEMINI_API_KEY is missing. Check {args.env_file}.")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{args.model}:generateContent"
    response = requests.post(
        url,
        headers={
            "x-goog-api-key": api_key,
            "Content-Type": "application/json",
        },
        json={
            "contents": [
                {
                    "parts": [
                        {"text": args.prompt},
                    ]
                }
            ]
        },
        timeout=120,
    )

    response.raise_for_status()
    text = extract_text(response.json())
    json_text = extract_json_block(text)
    if not json_text:
        raise ValueError("Gemini response did not contain a JSON object.")

    payload = json.loads(json_text)
    rows = rows_from_payload(payload, args.country)
    df = pd.DataFrame(rows)
    df.to_csv(args.output, index=False)
    print(f"Done. Wrote {args.output} with {len(df)} rows.")


if __name__ == "__main__":
    main()
