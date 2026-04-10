#!/usr/bin/env python3
"""Query Gemini for current and historical board members of Latin American central banks."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
from dotenv import load_dotenv


PROJECT_DIR = Path("/Users/sbc/projects/central-banks-board")
DEFAULT_ENV_PATH = PROJECT_DIR / ".env"
DEFAULT_INPUT = PROJECT_DIR / "central_bank_sources.csv"
DEFAULT_OUTPUT = PROJECT_DIR / "gemini_latin_america_boards.csv"
DEFAULT_ERRORS_OUTPUT = PROJECT_DIR / "gemini_latin_america_boards_errors.csv"
DEFAULT_MODEL = "gemini-2.5-flash"
DEFAULT_DELAY_SECONDS = 60.0
DEFAULT_RETRY_DELAYS = [15.0, 30.0, 60.0]

LATIN_AMERICA_COUNTRIES = {
    "Argentina",
    "Bolivia",
    "Brazil",
    "Chile",
    "Colombia",
    "Costa Rica",
    "Cuba",
    "Dominican Republic",
    "Ecuador",
    "El Salvador",
    "Guatemala",
    "Haiti",
    "Honduras",
    "Mexico",
    "Nicaragua",
    "Panama",
    "Paraguay",
    "Peru",
    "Uruguay",
    "Venezuela",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_PATH), help="Path to .env file.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="CSV with central bank list.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV path.")
    parser.add_argument("--errors-output", default=str(DEFAULT_ERRORS_OUTPUT), help="Output CSV path for per-country errors.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Gemini model.")
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N Latin American banks.")
    parser.add_argument("--delay-seconds", type=float, default=DEFAULT_DELAY_SECONDS, help="Base delay between countries.")
    return parser.parse_args()


def read_latin_america_banks(input_path: Path, limit: int) -> List[Dict[str, str]]:
    with input_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    banks = [
        {"country": row["country"], "central_bank_name": row["central_bank_name"]}
        for row in rows
        if row["country"] in LATIN_AMERICA_COUNTRIES
    ]
    return banks[:limit] if limit > 0 else banks


def build_prompt(country: str, central_bank_name: str) -> str:
    return f"""Return ONLY valid JSON.
No markdown.
No commentary.

Task:
Identify the current and historical board/council members of {central_bank_name} in {country}.

Rules:
- Include only people you are reasonably confident about.
- Do not invent names.
- If uncertain, omit the person.
- Use "current" for current members and "historical" for former or historical members.
- Use exactly this schema:
{{
  "current": [
    {{"name": "", "role": "", "period": ""}}
  ],
  "historical": [
    {{"name": "", "role": "", "period": ""}}
  ]
}}
"""


def extract_text(payload: dict) -> str:
    parts = payload.get("candidates", [{}])[0].get("content", {}).get("parts", [])
    return "\n".join(part.get("text", "") for part in parts)


def extract_json_block(text: str) -> str:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return ""
    return text[start : end + 1]


def clean_json_text(text: str) -> str:
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = re.sub(r",(\s*[}\]])", r"\1", cleaned)
    cleaned = re.sub(r'(\}|\])(\s*)(\{)', r'\1,\2\3', cleaned)
    cleaned = re.sub(r'("|\d|true|false|null)(\s*\n\s*)(")', r'\1,\2\3', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'(\}|\]|\")(\s*\n\s*)("[^"]+"\s*:)', r'\1,\2\3', cleaned)
    return cleaned


def parse_llm_json(text: str) -> dict:
    json_text = extract_json_block(text)
    if not json_text:
        raise ValueError("No JSON found in response.")

    try:
        return json.loads(json_text)
    except json.JSONDecodeError:
        cleaned = clean_json_text(json_text)
        return json.loads(cleaned)


def rows_from_payload(payload: dict, country: str, central_bank_name: str) -> List[dict]:
    rows: List[dict] = []
    for status in ("current", "historical"):
        members = payload.get(status, [])
        if not isinstance(members, list):
            continue
        for item in members:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            role = str(item.get("role", "")).strip()
            period = str(item.get("period", "")).strip()
            if not name or not role:
                continue
            rows.append(
                {
                    "country": country,
                    "central_bank_name": central_bank_name,
                    "status": status,
                    "name": name,
                    "role": role,
                    "period": period,
                }
            )
    return rows


def ask_gemini(api_key: str, model: str, prompt: str, retry_delays: List[float]) -> dict:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    for attempt in range(len(retry_delays) + 1):
        response = requests.post(
            url,
            headers={
                "x-goog-api-key": api_key,
                "Content-Type": "application/json",
            },
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0,
                    "maxOutputTokens": 4000,
                    "responseMimeType": "application/json",
                },
            },
            timeout=180,
        )
        if response.status_code not in {429, 503}:
            response.raise_for_status()
            return response.json()

        if attempt >= len(retry_delays):
            response.raise_for_status()

        wait_seconds = retry_delays[attempt]
        retry_after = response.headers.get("Retry-After", "").strip()
        if retry_after:
            try:
                wait_seconds = max(wait_seconds, float(retry_after))
            except ValueError:
                pass
        time.sleep(wait_seconds)

    raise RuntimeError("Gemini request failed after retries.")


def dedupe_rows(rows: List[dict]) -> List[dict]:
    seen = set()
    clean = []
    for row in rows:
        key = (
            row["country"].lower(),
            row["central_bank_name"].lower(),
            row["status"].lower(),
            re.sub(r"\s+", " ", row["name"].lower()).strip(),
            re.sub(r"\s+", " ", row["role"].lower()).strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        clean.append(row)
    return clean


def main() -> None:
    args = parse_args()
    load_dotenv(args.env_file)

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise ValueError(f"GEMINI_API_KEY is missing. Check {args.env_file}.")

    banks = read_latin_america_banks(Path(args.input), args.limit)
    all_rows: List[dict] = []
    error_rows: List[dict] = []
    retry_delays = list(DEFAULT_RETRY_DELAYS)

    for index, bank in enumerate(banks):
        country = bank["country"]
        central_bank_name = bank["central_bank_name"]
        print(f"Processing {country} -> {central_bank_name}")
        prompt = build_prompt(country, central_bank_name)
        payload = None
        try:
            payload = ask_gemini(api_key, args.model, prompt, retry_delays)
            text = extract_text(payload)
            parsed = parse_llm_json(text)
            rows = rows_from_payload(parsed, country, central_bank_name)
            print(f"  Rows: {len(rows)}")
            all_rows.extend(rows)
        except Exception as exc:
            print(f"  Error: {exc}")
            raw_text = ""
            if payload is not None:
                try:
                    raw_text = extract_text(payload)[:1000]
                except Exception:
                    raw_text = ""
            error_rows.append(
                {
                    "country": country,
                    "central_bank_name": central_bank_name,
                    "error": str(exc),
                    "raw_response_excerpt": raw_text,
                }
            )

        if index < len(banks) - 1 and args.delay_seconds > 0:
            time.sleep(args.delay_seconds)

    final_rows = dedupe_rows(all_rows)
    df = pd.DataFrame(final_rows)
    df.to_csv(args.output, index=False)
    pd.DataFrame(error_rows).to_csv(args.errors_output, index=False)
    print(f"Done. Wrote {args.output} with {len(df)} rows.")
    print(f"Errors saved to {args.errors_output} with {len(error_rows)} rows.")


if __name__ == "__main__":
    main()
