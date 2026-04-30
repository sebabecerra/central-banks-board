#!/usr/bin/env python3
"""Compare LLM answers for central-bank board members across providers.

Output schema:
country,central_bank_name,status,name,role,llama,gpt,gemini

Each provider column is 1 when that provider returned the member/role pair,
otherwise 0.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import requests
from dotenv import load_dotenv

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None  # type: ignore


PROJECT_DIR = Path("/Users/sbc/projects/central-banks-board")
DEFAULT_INPUT = PROJECT_DIR / "central_bank_sources.csv"
DEFAULT_OUTPUT = PROJECT_DIR / "central_bank_llm_comparison.csv"
DEFAULT_ENV = PROJECT_DIR / ".env"
OLLAMA_URL = "http://127.0.0.1:11434/api/generate"


@dataclass(frozen=True)
class BankRow:
    country: str
    central_bank_name: str


@dataclass(frozen=True)
class MemberRow:
    country: str
    central_bank_name: str
    status: str
    name: str
    role: str
    llama: int
    gpt: int
    gemini: int


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def normalize_key(country: str, bank: str, status: str, name: str, role: str) -> Tuple[str, str, str, str, str]:
    def norm(value: str) -> str:
        value = clean_text(value).lower()
        value = re.sub(r"[^a-z0-9 áéíóúñü'-]+", " ", value)
        return re.sub(r"\s+", " ", value).strip()

    return (norm(country), norm(bank), norm(status), norm(name), norm(role))


def build_prompt(country: str, central_bank_name: str) -> str:
    return f"""
List the current and historical board/council members of "{central_bank_name}" in "{country}".

Return valid JSON only with this structure:
{{
  "current": [
    {{"name": "", "role": "", "period": ""}}
  ],
  "historical": [
    {{"name": "", "role": "", "period": ""}}
  ]
}}

Rules:
- Include only people you are reasonably confident about.
- Do not invent names.
- If uncertain, omit the person.
- Use "current" for current members and "historical" for former or historical members.
- Return JSON only.
"""


def extract_json_block(text: str) -> str:
    text = clean_text(text)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return ""
    return text[start : end + 1]


def parse_provider_payload(text: str, country: str, central_bank_name: str) -> List[Tuple[str, str, str]]:
    text = extract_json_block(text)
    if not text:
        return []

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []

    rows: List[Tuple[str, str, str]] = []
    for status in ("current", "historical"):
        members = payload.get(status, [])
        if not isinstance(members, list):
            continue
        for item in members:
            if not isinstance(item, dict):
                continue
            name = clean_text(str(item.get("name", "")))
            role = clean_text(str(item.get("role", "")))
            if not name or not role:
                continue
            rows.append((status, name, role))
    return rows


def ask_ollama(country: str, central_bank_name: str, model: str) -> List[Tuple[str, str, str]]:
    prompt = build_prompt(country, central_bank_name)
    response = requests.post(
        OLLAMA_URL,
        json={"model": model, "prompt": prompt, "stream": False},
        timeout=180,
    )
    response.raise_for_status()
    text = response.json().get("response", "")
    return parse_provider_payload(text, country, central_bank_name)


def ask_openai(country: str, central_bank_name: str, model: str, api_key: str) -> List[Tuple[str, str, str]]:
    if OpenAI is None:
        raise RuntimeError("openai package is not installed")
    client = OpenAI(api_key=api_key)
    response = client.responses.create(
        model=model,
        input=build_prompt(country, central_bank_name),
    )
    return parse_provider_payload(response.output_text, country, central_bank_name)


def ask_gemini(country: str, central_bank_name: str, model: str, api_key: str) -> List[Tuple[str, str, str]]:
    prompt = build_prompt(country, central_bank_name)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    response = requests.post(
        url,
        json={"contents": [{"parts": [{"text": prompt}]}]},
        timeout=180,
    )
    response.raise_for_status()
    payload = response.json()
    parts = payload.get("candidates", [{}])[0].get("content", {}).get("parts", [])
    text = "\n".join(part.get("text", "") for part in parts)
    return parse_provider_payload(text, country, central_bank_name)


def read_banks(input_path: Path, limit: int) -> List[BankRow]:
    with input_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    banks = [BankRow(country=row["country"], central_bank_name=row["central_bank_name"]) for row in rows]
    return banks[:limit] if limit > 0 else banks


def compare_banks(
    banks: Sequence[BankRow],
    *,
    llama_model: str,
    gpt_model: str,
    gemini_model: str,
    enable_llama: bool,
    enable_gpt: bool,
    enable_gemini: bool,
    openai_api_key: str,
    gemini_api_key: str,
) -> List[MemberRow]:
    merged: Dict[Tuple[str, str, str, str, str], MemberRow] = {}

    for bank in banks:
        provider_rows: Dict[str, List[Tuple[str, str, str]]] = {"llama": [], "gpt": [], "gemini": []}
        print(f"Processing {bank.country} -> {bank.central_bank_name}")

        if enable_llama:
            try:
                provider_rows["llama"] = ask_ollama(bank.country, bank.central_bank_name, llama_model)
                print(f"  llama rows: {len(provider_rows['llama'])}")
            except Exception as exc:
                print(f"  llama error: {exc}")

        if enable_gpt:
            try:
                provider_rows["gpt"] = ask_openai(bank.country, bank.central_bank_name, gpt_model, openai_api_key)
                print(f"  gpt rows: {len(provider_rows['gpt'])}")
            except Exception as exc:
                print(f"  gpt error: {exc}")

        if enable_gemini:
            try:
                provider_rows["gemini"] = ask_gemini(bank.country, bank.central_bank_name, gemini_model, gemini_api_key)
                print(f"  gemini rows: {len(provider_rows['gemini'])}")
            except Exception as exc:
                print(f"  gemini error: {exc}")

        for provider_name, rows in provider_rows.items():
            for status, name, role in rows:
                key = normalize_key(bank.country, bank.central_bank_name, status, name, role)
                existing = merged.get(key)
                if existing is None:
                    existing = MemberRow(
                        country=bank.country,
                        central_bank_name=bank.central_bank_name,
                        status=status,
                        name=name,
                        role=role,
                        llama=0,
                        gpt=0,
                        gemini=0,
                    )
                updated = asdict(existing)
                updated[provider_name] = 1
                merged[key] = MemberRow(**updated)

    return sorted(
        merged.values(),
        key=lambda row: (row.country.lower(), row.central_bank_name.lower(), row.status, row.name.lower(), row.role.lower()),
    )


def write_csv(rows: Iterable[MemberRow], output_path: Path) -> None:
    fieldnames = ["country", "central_bank_name", "status", "name", "role", "llama", "gpt", "gemini"]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input CSV with central bank list.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV path.")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV), help="Path to .env file.")
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N banks.")
    parser.add_argument("--llama-model", default="llama3.2:latest", help="Ollama model.")
    parser.add_argument("--gpt-model", default="gpt-5.2", help="OpenAI model.")
    parser.add_argument("--gemini-model", default="gemini-2.5-pro", help="Gemini model.")
    parser.add_argument("--skip-llama", action="store_true")
    parser.add_argument("--skip-gpt", action="store_true")
    parser.add_argument("--skip-gemini", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv(args.env_file)

    openai_api_key = os.environ.get("OPENAI_API_KEY", "")
    gemini_api_key = os.environ.get("GEMINI_API_KEY", "")

    banks = read_banks(Path(args.input), args.limit)
    rows = compare_banks(
        banks,
        llama_model=args.llama_model,
        gpt_model=args.gpt_model,
        gemini_model=args.gemini_model,
        enable_llama=not args.skip_llama,
        enable_gpt=not args.skip_gpt,
        enable_gemini=not args.skip_gemini,
        openai_api_key=openai_api_key,
        gemini_api_key=gemini_api_key,
    )
    write_csv(rows, Path(args.output))
    print(f"Wrote {args.output}")
    print(f"Rows: {len(rows)}")


if __name__ == "__main__":
    main()
