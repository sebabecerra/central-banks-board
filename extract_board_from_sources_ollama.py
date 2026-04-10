#!/usr/bin/env python3
"""Extract central-bank board members from official source pages using Ollama."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from typing import Iterable, List

import requests
from bs4 import BeautifulSoup


DEFAULT_BASE_URL = "http://127.0.0.1:11434"
DEFAULT_MODEL = "mistral:latest"


@dataclass(frozen=True)
class SourceDoc:
    url: str
    text: str


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def fetch_source_text(url: str) -> SourceDoc:
    response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    for tag in soup(["script", "style", "noscript", "svg", "footer", "nav", "header"]):
        tag.decompose()

    texts: List[str] = []
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "li", "p", "td", "span"]):
        text = clean_text(tag.get_text(" ", strip=True))
        lowered = text.lower()
        if len(text) < 4:
            continue
        if any(bad in lowered for bad in ("cookie", "privacy", "newsletter", "all rights reserved")):
            continue
        texts.append(text)

    return SourceDoc(url=url, text="\n".join(texts[:1200]))


def build_prompt(country: str, central_bank_name: str, docs: Iterable[SourceDoc]) -> str:
    parts = []
    for index, doc in enumerate(docs, start=1):
        parts.append(f"SOURCE {index}: {doc.url}\n{doc.text[:12000]}")

    joined_sources = "\n\n".join(parts)
    return f"""
Return ONLY valid JSON.
No markdown.
No commentary.
No reasoning.

Task:
Using only the supplied official source texts, identify the current and historical board/council members of {central_bank_name} in {country}.

Rules:
- Use only the supplied source texts.
- Do not invent names.
- If uncertain, omit the person.
- Separate current and historical members.
- Include source_url from the supplied sources when possible.
- If a member appears to be current, put them in "current".
- If a member appears to be former/historical, put them in "historical".
- If nothing reliable is found, return empty arrays.

Return exactly this schema:
{{
  "current": [
    {{"name": "", "role": "", "period": "", "source_url": ""}}
  ],
  "historical": [
    {{"name": "", "role": "", "period": "", "source_url": ""}}
  ]
}}

Sources:
{joined_sources}
"""


def ask_ollama(base_url: str, model: str, prompt: str) -> str:
    response = requests.post(
        f"{base_url}/api/generate",
        json={"model": model, "prompt": prompt, "stream": False},
        timeout=240,
    )
    response.raise_for_status()
    return response.json().get("response", "")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--country", required=True)
    parser.add_argument("--central-bank-name", required=True)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--source-url", action="append", required=True, help="Official source URL. Can be passed multiple times.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    docs = [fetch_source_text(url) for url in args.source_url]
    print("Fetched sources:")
    for doc in docs:
        print(f"- {doc.url} ({len(doc.text)} chars)")

    prompt = build_prompt(args.country, args.central_bank_name, docs)
    response_text = ask_ollama(args.base_url, args.model, prompt)
    print("Model response:")
    print(response_text)


if __name__ == "__main__":
    main()
