#!/usr/bin/env python3
"""Pregunta a un LLM por los miembros del board de un banco central."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import requests
from bs4 import BeautifulSoup


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def ask_mistral(prompt: str, *, api_key: str, model: str) -> str:
    response = requests.post(
        "https://api.mistral.ai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "temperature": 0,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a research assistant. Return concise, structured answers "
                        "about central bank board members."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        },
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"]


def check_url(url: str) -> str:
    if not url or url == "unknown":
        return "unknown"
    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=20,
            allow_redirects=True,
        )
        if response.ok:
            return f"ok:{response.status_code}"
        return f"error:{response.status_code}"
    except requests.RequestException:
        return "error:request_failed"


def fetch_page_text(url: str) -> str:
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20,
        allow_redirects=True,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    for node in soup(["script", "style", "noscript"]):
        node.decompose()
    return " ".join(soup.stripped_strings)


def normalize_text(text: str) -> str:
    return " ".join((text or "").lower().split())


def extract_json(text: str):
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
        if text.lower().startswith("json"):
            text = text[4:].strip()
    return json.loads(text)


def build_prompt(target: str) -> str:
    return f"""
Who are the current board members of the central bank of {target}?

Return only valid JSON with this schema:
{{
  "target": "{target}",
  "central_bank_name": "...",
  "as_of_date": "YYYY-MM-DD or unknown",
  "confidence_note": "...",
  "members": [
    {{
      "name": "...",
      "role": "...",
      "source_url": "https://... or unknown"
    }}
  ]
}}

Rules:
- Be conservative.
- If you are not sure a person is a current member, do not include them.
- If you are uncertain about the bank name, date, role, or source, use "unknown".
- Prefer fewer names over incorrect names.
- Do not return markdown fences.
"""


def verify_sources(payload: dict) -> dict:
    members = payload.get("members", [])
    if not isinstance(members, list):
        return payload

    for member in members:
        if not isinstance(member, dict):
            continue
        source_url = str(member.get("source_url", "unknown"))
        source_status = check_url(source_url)
        member["source_status"] = source_status
        member["name_found_in_source"] = False

        if not source_status.startswith("ok:"):
            continue

        try:
            page_text = normalize_text(fetch_page_text(source_url))
            member_name = normalize_text(str(member.get("name", "")))
            member["name_found_in_source"] = bool(member_name and member_name in page_text)
        except requests.RequestException:
            member["name_found_in_source"] = False
    return payload


def main() -> None:
    load_dotenv(Path(__file__).resolve().with_name(".env"))

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", help="País o nombre del banco central")
    parser.add_argument("--model", default=os.getenv("MISTRAL_MODEL", "mistral-small-latest"), help="Modelo de Mistral")
    parser.add_argument("--verify", action="store_true", help="Verifica que las source_url respondan")
    args = parser.parse_args()

    api_key = os.getenv("MISTRAL_API_KEY", "")
    if not api_key:
        raise SystemExit("Falta MISTRAL_API_KEY en el entorno o en .env")

    answer = ask_mistral(build_prompt(args.target), api_key=api_key, model=args.model)

    try:
        parsed = extract_json(answer)
        if args.verify:
            parsed = verify_sources(parsed)
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
    except json.JSONDecodeError:
        print(answer)


if __name__ == "__main__":
    main()
