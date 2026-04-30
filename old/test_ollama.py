#!/usr/bin/env python3
"""Minimal Ollama health check and test query."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List

import requests


DEFAULT_BASE_URL = "http://localhost:11434"
DEFAULT_MODEL = "llama3.1"
DEFAULT_PROMPT = """Return ONLY valid JSON.
No markdown.
No commentary.
No reasoning.

Task:
Identify the current and historical board/council members of the Central Bank of Chile.

Rules:
- Include only people you are reasonably confident about.
- Do not invent names.
- If uncertain, omit the person.
- If you do not know, return empty arrays.
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


def get_json(url: str) -> Dict[str, Any]:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    response = requests.post(url, json=payload, timeout=180)
    response.raise_for_status()
    return response.json()


def list_models(base_url: str) -> List[str]:
    payload = get_json(f"{base_url}/api/tags")
    return [item.get("name", "") for item in payload.get("models", [])]


def test_prompt(base_url: str, model: str, prompt: str) -> str:
    payload = post_json(
        f"{base_url}/api/generate",
        {
            "model": model,
            "prompt": prompt,
            "stream": False,
        },
    )
    return payload.get("response", "")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Ollama base URL.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Model to test.")
    parser.add_argument(
        "--prompt",
        default=DEFAULT_PROMPT,
        help="Prompt to send if the server and model are available.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    print(f"Checking Ollama server at {args.base_url} ...")
    try:
        models = list_models(args.base_url)
    except Exception as exc:
        print("ERROR: Ollama server is not responding.")
        print(exc)
        return 1

    print("Server OK")
    print("Available models:")
    for model in models:
        print(f"- {model}")

    if args.model not in models:
        print(f"ERROR: Model '{args.model}' is not available.")
        print(f"Try: ollama pull {args.model}")
        return 2

    print(f"Running test prompt on model '{args.model}' ...")
    try:
        text = test_prompt(args.base_url, args.model, args.prompt)
    except Exception as exc:
        print("ERROR: Request to /api/generate failed.")
        print(exc)
        return 3

    print("Response:")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
