"""
Extract central-bank leadership data from Wikipedia using the list of central banks
as the entry point.

Source pages
------------
- https://en.wikipedia.org/wiki/List_of_central_banks
- The individual Wikipedia page of each central bank listed in that page

Method
------
1. Download the Wikipedia page `List of central banks`.
2. Build a base table of central banks with:
   - country
   - central bank name
   - Wikipedia URL for the bank, when available
3. Visit the Wikipedia page of each bank.
4. Extract relevant `wikitable` blocks and, when no suitable table is found,
   fall back to the page infobox.
5. Preserve the raw extracted rows in `central_bank_governors_df`.
6. Build a derived long-format table, `central_bank_governors_long_df`,
   with one row per detected person.

Core outputs
------------
- `central_banks_df`: master list of banks parsed from `List of central banks`
- `central_bank_governors_df`: raw extracted rows with full `row_data`
- `central_bank_governors_long_df`: parsed long-format person-level dataset
- `request_errors_df`: pages that failed to download or parse

Notes
-----
This script prioritizes traceability. The raw extraction layer is preserved so
that person-level parsing can be audited against the original table or infobox
content.
"""

import ast
import re
import time
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CentralBankNotebook/1.0)"}
WIKI_BANKS_URL = "https://en.wikipedia.org/wiki/List_of_central_banks"
ROLE_PATTERNS = ["governor", "governors", "president", "presidents", "chair", "chairs", "key people"]
BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "data"
OUTPUT_AUX_DIR = BASE_DIR / "data-aux"

NAME_KEYS = [
    "Name", "Governor", "President", "Chairman", "Chair",
    "Current governor", "Key people", "Name | Name",
    "Governor | Governor", "Governor | Governor.1",
    "Name | Governor of CBC (Guangzhou)",
    "Name (governor) | Name (governor)", "Name (Signature)",
    "Governor and Chairman", "President of the Bank of Guatemala",
    "Governor & Other Positions", "Board of Directors", "1",
]
START_KEYS = [
    "Took office", "Term of office | Start of term", "Term start",
    "Entered office", "Start", "From",
    "Term of office | Took office", "tenure start", "2",
]
END_KEYS = [
    "Left office", "Term of office | End of term", "Term end",
    "Exited office", "End", "Until", "Term expires",
    "Term of office | Left office", "tenure end", "4",
]
PERIOD_KEYS = ["Term", "Period", "Tenure", "In office", "Term of office",
               "Tenure length | Tenure length"]
HEADER_VALUES = {
    "name", "governor", "president", "chairman", "chair",
    "key people", "directors", "board of directors",
    "governors of the commonwealth bank of australia",
    "governors of the reserve bank of australia",
}
INVALID_NAME_RE = [
    r"^\d+$", r"^\(\d+\)$", r"^disestablished",
    r"^acting director", r"^director of", r"^deputy director",
    r"^executive assistant", r"^first deputy governor$", r"^second deputy governor$",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_text(value):
    value = str(value or "")
    value = re.sub(r"\[[^\]]*\]", "", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" -–—,;")


def print_preview(name, df, rows=10):
    print(f"{name}: {df.shape}")
    if df.empty:
        print(f"{name} is empty\n")
        return
    print(df.head(rows).to_string(index=False))
    print()


def normalize_position(value):
    value = clean_text(value)
    lowered = value.lower()
    if "deputy governor" in lowered:
        return "Deputy Governor"
    if "governor" in lowered:
        return "Governor"
    if "president" in lowered:
        return "President"
    if "chair" in lowered:
        return "Chair"
    if "board" in lowered:
        return "Board Member"
    return value


def flatten_columns(columns):
    if not isinstance(columns, pd.MultiIndex):
        return [clean_text(col) for col in columns]
    flattened = []
    for col in columns:
        parts = [clean_text(x) for x in col if clean_text(x) and str(x) != "nan"]
        flattened.append(" | ".join(parts))
    return flattened


def table_context_text(table):
    pieces = []
    caption = table.find("caption")
    if caption:
        pieces.append(caption.get_text(" ", strip=True))
    rows = table.select("tr")
    if rows:
        pieces.append(" ".join(cell.get_text(" ", strip=True) for cell in rows[0].find_all(["th", "td"])))
    prev = table.find_previous(["h2", "h3", "h4"])
    if prev:
        pieces.append(prev.get_text(" ", strip=True))
    return clean_text(" | ".join(piece for piece in pieces if piece))


def sanitize_table_html(table):
    html = str(table)
    html = re.sub(r'colspan="(\d+);"', r'colspan="\1"', html)
    html = re.sub(r'rowspan="(\d+);"', r'rowspan="\1"', html)
    return html


def parse_html_table(table):
    return pd.read_html(StringIO(sanitize_table_html(table)))[0]


def parse_row_data(raw):
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return {}
    if isinstance(raw, dict):
        data = raw
    else:
        try:
            data = ast.literal_eval(raw)
        except Exception:
            return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): clean_text(v) for k, v in data.items() if clean_text(v) and clean_text(v).lower() != "nan"}


def get_year(s):
    if not s:
        return ""
    if re.search(r"[Ii]ncumbent|[Pp]resent|[Cc]urrent", s):
        return "Incumbent"
    years = re.findall(r"\d{4}", s)
    return years[0] if years else s.strip()


def clean_name(name):
    name = re.sub(r"\s*,\s*(Governor|President|Chair|Director|Acting|Deputy|Vice).*", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s*\(\d{4}\s*[–\-]\s*\d{4}\)", "", name)
    name = re.sub(r"\s*\(\d{4}[–\-]?\)", "", name)
    name = re.sub(r"\s*\(\d{4}-present\)", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s*\(effective[^)]*\)", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s*\(born\s+\d{4}\).*$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s*\([^)]*(?:governor|president|chair|director|economist|vice|acting)[^)]*\).*$", "", name, flags=re.IGNORECASE)
    name = name.replace(",", "")
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def is_invalid(name):
    for pat in INVALID_NAME_RE:
        if re.match(pat, name, re.IGNORECASE):
            return True
    return False


def is_too_long(name):
    return len(name.split()) > 8


def looks_like_header_row(d):
    values = {clean_text(v).lower() for v in d.values() if clean_text(v)}
    return len(values & HEADER_VALUES) >= 2


def normalize_header_map(d):
    return {clean_text(k): clean_text(v) for k, v in d.items() if clean_text(k) and clean_text(v)}


def relabel_numeric_dict(d, header_map):
    relabeled = {}
    for k, v in d.items():
        kk = clean_text(k)
        vv = clean_text(v)
        if not vv:
            continue
        relabeled[header_map.get(kk, kk)] = vv
    return relabeled


def infer_cargo(source_label, name_key):
    text = f"{source_label} | {name_key}".lower()
    if "chair" in text:
        return "Chair"
    if "president" in text:
        return "President"
    if "governor" in text:
        return "Governor"
    if "key people" in text or "board" in text:
        return "Board Member"
    return ""


def split_multi_person(value):
    pattern = r"([A-ZÀ-ÿ][^(]{2,40?}?)\s*\(([^)]+)\)"
    matches = re.findall(pattern, value)
    if len(matches) > 1:
        return [(m[0].strip(), m[1].strip()) for m in matches]
    return None


# ── Scraping ──────────────────────────────────────────────────────────────────

def extract_table_rows(parsed_table, bank_row, context_text):
    parsed_table = parsed_table.copy()
    parsed_table.columns = flatten_columns(parsed_table.columns)
    records = []
    for _, row in parsed_table.iterrows():
        row_data = {
            clean_text(col): clean_text(row.get(col, ""))
            for col in parsed_table.columns
            if clean_text(row.get(col, ""))
        }
        if not row_data:
            continue
        records.append({
            "country": clean_text(bank_row.country),
            "central_bank": clean_text(bank_row.central_bank),
            "wikipedia_bank_url": clean_text(bank_row.wikipedia_bank_url),
            "source_type": "wikitable",
            "source_label": context_text,
            "row_data": row_data,
        })
    return records


def extract_infobox_rows(bank_row, soup):
    records = []
    infobox = soup.select_one("table.infobox")
    if not infobox:
        return records
    for tr in infobox.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        label = clean_text(th.get_text(" ", strip=True))
        value = clean_text(td.get_text(" ", strip=True))
        if not label or not value:
            continue
        if any(pattern in label.lower() for pattern in ROLE_PATTERNS):
            records.append({
                "country": clean_text(bank_row.country),
                "central_bank": clean_text(bank_row.central_bank),
                "wikipedia_bank_url": clean_text(bank_row.wikipedia_bank_url),
                "source_type": "infobox",
                "source_label": label,
                "row_data": {label: value},
            })
    return records


def extract_governor_rows(bank_row):
    url = bank_row.wikipedia_bank_url
    if not url:
        return []
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        return [{
            "country": clean_text(bank_row.country),
            "central_bank": clean_text(bank_row.central_bank),
            "wikipedia_bank_url": clean_text(url),
            "source_type": "request_error",
            "source_label": type(exc).__name__,
            "row_data": {"error": str(exc)},
        }]
    soup = BeautifulSoup(response.text, "html.parser")
    records = []
    for table in soup.select("table.wikitable"):
        context_text = table_context_text(table)
        if not any(pattern in context_text.lower() for pattern in ROLE_PATTERNS):
            continue
        try:
            parsed_table = parse_html_table(table)
        except ValueError:
            continue
        records.extend(extract_table_rows(parsed_table, bank_row, context_text))
    if records:
        return records
    return extract_infobox_rows(bank_row, soup)


# ── Name extraction (same logic as expand_row_data.py) ───────────────────────

def extract_person_rows(row, header_map):
    """
    Given a raw row and a header_map for numeric keys,
    returns a list of person dicts with name/start/end.
    """
    d = parse_row_data(row["row_data"])
    if not d:
        return []

    if looks_like_header_row(d):
        return []

    if header_map:
        d = relabel_numeric_dict(d, header_map)

    raw_name = next((clean_text(d[k]) for k in NAME_KEYS if clean_text(d.get(k, ""))), "")
    if not raw_name:
        return []

    start = next((get_year(clean_text(d[k])) for k in START_KEYS if clean_text(d.get(k, ""))), "")
    if not start:
        for k in PERIOD_KEYS:
            if clean_text(d.get(k, "")):
                yrs = re.findall(r"\d{4}", clean_text(d[k]))
                if yrs:
                    start = yrs[0]
                    break

    end = next((get_year(clean_text(d[k])) for k in END_KEYS if clean_text(d.get(k, ""))), "")
    if not end:
        for k in PERIOD_KEYS:
            if clean_text(d.get(k, "")):
                val = clean_text(d[k])
                if re.search(r"[Pp]resent|[Cc]urrent|[Ii]ncumbent", val):
                    end = "Incumbent"
                    break
                yrs = re.findall(r"\d{4}", val)
                if len(yrs) > 1:
                    end = yrs[-1]
                    break

    name_source = next((k for k in NAME_KEYS if clean_text(d.get(k, "")) == raw_name), "")
    cargo = infer_cargo(row["source_label"], name_source)

    # Handle multiple people in one field
    multi = split_multi_person(raw_name)
    if multi:
        results = []
        for name, role in multi:
            name = clean_name(name)
            if not name or name.lower() in HEADER_VALUES or is_invalid(name) or is_too_long(name):
                continue
            results.append({
                "country": row["country"],
                "central_bank": row["central_bank"],
                "wikipedia_bank_url": row["wikipedia_bank_url"],
                "source_type": row["source_type"],
                "source_label": row["source_label"],
                "wiki_name": name,
                "cargo": role,
                "start_year": start,
                "end_year": end,
            })
        return results

    name = clean_name(raw_name)
    if not name or name.lower() in HEADER_VALUES or is_invalid(name) or is_too_long(name):
        return []

    return [{
        "country": row["country"],
        "central_bank": row["central_bank"],
        "wikipedia_bank_url": row["wikipedia_bank_url"],
        "source_type": row["source_type"],
        "source_label": row["source_label"],
        "wiki_name": name,
        "cargo": cargo,
        "start_year": start,
        "end_year": end,
    }]


def fetch_central_banks():
    response = requests.get(WIKI_BANKS_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    wiki_tables = pd.read_html(StringIO(response.text))
    soup = BeautifulSoup(response.text, "html.parser")
    html_tables = soup.select("table.wikitable")

    if len(wiki_tables) < 2 or len(html_tables) < 2:
        raise ValueError("Expected at least two tables on the Wikipedia page")

    alphabetical_html_table = html_tables[0]
    alphabetical_records = []

    for row in alphabetical_html_table.select("tr")[1:]:
        cols = row.find_all(["th", "td"])
        if len(cols) < 3:
            continue

        country = cols[0].get_text(" ", strip=True)
        bank_col = cols[2]
        central_bank = bank_col.get_text(" ", strip=True)
        link = bank_col.find("a", href=True)
        wikipedia_bank_url = ""
        if link and link["href"].startswith("/wiki/"):
            wikipedia_bank_url = "https://en.wikipedia.org" + link["href"]

        alphabetical_records.append(
            {
                "country": country,
                "central_bank": central_bank,
                "wikipedia_bank_url": wikipedia_bank_url,
            }
        )

    alphabetical_banks_df = pd.DataFrame(alphabetical_records)
    major_table_raw = wiki_tables[1].copy()
    major_banks_df = major_table_raw.rename(
        columns={major_table_raw.columns[0]: "country", major_table_raw.columns[1]: "central_bank"}
    )[["country", "central_bank"]].copy()
    major_banks_df["wikipedia_bank_url"] = ""

    for frame in (alphabetical_banks_df, major_banks_df):
        frame["country"] = frame["country"].astype(str).str.strip()
        frame["central_bank"] = frame["central_bank"].astype(str).str.strip()
        frame["wikipedia_bank_url"] = frame["wikipedia_bank_url"].astype(str).str.strip()

    return pd.concat([alphabetical_banks_df, major_banks_df], ignore_index=True).drop_duplicates(
        subset=["country", "central_bank"]
    ).reset_index(drop=True)


def build_raw_governors(central_banks_df):
    governor_rows = []
    for i, bank_row in enumerate(central_banks_df.itertuples(index=False), start=1):
        if i % 25 == 0:
            print(f"Processed {i}/{len(central_banks_df)} banks")
        governor_rows.extend(extract_governor_rows(bank_row))
        time.sleep(0.3)

    governors_df = pd.DataFrame(governor_rows)
    errors_df = governors_df[governors_df["source_type"] == "request_error"].copy()
    return governors_df, errors_df


def build_long_governors(central_bank_governors_df):
    group_cols = ["country", "central_bank", "wikipedia_bank_url", "source_type", "source_label"]
    long_rows = []

    for _, group in central_bank_governors_df.groupby(group_cols, dropna=False, sort=False):
        header_map = {}
        for _, row in group.iterrows():
            d = parse_row_data(row["row_data"])
            if d and looks_like_header_row(d):
                header_map = normalize_header_map(d)
                break

        for _, row in group.iterrows():
            long_rows.extend(extract_person_rows(row, header_map))

    long_df = pd.DataFrame(long_rows)
    if not long_df.empty:
        long_df = long_df.drop_duplicates(
            subset=["country", "central_bank", "wiki_name", "start_year", "end_year"]
        ).reset_index(drop=True)
    return long_df


def build_final_output(long_df):
    if long_df.empty:
        return pd.DataFrame(
            columns=[
                "country",
                "central_bank_name",
                "name",
                "position",
                "start_year",
                "end_year",
                "source_dataset",
                "source_method",
                "source_page",
                "source_detail",
            ]
        )

    final_df = long_df.copy()
    final_df["central_bank_name"] = final_df["central_bank"]
    final_df["name"] = final_df["wiki_name"]
    final_df["position"] = final_df["cargo"].apply(normalize_position)
    final_df["source_dataset"] = "banks"
    final_df["source_method"] = "bank_page_tables_and_infobox"
    final_df["source_page"] = final_df["wikipedia_bank_url"]
    final_df["source_detail"] = final_df["source_label"]

    final_df = final_df[
        [
            "country",
            "central_bank_name",
            "name",
            "position",
            "start_year",
            "end_year",
            "source_dataset",
            "source_method",
            "source_page",
            "source_detail",
        ]
    ].drop_duplicates().reset_index(drop=True)

    return final_df


def main():
    central_banks_df = fetch_central_banks()
    central_bank_governors_df, request_errors_df = build_raw_governors(central_banks_df)
    central_bank_governors_long_df = build_long_governors(central_bank_governors_df)
    final_df = build_final_output(central_bank_governors_long_df)

    print_preview("central_bank_people_from_banks_long", final_df, rows=20)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_AUX_DIR.mkdir(parents=True, exist_ok=True)

    final_df.to_csv(OUTPUT_DIR / "central_bank_people_from_banks_long.csv", index=False, sep=";")
    central_banks_df.to_csv(OUTPUT_AUX_DIR / "central_banks.csv", index=False, sep=";")
    central_bank_governors_df.to_csv(OUTPUT_AUX_DIR / "central_bank_governors.csv", index=False, sep=";")
    central_bank_governors_long_df.to_csv(OUTPUT_AUX_DIR / "central_bank_governors_long.csv", index=False, sep=";")
    request_errors_df.to_csv(OUTPUT_AUX_DIR / "central_bank_governors_request_errors.csv", index=False, sep=";")

    print("Exported OK")


if __name__ == "__main__":
    main()
