"""
Stage 1 for Bios_WebScrapping: enrich the workbook with Wikipedia and Wikidata.

This script is the raw enrichment stage for the Bios pipeline. It starts from
`CM/data/Bios_WebScrapping.xlsx`, searches each person on Wikipedia, consults
Wikidata when a plausible match exists, and writes a traceable enriched output.

Primary input
-------------
- CM/data/Bios_WebScrapping.xlsx

Target fields
-------------
- Birth_year
- Birth_month
- Start_year
- Start_month
- End_year
- End_month
- Education
- BA_or_MA
- MBA
- PhD
- CountryBirth
- CityBirth
- Sex

Position recoding
-----------------
- 0 = Board member
- 1 = Deputy governor / deputy president / deputy chair
- 2 = Governor / president / chair

Primary outputs in CM/data
--------------------------
- bios_webscrapping_wikipedia_enriched.csv
- bios_webscrapping_wikipedia_enriched.xlsx

Auxiliary outputs in CM/data-aux
--------------------------------
- bios_webscrapping_wikipedia_matches.csv
- bios_webscrapping_wikipedia_unmatched.csv
- bios_webscrapping_wikipedia_cache.json
- bios_webscrapping_wikipedia_tenure_cache.json

Operational note
----------------
This is the raw enrichment stage. The result may still contain doubtful matches,
so it should be cleaned afterward with `clean_bios_webscrapping_delivery.py`.
"""

from __future__ import annotations

import argparse
from difflib import SequenceMatcher
import json
import math
import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_CANDIDATES = [
    BASE_DIR / "data" / "Bios_WebScrapping.xlsx",
    BASE_DIR / "data-aux" / "Bios_WebScrapping.xlsx",
    BASE_DIR.parent / "old" / "Bios_WebScrapping.xlsx",
    BASE_DIR.parent / "old" / "data" / "Bios_WebScrapping.xlsx",
]
OUTPUT_DIR = BASE_DIR / "data"
OUTPUT_AUX_DIR = BASE_DIR / "data-aux"
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "bios_webscrapping_wikipedia_enriched.csv"
DEFAULT_OUTPUT_XLSX = OUTPUT_DIR / "bios_webscrapping_wikipedia_enriched.xlsx"
DEFAULT_MATCHES_OUTPUT = OUTPUT_AUX_DIR / "bios_webscrapping_wikipedia_matches.csv"
DEFAULT_UNMATCHED_OUTPUT = OUTPUT_AUX_DIR / "bios_webscrapping_wikipedia_unmatched.csv"
DEFAULT_CACHE_FILE = OUTPUT_AUX_DIR / "bios_webscrapping_wikipedia_cache.json"
DEFAULT_TENURE_CACHE_FILE = OUTPUT_AUX_DIR / "bios_webscrapping_wikipedia_tenure_cache.json"

HEADERS = {
    "User-Agent": "CentralBanksBoardBiosEnricher/1.0 (research script)"
}
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"

ROLE_HINT_KEYWORDS = {
    "0": ["board", "director", "member", "council"],
    "1": ["deputy", "vice", "vice governor", "vice president", "vice chair"],
    "2": ["governor", "president", "chair", "chairman", "chairwoman", "chairperson"],
}

ECON_KEYWORDS = [
    "central bank",
    "central banker",
    "governor",
    "deputy governor",
    "president",
    "chair",
    "banker",
    "economist",
    "monetary",
    "finance",
    "board member",
]

POSITION_MAPPING = {
    "board member": "0",
    "vice-president / deputy-governor": "1",
    "vice president / deputy governor": "1",
    "deputy governor": "1",
    "president / governor": "2",
    "governor": "2",
    "president": "2",
    "chair": "2",
}

SEX_MAPPING = {
    "male": "0",
    "female": "1",
}

ISO3_TO_COUNTRY = {
    "ABW": "Aruba",
    "AFG": "Afghanistan",
    "AGO": "Angola",
    "ALB": "Albania",
    "ANT": "Netherlands Antilles",
    "AOA": "Angola",
    "ARE": "United Arab Emirates",
    "ARG": "Argentina",
    "ARM": "Armenia",
    "AUS": "Australia",
    "AUT": "Austria",
    "AZE": "Azerbaijan",
    "BCEAO": "Central Bank of West African States",
    "BDI": "Burundi",
    "BEAC": "Bank of Central African States",
    "BEL": "Belgium",
    "BEN": "Benin",
    "BFA": "Burkina Faso",
    "BGD": "Bangladesh",
    "BGR": "Bulgaria",
    "BHR": "Bahrain",
    "BHS": "Bahamas, The",
    "BIH": "Bosnia and Herzegovina",
    "BLR": "Belarus",
    "BLZ": "Belize",
    "BMU": "Bermuda",
    "BOL": "Bolivia",
    "BRA": "Brazil",
    "BRB": "Barbados",
    "BRN": "Brunei",
    "BTN": "Bhutan",
    "BUR": "Myanmar",
    "BWA": "Botswana",
    "CAF": "Central African Republic",
    "CAN": "Canada",
    "CHE": "Switzerland",
    "CHL": "Chile",
    "CHN": "China",
    "CIV": "Cote d'Ivoire",
    "CMR": "Cameroon",
    "COD": "Congo, Democratic Republic of the",
    "COG": "Congo",
    "COL": "Colombia",
    "COM": "Comoros",
    "CPV": "Cape Verde",
    "CRI": "Costa Rica",
    "CUB": "Cuba",
    "CUW": "Curacao",
    "CYM": "Cayman Islands",
    "CYP": "Cyprus",
    "CZE": "Czech Republic",
    "DEU": "Germany",
    "DJI": "Djibouti",
    "DNK": "Denmark",
    "DOM": "Dominican Republic",
    "DZA": "Algeria",
    "ECB": "European Central Bank",
    "ECCB": "Eastern Caribbean Central Bank",
    "ECU": "Ecuador",
    "EGY": "Egypt, Arab Rep.",
    "ESP": "Spain",
    "EST": "Estonia",
    "ETH": "Ethiopia",
    "EUR": "European Union",
    "FIN": "Finland",
    "FJI": "Fiji",
    "FRA": "France",
    "GAB": "Gabon",
    "GBR": "United Kingdom",
    "GEO": "Georgia",
    "GHA": "Ghana",
    "GIN": "Guinea",
    "GMB": "Gambia, The",
    "GNB": "Guinea Bissau",
    "GNQ": "Equatorial Guinea",
    "GRC": "Greece",
    "GRD": "Grenada",
    "GTM": "Guatemala",
    "GUY": "Guyana",
    "HKG": "Hong Kong, China",
    "HND": "Honduras",
    "HRV": "Croatia",
    "HTI": "Haiti",
    "HUN": "Hungary",
    "IDN": "Indonesia",
    "IND": "India",
    "IRL": "Ireland",
    "IRN": "Iran, Islamic Rep.",
    "IRQ": "Iraq",
    "ISL": "Iceland",
    "ISR": "Israel",
    "ITA": "Italy",
    "JAM": "Jamaica",
    "JOR": "Jordan",
    "JPN": "Japan",
    "KAZ": "Kazakhstan",
    "KEN": "Kenya",
    "KGZ": "Kyrgyz Republic",
    "KHM": "Cambodia",
    "KIR": "Kiribati",
    "KNA": "Saint Kitts and Nevis",
    "KOR": "Korea, Rep.",
    "KWT": "Kuwait",
    "LAO": "Lao PDR",
    "LBN": "Lebanon",
    "LBR": "Liberia",
    "LBY": "Libya",
    "LKA": "Sri Lanka",
    "LSO": "Lesotho",
    "LTU": "Lithuania",
    "LUX": "Luxembourg",
    "LVA": "Latvia",
    "MAC": "Macao, China",
    "MAR": "Morocco",
    "MDA": "Moldova",
    "MDG": "Madagascar",
    "MDV": "Maldives",
    "MEX": "Mexico",
    "MKD": "Macedonia, FYR",
    "MLI": "Mali",
    "MLT": "Malta",
    "MMR": "Myanmar",
    "MNE": "Montenegro",
    "MNG": "Mongolia",
    "MOZ": "Mozambique",
    "MRT": "Mauritania",
    "MUS": "Mauritius",
    "MWI": "Malawi",
    "MYS": "Malaysia",
    "NAM": "Namibia",
    "NER": "Niger",
    "NGA": "Nigeria",
    "NIC": "Nicaragua",
    "NLD": "Netherlands, The",
    "NOR": "Norway",
    "NPL": "Nepal",
    "NRU": "Nauru",
    "NZL": "New Zealand",
    "OMN": "Oman",
    "PAK": "Pakistan",
    "PAN": "Panama",
    "PER": "Peru",
    "PHL": "Philippines",
    "PNG": "Papua New Guinea",
    "POL": "Poland",
    "PRT": "Portugal",
    "PRY": "Paraguay",
    "PSE": "Palestine",
    "PYF": "French Polynesia",
    "QAT": "Qatar",
    "ROM": "Romania",
    "ROU": "Romania",
    "RUS": "Russian Federation",
    "RWA": "Rwanda",
    "SAU": "Saudi Arabia",
    "SDN": "Sudan",
    "SEN": "Senegal",
    "SER": "Serbia",
    "SGP": "Singapore",
    "SLB": "Solomon Islands",
    "SLE": "Sierra Leone",
    "SLV": "El Salvador",
    "SMR": "San Marino",
    "SOM": "Somalia",
    "SRB": "Serbia",
    "SSD": "South Sudan",
    "STP": "Sao Tome and Principe",
    "SUR": "Suriname",
    "SVK": "Slovakia",
    "SVN": "Slovenia",
    "SWE": "Sweden",
    "SWZ": "Swaziland/Eswatini",
    "SYC": "Seychelles",
    "SYR": "Syrian Arab Republic",
    "TCD": "Chad",
    "TGO": "Togo",
    "THA": "Thailand",
    "TJK": "Tajikistan",
    "TKM": "Turkmenistan",
    "TLS": "Timor-Leste",
    "TON": "Tonga",
    "TTO": "Trinidad and Tobago",
    "TUN": "Tunisia",
    "TUR": "Turkey",
    "TWN": "Taiwan",
    "TZA": "Tanzania",
    "UGA": "Uganda",
    "UKR": "Ukraine",
    "URY": "Uruguay",
    "USA": "United States",
    "UZB": "Uzbekistan",
    "VCT": "Saint Vincent and the Grenadines",
    "VEN": "Venezuela, RB",
    "VNM": "Vietnam",
    "VUT": "Vanuatu",
    "WSM": "Samoa (Western -)",
    "XKX": "Kosovo",
    "YEM": "Yemen, Rep.",
    "ZAF": "South Africa",
    "ZAR": "Congo, Dem. Rep. (Zaire)",
    "ZMB": "Zambia",
    "ZWE": "Zimbabwe",
}


@dataclass
class CandidateMatch:
    search_name: str
    title: str
    page_url: str
    qid: str
    score: float
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="", help="Path to Bios_WebScrapping.xlsx")
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV), help="CSV output path.")
    parser.add_argument("--output-xlsx", default=str(DEFAULT_OUTPUT_XLSX), help="Excel output path.")
    parser.add_argument("--matches-output", default=str(DEFAULT_MATCHES_OUTPUT), help="Auxiliary matches CSV.")
    parser.add_argument("--unmatched-output", default=str(DEFAULT_UNMATCHED_OUTPUT), help="Auxiliary unmatched CSV.")
    parser.add_argument("--cache-file", default=str(DEFAULT_CACHE_FILE), help="Persistent JSON cache for profile matches.")
    parser.add_argument("--tenure-cache-file", default=str(DEFAULT_TENURE_CACHE_FILE), help="Persistent JSON cache for tenure extraction.")
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N rows.")
    parser.add_argument("--sleep-seconds", type=float, default=0.35, help="Delay between web requests.")
    parser.add_argument("--save-every", type=int, default=50, help="Persist progress every N processed rows.")
    return parser.parse_args()


def resolve_input_path(raw_input: str) -> Path:
    if raw_input:
        path = Path(raw_input).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {path}")
        return path

    for candidate in DEFAULT_INPUT_CANDIDATES:
        if candidate.exists():
            return candidate

    raise FileNotFoundError("No se encontró Bios_WebScrapping.xlsx en las rutas esperadas.")


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def clean_text(value: object) -> str:
    text = normalize_text(value)
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_name(value: object) -> str:
    text = clean_text(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def canonical_position_code(value: object) -> str:
    text = clean_text(value).lower()
    return POSITION_MAPPING.get(text, "")


def country_hint_from_iso3(iso3: object) -> str:
    return ISO3_TO_COUNTRY.get(clean_text(iso3).upper(), "")


def wikipedia_api(session: requests.Session, **params) -> dict:
    return request_json_with_retries(session, WIKIPEDIA_API, {"format": "json", **params})


def wikidata_api(session: requests.Session, **params) -> dict:
    return request_json_with_retries(session, WIKIDATA_API, {"format": "json", **params})


def request_json_with_retries(
    session: requests.Session,
    url: str,
    params: dict,
    retries: int = 5,
    timeout: int = 30,
) -> dict:
    last_error = None
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, headers=HEADERS, timeout=timeout)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    delay = float(retry_after)
                else:
                    delay = 2.0 * (attempt + 1)
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt == retries - 1:
                break
            time.sleep(1.5 * (attempt + 1))
    raise last_error


def query_search(session: requests.Session, query: str) -> list[dict]:
    payload = wikipedia_api(
        session,
        action="query",
        list="search",
        srsearch=query,
        srlimit=5,
    )
    return payload.get("query", {}).get("search", [])


def fetch_pageprops(session: requests.Session, title: str) -> dict:
    payload = wikipedia_api(
        session,
        action="query",
        titles=title,
        prop="pageprops",
        formatversion="2",
    )
    pages = payload.get("query", {}).get("pages", [])
    return pages[0] if pages else {}


def fetch_page_html(session: requests.Session, title: str) -> str:
    payload = wikipedia_api(
        session,
        action="parse",
        page=title,
        prop="text",
    )
    return payload["parse"]["text"]["*"]


def get_claim_value(statement: dict):
    return statement.get("mainsnak", {}).get("datavalue", {}).get("value")


def parse_wikidata_time(raw: str) -> Tuple[str, str]:
    if not raw:
        return "", ""
    match = re.match(r"^[+-]?(\d+)-(\d{2})-\d{2}T", raw)
    if not match:
        return "", ""
    year, month = match.groups()
    year_value = str(int(year))
    month_value = "" if month == "00" else str(int(month))
    return year_value, month_value


def extract_lead_text(soup: BeautifulSoup) -> str:
    content = soup.select_one("div.mw-parser-output")
    if not content:
        return ""
    pieces = []
    for paragraph in content.find_all("p", recursive=False):
        text = clean_text(paragraph.get_text(" ", strip=True))
        if text:
            pieces.append(text)
        if len(pieces) >= 3:
            break
    return " ".join(pieces)


def extract_infobox_fields(soup: BeautifulSoup) -> dict[str, str]:
    infobox = soup.select_one("table.infobox")
    if not infobox:
        return {}

    fields = {}
    for tr in infobox.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        label = normalize_name(th.get_text(" ", strip=True))
        value = clean_text(td.get_text(" ", strip=True))
        if label and value:
            fields[label] = value
    return fields


def score_candidate(
    search_name: str,
    title: str,
    snippet: str,
    lead_text: str,
    infobox_fields: dict[str, str],
    country_hint: str,
    position_code: str,
) -> Tuple[float, str]:
    name_key = normalize_name(search_name)
    title_key = normalize_name(title)
    snippet_key = normalize_name(snippet)
    lead_key = normalize_name(lead_text)
    infobox_text = " ".join(infobox_fields.values())
    infobox_key = normalize_name(infobox_text)
    country_key = normalize_name(country_hint)

    score = 0.0
    reasons = []
    title_tokens = set(title_key.split())
    name_tokens = set(name_key.split())
    shared_tokens = title_tokens & name_tokens
    similarity = SequenceMatcher(None, title_key, name_key).ratio()
    last_name_match = False
    if title_key and name_key:
        last_name_match = title_key.split()[-1] == name_key.split()[-1]

    if similarity < 0.55 and not last_name_match:
        return -100.0, "name_mismatch"

    if title_key == name_key:
        score += 25
        reasons.append("exact_title")
    elif title_key.startswith(name_key) or name_key.startswith(title_key):
        score += 18
        reasons.append("strong_title")
    elif len(shared_tokens) >= 2:
        score += 10
        reasons.append("partial_title")
    elif len(shared_tokens) == 1 and last_name_match:
        score += 6
        reasons.append("shared_last_name")

    score += similarity * 10
    reasons.append(f"name_similarity:{similarity:.2f}")

    joined_text = " ".join([snippet_key, lead_key, infobox_key])
    if any(keyword in joined_text for keyword in ECON_KEYWORDS):
        score += 8
        reasons.append("econ_context")

    if country_key and country_key in joined_text:
        score += 4
        reasons.append("country_hint")

    role_keywords = ROLE_HINT_KEYWORDS.get(position_code, [])
    if any(keyword in joined_text for keyword in role_keywords):
        score += 5
        reasons.append("role_hint")

    if "may refer to" in lead_key or "disambiguation" in title_key:
        score -= 10
        reasons.append("disambiguation_penalty")

    return score, "|".join(reasons)


def build_queries(search_name: str, country_hint: str) -> list[str]:
    queries = [f'"{search_name}"']
    if country_hint:
        queries.append(f'"{search_name}" "{country_hint}"')
    queries.append(f'"{search_name}" "central bank"')
    queries.append(f'"{search_name}" economist')
    return queries


def find_best_candidate(
    session: requests.Session,
    search_name: str,
    country_hint: str,
    position_code: str,
    sleep_seconds: float,
) -> Optional[CandidateMatch]:
    seen_titles = set()
    best: Optional[CandidateMatch] = None

    for query in build_queries(search_name, country_hint):
        results = query_search(session, query)
        time.sleep(sleep_seconds)
        for result in results:
            title = clean_text(result.get("title", ""))
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)

            snippet = clean_text(BeautifulSoup(result.get("snippet", ""), "html.parser").get_text(" ", strip=True))
            try:
                html = fetch_page_html(session, title)
                time.sleep(sleep_seconds)
            except Exception:
                continue

            soup = BeautifulSoup(html, "html.parser")
            lead_text = extract_lead_text(soup)
            infobox_fields = extract_infobox_fields(soup)
            score, reason = score_candidate(
                search_name=search_name,
                title=title,
                snippet=snippet,
                lead_text=lead_text,
                infobox_fields=infobox_fields,
                country_hint=country_hint,
                position_code=position_code,
            )

            if score < 15:
                continue

            pageprops = fetch_pageprops(session, title)
            time.sleep(sleep_seconds)
            qid = pageprops.get("pageprops", {}).get("wikibase_item", "")
            if not qid:
                continue

            match = CandidateMatch(
                search_name=search_name,
                title=title,
                page_url=f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
                qid=qid,
                score=score,
                reason=reason,
            )
            if best is None or match.score > best.score:
                best = match

    return best


def fetch_entity(session: requests.Session, qid: str) -> dict:
    payload = wikidata_api(
        session,
        action="wbgetentities",
        ids=qid,
        props="labels|claims",
        languages="en",
    )
    return payload.get("entities", {}).get(qid, {})


def entity_label(session: requests.Session, qid: str, label_cache: dict[str, str], entity_cache: dict[str, dict]) -> str:
    if not qid:
        return ""
    if qid not in label_cache:
        entity = entity_cache.get(qid)
        if entity is None:
            entity = fetch_entity(session, qid)
            entity_cache[qid] = entity
        label_cache[qid] = entity.get("labels", {}).get("en", {}).get("value", "")
    return label_cache[qid]


def parse_birth_from_infobox_fields(infobox_fields: dict[str, str]) -> Tuple[str, str]:
    born = infobox_fields.get("born", "")
    if not born:
        return "", ""
    match = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+(\d{4})",
        born,
        flags=re.IGNORECASE,
    )
    if match:
        month_map = {
            "january": "1",
            "february": "2",
            "march": "3",
            "april": "4",
            "may": "5",
            "june": "6",
            "july": "7",
            "august": "8",
            "september": "9",
            "october": "10",
            "november": "11",
            "december": "12",
        }
        return match.group(2), month_map[match.group(1).lower()]

    year_match = re.search(r"\b(18|19|20)\d{2}\b", born)
    if year_match:
        return year_match.group(0), ""
    return "", ""


def extract_birth_and_place(
    session: requests.Session,
    entity: dict,
    soup: BeautifulSoup,
    infobox_fields: dict[str, str],
    label_cache: dict[str, str],
    entity_cache: dict[str, dict],
) -> Tuple[str, str, str, str]:
    birth_year, birth_month = "", ""
    country_birth, city_birth = "", ""

    claims = entity.get("claims", {})
    for statement in claims.get("P569", []):
        value = get_claim_value(statement)
        if isinstance(value, dict):
            birth_year, birth_month = parse_wikidata_time(value.get("time", ""))
            if birth_year:
                break

    if not birth_year:
        birth_year, birth_month = parse_birth_from_infobox_fields(infobox_fields)

    for statement in claims.get("P19", []):
        value = get_claim_value(statement)
        if not isinstance(value, dict) or not value.get("id"):
            continue
        city_qid = value["id"]
        city_birth = entity_label(session, city_qid, label_cache, entity_cache)
        country_birth = place_country(session, city_qid, label_cache, entity_cache)
        if city_birth or country_birth:
            break

    if not country_birth:
        born = infobox_fields.get("born", "")
        pieces = [piece.strip() for piece in born.split(",") if piece.strip()]
        if len(pieces) >= 2:
            city_birth = city_birth or pieces[-2]
            country_birth = country_birth or pieces[-1]

    return birth_year, birth_month, country_birth, city_birth


def place_country(
    session: requests.Session,
    qid: str,
    label_cache: dict[str, str],
    entity_cache: dict[str, dict],
    depth: int = 0,
) -> str:
    if not qid or depth > 4:
        return ""

    entity = entity_cache.get(qid)
    if entity is None:
        entity = fetch_entity(session, qid)
        entity_cache[qid] = entity

    claims = entity.get("claims", {})
    for prop in ("P17", "P495"):
        for statement in claims.get(prop, []):
            value = get_claim_value(statement)
            if isinstance(value, dict) and value.get("id"):
                return entity_label(session, value["id"], label_cache, entity_cache)

    for statement in claims.get("P131", []):
        value = get_claim_value(statement)
        if isinstance(value, dict) and value.get("id"):
            country = place_country(session, value["id"], label_cache, entity_cache, depth + 1)
            if country:
                return country
    return ""


def extract_sex(entity: dict) -> str:
    for statement in entity.get("claims", {}).get("P21", []):
        value = get_claim_value(statement)
        if not isinstance(value, dict) or not value.get("id"):
            continue
        if value["id"] == "Q6581097":
            return "0"
        if value["id"] == "Q6581072":
            return "1"
    return ""


def degree_bucket(label: str) -> Tuple[str, str]:
    lowered = normalize_name(label)
    if any(token in lowered for token in ["doctor of philosophy", "phd", "dphil", "doctorate", "dba"]):
        return "phd", "6"
    if "mba" in lowered or "master of business administration" in lowered:
        return "mba", "4"
    if any(token in lowered for token in ["master", "ma ", "m a", "mpa", "llb", "jd", "juris doctor", "law degree", "licentiate in law"]):
        return "ba_or_ma", "3"
    if any(token in lowered for token in ["bachelor", "ba ", "b a", "bs", "bsc", "ab "]):
        return "ba_or_ma", "2"
    return "", ""


def split_institutions(text: str) -> list[str]:
    text = clean_text(text)
    if not text:
        return []
    pieces = re.split(r";| / | and |, (?=[A-Z])", text)
    return [clean_text(piece) for piece in pieces if clean_text(piece)]


def join_sorted(values: Iterable[str]) -> str:
    cleaned = sorted({clean_text(value) for value in values if clean_text(value)})
    return "; ".join(cleaned)


def extract_education(
    session: requests.Session,
    entity: dict,
    infobox_fields: dict[str, str],
    label_cache: dict[str, str],
    entity_cache: dict[str, dict],
) -> Tuple[str, str, str, str]:
    ba_or_ma = set()
    mba = set()
    phd = set()
    degree_levels = set()
    had_education_without_degree = False

    for statement in entity.get("claims", {}).get("P69", []):
        value = get_claim_value(statement)
        if not isinstance(value, dict) or not value.get("id"):
            continue
        institution = entity_label(session, value["id"], label_cache, entity_cache)
        qualifiers = statement.get("qualifiers", {})
        matched_degree = False
        for qualifier in qualifiers.get("P512", []):
            degree_value = qualifier.get("datavalue", {}).get("value", {})
            if not isinstance(degree_value, dict) or not degree_value.get("id"):
                continue
            degree_label = entity_label(session, degree_value["id"], label_cache, entity_cache)
            bucket, level = degree_bucket(degree_label)
            if bucket == "ba_or_ma":
                ba_or_ma.add(institution)
                degree_levels.add(level)
                matched_degree = True
            elif bucket == "mba":
                mba.add(institution)
                degree_levels.add(level)
                matched_degree = True
            elif bucket == "phd":
                phd.add(institution)
                degree_levels.add(level)
                matched_degree = True
        if institution and not matched_degree:
            had_education_without_degree = True

    if not (ba_or_ma or mba or phd):
        fallback_text = infobox_fields.get("alma mater") or infobox_fields.get("education") or ""
        fallback_institutions = split_institutions(fallback_text)
        if fallback_institutions:
            ba_or_ma.update(fallback_institutions)
            if "phd" in normalize_name(fallback_text) or "doctor" in normalize_name(fallback_text):
                phd.update(fallback_institutions)
                ba_or_ma.clear()
                degree_levels.add("6")
            elif "mba" in normalize_name(fallback_text):
                mba.update(fallback_institutions)
                ba_or_ma.clear()
                degree_levels.add("4")
            elif any(token in normalize_name(fallback_text) for token in ["master", "law", "ma", "mpa", "jd", "llb"]):
                degree_levels.add("3")
            elif any(token in normalize_name(fallback_text) for token in ["bachelor", "ba", "bs", "bsc"]):
                degree_levels.add("2")
            else:
                had_education_without_degree = True

    education_code = ""
    if "6" in degree_levels or phd:
        education_code = "6"
    elif "4" in degree_levels or mba:
        education_code = "4"
    elif "3" in degree_levels:
        education_code = "3"
    elif "2" in degree_levels or ba_or_ma:
        education_code = "2"
    elif had_education_without_degree:
        education_code = "1"

    return (
        education_code,
        join_sorted(ba_or_ma),
        join_sorted(mba),
        join_sorted(phd),
    )


def role_score(position_code: str, office_label: str) -> int:
    lowered = normalize_name(office_label)
    score = 0
    for keyword in ROLE_HINT_KEYWORDS.get(position_code, []):
        if keyword in lowered:
            score += 5
    if "central bank" in lowered or "bank" in lowered or "monetary authority" in lowered:
        score += 2
    return score


def match_tenure(
    session: requests.Session,
    entity: dict,
    country_hint: str,
    position_code: str,
    label_cache: dict[str, str],
    entity_cache: dict[str, dict],
) -> Tuple[str, str, str, str]:
    best_score = -1
    best_tenure = ("", "", "", "")
    country_key = normalize_name(country_hint)

    for statement in entity.get("claims", {}).get("P39", []):
        value = get_claim_value(statement)
        if not isinstance(value, dict) or not value.get("id"):
            continue
        office_label = entity_label(session, value["id"], label_cache, entity_cache)
        if not office_label:
            continue

        score = role_score(position_code, office_label)
        office_key = normalize_name(office_label)
        if country_key and country_key in office_key:
            score += 4
        if score <= 0:
            continue

        qualifiers = statement.get("qualifiers", {})
        start_raw = qualifiers.get("P580", [{}])[0].get("datavalue", {}).get("value", {}).get("time", "")
        end_raw = qualifiers.get("P582", [{}])[0].get("datavalue", {}).get("value", {}).get("time", "")
        start_year, start_month = parse_wikidata_time(start_raw)
        end_year, end_month = parse_wikidata_time(end_raw)

        if score > best_score:
            best_score = score
            best_tenure = (start_year, start_month, end_year, end_month)

    return best_tenure


def load_json_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json_cache(path: Path, payload: dict) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def choose_search_name(row: pd.Series) -> str:
    pname = clean_text(row.get("PName", ""))
    pname_original = clean_text(row.get("PName_original", ""))
    return pname or pname_original


def profile_from_match(
    session: requests.Session,
    match: CandidateMatch,
    label_cache: dict[str, str],
    entity_cache: dict[str, dict],
    sleep_seconds: float,
) -> dict:
    html = fetch_page_html(session, match.title)
    time.sleep(sleep_seconds)
    soup = BeautifulSoup(html, "html.parser")
    infobox_fields = extract_infobox_fields(soup)
    entity = fetch_entity(session, match.qid)
    time.sleep(sleep_seconds)

    birth_year, birth_month, country_birth, city_birth = extract_birth_and_place(
        session=session,
        entity=entity,
        soup=soup,
        infobox_fields=infobox_fields,
        label_cache=label_cache,
        entity_cache=entity_cache,
    )
    sex = extract_sex(entity)
    education_code, ba_or_ma, mba, phd = extract_education(
        session=session,
        entity=entity,
        infobox_fields=infobox_fields,
        label_cache=label_cache,
        entity_cache=entity_cache,
    )

    return {
        "Wikipedia_title": match.title,
        "Wikipedia_person_url": match.page_url,
        "Wikipedia_qid": match.qid,
        "Wikipedia_match_score": str(match.score),
        "Wikipedia_match_reason": match.reason,
        "Birth_year_enriched": birth_year,
        "Birth_month_enriched": birth_month,
        "Sex_enriched": sex,
        "Education_enriched": education_code,
        "BA_or_MA_enriched": ba_or_ma,
        "MBA_enriched": mba,
        "PhD_enriched": phd,
        "CountryBirth_enriched": country_birth,
        "CityBirth_enriched": city_birth,
    }


def enrich_row_from_profile(
    row: pd.Series,
    profile: dict,
    tenure: Tuple[str, str, str, str],
) -> dict:
    enriched = row.to_dict()

    enriched["Position_original"] = clean_text(row.get("Position", ""))
    enriched["Position"] = canonical_position_code(row.get("Position", ""))

    if not normalize_text(row.get("Sex", "")):
        enriched["Sex"] = profile.get("Sex_enriched", "")

    for output_col, profile_col in [
        ("Birth_year", "Birth_year_enriched"),
        ("Birth_month", "Birth_month_enriched"),
        ("CountryBirth", "CountryBirth_enriched"),
        ("CityBirth", "CityBirth_enriched"),
        ("Education", "Education_enriched"),
        ("BA_or_MA", "BA_or_MA_enriched"),
        ("MBA", "MBA_enriched"),
        ("PhD", "PhD_enriched"),
    ]:
        if not normalize_text(row.get(output_col, "")):
            enriched[output_col] = profile.get(profile_col, "")

    start_year, start_month, end_year, end_month = tenure
    if not normalize_text(row.get("Start_year", "")):
        enriched["Start_year"] = start_year
    if not normalize_text(row.get("Start_month", "")):
        enriched["Start_month"] = start_month
    if not normalize_text(row.get("End_year", "")):
        enriched["End_year"] = end_year
    if not normalize_text(row.get("End_month", "")):
        enriched["End_month"] = end_month

    enriched["Wikipedia_title"] = profile.get("Wikipedia_title", "")
    enriched["Wikipedia_person_url"] = profile.get("Wikipedia_person_url", "")
    enriched["Wikipedia_qid"] = profile.get("Wikipedia_qid", "")
    enriched["Wikipedia_match_score"] = profile.get("Wikipedia_match_score", "")
    enriched["Wikipedia_match_reason"] = profile.get("Wikipedia_match_reason", "")
    enriched["Wikipedia_match_status"] = "matched" if profile.get("Wikipedia_qid") else "unmatched"

    return enriched


def save_progress(
    output_csv: Path,
    output_xlsx: Path,
    matches_output: Path,
    unmatched_output: Path,
    enriched_rows: list[dict],
    matches_rows: list[dict],
    unmatched_rows: list[dict],
) -> None:
    ensure_parent(output_csv)
    ensure_parent(output_xlsx)
    ensure_parent(matches_output)
    ensure_parent(unmatched_output)

    enriched_df = pd.DataFrame(enriched_rows)
    matches_df = pd.DataFrame(matches_rows)
    unmatched_df = pd.DataFrame(unmatched_rows)

    enriched_df.to_csv(output_csv, index=False, sep=";")
    enriched_df.to_excel(output_xlsx, index=False)
    matches_df.to_csv(matches_output, index=False, sep=";")
    unmatched_df.to_csv(unmatched_output, index=False, sep=";")
    print(
        f"Saved progress: rows={len(enriched_rows)} matches={len(matches_rows)} unmatched={len(unmatched_rows)}",
        flush=True,
    )


def main() -> None:
    args = parse_args()
    input_path = resolve_input_path(args.input)
    output_csv = Path(args.output_csv).expanduser().resolve()
    output_xlsx = Path(args.output_xlsx).expanduser().resolve()
    matches_output = Path(args.matches_output).expanduser().resolve()
    unmatched_output = Path(args.unmatched_output).expanduser().resolve()
    cache_file = Path(args.cache_file).expanduser().resolve()
    tenure_cache_file = Path(args.tenure_cache_file).expanduser().resolve()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_AUX_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(input_path)
    if args.limit and args.limit > 0:
        df = df.head(args.limit).copy()

    print(f"Input file          : {input_path}", flush=True)
    print(f"Total rows to parse : {len(df)}", flush=True)
    print(f"Output CSV          : {output_csv}", flush=True)
    print(f"Output XLSX         : {output_xlsx}", flush=True)
    print(f"Cache file          : {cache_file}", flush=True)
    print(f"Tenure cache file   : {tenure_cache_file}", flush=True)
    print("-" * 60, flush=True)

    cache = load_json_cache(cache_file)
    tenure_cache = load_json_cache(tenure_cache_file)
    label_cache: dict[str, str] = {}
    entity_cache: dict[str, dict] = {}

    session = requests.Session()
    session.headers.update(HEADERS)

    enriched_rows = []
    matches_rows = []
    unmatched_rows = []

    total = len(df)
    for index, row in enumerate(df.to_dict(orient="records"), start=1):
        row_series = pd.Series(row)
        search_name = choose_search_name(row_series)
        position_code = canonical_position_code(row_series.get("Position", ""))
        country_hint = country_hint_from_iso3(row_series.get("iso3", ""))

        print(
            f"[{index}/{total}] Processing name='{search_name}' iso3='{clean_text(row_series.get('iso3', ''))}' position='{clean_text(row_series.get('Position', ''))}'",
            flush=True,
        )

        if not search_name:
            enriched = row.copy()
            enriched["Position_original"] = clean_text(row_series.get("Position", ""))
            enriched["Position"] = position_code
            enriched["Wikipedia_match_status"] = "blank_name"
            enriched_rows.append(enriched)
            unmatched_rows.append(
                {
                    "search_name": "",
                    "iso3": clean_text(row_series.get("iso3", "")),
                    "position_original": clean_text(row_series.get("Position", "")),
                    "reason": "blank_name",
                }
            )
            print(f"[{index}/{total}] Skipped because name is blank", flush=True)
            continue

        cache_key = normalize_name(search_name)
        profile = cache.get(cache_key)

        if profile is None:
            print(f"[{index}/{total}] Cache miss, searching Wikipedia", flush=True)
            try:
                match = find_best_candidate(
                    session=session,
                    search_name=search_name,
                    country_hint=country_hint,
                    position_code=position_code,
                    sleep_seconds=args.sleep_seconds,
                )
            except Exception as exc:
                match = None
                unmatched_rows.append(
                    {
                        "search_name": search_name,
                        "iso3": clean_text(row_series.get("iso3", "")),
                        "position_original": clean_text(row_series.get("Position", "")),
                        "reason": f"search_error:{type(exc).__name__}",
                    }
                )

            if match is None:
                print(f"[{index}/{total}] No reliable Wikipedia match found", flush=True)
                profile = {
                    "Wikipedia_title": "",
                    "Wikipedia_person_url": "",
                    "Wikipedia_qid": "",
                    "Wikipedia_match_score": "",
                    "Wikipedia_match_reason": "",
                }
                cache[cache_key] = profile
            else:
                print(
                    f"[{index}/{total}] Matched title='{match.title}' qid='{match.qid}' score={match.score}",
                    flush=True,
                )
                profile = profile_from_match(
                    session=session,
                    match=match,
                    label_cache=label_cache,
                    entity_cache=entity_cache,
                    sleep_seconds=args.sleep_seconds,
                )
                cache[cache_key] = profile

            save_json_cache(cache_file, cache)
        else:
            print(
                f"[{index}/{total}] Cache hit qid='{profile.get('Wikipedia_qid', '')}' title='{profile.get('Wikipedia_title', '')}'",
                flush=True,
            )

        qid = profile.get("Wikipedia_qid", "")
        tenure_key = f"{qid}|{country_hint}|{position_code}"
        if qid and tenure_key not in tenure_cache:
            print(f"[{index}/{total}] Resolving tenure from Wikidata", flush=True)
            try:
                entity = entity_cache.get(qid)
                if entity is None:
                    entity = fetch_entity(session, qid)
                    entity_cache[qid] = entity
                    time.sleep(args.sleep_seconds)
                tenure_cache[tenure_key] = {
                    "Start_year": "",
                    "Start_month": "",
                    "End_year": "",
                    "End_month": "",
                }
                start_year, start_month, end_year, end_month = match_tenure(
                    session=session,
                    entity=entity,
                    country_hint=country_hint,
                    position_code=position_code,
                    label_cache=label_cache,
                    entity_cache=entity_cache,
                )
                tenure_cache[tenure_key] = {
                    "Start_year": start_year,
                    "Start_month": start_month,
                    "End_year": end_year,
                    "End_month": end_month,
                }
                save_json_cache(tenure_cache_file, tenure_cache)
            except Exception:
                tenure_cache[tenure_key] = {
                    "Start_year": "",
                    "Start_month": "",
                    "End_year": "",
                    "End_month": "",
                }
                save_json_cache(tenure_cache_file, tenure_cache)
        elif qid:
            print(f"[{index}/{total}] Tenure cache hit", flush=True)

        tenure_payload = tenure_cache.get(
            tenure_key,
            {"Start_year": "", "Start_month": "", "End_year": "", "End_month": ""},
        )
        tenure = (
            tenure_payload.get("Start_year", ""),
            tenure_payload.get("Start_month", ""),
            tenure_payload.get("End_year", ""),
            tenure_payload.get("End_month", ""),
        )

        enriched = enrich_row_from_profile(row_series, profile, tenure)
        enriched_rows.append(enriched)

        if qid:
            matches_rows.append(
                {
                    "search_name": search_name,
                    "iso3": clean_text(row_series.get("iso3", "")),
                    "position_original": clean_text(row_series.get("Position_original", row_series.get("Position", ""))),
                    "wikipedia_title": profile.get("Wikipedia_title", ""),
                    "wikipedia_person_url": profile.get("Wikipedia_person_url", ""),
                    "wikipedia_qid": qid,
                    "match_score": profile.get("Wikipedia_match_score", ""),
                    "match_reason": profile.get("Wikipedia_match_reason", ""),
                }
            )
        else:
            unmatched_rows.append(
                {
                    "search_name": search_name,
                    "iso3": clean_text(row_series.get("iso3", "")),
                    "position_original": clean_text(row_series.get("Position", "")),
                    "reason": "no_reliable_match",
                }
            )
            print(f"[{index}/{total}] Stored as unmatched", flush=True)

        if index % args.save_every == 0 or index == total:
            save_progress(
                output_csv=output_csv,
                output_xlsx=output_xlsx,
                matches_output=matches_output,
                unmatched_output=unmatched_output,
                enriched_rows=enriched_rows,
                matches_rows=matches_rows,
                unmatched_rows=unmatched_rows,
            )
            print(f"Processed {index}/{total}", flush=True)

    print(f"Saved enriched CSV : {output_csv}")
    print(f"Saved enriched XLSX: {output_xlsx}")
    print(f"Saved matches CSV  : {matches_output}")
    print(f"Saved unmatched CSV: {unmatched_output}")


if __name__ == "__main__":
    main()
