#!/usr/bin/env python3
import argparse
import csv
import json
import re
import socket
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup


BASE_WIKI = "https://en.wikipedia.org"
USER_AGENT = "Mozilla/5.0 (compatible; CodexCentralBankScraper/1.0)"
LIST_URL = f"{BASE_WIKI}/wiki/List_of_central_banks"
OUTPUT_CSV = "central_bank_governors_wikipedia.csv"

HEADERS = {"User-Agent": USER_AGENT}
STOPWORDS = {
    "the",
    "of",
    "and",
    "for",
    "de",
    "del",
    "la",
    "bank",
    "central",
    "national",
    "reserve",
    "federal",
    "republic",
    "state",
}


@dataclass
class BankRecord:
    country: str
    bank_name: str
    bank_url: str


def fetch_bytes(url: str, retries: int = 4, timeout: int = 45) -> bytes:
    last_error = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return response.read()
        except HTTPError as exc:
            last_error = exc
            if exc.code == 429 and attempt < retries - 1:
                retry_after = exc.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else 5.0 * (attempt + 1)
                time.sleep(delay)
                continue
            if attempt == retries - 1:
                break
            time.sleep(1.5 * (attempt + 1))
        except (URLError, TimeoutError, socket.timeout) as exc:
            last_error = exc
            if attempt == retries - 1:
                break
            time.sleep(1.5 * (attempt + 1))
    raise last_error


def fetch_json(url: str) -> dict:
    return json.loads(fetch_bytes(url).decode("utf-8"))


def soup_from_url(url: str) -> BeautifulSoup:
    return BeautifulSoup(fetch_bytes(url), "html.parser")


def wikipedia_api(params: Dict[str, str]) -> dict:
    base = "https://en.wikipedia.org/w/api.php"
    query = urllib.parse.urlencode(params)
    return fetch_json(f"{base}?{query}")


def wikidata_api(params: Dict[str, str]) -> dict:
    base = "https://www.wikidata.org/w/api.php"
    query = urllib.parse.urlencode(params)
    return fetch_json(f"{base}?{query}")


def clean_text(text: str) -> str:
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text or "").strip()
    return text


def normalize(text: str) -> str:
    text = clean_text(text).lower()
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def slug_from_url(url: str) -> str:
    return urllib.parse.unquote(url.split("/wiki/", 1)[1])


def parse_wikidata_time(raw: str) -> Tuple[Optional[str], Optional[str]]:
    if not raw:
        return None, None
    match = re.match(r"^[+-]?(\d+)-(\d{2})-\d{2}T", raw)
    if not match:
        return None, None
    year, month = match.groups()
    month_value = None if month == "00" else str(int(month))
    return str(int(year)), month_value


def get_claim_value(statement: dict):
    return statement.get("mainsnak", {}).get("datavalue", {}).get("value")


class Scraper:
    def __init__(self):
        self.pageprops_cache: Dict[str, dict] = {}
        self.wikidata_entity_cache: Dict[str, dict] = {}
        self.html_cache: Dict[str, BeautifulSoup] = {}

    def get_pageprops(self, title: str) -> dict:
        if title not in self.pageprops_cache:
            data = wikipedia_api(
                {
                    "action": "query",
                    "titles": title,
                    "prop": "pageprops",
                    "format": "json",
                    "formatversion": "2",
                }
            )
            self.pageprops_cache[title] = data["query"]["pages"][0]
        return self.pageprops_cache[title]

    def get_entity(self, qid: str) -> dict:
        if qid not in self.wikidata_entity_cache:
            data = wikidata_api(
                {
                    "action": "wbgetentities",
                    "ids": qid,
                    "props": "labels|claims",
                    "languages": "en",
                    "format": "json",
                }
            )
            self.wikidata_entity_cache[qid] = data["entities"].get(qid, {})
        return self.wikidata_entity_cache[qid]

    def get_soup(self, url: str) -> BeautifulSoup:
        if url not in self.html_cache:
            self.html_cache[url] = soup_from_url(url)
        return self.html_cache[url]

    def entity_label(self, qid: str) -> str:
        entity = self.get_entity(qid)
        return entity.get("labels", {}).get("en", {}).get("value", "")

    def list_banks(self) -> List[BankRecord]:
        soup = self.get_soup(LIST_URL)
        table = soup.select_one("table.wikitable")
        if not table:
            raise RuntimeError("Could not find the main central bank table on Wikipedia.")

        banks: List[BankRecord] = []
        for row in table.select("tr")[1:]:
            cols = row.find_all(["th", "td"])
            if len(cols) < 3:
                continue

            country = clean_text(cols[0].get_text(" ", strip=True))
            bank_cell_text = clean_text(cols[2].get_text(" ", strip=True))
            if normalize(bank_cell_text).startswith("no central bank"):
                bank_name = bank_cell_text
                bank_url = ""
                banks.append(BankRecord(country=country, bank_name=bank_name, bank_url=bank_url))
                continue

            bank_link = None
            for candidate in cols[2].find_all("a", href=re.compile(r"^/wiki/")):
                link_text = normalize(candidate.get_text(" ", strip=True))
                if any(token in link_text.split() for token in ("bank", "authority", "reserve", "monetary")):
                    bank_link = candidate
                    break
            if not bank_link:
                bank_link = cols[2].find("a", href=re.compile(r"^/wiki/"))

            bank_name = clean_text(bank_link.get_text(" ", strip=True)) if bank_link else bank_cell_text
            bank_url = urllib.parse.urljoin(BASE_WIKI, bank_link["href"]) if bank_link else ""
            banks.append(BankRecord(country=country, bank_name=bank_name, bank_url=bank_url))

        return banks

    def extract_governor_from_bank_page(self, bank: BankRecord) -> Tuple[str, str, str]:
        if not bank.bank_url:
            return "", "", ""

        soup = self.get_soup(bank.bank_url)
        infobox = soup.select_one("table.infobox")
        if not infobox:
            return "", "", ""

        accepted_labels = {
            "governor",
            "president",
            "chairperson",
            "chairman",
            "chair",
            "administrator",
            "chief executive",
            "chief executive officer",
        }

        for tr in infobox.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue

            label = normalize(th.get_text(" ", strip=True))
            if label not in accepted_labels:
                continue

            person_link = None
            for candidate in td.find_all("a", href=re.compile(r"^/wiki/")):
                href = candidate.get("href", "")
                if ":" in href:
                    continue
                name = clean_text(candidate.get_text(" ", strip=True))
                if not name or normalize(name) == normalize(bank.bank_name):
                    continue
                person_link = candidate
                break

            value_text = clean_text(td.get_text(" ", strip=True))
            if person_link:
                person_name = clean_text(person_link.get_text(" ", strip=True))
                person_url = urllib.parse.urljoin(BASE_WIKI, person_link["href"])
                return person_name, person_url, value_text

            plain_name = re.split(r"\(|,| since ", value_text, maxsplit=1)[0].strip()
            if plain_name and len(plain_name.split()) >= 2:
                return plain_name, "", value_text

        return "", "", ""

    def resolve_person_qid(self, person_url: str) -> Optional[str]:
        if not person_url:
            return None
        title = slug_from_url(person_url)
        page = self.get_pageprops(title)
        return page.get("pageprops", {}).get("wikibase_item")

    def parse_infobox_person_fields(self, person_url: str) -> Dict[str, str]:
        if not person_url:
            return {}

        soup = self.get_soup(person_url)
        infobox = soup.select_one("table.infobox")
        if not infobox:
            return {}

        fields: Dict[str, str] = {}
        for tr in infobox.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = normalize(th.get_text(" ", strip=True))
            value = clean_text(td.get_text(" ", strip=True))
            fields[label] = value
        return fields

    def parse_birth_from_infobox(self, person_url: str) -> Tuple[str, str, str, str]:
        if not person_url:
            return "", "", "", ""

        soup = self.get_soup(person_url)
        infobox = soup.select_one("table.infobox")
        if not infobox:
            return "", "", "", ""

        for tr in infobox.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            if normalize(th.get_text(" ", strip=True)) != "born":
                continue

            birth_year = ""
            birth_month = ""
            bday = td.find("span", class_="bday")
            if bday:
                year, month = parse_wikidata_time(f"+{bday.get_text(strip=True)}T00:00:00Z")
                birth_year = year or ""
                birth_month = month or ""
            else:
                text = clean_text(td.get_text(" ", strip=True))
                date_match = re.search(
                    r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+(\d{4})",
                    text,
                    flags=re.IGNORECASE,
                )
                if date_match:
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
                    birth_month = month_map[date_match.group(1).lower()]
                    birth_year = date_match.group(2)
                else:
                    year_match = re.search(r"\b(19|20)\d{2}\b", text)
                    if year_match:
                        birth_year = year_match.group(0)

            place_links = []
            for link in td.find_all("a", href=re.compile(r"^/wiki/")):
                name = clean_text(link.get_text(" ", strip=True))
                href = link.get("href", "")
                if not name or ":" in href:
                    continue
                if re.fullmatch(r"\d+", name):
                    continue
                place_links.append(name)

            city = ""
            country = ""
            if place_links:
                country = place_links[-1]
                city = place_links[-2] if len(place_links) >= 2 else place_links[-1]

            return birth_year, birth_month, country, city

        return "", "", "", ""

    def degree_labels_from_text(self, text: str) -> List[str]:
        if not text:
            return []
        labels = []
        patterns = (
            r"\bMBA\b",
            r"\bPhD\b",
            r"\bDPhil\b",
            r"\bDBA\b",
            r"\bMA\b",
            r"\bMS\b",
            r"\bMSc\b",
            r"\bBA\b",
            r"\bBS\b",
            r"\bBSc\b",
        )
        for pattern in patterns:
            labels.extend(re.findall(pattern, text, flags=re.IGNORECASE))
        return labels

    def education_flags(self, degree_labels: List[str]) -> Tuple[str, str, str]:
        degree_text = " | ".join(normalize(label) for label in degree_labels if label)

        ba_or_ma = ""
        mba = ""
        phd = ""

        if re.search(r"\b(ba|ab|bs|bsc|b a|m a|ma|ms|msc|master of arts|master of science|bachelor of arts|bachelor of science)\b", degree_text):
            ba_or_ma = "1"
        if re.search(r"\bmba\b|master of business administration", degree_text):
            mba = "1"
        if re.search(r"\bphd\b|\bdphil\b|doctor of philosophy|doctorate|\bedd\b|\bdba\b", degree_text):
            phd = "1"

        return ba_or_ma, mba, phd

    def place_country(self, qid: str, depth: int = 0) -> str:
        if not qid or depth > 4:
            return ""
        entity = self.get_entity(qid)
        claims = entity.get("claims", {})

        for prop in ("P17", "P495"):
            for statement in claims.get(prop, []):
                value = get_claim_value(statement)
                if isinstance(value, dict) and value.get("id"):
                    return self.entity_label(value["id"])

        for statement in claims.get("P131", []):
            value = get_claim_value(statement)
            if isinstance(value, dict) and value.get("id"):
                country = self.place_country(value["id"], depth + 1)
                if country:
                    return country

        return ""

    def office_match_score(self, office_label: str, bank: BankRecord) -> int:
        office_norm = normalize(office_label)
        bank_norm = normalize(bank.bank_name)
        country_norm = normalize(bank.country)

        score = 0
        if bank_norm and (bank_norm in office_norm or office_norm in bank_norm):
            score += 10

        office_tokens = set(office_norm.split())
        bank_tokens = {token for token in bank_norm.split() if token not in STOPWORDS}
        shared = bank_tokens & office_tokens
        score += len(shared) * 2

        if country_norm and country_norm in office_norm:
            score += 3
        if any(word in office_norm for word in ("governor", "president", "chair", "head")):
            score += 2
        if "bank" in office_norm:
            score += 1
        return score

    def tenure_from_person_entity(self, qid: str, bank: BankRecord) -> Tuple[str, str, str, str]:
        if not qid:
            return "", "", "", ""

        entity = self.get_entity(qid)
        claims = entity.get("claims", {})
        statements = claims.get("P39", [])
        if not statements:
            return "", "", "", ""

        scored = []
        for statement in statements:
            value = get_claim_value(statement)
            if not isinstance(value, dict) or not value.get("id"):
                continue
            office_qid = value["id"]
            office_label = self.entity_label(office_qid)
            score = self.office_match_score(office_label, bank)
            if score <= 0:
                continue
            scored.append((score, office_label, statement))

        if not scored:
            return "", "", "", ""

        scored.sort(key=lambda item: (item[0], json.dumps(item[2].get("qualifiers", {}))), reverse=True)
        statement = scored[0][2]
        qualifiers = statement.get("qualifiers", {})
        start = qualifiers.get("P580", [{}])[0].get("datavalue", {}).get("value", {}).get("time")
        end = qualifiers.get("P582", [{}])[0].get("datavalue", {}).get("value", {}).get("time")
        start_year, start_month = parse_wikidata_time(start)
        end_year, end_month = parse_wikidata_time(end)
        return start_year or "", start_month or "", end_year or "", end_month or ""

    def fallback_start_from_bank_text(self, raw_value: str) -> Tuple[str, str]:
        if not raw_value:
            return "", ""

        match = re.search(
            r"since\s+(?:(\d{1,2})\s+)?([A-Za-z]+)\s+(\d{4})",
            raw_value,
            flags=re.IGNORECASE,
        )
        if match:
            month_name = match.group(2)
            year = match.group(3)
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
            return year, month_map.get(month_name.lower(), "")

        match = re.search(r"since\s+(\d{4})", raw_value, flags=re.IGNORECASE)
        if match:
            return match.group(1), ""

        return "", ""

    def person_row(self, bank: BankRecord) -> Dict[str, str]:
        person_name, person_url, raw_bank_role = self.extract_governor_from_bank_page(bank)
        row = {
            "Country": bank.country,
            "Central_bank_name": bank.bank_name,
            "Central_bank_Wikipedia": bank.bank_url,
            "Person_name": person_name,
            "Person_Wikipedia": person_url,
            "Birth_year": "",
            "Birth_month": "",
            "Start_year": "",
            "Start_month": "",
            "End_year": "",
            "End_month": "",
            "Education": "",
            "BA_or_MA": "",
            "MBA": "",
            "PhD": "",
            "CountryBirth": "",
            "CityBirth": "",
        }

        if not person_name:
            return row

        infobox_fields = self.parse_infobox_person_fields(person_url)
        qid = None

        degree_labels: List[str] = []
        education_labels: List[str] = []

        birth_year, birth_month, country_birth, city_birth = self.parse_birth_from_infobox(person_url)
        row["Birth_year"] = birth_year
        row["Birth_month"] = birth_month
        row["CountryBirth"] = country_birth
        row["CityBirth"] = city_birth

        if not education_labels:
            fallback_education = infobox_fields.get("education") or infobox_fields.get("alma mater")
            if fallback_education:
                education_labels = [piece.strip() for piece in re.split(r";|, ", fallback_education) if piece.strip()]
                degree_labels.extend(self.degree_labels_from_text(fallback_education))

        if person_url:
            try:
                qid = self.resolve_person_qid(person_url)
            except Exception:
                qid = None

        if qid:
            try:
                entity = self.get_entity(qid)
                claims = entity.get("claims", {})

                if not row["Birth_year"]:
                    for statement in claims.get("P569", []):
                        time_value = get_claim_value(statement)
                        if isinstance(time_value, dict):
                            year, month = parse_wikidata_time(time_value.get("time"))
                            row["Birth_year"] = year or ""
                            row["Birth_month"] = month or ""
                            break

                if not row["CityBirth"] or not row["CountryBirth"]:
                    for statement in claims.get("P19", []):
                        place_value = get_claim_value(statement)
                        if isinstance(place_value, dict) and place_value.get("id"):
                            row["CityBirth"] = row["CityBirth"] or self.entity_label(place_value["id"])
                            row["CountryBirth"] = row["CountryBirth"] or self.place_country(place_value["id"])
                            break

                for prop in ("P69",):
                    for statement in claims.get(prop, []):
                        edu_value = get_claim_value(statement)
                        if isinstance(edu_value, dict) and edu_value.get("id"):
                            label = self.entity_label(edu_value["id"])
                            if label:
                                education_labels.append(label)

                for prop in ("P512",):
                    for statement in claims.get(prop, []):
                        degree_value = get_claim_value(statement)
                        if isinstance(degree_value, dict) and degree_value.get("id"):
                            label = self.entity_label(degree_value["id"])
                            if label:
                                degree_labels.append(label)

                start_year, start_month, end_year, end_month = self.tenure_from_person_entity(qid, bank)
                row["Start_year"] = start_year
                row["Start_month"] = start_month
                row["End_year"] = end_year
                row["End_month"] = end_month
            except Exception:
                pass

        if not degree_labels:
            inferred_degree_fields = " | ".join(
                value
                for key, value in infobox_fields.items()
                if key in {"education", "alma mater"}
            )
            if inferred_degree_fields:
                for pattern in (
                    r"\bMBA\b",
                    r"\bPhD\b",
                    r"\bDPhil\b",
                    r"\bMA\b",
                    r"\bMS\b",
                    r"\bMSc\b",
                    r"\bBA\b",
                    r"\bBS\b",
                    r"\bBSc\b",
                ):
                    degree_labels.extend(re.findall(pattern, inferred_degree_fields, flags=re.IGNORECASE))

        education_labels = sorted({label for label in education_labels if label})
        degree_labels = sorted({label for label in degree_labels if label})

        row["Education"] = "; ".join(education_labels)
        row["BA_or_MA"], row["MBA"], row["PhD"] = self.education_flags(degree_labels)

        if not row["Start_year"]:
            row["Start_year"], row["Start_month"] = self.fallback_start_from_bank_text(raw_bank_role)

        return row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N banks.")
    parser.add_argument("--output", default=OUTPUT_CSV, help="Output CSV path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    scraper = Scraper()
    banks = scraper.list_banks()
    if args.limit and args.limit > 0:
        banks = banks[: args.limit]

    rows = []
    total = len(banks)
    for index, bank in enumerate(banks, start=1):
        try:
            row = scraper.person_row(bank)
        except Exception as exc:
            print(f"Warning: failed on {bank.bank_name} ({bank.country}): {exc}", flush=True)
            row = {
                "Country": bank.country,
                "Central_bank_name": bank.bank_name,
                "Central_bank_Wikipedia": bank.bank_url,
                "Person_name": "",
                "Person_Wikipedia": "",
                "Birth_year": "",
                "Birth_month": "",
                "Start_year": "",
                "Start_month": "",
                "End_year": "",
                "End_month": "",
                "Education": "",
                "BA_or_MA": "",
                "MBA": "",
                "PhD": "",
                "CountryBirth": "",
                "CityBirth": "",
            }
        rows.append(row)
        if index % 25 == 0 or index == total:
            print(f"Processed {index}/{total}", flush=True)
        time.sleep(0.05)

    fieldnames = [
        "Country",
        "Central_bank_name",
        "Central_bank_Wikipedia",
        "Person_name",
        "Person_Wikipedia",
        "Birth_year",
        "Birth_month",
        "Start_year",
        "Start_month",
        "End_year",
        "End_month",
        "Education",
        "BA_or_MA",
        "MBA",
        "PhD",
        "CountryBirth",
        "CityBirth",
    ]

    with open(args.output, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    filled_people = sum(1 for row in rows if row["Person_name"])
    birth_hits = sum(1 for row in rows if row["Birth_year"])
    tenure_hits = sum(1 for row in rows if row["Start_year"])
    print(f"Wrote {args.output}", flush=True)
    print(f"Rows: {len(rows)}", flush=True)
    print(f"People found: {filled_people}", flush=True)
    print(f"Birth year found: {birth_hits}", flush=True)
    print(f"Start year found: {tenure_hits}", flush=True)


if __name__ == "__main__":
    main()
