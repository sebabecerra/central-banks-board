"""
Extract central-bank governors and related leadership names from Wikipedia
category pages.

Source pages
------------
- https://en.wikipedia.org/wiki/Category:Central_bankers
- https://en.wikipedia.org/wiki/List_of_central_banks

Method
------
1. Download `List of central banks` to build a base lookup table of:
   - country
   - central bank name
   - Wikipedia URL for the bank, when available
2. Download `Category:Central_bankers`.
3. Identify relevant subcategories such as:
   - `Governors of ...`
   - `Presidents of ...`
   - `Chairs of ...`
4. Visit each relevant subcategory page.
5. Extract the names listed in that category.
6. Expand the category-level lists to a person-level long-format table.
7. Infer position, bank name, and country by combining category metadata
   with the bank lookup table.

Core outputs
------------
- `central_banks_df`: master bank lookup table
- `central_bankers_categories_df`: category-level intermediate dataset
- `df`: final person-level dataset exported as `governors_clean_names.csv`

Notes
-----
This strategy is optimized for quickly recovering names from Wikipedia category
structure. It is lighter than table parsing, but it depends on the quality and
consistency of Wikipedia category naming.
"""

import re
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CentralBankNotebook/1.0)"}
WIKI_BANKS_URL = "https://en.wikipedia.org/wiki/List_of_central_banks"
CATEGORY_URL = "https://en.wikipedia.org/wiki/Category:Central_bankers"
BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "data"
OUTPUT_AUX_DIR = BASE_DIR / "data-aux"


def clean_text(value):
    value = str(value or "")
    value = re.sub(r"\[[^\]]*\]", "", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


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
    if "governor" in lowered:
        return "Governor"
    if "president" in lowered:
        return "President"
    if "chair" in lowered:
        return "Chair"
    return value


def fetch_central_banks():
    banks_response = requests.get(WIKI_BANKS_URL, headers=HEADERS, timeout=30)
    banks_response.raise_for_status()

    banks_soup = BeautifulSoup(banks_response.text, "html.parser")
    html_tables = banks_soup.select("table.wikitable")
    wiki_tables = pd.read_html(StringIO(banks_response.text))

    if len(html_tables) < 1 or len(wiki_tables) < 1:
        raise ValueError("Could not find the main central bank table on Wikipedia")

    alphabetical_html_table = html_tables[0]
    alphabetical_records = []

    for row in alphabetical_html_table.select("tr")[1:]:
        cols = row.find_all(["th", "td"])
        if len(cols) < 3:
            continue

        country = clean_text(cols[0].get_text(" ", strip=True))
        bank_col = cols[2]
        central_bank = clean_text(bank_col.get_text(" ", strip=True))
        link = bank_col.find("a", href=True)
        wikipedia_bank_url = ""
        if link and link["href"].startswith("/wiki/"):
            wikipedia_bank_url = "https://en.wikipedia.org" + link["href"]

        if country and central_bank:
            alphabetical_records.append(
                {
                    "country": country,
                    "central_bank": central_bank,
                    "wikipedia_bank_url": wikipedia_bank_url,
                }
            )

    alphabetical_banks_df = pd.DataFrame(alphabetical_records)

    if len(wiki_tables) > 1:
        major_table_raw = wiki_tables[1].copy()
        major_banks_df = major_table_raw.rename(
            columns={
                major_table_raw.columns[0]: "country",
                major_table_raw.columns[1]: "central_bank",
            }
        )[["country", "central_bank"]].copy()
        major_banks_df["wikipedia_bank_url"] = ""
    else:
        major_banks_df = pd.DataFrame(columns=["country", "central_bank", "wikipedia_bank_url"])

    for frame in (alphabetical_banks_df, major_banks_df):
        if frame.empty:
            continue
        frame["country"] = frame["country"].astype(str).str.strip()
        frame["central_bank"] = frame["central_bank"].astype(str).str.strip()
        frame["wikipedia_bank_url"] = frame["wikipedia_bank_url"].astype(str).str.strip()

    return pd.concat([alphabetical_banks_df, major_banks_df], ignore_index=True).drop_duplicates(
        subset=["country", "central_bank"]
    ).reset_index(drop=True)


def fetch_relevant_categories():
    category_response = requests.get(CATEGORY_URL, headers=HEADERS, timeout=30)
    category_response.raise_for_status()

    category_soup = BeautifulSoup(category_response.text, "html.parser")
    category_section = category_soup.select_one("#mw-subcategories")

    category_rows = []
    if category_section:
        for link in category_section.select("a[href]"):
            href = link.get("href", "")
            if not href.startswith("/wiki/Category:"):
                continue

            category_rows.append(
                {
                    "category_name": clean_text(link.get_text(" ", strip=True)),
                    "category_url": "https://en.wikipedia.org" + href,
                }
            )

    categories_df = pd.DataFrame(category_rows).drop_duplicates()
    return categories_df[
        categories_df["category_name"].str.contains(
            r"governors?|presidents?|chair(?:men|women|persons?)?",
            case=False,
            na=False,
        )
    ].reset_index(drop=True)


def extract_people_from_category(category_url):
    response = requests.get(category_url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    pages_section = soup.select_one("#mw-pages")

    names = []
    if pages_section:
        for item in pages_section.select("div.mw-category-group li"):
            link = item.find("a", href=True)
            if not link:
                continue

            href = link.get("href", "")
            if not href.startswith("/wiki/") or href.startswith("/wiki/Category:"):
                continue

            names.append(clean_text(link.get_text(" ", strip=True)))

    return sorted(set(name for name in names if name))


def infer_position(category_name):
    text = str(category_name).lower()
    if "deputy governor" in text:
        return "Deputy Governor"
    if "governor" in text:
        return "Governor"
    if "president" in text:
        return "President"
    if "chairwoman" in text or "chairperson" in text:
        return "Chair"
    if "chair" in text:
        return "Chair"
    return ""


def infer_bank_name(category_name):
    text = str(category_name)
    prefixes = [
        "Deputy governors of the ",
        "Deputy governors of ",
        "Governors of the ",
        "Governors of ",
        "Presidents of the ",
        "Presidents of ",
        "President of the ",
        "President of ",
        "Chairmen of the ",
        "Chairmen of ",
        "Chairwomen of the ",
        "Chairwomen of ",
        "Chairpersons of the ",
        "Chairpersons of ",
        "Chairs of the ",
        "Chairs of ",
    ]
    for prefix in prefixes:
        if text.lower().startswith(prefix.lower()):
            return text[len(prefix):].strip()
    return text.strip()


BANK_NAME_TO_COUNTRY = {
    "azerbaijan central bank": "Azerbaijan",
    "bangko sentral ng pilipinas": "Philippines",
    "bangladesh bank": "Bangladesh",
    "bank al-maghrib": "Morocco",
    "bank indonesia": "Indonesia",
    "bank of albania": "Albania",
    "bank of algeria": "Algeria",
    "bank of angola": "Angola",
    "bank of botswana": "Botswana",
    "bank of canada": "Canada",
    "bank of central african states": "Central African Republic",
    "bank of denmark": "Denmark",
    "bank of estonia": "Estonia",
    "bank of finland": "Finland",
    "bank of ghana": "Ghana",
    "bank of greece": "Greece",
    "bank of guatemala": "Guatemala",
    "bank of guyana": "Guyana",
    "bank of israel": "Israel",
    "bank of italy": "Italy",
    "bank of japan": "Japan",
    "bank of korea": "Korea, Republic of",
    "bank of latvia": "Latvia",
    "bank of lithuania": "Lithuania",
    "bank of mauritius": "Mauritius",
    "bank of mexico": "Mexico",
    "bank of mongolia": "Mongolia",
    "bank of mozambique": "Mozambique",
    "bank of namibia": "Namibia",
    "bank of russia": "Russia",
    "bank of sierra leone": "Sierra Leone",
    "bank of spain": "Spain",
    "bank of tanzania": "Tanzania",
    "bank of thailand": "Thailand",
    "bank of the lao pdr": "Laos",
    "bank of uganda": "Uganda",
    "bank of zambia": "Zambia",
    "banque centrale de tunisie": "Tunisia",
    "banque de france": "France",
    "banque du liban": "Lebanon",
    "banque nationale de belgique": "Belgium",
    "banca d'italia": "Italy",
    "banco central de bolivia": "Bolivia",
    "banco central de chile": "Chile",
    "banco central de costa rica": "Costa Rica",
    "banco central de la república argentina": "Argentina",
    "banco central de reserva del perú": "Peru",
    "banco central del uruguay": "Uruguay",
    "banco central do brasil": "Brazil",
    "banco de españa": "Spain",
    "banco de guatemala": "Guatemala",
    "banco de mexico": "Mexico",
    "banco de portugal": "Portugal",
    "banco nacional de cuba": "Cuba",
    "bank negara malaysia": "Malaysia",
    "bulgarian national bank": "Bulgaria",
    "central bank of armenia": "Armenia",
    "central bank of bahrain": "Bahrain",
    "central bank of chile": "Chile",
    "central bank of cyprus": "Cyprus",
    "central bank of egypt": "Egypt",
    "central bank of eswatini": "Eswatini",
    "central bank of iceland": "Iceland",
    "central bank of india": "India",
    "central bank of iran": "Iran",
    "central bank of iraq": "Iraq",
    "central bank of ireland": "Ireland",
    "central bank of jordan": "Jordan",
    "central bank of kenya": "Kenya",
    "central bank of kuwait": "Kuwait",
    "central bank of liberia": "Liberia",
    "central bank of libya": "Libya",
    "central bank of luxembourg": "Luxembourg",
    "central bank of malta": "Malta",
    "central bank of myanmar": "Myanmar",
    "central bank of nigeria": "Nigeria",
    "central bank of oman": "Oman",
    "central bank of qatar": "Qatar",
    "central bank of russia": "Russia",
    "central bank of somalia": "Somalia",
    "central bank of sri lanka": "Sri Lanka",
    "central bank of sudan": "Sudan",
    "central bank of the bahamas": "Bahamas",
    "central bank of the dominican republic": "Dominican Republic",
    "central bank of the philippines": "Philippines",
    "central bank of the republic of turkey": "Turkey",
    "central bank of trinidad and tobago": "Trinidad and Tobago",
    "central bank of tunisia": "Tunisia",
    "central bank of the uae": "United Arab Emirates",
    "central bank of uzbekistan": "Uzbekistan",
    "central bank of venezuela": "Venezuela",
    "central bank of west african states": "Senegal",
    "central bank of yemen": "Yemen",
    "central bank of zambia": "Zambia",
    "central bank of zimbabwe": "Zimbabwe",
    "czech national bank": "Czech Republic",
    "danmarks nationalbank": "Denmark",
    "de nederlandsche bank": "Netherlands",
    "deutsche bundesbank": "Germany",
    "european central bank": "European Union",
    "federal reserve": "United States",
    "gosbank": "Russia",
    "hong kong monetary authority": "Hong Kong",
    "hungarian national bank": "Hungary",
    "magyar nemzeti bank": "Hungary",
    "monetary authority of singapore": "Singapore",
    "national bank of belarus": "Belarus",
    "national bank of belgium": "Belgium",
    "national bank of cambodia": "Cambodia",
    "national bank of ethiopia": "Ethiopia",
    "national bank of georgia": "Georgia",
    "national bank of kazakhstan": "Kazakhstan",
    "national bank of kyrgyz republic": "Kyrgyzstan",
    "national bank of the kyrgyz republic": "Kyrgyzstan",
    "national bank of moldova": "Moldova",
    "national bank of north macedonia": "North Macedonia",
    "national bank of pakistan": "Pakistan",
    "national bank of poland": "Poland",
    "national bank of romania": "Romania",
    "national bank of rwanda": "Rwanda",
    "national bank of serbia": "Serbia",
    "national bank of tajikistan": "Tajikistan",
    "national bank of ukraine": "Ukraine",
    "national reserve bank of tonga": "Tonga",
    "norges bank": "Norway",
    "oesterreichische nationalbank": "Austria",
    "people's bank of china": "China",
    "reserve bank of australia": "Australia",
    "reserve bank of fiji": "Fiji",
    "reserve bank of india": "India",
    "reserve bank of malawi": "Malawi",
    "reserve bank of new zealand": "New Zealand",
    "reserve bank of vanuatu": "Vanuatu",
    "reserve bank of zimbabwe": "Zimbabwe",
    "riksbank": "Sweden",
    "saudi central bank": "Saudi Arabia",
    "south african reserve bank": "South Africa",
    "state bank of pakistan": "Pakistan",
    "state bank of vietnam": "Vietnam",
    "swiss national bank": "Switzerland",
    "sveriges riksbank": "Sweden",
    "bank of jamaica": "Jamaica",
    "bank of papua new guinea": "Papua New Guinea",
    "bank of portugal": "Portugal",
    "bank of slovenia": "Slovenia",
    "bank of south sudan": "South Sudan",
    "bank of the lao p.d.r.": "Laos",
    "bank of the republic (colombia)": "Colombia",
    "bank of the republic of burundi": "Burundi",
    "bank of the republic of haiti": "Haiti",
    "banque centrale du congo": "Congo, Democratic Republic of the",
    "central bank of argentina": "Argentina",
    "central bank of barbados": "Barbados",
    "central bank of belize": "Belize",
    "central bank of bolivia": "Bolivia",
    "central bank of bosnia and herzegovina": "Bosnia and Herzegovina",
    "central bank of brazil": "Brazil",
    "central bank of costa rica": "Costa Rica",
    "central bank of guinea": "Guinea",
    "central bank of honduras": "Honduras",
    "central bank of lesotho": "Lesotho",
    "central bank of malaysia": "Malaysia",
    "central bank of mauritania": "Mauritania",
    "central bank of montenegro": "Montenegro",
    "central bank of nicaragua": "Nicaragua",
    "central bank of norway": "Norway",
    "central bank of paraguay": "Paraguay",
    "central bank of samoa": "Samoa",
    "central bank of seychelles": "Seychelles",
    "central bank of suriname": "Suriname",
    "central bank of syria": "Syria",
    "central bank of são tomé and príncipe": "Sao Tome and Principe",
    "central bank of the gambia": "Gambia",
    "central bank of turkey": "Turkey",
    "central bank of uruguay": "Uruguay",
    "central bank of the netherlands": "Netherlands",
    "central bank of the republic of china": "Taiwan",
    "central bank of the united arab emirates": "United Arab Emirates",
    "central reserve bank of peru": "Peru",
    "croatian national bank": "Croatia",
    "da afghanistan bank": "Afghanistan",
    "maldives monetary authority": "Maldives",
    "national bank of slovakia": "Slovakia",
    "national bank of yugoslavia": "Yugoslavia",
    "nepal rastra bank": "Nepal",
    "palestine monetary authority": "Palestine",
    "president of the central bank of cuba": "Cuba",
    "president of the central bank of the democratic people's republic of korea": "Korea, Democratic People's Republic of",
    "qatar central bank": "Qatar",
    "reserve bank of rhodesia": "Zimbabwe",
    "saudi arabian monetary agency": "Saudi Arabia",
}


def find_country(bank_name):
    norm = bank_name.lower().strip()
    if norm in BANK_NAME_TO_COUNTRY:
        return BANK_NAME_TO_COUNTRY[norm]
    for key, country in BANK_NAME_TO_COUNTRY.items():
        if key in norm or norm in key:
            return country
    return ""


def build_final_dataset(central_banks_df, central_bankers_categories_df):
    df = central_bankers_categories_df.copy()
    df["governor_names"] = df["category_url"].apply(extract_people_from_category)
    df["governor_count"] = df["governor_names"].apply(len)

    long_df = df.explode("governor_names").reset_index(drop=True)
    long_df = long_df.rename(columns={"governor_names": "PName_original"})
    long_df = long_df[long_df["PName_original"].notna()].copy()

    long_df["PName_original"] = long_df["PName_original"].astype(str).str.strip()
    long_df = long_df[long_df["PName_original"] != ""]
    long_df = long_df[long_df["PName_original"].str.split().str.len() >= 2]

    long_df["central_bank_name"] = long_df["category_name"].apply(infer_bank_name)
    long_df["country"] = long_df["central_bank_name"].apply(find_country)
    long_df["PName"] = long_df["PName_original"]
    long_df[["first", "last"]] = long_df["PName"].str.split(" ", n=1, expand=True)
    long_df["last"] = long_df["last"].fillna("")
    long_df["Position"] = long_df["category_name"].apply(infer_position)

    long_df = long_df.drop_duplicates(subset=["country", "central_bank_name", "PName_original", "Position"])
    long_df = long_df[
        ["country", "central_bank_name", "PName_original", "PName", "first", "last", "Position", "category_name", "category_url"]
    ]
    return long_df.sort_values(["country", "central_bank_name", "PName_original"]).reset_index(drop=True)


def build_final_output(df):
    if df.empty:
        return pd.DataFrame(
            columns=[
                "country",
                "central_bank_name",
                "name",
                "position",
                "start_year",
                "end_year",
                "source_method",
                "source_page",
                "source_detail",
            ]
        )

    final_df = df.copy()
    final_df["name"] = final_df["PName"]
    final_df["position"] = final_df["Position"].apply(normalize_position)
    final_df["start_year"] = ""
    final_df["end_year"] = ""
    final_df["source_method"] = "wikipedia_categories"
    final_df["source_page"] = final_df["category_url"]
    final_df["source_detail"] = final_df["category_name"]

    final_df = final_df[
        [
            "country",
            "central_bank_name",
            "name",
            "position",
            "start_year",
            "end_year",
            "source_method",
            "source_page",
            "source_detail",
        ]
    ].drop_duplicates().reset_index(drop=True)

    return final_df


def main():
    central_banks_df = fetch_central_banks()
    central_bankers_categories_df = fetch_relevant_categories()
    df = build_final_dataset(central_banks_df, central_bankers_categories_df)
    final_df = build_final_output(df)

    print_preview("central_bank_people_from_categories_long", final_df, rows=20)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_AUX_DIR.mkdir(parents=True, exist_ok=True)

    final_df.to_csv(OUTPUT_DIR / "central_bank_people_from_categories_long.csv", index=False, sep=";")
    central_banks_df.to_csv(OUTPUT_AUX_DIR / "central_banks_from_categories_lookup.csv", index=False, sep=";")
    central_bankers_categories_df.to_csv(OUTPUT_AUX_DIR / "central_bankers_categories.csv", index=False, sep=";")
    df.to_csv(OUTPUT_AUX_DIR / "governors_clean_names.csv", index=False, sep=";")
    print(f"Total names: {len(final_df)}")


if __name__ == "__main__":
    main()
