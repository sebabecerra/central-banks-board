#!/usr/bin/env python3
import urllib.request

import bs4
import pandas as pd
import pycountry


EXCEL_PATH = "/Users/sbc/projects/central banks/Bios_WebScrapping.xlsx"
OUTPUT_PATH = "/Users/sbc/projects/central banks/wikipedia_vs_excel_comparison.csv"
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_central_banks"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CodexCentralBankCompare/1.0)"}


MANUAL_CODES = {
    "Abkhazia *": "ABK",
    "Turkish Republic of Northern Cyprus *": "CYP",
    "Czech Republic": "CZE",
    "The Gambia": "GMB",
    "Korea, Democratic People's Republic of": "PRK",
    "Korea, Republic of": "KOR",
    "Kosovo *": "XKX",
    "Laos": "LAO",
    "Macau": "MAC",
    "Marshall Islands": "MHL",
    "Micronesia": "FSM",
    "North Macedonia": "MKD",
    "Palestine *": "PSE",
    "Russia": "RUS",
    "Saint Kitts and Nevis": "KNA",
    "Saint Lucia": "LCA",
    "Saint Vincent and the Grenadines": "VCT",
    "São Tomé and Príncipe": "STP",
    "Sint Maarten": "SXM",
    "Somaliland *": "SOM",
    "South Ossetia *": "OST",
    "Syria": "SYR",
    "Taiwan *": "TWN",
    "Timor-Leste": "TLS",
    "Transnistria *": "MDA",
    "United States": "USA",
    "Vatican City": "VAT",
    "Congo, Democratic Republic of": "COD",
    "Congo, Republic of": "COG",
    "Cote d'Ivoire": "CIV",
    "Cape Verde": "CPV",
    "Curaçao": "CUW",
    "Dominica": "DMA",
    "Egypt": "EGY",
    "Faroe Islands": "FRO",
    "French Polynesia": "PYF",
    "Greenland": "GRL",
    "Hong Kong": "HKG",
    "Iran": "IRN",
    "Moldova": "MDA",
    "New Caledonia": "NCL",
    "Slovakia": "SVK",
    "Solomon Islands": "SLB",
    "Tanzania": "TZA",
    "Turkey": "TUR",
    "Venezuela": "VEN",
    "Vietnam": "VNM",
    "Wallis and Futuna": "WLF",
    "Brunei": "BRN",
    "Bolivia": "BOL",
    "Eswatini": "SWZ",
    "European Union": "EUR",
}

EXCEL_ONLY_NAMES = {
    "ANT": "Netherlands Antilles (legacy code in Excel)",
    "BUR": "Burma/Myanmar legacy code in Excel",
    "EUR": "European Union",
}


def wiki_to_iso3(name: str) -> str | None:
    if name in MANUAL_CODES:
        return MANUAL_CODES[name]
    clean_name = name.replace(" *", "")
    try:
        return pycountry.countries.lookup(clean_name).alpha_3
    except LookupError:
        return None


def fetch_wikipedia_rows() -> list[dict]:
    request = urllib.request.Request(WIKI_URL, headers=HEADERS)
    html = urllib.request.urlopen(request, timeout=30).read()
    soup = bs4.BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.wikitable")

    rows = []
    for row in table.select("tr")[1:]:
        cols = row.find_all(["th", "td"])
        if len(cols) < 3:
            continue
        country_name = cols[0].get_text(" ", strip=True)
        bank_name = cols[2].get_text(" ", strip=True)
        iso3 = wiki_to_iso3(country_name)
        rows.append(
            {
                "country_name": country_name,
                "iso3": iso3 or "",
                "bank_name_wikipedia": bank_name,
                "in_wikipedia": 1,
            }
        )
    return rows


def main() -> None:
    excel_df = pd.read_excel(EXCEL_PATH)
    excel_codes = set(excel_df["iso3"].dropna().astype(str).str.strip())

    wiki_rows = fetch_wikipedia_rows()
    wiki_by_iso3 = {row["iso3"]: row for row in wiki_rows if row["iso3"]}

    output_rows = []
    seen = set()

    for row in wiki_rows:
        iso3 = row["iso3"]
        output_rows.append(
            {
                "country_name": row["country_name"],
                "iso3": iso3,
                "bank_name_wikipedia": row["bank_name_wikipedia"],
                "in_wikipedia": 1,
                "in_excel": 1 if iso3 in excel_codes else 0,
                "status": "both" if iso3 in excel_codes else "missing_in_excel",
            }
        )
        seen.add(iso3)

    for iso3 in sorted(excel_codes):
        if iso3 in seen:
            continue
        output_rows.append(
            {
                "country_name": EXCEL_ONLY_NAMES.get(iso3, ""),
                "iso3": iso3,
                "bank_name_wikipedia": "",
                "in_wikipedia": 0,
                "in_excel": 1,
                "status": "excel_only",
            }
        )

    output_df = pd.DataFrame(output_rows).sort_values(
        by=["status", "country_name", "iso3"], na_position="last"
    )
    output_df.to_csv(OUTPUT_PATH, index=False)

    print(f"Wrote {OUTPUT_PATH}")
    print(f"Rows: {len(output_df)}")
    print(f"Missing in Excel: {(output_df['status'] == 'missing_in_excel').sum()}")
    print(f"Excel only: {(output_df['status'] == 'excel_only').sum()}")


if __name__ == "__main__":
    main()
