import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import re
import time

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CentralBankNotebook/1.0)"}
WIKI_BANKS_URL = "https://en.wikipedia.org/wiki/List_of_central_banks"

# CORRECCIÓN 1: patrones ampliados para capturar todas las tablas relevantes
ROLE_PATTERNS = [
    "governor", "governors",
    "president", "presidents",
    "chair", "chairs", "chairman",
    "key people",
    "board", "board of directors", "board of governors",
    "director", "directors",
    "member", "members",
    "deputy", "vice",
    "management", "committee",
    "monetary policy",
    "executive",
]

# Patrones de tablas a IGNORAR (no tienen personas)
SKIP_PATTERNS = [
    "balance sheet", "interest rate", "reserve", "currency",
    "exchange rate", "inflation", "gdp", "statistics",
    "branches", "subsidiaries", "notes and coins",
]


def clean_text(value):
    value = str(value or "")
    value = re.sub(r"\[[^\]]*\]", "", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" -–—,;")


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

    # Sacar texto de TODAS las filas header (th), no solo la primera
    for row in table.select("tr"):
        ths = row.find_all("th")
        if ths:
            pieces.append(" ".join(th.get_text(" ", strip=True) for th in ths))

    # Buscar heading previo (h2, h3, h4, h5)
    for tag in ["h2", "h3", "h4", "h5"]:
        prev = table.find_previous(tag)
        if prev:
            pieces.append(prev.get_text(" ", strip=True))
            break

    # También el párrafo previo inmediato
    prev_p = table.find_previous("p")
    if prev_p:
        pieces.append(prev_p.get_text(" ", strip=True)[:200])

    return clean_text(" | ".join(piece for piece in pieces if piece))


def sanitize_table_html(table):
    html = str(table)
    html = re.sub(r'colspan="(\d+);"', r'colspan="\1"', html)
    html = re.sub(r'rowspan="(\d+);"', r'rowspan="\1"', html)
    return html


def parse_html_table(table):
    return pd.read_html(StringIO(sanitize_table_html(table)))[0]


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
    """
    CORRECCIÓN 4: Infobox extrae gobernador actual Y todos los roles relevantes.
    También intenta sacar fechas si están disponibles.
    """
    records = []

    # Puede haber más de un infobox en la página
    for infobox in soup.select("table.infobox"):
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


def table_is_relevant(context_text):
    """
    Decide si una tabla es relevante (tiene personas) o es de datos económicos.
    CORRECCIÓN 1+3: lógica mejorada.
    """
    lowered = context_text.lower()

    # Si contiene patrones a ignorar Y no contiene patrones de personas -> skip
    has_skip = any(p in lowered for p in SKIP_PATTERNS)
    has_role = any(p in lowered for p in ROLE_PATTERNS)

    if has_role:
        return True

    # CORRECCIÓN 3: si la tabla tiene columnas como Name/Took office/Left office
    # aunque el contexto no diga "governor", igual es relevante
    person_column_keywords = [
        "name", "took office", "left office", "term", "tenure",
        "period", "from", "until", "entered", "exited",
        "start", "end", "appointed", "portrait"
    ]
    if any(kw in lowered for kw in person_column_keywords):
        return not has_skip  # relevante si no es tabla de datos

    return False


def extract_governor_rows(bank_row):
    url = bank_row.wikipedia_bank_url
    if not url or not url.startswith("http"):
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

        if not table_is_relevant(context_text):
            continue

        try:
            parsed_table = parse_html_table(table)
        except ValueError:
            continue

        rows = extract_table_rows(parsed_table, bank_row, context_text)
        records.extend(rows)

    # Siempre intentar infobox también (no solo si no hay wikitable)
    # CORRECCIÓN 4: infobox SIEMPRE se extrae como complemento
    infobox_rows = extract_infobox_rows(bank_row, soup)
    records.extend(infobox_rows)

    return records


# ─── 1. Bajar lista de bancos centrales con URLs ──────────────────────────────

response = requests.get(WIKI_BANKS_URL, headers=HEADERS, timeout=30)
response.raise_for_status()

wiki_tables = pd.read_html(StringIO(response.text))
soup = BeautifulSoup(response.text, "html.parser")
html_tables = soup.select("table.wikitable")

if len(wiki_tables) < 1 or len(html_tables) < 1:
    raise ValueError("No tables found on Wikipedia page")

# Primero inspeccionar la estructura de la tabla para saber qué columna es el banco central
# La tabla en https://en.wikipedia.org/wiki/List_of_central_banks tiene columnas:
# Country | Currency | Central bank | Reserves | Notes
# El banco central está en la columna 2 (índice 2), NO en la columna 1 (que es la moneda)

alphabetical_records = []
seen_banks = set()

# Palabras clave que indican que una URL es de un banco central (no moneda)
BANK_KEYWORDS = [
    "bank", "reserve", "bundesbank", "banque", "banca", "banco",
    "monetary", "national_bank", "central", "banko", "bangko",
    "riksbank", "norges", "Danmarks", "nederlandsche"
]

def is_bank_url(href, text):
    """Retorna True si la URL parece ser de un banco central, no de una moneda."""
    combined = (href + " " + text).lower()
    # Excluir páginas de monedas
    currency_words = ["dollar", "euro", "pound", "franc", "yen", "peso",
                      "rupee", "ruble", "won", "lira", "dinar", "krona",
                      "krone", "ringgit", "baht", "dirham", "riyal", "afghani",
                      "lek", "lev", "forint", "zloty", "koruna", "kuna",
                      "denar", "hryvnia", "tenge", "som", "manat", "lari",
                      "dram", "shekel", "pula", "shilling", "nakfa", "birr",
                      "cedi", "naira", "kwanza", "metical", "kwacha", "rand",
                      "leone", "dalasi", "ouguiya", "ariary", "rufiyaa",
                      "ngultrum", "kyat", "kip", "riel", "dong", "tala",
                      "pa'anga", "vatu", "dobra", "escudo", "lilangeni",
                      "loti", "maloti", "tugrik", "togrog", "pataca",
                      "convertible_mark", "marka", "dram", "lempira",
                      "cordoba", "colon", "quetzal", "balboa", "guarani",
                      "boliviano", "sol", "sucre", "bolívar", "gourde",
                      "florin", "guilder", "gulden", "kroon", "lats", "litas"]
    has_currency = any(w in combined for w in currency_words)
    has_bank = any(w in combined for w in BANK_KEYWORDS)
    return has_bank and not has_currency

for html_table in html_tables:
    # Detectar headers para saber qué columna es el banco
    header_row = html_table.find("tr")
    if not header_row:
        continue
    headers = [clean_text(th.get_text()).lower() for th in header_row.find_all(["th", "td"])]

    # Encontrar índice de la columna "central bank"
    bank_col_idx = None
    for i, h in enumerate(headers):
        if "central bank" in h or "bank" in h and "currency" not in h:
            bank_col_idx = i
            break

    for row in html_table.select("tr")[1:]:
        cols = row.find_all(["th", "td"])
        if len(cols) < 2:
            continue

        country = clean_text(cols[0].get_text(" ", strip=True))
        if not country or country in ("Country", "State", ""):
            continue

        wikipedia_bank_url = ""
        central_bank = ""

        # Estrategia 1: usar columna detectada como "central bank"
        if bank_col_idx and bank_col_idx < len(cols):
            col = cols[bank_col_idx]
            link = col.find("a", href=True)
            if link and link["href"].startswith("/wiki/"):
                href = link["href"]
                text = link.get_text()
                if is_bank_url(href, text):
                    wikipedia_bank_url = "https://en.wikipedia.org" + href
                    central_bank = clean_text(col.get_text(" ", strip=True))

        # Estrategia 2: buscar en todas las columnas el link que sea de banco
        if not wikipedia_bank_url:
            for col in cols[1:]:
                for link in col.find_all("a", href=True):
                    href = link["href"]
                    text = link.get_text()
                    if href.startswith("/wiki/") and is_bank_url(href, text):
                        wikipedia_bank_url = "https://en.wikipedia.org" + href
                        central_bank = clean_text(col.get_text(" ", strip=True))
                        break
                if wikipedia_bank_url:
                    break

        # Si aún no hay URL, tomar el texto de la columna de banco sin URL
        if not central_bank:
            if bank_col_idx and bank_col_idx < len(cols):
                central_bank = clean_text(cols[bank_col_idx].get_text(" ", strip=True))
            elif len(cols) > 2:
                central_bank = clean_text(cols[2].get_text(" ", strip=True))
            elif len(cols) > 1:
                central_bank = clean_text(cols[1].get_text(" ", strip=True))

        if not central_bank:
            continue

        key = (country, central_bank)
        if key in seen_banks:
            continue
        seen_banks.add(key)

        alphabetical_records.append({
            "country": country,
            "central_bank": central_bank,
            "wikipedia_bank_url": wikipedia_bank_url,
        })

central_banks_df = pd.DataFrame(alphabetical_records)
central_banks_df = central_banks_df[central_banks_df["country"].str.len() > 0].reset_index(drop=True)

print(f"Bancos centrales encontrados: {len(central_banks_df)}")
print(f"Con URL de Wikipedia: {central_banks_df['wikipedia_bank_url'].str.startswith('http').sum()}")
print(central_banks_df.head(10).to_string())


# ─── 2. Scrapear cada banco ───────────────────────────────────────────────────

governor_rows = []
for i, bank_row in enumerate(central_banks_df.itertuples(index=False), start=1):
    if i % 10 == 0:
        print(f"  {i}/{len(central_banks_df)} - {bank_row.country}")
    governor_rows.extend(extract_governor_rows(bank_row))
    time.sleep(0.5)  # respetar Wikipedia

central_bank_governors_df = pd.DataFrame(governor_rows)
request_errors_df = central_bank_governors_df[
    central_bank_governors_df["source_type"] == "request_error"
].copy()

print(f"\nGovernor rows: {central_bank_governors_df.shape}")
print(f"Request errors: {request_errors_df.shape}")
print(central_bank_governors_df.head(20).to_string())


# ─── 3. Exportar ─────────────────────────────────────────────────────────────

import os
os.makedirs("data", exist_ok=True)  # crear carpeta si no existe

central_banks_df.to_csv("data/central_banks.csv", index=False)
central_bank_governors_df.to_csv("data/central_bank_governors.csv", index=False)
request_errors_df.to_csv("data/central_bank_governors_request_errors.csv", index=False)

print("\nExportado OK")
