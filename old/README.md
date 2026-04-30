# Central Banks Board

Repositorio de trabajo para:

- extraer bancos centrales desde Wikipedia
- procesar la base KOF de gobernadores
- enriquecer salidas con `source_url`
- probar extracción con LLMs
- visualizar la historia de gobernadores en una app de mapa

## Estructura recomendada

### `data/`
Archivos de entrada y salidas tabulares.

Entradas principales:
- [`data/Bios_WebScrapping.xlsx`](/Users/sbc/projects/central-banks-board/data/Bios_WebScrapping.xlsx)
- [`data/cbg_turnover_v23upload.xlsx`](/Users/sbc/projects/central-banks-board/data/cbg_turnover_v23upload.xlsx)

Salidas canónicas actuales:
- [`data/kof_governors_with_sources.csv`](/Users/sbc/projects/central-banks-board/data/kof_governors_with_sources.csv)
- [`data/kof_missing_source_url.csv`](/Users/sbc/projects/central-banks-board/data/kof_missing_source_url.csv)
- [`data/combined_people_roles.csv`](/Users/sbc/projects/central-banks-board/data/combined_people_roles.csv) if generated

Otras salidas útiles:
- [`data/kof_governors.csv`](/Users/sbc/projects/central-banks-board/data/kof_governors.csv)
- [`data/unique_names_countries.csv`](/Users/sbc/projects/central-banks-board/data/unique_names_countries.csv)
- [`data/unique_clean_names.csv`](/Users/sbc/projects/central-banks-board/data/unique_clean_names.csv)

### Notebooks principales

Notebook canónico para KOF con fuentes:
- [`kof_governors_with_sources_explained.ipynb`](/Users/sbc/projects/central-banks-board/kof_governors_with_sources_explained.ipynb)

Notebook canónico para combinar Bios + KOF:
- [`combined_people_roles.ipynb`](/Users/sbc/projects/central-banks-board/combined_people_roles.ipynb)

Notebook de apoyo para Bios:
- [`bios_webscrapping_explained.ipynb`](/Users/sbc/projects/central-banks-board/bios_webscrapping_explained.ipynb)

Notebook histórico / exploratorio:
- [`central_bank_boards.ipynb`](/Users/sbc/projects/central-banks-board/central_bank_boards.ipynb)
- [`extract_central_banks_from_wikipedia.ipynb`](/Users/sbc/projects/central-banks-board/extract_central_banks_from_wikipedia.ipynb)
- [`search_google_demo.ipynb`](/Users/sbc/projects/central-banks-board/search_google_demo.ipynb)
- [`kof_governors_explained.ipynb`](/Users/sbc/projects/central-banks-board/kof_governors_explained.ipynb)

### Scripts Python

Pipeline KOF / datos:
- [`kof_source_maps.py`](/Users/sbc/projects/central-banks-board/kof_source_maps.py)
- [`build_map_data.py`](/Users/sbc/projects/central-banks-board/build_map_data.py)
- [`build_central_bank_sources.py`](/Users/sbc/projects/central-banks-board/build_central_bank_sources.py)
- [`extract_central_bankers.py`](/Users/sbc/projects/central-banks-board/extract_central_bankers.py)
- [`extract_central_bank_boards.py`](/Users/sbc/projects/central-banks-board/extract_central_bank_boards.py)

LLMs / pruebas:
- [`ask_central_bank_llm.py`](/Users/sbc/projects/central-banks-board/ask_central_bank_llm.py)
- [`central_bank_boards_llm.py`](/Users/sbc/projects/central-banks-board/central_bank_boards_llm.py)
- [`compare_llms_central_banks.py`](/Users/sbc/projects/central-banks-board/compare_llms_central_banks.py)
- [`extract_board_from_sources_ollama.py`](/Users/sbc/projects/central-banks-board/extract_board_from_sources_ollama.py)
- [`extract_wikipedia_econ_roles.py`](/Users/sbc/projects/central-banks-board/extract_wikipedia_econ_roles.py)
- [`gemini_latin_america_boards.py`](/Users/sbc/projects/central-banks-board/gemini_latin_america_boards.py)
- [`search_google.py`](/Users/sbc/projects/central-banks-board/search_google.py)
- [`search_google_api.py`](/Users/sbc/projects/central-banks-board/search_google_api.py)
- [`test_gemini.py`](/Users/sbc/projects/central-banks-board/test_gemini.py)
- [`test_ollama.py`](/Users/sbc/projects/central-banks-board/test_ollama.py)

### `map-app/`
Frontend React/Vite del mapa de gobernadores.

Archivos clave:
- [`map-app/src/App.jsx`](/Users/sbc/projects/central-banks-board/map-app/src/App.jsx)
- [`map-app/public/governors_by_country.json`](/Users/sbc/projects/central-banks-board/map-app/public/governors_by_country.json)
- [`map-app/public/kof_governors_with_sources.csv`](/Users/sbc/projects/central-banks-board/map-app/public/kof_governors_with_sources.csv)

## Qué usar hoy

### Si quieres procesar la base KOF
Usa:
- [`kof_governors_with_sources_explained.ipynb`](/Users/sbc/projects/central-banks-board/kof_governors_with_sources_explained.ipynb)

### Si quieres combinar Bios + KOF
Usa:
- [`combined_people_roles.ipynb`](/Users/sbc/projects/central-banks-board/combined_people_roles.ipynb)

### Si quieres la app del mapa
1. Regenera datos:

```bash
python3 /Users/sbc/projects/central-banks-board/build_map_data.py
```

2. Levanta la app:

```bash
cd /Users/sbc/projects/central-banks-board/map-app
npm run dev
```

## Estado práctico del repo

- **Fuente tabular principal hoy**: [`data/kof_governors_with_sources.csv`](/Users/sbc/projects/central-banks-board/data/kof_governors_with_sources.csv)
- **Notebook principal hoy**: [`kof_governors_with_sources_explained.ipynb`](/Users/sbc/projects/central-banks-board/kof_governors_with_sources_explained.ipynb)
- **Visual principal hoy**: [`map-app/`](/Users/sbc/projects/central-banks-board/map-app)

## Pendientes razonables

- consolidar notebooks viejos en una carpeta `archive/`
- mover outputs viejos de la raíz a `data/` o `archive/`
- decidir un único flujo LLM estable
