# CM

Esta carpeta contiene el flujo actual de extracción de gobernadores, presidentes y chairs de bancos centrales desde Wikipedia.

## Estructura

- `code/`: scripts de extracción
- `data/`: outputs finales en formato long
- `data-aux/`: outputs intermedios, tablas crudas y archivos auxiliares

## Extraction Strategies

En `CM` conviven dos estrategias complementarias de extracción. No son duplicados: cada una parte desde una página distinta de Wikipedia y responde a una lógica de recuperación diferente.

### 1. Extraction From The List Of Central Banks

Script:

- [extract_central_banks_from_wikipedia.py](/Users/sbc/projects/central-banks-board/CM/code/extract_central_banks_from_wikipedia.py)

Primary source page:

- [List of central banks](https://en.wikipedia.org/wiki/List_of_central_banks)

Method:

1. Descarga la página `List of central banks`.
2. Construye una tabla base de bancos centrales.
3. Recupera, cuando existe, la URL de Wikipedia de cada banco central.
4. Entra a la página de Wikipedia de cada banco central.
5. Extrae tablas e infoboxes que contienen información sobre governors, presidents o chairs.
6. Genera una salida cruda y una salida en formato long.

Final output:

- [central_bank_people_from_banks_long.csv](/Users/sbc/projects/central-banks-board/CM/data/central_bank_people_from_banks_long.csv)

Auxiliary outputs:

- `central_banks.csv`
- `central_bank_governors.csv`
- `central_bank_governors_long.csv`
- `central_bank_governors_request_errors.csv`

When to use it:

- cuando el punto de partida correcto es la lista completa de bancos centrales
- cuando se necesita trazabilidad a nivel de fila de tabla o infobox
- cuando interesa reconstruir historia institucional desde la página del banco central

### 2. Extraction From Wikipedia Category Pages

Script:

- [extract_central_bankers_from_categories.py](/Users/sbc/projects/central-banks-board/CM/code/extract_central_bankers_from_categories.py)

Primary source page:

- [Category:Central_bankers](https://en.wikipedia.org/wiki/Category:Central_bankers)

Method:

1. Descarga la categoría `Category:Central_bankers`.
2. Identifica subcategorías relevantes, por ejemplo:
   - `Governors of ...`
   - `Presidents of ...`
   - `Chairs of ...`
3. Entra a cada subcategoría.
4. Extrae los nombres de las personas listadas en esa categoría.
5. Expande la lista a una fila por persona.
6. Intenta recuperar banco central y país cruzando contra la tabla base de bancos centrales.

Final output:

- [central_bank_people_from_categories_long.csv](/Users/sbc/projects/central-banks-board/CM/data/central_bank_people_from_categories_long.csv)

Auxiliary outputs:

- `central_banks_from_categories_lookup.csv`
- `central_bankers_categories.csv`
- `governors_clean_names.csv`

When to use it:

- cuando el objetivo principal es construir una lista de nombres desde categorías de personas
- cuando Wikipedia ya tiene categorías relativamente limpias por banco central
- cuando se necesita una extracción rápida de nombres sin parsear tablas históricas complejas

## Conceptual Difference

La primera estrategia sigue este flujo:

- banco central -> página del banco central -> tabla o infobox -> personas

La segunda estrategia sigue este flujo:

- categoría de personas -> nombres -> banco central -> país

En otras palabras:

- una vía obtiene los nombres desde la lista general de bancos centrales y luego entra a la página de cada banco
- la otra vía obtiene los nombres desde la categoría de banqueros centrales y sus subcategorías

## Output Convention

Ambos scripts generan un único archivo final en `CM/data/`, con esquema homogéneo y comparable.

Expected final columns:

- `country`
- `central_bank_name`
- `name`
- `position`
- `start_year`
- `end_year`
- `source_method`
- `source_page`
- `source_detail`

Todos los archivos intermedios o de depuración se guardan en `CM/data-aux/`.

## Current Recommendation

- usar [extract_central_banks_from_wikipedia.py](/Users/sbc/projects/central-banks-board/CM/code/extract_central_banks_from_wikipedia.py) como flujo principal cuando se prioriza cobertura institucional y trazabilidad
- usar [extract_central_bankers_from_categories.py](/Users/sbc/projects/central-banks-board/CM/code/extract_central_bankers_from_categories.py) como flujo complementario cuando se prioriza velocidad para obtener nombres desde categorías
