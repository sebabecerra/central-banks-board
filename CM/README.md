# CM

`CM` es la carpeta de trabajo actual para construir bases finales de autoridades de bancos centrales.

El objetivo operativo es producir datasets finales en formato long, comparables entre sí, con una estructura común para luego poder:

- comparar fuentes
- hacer append entre bases
- deduplicar
- revisar trazabilidad

## Estructura

- `code/`: scripts fuente del pipeline
- `data/`: outputs finales
- `data-aux/`: outputs crudos, intermedios y validaciones auxiliares

## Final Datasets

Los tres datasets finales que se construyen hoy son:

- [central_bank_people_from_banks_long.csv](/Users/sbc/projects/central-banks-board/CM/data/central_bank_people_from_banks_long.csv)
- [central_bank_people_from_categories_long.csv](/Users/sbc/projects/central-banks-board/CM/data/central_bank_people_from_categories_long.csv)
- [kof_governors_with_sources.csv](/Users/sbc/projects/central-banks-board/CM/data/kof_governors_with_sources.csv)

Luego esos tres se combinan en:

- [central_bank_people_combined_long.csv](/Users/sbc/projects/central-banks-board/CM/data/central_bank_people_combined_long.csv)

## Shared Final Schema

Los tres datasets finales deben salir con exactamente estas columnas:

- `country`
- `central_bank_name`
- `name`
- `position`
- `start_year`
- `end_year`
- `source_dataset`
- `source_method`
- `source_page`
- `source_detail`

### Meaning Of The Traceability Columns

- `source_dataset`: identifica de qué base final proviene la fila (`banks`, `categories`, `kof`)
- `source_method`: describe el método de extracción
- `source_page`: URL o referencia principal de origen
- `source_detail`: detalle adicional útil para auditoría

## Extraction Strategies

En `CM` conviven tres vías distintas para construir personas de bancos centrales. No son duplicados de código: cada una tiene una fuente primaria diferente y una lógica distinta de recuperación.

### 1. Wikipedia Bank Pages

Script:

- [extract_central_banks_from_wikipedia.py](/Users/sbc/projects/central-banks-board/CM/code/extract_central_banks_from_wikipedia.py)

Primary source:

- [List of central banks](https://en.wikipedia.org/wiki/List_of_central_banks)
- la página de Wikipedia de cada banco central listado ahí

Method:

1. Descarga `List of central banks`.
2. Construye una tabla base de bancos centrales.
3. Recupera la URL de Wikipedia de cada banco, cuando existe.
4. Entra a la página de cada banco central.
5. Extrae tablas e infoboxes relevantes.
6. Construye una salida final en formato long.

Final output:

- [central_bank_people_from_banks_long.csv](/Users/sbc/projects/central-banks-board/CM/data/central_bank_people_from_banks_long.csv)

Auxiliary outputs:

- [central_banks.csv](/Users/sbc/projects/central-banks-board/CM/data-aux/central_banks.csv)
- [central_bank_governors.csv](/Users/sbc/projects/central-banks-board/CM/data-aux/central_bank_governors.csv)
- [central_bank_governors_long.csv](/Users/sbc/projects/central-banks-board/CM/data-aux/central_bank_governors_long.csv)
- [central_bank_governors_request_errors.csv](/Users/sbc/projects/central-banks-board/CM/data-aux/central_bank_governors_request_errors.csv)

When to use it:

- cuando se quiere partir desde el universo de bancos centrales
- cuando importa conservar trazabilidad a nivel de tabla o infobox
- cuando interesa reconstruir historia institucional desde la página del banco

### 2. Wikipedia Category Pages

Script:

- [extract_central_bankers_from_categories.py](/Users/sbc/projects/central-banks-board/CM/code/extract_central_bankers_from_categories.py)

Primary source:

- [Category:Central_bankers](https://en.wikipedia.org/wiki/Category:Central_bankers)
- [List of central banks](https://en.wikipedia.org/wiki/List_of_central_banks) como tabla de apoyo para recuperar banco y país

Method:

1. Descarga `Category:Central_bankers`.
2. Identifica subcategorías relevantes como `Governors of ...`, `Presidents of ...` y `Chairs of ...`.
3. Entra a cada subcategoría.
4. Extrae los nombres listados.
5. Expande a una fila por persona.
6. Intenta recuperar banco central y país.

Final output:

- [central_bank_people_from_categories_long.csv](/Users/sbc/projects/central-banks-board/CM/data/central_bank_people_from_categories_long.csv)

Auxiliary outputs:

- [central_banks_from_categories_lookup.csv](/Users/sbc/projects/central-banks-board/CM/data-aux/central_banks_from_categories_lookup.csv)
- [central_bankers_categories.csv](/Users/sbc/projects/central-banks-board/CM/data-aux/central_bankers_categories.csv)
- [governors_clean_names.csv](/Users/sbc/projects/central-banks-board/CM/data-aux/governors_clean_names.csv)

When to use it:

- cuando el objetivo principal es recuperar nombres rápido desde categorías de personas
- cuando Wikipedia ya tiene categorías relativamente limpias por banco
- cuando se quiere una segunda fuente para contrastar la extracción por tablas

### 3. KOF Workbook

Scripts:

- [process_kof_governors_with_sources.py](/Users/sbc/projects/central-banks-board/CM/code/process_kof_governors_with_sources.py)
- [kof_source_maps.py](/Users/sbc/projects/central-banks-board/CM/code/kof_source_maps.py)

Primary source:

- `old/data/cbg_turnover_v23upload.xlsx`
- hoja `governors v2023`

Method:

1. Lee el workbook KOF.
2. Usa la fila 0 como ISO3 y la fila 1 como nombres de país.
3. Parsea las celdas de gobernadores por país.
4. Limpia nombres y periodos.
5. Agrega `source_url` vía mapa auxiliar.
6. Normaliza la salida al mismo esquema final que las dos bases de Wikipedia.

Final output:

- [kof_governors_with_sources.csv](/Users/sbc/projects/central-banks-board/CM/data/kof_governors_with_sources.csv)

Auxiliary outputs:

- [kof_missing_source_url.csv](/Users/sbc/projects/central-banks-board/CM/data-aux/kof_missing_source_url.csv)

When to use it:

- cuando se quiere una base histórica estructurada por país
- cuando se necesita una tercera fuente distinta de Wikipedia
- cuando interesa contrastar cobertura y periodos contra Wikipedia

## Combination And Deduplication

Una vez generadas las tres bases finales, el append y la deduplicación se hacen con:

- [combine_final_long_datasets.py](/Users/sbc/projects/central-banks-board/CM/code/combine_final_long_datasets.py)

Este script:

1. lee las tres bases finales
2. valida que compartan el mismo esquema
3. hace append
4. elimina duplicados exactos
5. aplica una deduplicación lógica con normalización de:
   - nombre
   - posición
   - años
6. fusiona la trazabilidad cuando una fila aparece en más de una fuente

Output:

- [central_bank_people_combined_long.csv](/Users/sbc/projects/central-banks-board/CM/data/central_bank_people_combined_long.csv)

## Possible Duplicate Review

Para revisar nombres potencialmente duplicados por similaridad, existe:

- [find_possible_name_duplicates.py](/Users/sbc/projects/central-banks-board/CM/code/find_possible_name_duplicates.py)

Output:

- `possible_name_duplicates.csv` en `CM/data-aux/`

Este script no reemplaza la deduplicación principal. Sirve como capa de revisión para casos donde cambian iniciales, tildes o apellidos parciales.

## Full Pipeline

Para correr todo en orden con una sola entrada, usar:

- [build_all_final_datasets.py](/Users/sbc/projects/central-banks-board/CM/code/build_all_final_datasets.py)

Ese script ejecuta:

1. [extract_central_banks_from_wikipedia.py](/Users/sbc/projects/central-banks-board/CM/code/extract_central_banks_from_wikipedia.py)
2. [extract_central_bankers_from_categories.py](/Users/sbc/projects/central-banks-board/CM/code/extract_central_bankers_from_categories.py)
3. [process_kof_governors_with_sources.py](/Users/sbc/projects/central-banks-board/CM/code/process_kof_governors_with_sources.py)
4. [combine_final_long_datasets.py](/Users/sbc/projects/central-banks-board/CM/code/combine_final_long_datasets.py)

## Recommended Workflow

Para regenerar todo:

1. correr [build_all_final_datasets.py](/Users/sbc/projects/central-banks-board/CM/code/build_all_final_datasets.py)
2. revisar [central_bank_people_combined_long.csv](/Users/sbc/projects/central-banks-board/CM/data/central_bank_people_combined_long.csv)
3. si hace falta, revisar duplicados sospechosos con [find_possible_name_duplicates.py](/Users/sbc/projects/central-banks-board/CM/code/find_possible_name_duplicates.py)

## Current Caveats

- la extracción por páginas de bancos todavía puede capturar ruido en algunos casos puntuales
- la extracción por categorías depende de cómo Wikipedia nombre las categorías
- KOF no siempre recupera `central_bank_name` para todos los países
- el combinado ya deduplica mejor, pero todavía puede requerir una segunda capa para casos de nombres casi iguales
