# CM

`CM` es la carpeta de trabajo activa para **rellenar `Bios_WebScrapping.xlsx` con la información pedida en el mail**, usando Wikipedia y Wikidata, y para mantener pipelines auxiliares de autoridades de bancos centrales.

## Objetivo Principal

El foco principal de este workspace es completar la base:

- [Bios_WebScrapping.xlsx](data/Bios_WebScrapping.xlsx)

según el requerimiento del mail, rellenando cuando exista un match confiable en Wikipedia/Wikidata las variables:

- `Birth_year`
- `Birth_month`
- `Start_year`
- `Start_month`
- `End_year`
- `End_month`
- `Education`
- `BA_or_MA`
- `MBA`
- `PhD`
- `CountryBirth`
- `CityBirth`
- `Sex`

y recodificando `Position` a:

- `0` = Board member
- `1` = Deputy governor / deputy president / deputy chair
- `2` = Governor / president / chair

La salida final de entrega para este objetivo es:

- [Bios_WebScrapping_enriched.csv](data/Bios_WebScrapping_enriched.csv)

## Flujo Principal De Bios

### Input

- [Bios_WebScrapping.xlsx](data/Bios_WebScrapping.xlsx)

### Etapa 1. Enriquecimiento Crudo Desde Wikipedia/Wikidata

Script:

- [enrich_bios_webscrapping_from_wikipedia.py](code/enrich_bios_webscrapping_from_wikipedia.py)

Qué hace:

- busca cada persona por nombre en Wikipedia
- usa Wikidata para extraer datos biográficos, educativos y de tenure
- deja trazabilidad del matching
- genera una salida cruda enriquecida, todavía no validada del todo

Salidas:

- [bios_webscrapping_wikipedia_enriched.csv](data/bios_webscrapping_wikipedia_enriched.csv)
- [bios_webscrapping_wikipedia_enriched.xlsx](data/bios_webscrapping_wikipedia_enriched.xlsx)
- [bios_webscrapping_wikipedia_matches.csv](data-aux/bios_webscrapping_wikipedia_matches.csv)
- [bios_webscrapping_wikipedia_unmatched.csv](data-aux/bios_webscrapping_wikipedia_unmatched.csv)
- [bios_webscrapping_wikipedia_cache.json](data-aux/bios_webscrapping_wikipedia_cache.json)
- [bios_webscrapping_wikipedia_tenure_cache.json](data-aux/bios_webscrapping_wikipedia_tenure_cache.json)

### Etapa 2. Limpieza Y Archivo De Entrega

Script:

- [clean_bios_webscrapping_delivery.py](code/clean_bios_webscrapping_delivery.py)

Qué hace:

- conserva solo las columnas requeridas por el mail
- normaliza `Position`, `Sex`, años y meses
- limpia los campos educativos
- vacía los campos enriquecidos cuando el match parece poco confiable
- produce el archivo final de entrega

Salida final de entrega:

- [Bios_WebScrapping_enriched.csv](data/Bios_WebScrapping_enriched.csv)

Salida de auditoría:

- [bios_webscrapping_wikipedia_enriched_audit.csv](data-aux/bios_webscrapping_wikipedia_enriched_audit.csv)

## Estado Actual De Bios

- el pipeline está implementado
- la lógica de enriquecimiento y limpieza ya existe
- la salida final está formateada según el requerimiento del mail
- la cobertura actual sigue siendo parcial y requiere corridas adicionales para completar todo el universo original

## Otros Componentes Del Workspace

Además del trabajo principal de `Bios_WebScrapping`, `CM` conserva pipelines complementarios para construir bases de autoridades de bancos centrales y una app visual de mapa basada en KOF.

## Estructura

- `code/`: scripts fuente del pipeline
- `map-app/`: app visual de mapa basada en KOF
- `data/`: inputs principales y outputs finales vigentes
- `data-aux/`: salidas crudas, caches, auditorías y auxiliares

## Inventario De Scripts

### Bases de autoridades de bancos centrales

- [extract_central_banks_from_wikipedia.py](code/extract_central_banks_from_wikipedia.py)
- [extract_central_bankers_from_categories.py](code/extract_central_bankers_from_categories.py)
- [process_kof_governors_with_sources.py](code/process_kof_governors_with_sources.py)
- [kof_source_maps.py](code/kof_source_maps.py)
- [combine_final_long_datasets.py](code/combine_final_long_datasets.py)
- [find_possible_name_duplicates.py](code/find_possible_name_duplicates.py)
- [build_all_final_datasets.py](code/build_all_final_datasets.py)

### Enriquecimiento de Bios

- [enrich_bios_webscrapping_from_wikipedia.py](code/enrich_bios_webscrapping_from_wikipedia.py)
- [clean_bios_webscrapping_delivery.py](code/clean_bios_webscrapping_delivery.py)

### App visual

- [map-app](map-app)

## Final Datasets De Autoridades

Las tres bases finales que hoy se producen con esquema común son:

- [central_bank_people_from_banks_long.csv](data/central_bank_people_from_banks_long.csv)
- [central_bank_people_from_categories_long.csv](data/central_bank_people_from_categories_long.csv)
- [kof_governors_with_sources.csv](data/kof_governors_with_sources.csv)

Luego esas tres se combinan en:

- [central_bank_people_combined_long.csv](data/central_bank_people_combined_long.csv)

## Shared Final Schema

Las tres bases finales de autoridades salen con estas columnas:

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

### Trazabilidad

- `source_dataset`: base final de origen (`banks`, `categories`, `kof`)
- `source_method`: método de extracción
- `source_page`: URL o referencia principal de origen
- `source_detail`: detalle adicional útil para auditoría

## Estrategias De Extracción De Autoridades

### 1. Wikipedia Bank Pages

Script:

- [extract_central_banks_from_wikipedia.py](code/extract_central_banks_from_wikipedia.py)

Fuente primaria:

- [List of central banks](https://en.wikipedia.org/wiki/List_of_central_banks)
- la página de Wikipedia de cada banco central listado ahí

Método:

1. Descarga `List of central banks`.
2. Construye la tabla base de bancos centrales.
3. Recupera la URL de Wikipedia de cada banco.
4. Entra a cada página institucional.
5. Extrae tablas e infoboxes relevantes.
6. Construye una salida final en formato long.

Salida final:

- [central_bank_people_from_banks_long.csv](data/central_bank_people_from_banks_long.csv)

Salidas auxiliares:

- [central_banks.csv](data-aux/central_banks.csv)
- [central_bank_governors.csv](data-aux/central_bank_governors.csv)
- [central_bank_governors_long.csv](data-aux/central_bank_governors_long.csv)
- [central_bank_governors_request_errors.csv](data-aux/central_bank_governors_request_errors.csv)

### 2. Wikipedia Category Pages

Script:

- [extract_central_bankers_from_categories.py](code/extract_central_bankers_from_categories.py)

Fuente primaria:

- [Category:Central_bankers](https://en.wikipedia.org/wiki/Category:Central_bankers)
- [List of central banks](https://en.wikipedia.org/wiki/List_of_central_banks) como lookup para banco y país

Método:

1. Descarga `Category:Central_bankers`.
2. Identifica subcategorías relevantes como `Governors of ...`, `Presidents of ...` y `Chairs of ...`.
3. Entra a cada subcategoría.
4. Extrae los nombres listados.
5. Expande a una fila por persona.
6. Recupera banco central y país cuando es posible.

Salida final:

- [central_bank_people_from_categories_long.csv](data/central_bank_people_from_categories_long.csv)

Salidas auxiliares:

- [central_banks_from_categories_lookup.csv](data-aux/central_banks_from_categories_lookup.csv)
- [central_bankers_categories.csv](data-aux/central_bankers_categories.csv)
- [governors_clean_names.csv](data-aux/governors_clean_names.csv)

### 3. KOF Workbook

Scripts:

- [process_kof_governors_with_sources.py](code/process_kof_governors_with_sources.py)
- [kof_source_maps.py](code/kof_source_maps.py)

Fuente primaria:

- workbook KOF histórico
- hoja `governors v2023`

Método:

1. Lee el workbook KOF.
2. Usa la fila 0 como ISO3 y la fila 1 como nombres de país.
3. Parsea las celdas de gobernadores por país.
4. Limpia nombres y periodos.
5. Agrega `source_url` vía mapa auxiliar.
6. Normaliza la salida al mismo esquema final que las dos bases de Wikipedia.

Salida final:

- [kof_governors_with_sources.csv](data/kof_governors_with_sources.csv)

Salida auxiliar:

- [kof_missing_source_url.csv](data-aux/kof_missing_source_url.csv)

## Combinación Y Dedupe

Script:

- [combine_final_long_datasets.py](code/combine_final_long_datasets.py)

Este script:

1. lee las tres bases finales
2. valida que compartan el mismo esquema
3. hace append
4. elimina duplicados exactos
5. aplica deduplicación lógica con normalización de nombre, posición y años
6. fusiona la trazabilidad cuando una fila aparece en más de una fuente

Salida:

- [central_bank_people_combined_long.csv](data/central_bank_people_combined_long.csv)

## Revisión De Duplicados Probables

Script:

- [find_possible_name_duplicates.py](code/find_possible_name_duplicates.py)

Uso:

- revisión auxiliar de nombres parecidos por similaridad
- no reemplaza la deduplicación principal

Salida:

- `possible_name_duplicates.csv` en `CM/data-aux/`

## Pipeline Maestro De Autoridades

Script:

- [build_all_final_datasets.py](code/build_all_final_datasets.py)

Ejecuta en orden:

1. [extract_central_banks_from_wikipedia.py](code/extract_central_banks_from_wikipedia.py)
2. [extract_central_bankers_from_categories.py](code/extract_central_bankers_from_categories.py)
3. [process_kof_governors_with_sources.py](code/process_kof_governors_with_sources.py)
4. [combine_final_long_datasets.py](code/combine_final_long_datasets.py)

## Pipeline De Bios_WebScrapping

El trabajo de Bios es una línea aparte del pipeline de autoridades. Parte desde una base dada por el usuario y busca enriquecerla con Wikipedia y Wikidata.

### Input Principal

- [Bios_WebScrapping.xlsx](data/Bios_WebScrapping.xlsx)

### Etapa 1. Enriquecimiento Crudo Desde Wikipedia/Wikidata

Script:

- [enrich_bios_webscrapping_from_wikipedia.py](code/enrich_bios_webscrapping_from_wikipedia.py)

Objetivo:

- buscar cada persona por nombre en Wikipedia
- usar Wikidata cuando exista match razonable
- llenar variables biográficas, de educación y de tenure
- dejar trazabilidad del match

Variables objetivo:

- `Birth_year`
- `Birth_month`
- `Start_year`
- `Start_month`
- `End_year`
- `End_month`
- `Education`
- `BA_or_MA`
- `MBA`
- `PhD`
- `CountryBirth`
- `CityBirth`
- `Sex`

También recodifica `Position` a:

- `0` = Board member
- `1` = Deputy governor / deputy president / deputy chair
- `2` = Governor / president / chair

Salidas principales de esta etapa:

- [bios_webscrapping_wikipedia_enriched.csv](data/bios_webscrapping_wikipedia_enriched.csv)
- [bios_webscrapping_wikipedia_enriched.xlsx](data/bios_webscrapping_wikipedia_enriched.xlsx)

Salidas auxiliares de esta etapa:

- [bios_webscrapping_wikipedia_matches.csv](data-aux/bios_webscrapping_wikipedia_matches.csv)
- [bios_webscrapping_wikipedia_unmatched.csv](data-aux/bios_webscrapping_wikipedia_unmatched.csv)
- [bios_webscrapping_wikipedia_cache.json](data-aux/bios_webscrapping_wikipedia_cache.json)
- [bios_webscrapping_wikipedia_tenure_cache.json](data-aux/bios_webscrapping_wikipedia_tenure_cache.json)

Notas operativas:

- esta etapa es lenta
- la corrida completa requiere batching y tolerancia a rate limiting de Wikipedia
- la salida cruda puede contener matches dudosos y por eso necesita una segunda limpieza

### Etapa 2. Limpieza Y Archivo De Entrega

Script:

- [clean_bios_webscrapping_delivery.py](code/clean_bios_webscrapping_delivery.py)

Objetivo:

- transformar la salida cruda en un archivo de entrega alineado al requerimiento
- conservar solo las columnas pedidas
- limpiar años, meses y campos educativos
- vaciar los campos enriquecidos cuando el match de Wikipedia no sea confiable

Salida final de entrega:

- [Bios_WebScrapping_enriched.csv](data/Bios_WebScrapping_enriched.csv)

Salida normalizada adicional:

- [bios_webscrapping_wikipedia_enriched.csv](data/bios_webscrapping_wikipedia_enriched.csv)

Salida de auditoría:

- [bios_webscrapping_wikipedia_enriched_audit.csv](data-aux/bios_webscrapping_wikipedia_enriched_audit.csv)

Esquema final de entrega:

- `PName_original`
- `PName`
- `iso3`
- `first`
- `last`
- `iso3Birth`
- `Start_year`
- `Start_month`
- `End_year`
- `End_month`
- `Position`
- `Sex`
- `Birth_year`
- `Birth_month`
- `Education`
- `BA_or_MA`
- `MBA`
- `PhD`
- `CountryBirth`
- `CityBirth`

## Flujo Recomendado Para Bios

1. revisar [Bios_WebScrapping.xlsx](data/Bios_WebScrapping.xlsx)
2. correr [enrich_bios_webscrapping_from_wikipedia.py](code/enrich_bios_webscrapping_from_wikipedia.py)
3. correr [clean_bios_webscrapping_delivery.py](code/clean_bios_webscrapping_delivery.py)
4. entregar [Bios_WebScrapping_enriched.csv](data/Bios_WebScrapping_enriched.csv)
5. usar [bios_webscrapping_wikipedia_enriched_audit.csv](data-aux/bios_webscrapping_wikipedia_enriched_audit.csv) si hay que revisar matches

## Map App

La app visual recuperada desde `old/` quedó en:

- [map-app](map-app)

Archivos principales:

- [package.json](map-app/package.json)
- [src/App.jsx](map-app/src/App.jsx)
- [src/main.jsx](map-app/src/main.jsx)
- [src/styles.css](map-app/src/styles.css)
- [public/governors_by_country.json](map-app/public/governors_by_country.json)
- [public/kof_governors_with_sources.csv](map-app/public/kof_governors_with_sources.csv)

Tecnología:

- Vite
- React
- react-simple-maps

Objetivo:

- mostrar un mapa mundial interactivo de gobernadores de bancos centrales
- usar como base la salida KOF ya procesada
- permitir exploración visual por país, con detalle histórico y descarga del CSV

Nota:

- en `CM/map-app` quedó solo el código fuente útil
- `node_modules/` y `dist/` se dejaron fuera para no ensuciar el repo

## Caveats Actuales

- la extracción por páginas de bancos todavía puede capturar ruido en algunos casos puntuales
- la extracción por categorías depende de cómo Wikipedia nombre las categorías
- KOF no siempre recupera `central_bank_name` para todos los países
- el combinado ya deduplica mejor, pero todavía puede requerir una segunda capa para casos de nombres casi iguales
- el pipeline de Bios hoy está operativo, pero la salida actual sigue siendo parcial y necesita corridas adicionales para cubrir todo el universo original
