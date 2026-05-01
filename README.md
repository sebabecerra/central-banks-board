# central-banks-board

Este repositorio está centrado principalmente en **rellenar `Bios_WebScrapping.xlsx` con la información pedida en el mail**, usando Wikipedia y Wikidata.

## Qué Se Hizo

Se construyó un pipeline reproducible para procesar la base [Bios_WebScrapping.xlsx](CM/data/Bios_WebScrapping.xlsx), buscar personas en Wikipedia, complementar información con Wikidata y generar un archivo final de entrega con el formato pedido.

En concreto, el repositorio hoy ya incluye:

- un script de enriquecimiento crudo desde Wikipedia y Wikidata
- un script de limpieza y validación para la entrega
- archivos auxiliares para revisar matches, unmatched y auditoría
- una salida final limpia en formato CSV

## Qué Entrega Hoy El Repo

El archivo principal de entrega es:

- [Bios_WebScrapping_enriched.csv](CM/data/Bios_WebScrapping_enriched.csv)

Y el flujo de trabajo principal queda respaldado por:

- [Bios_WebScrapping.xlsx](CM/data/Bios_WebScrapping.xlsx)
- [enrich_bios_webscrapping_from_wikipedia.py](CM/code/enrich_bios_webscrapping_from_wikipedia.py)
- [clean_bios_webscrapping_delivery.py](CM/code/clean_bios_webscrapping_delivery.py)
- [bios_webscrapping_wikipedia_enriched.csv](CM/data/bios_webscrapping_wikipedia_enriched.csv)
- [bios_webscrapping_wikipedia_enriched.xlsx](CM/data/bios_webscrapping_wikipedia_enriched.xlsx)
- [bios_webscrapping_wikipedia_enriched_audit.csv](CM/data-aux/bios_webscrapping_wikipedia_enriched_audit.csv)

## Qué Falta

El pipeline ya existe y la salida final ya está formateada, pero el trabajo todavía **no está cerrado para todo el universo original**.

Lo pendiente es:

- correr el enriquecimiento en más tandas para aumentar cobertura
- seguir revisando matches dudosos
- validar con más cuidado algunos campos de educación y tenure
- completar el universo total cuando Wikipedia/Wikidata no resuelvan de forma inmediata

En otras palabras:

- **sí** está implementado el proceso
- **sí** existe una salida final utilizable
- **no** se puede decir todavía que el archivo final cubre perfectamente todas las personas de la base original

## Objetivo Principal

La base de trabajo es:

- [Bios_WebScrapping.xlsx](CM/data/Bios_WebScrapping.xlsx)

El objetivo es completar, cuando exista un match confiable, las variables:

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

y recodificar `Position` a:

- `0` = Board member
- `1` = Deputy governor / deputy president / deputy chair
- `2` = Governor / president / chair

## Flujo Principal

### 1. Enriquecimiento crudo

Script:

- [enrich_bios_webscrapping_from_wikipedia.py](CM/code/enrich_bios_webscrapping_from_wikipedia.py)

Salida:

- [bios_webscrapping_wikipedia_enriched.csv](CM/data/bios_webscrapping_wikipedia_enriched.csv)
- [bios_webscrapping_wikipedia_enriched.xlsx](CM/data/bios_webscrapping_wikipedia_enriched.xlsx)

Auxiliares:

- [bios_webscrapping_wikipedia_matches.csv](CM/data-aux/bios_webscrapping_wikipedia_matches.csv)
- [bios_webscrapping_wikipedia_unmatched.csv](CM/data-aux/bios_webscrapping_wikipedia_unmatched.csv)
- [bios_webscrapping_wikipedia_enriched_audit.csv](CM/data-aux/bios_webscrapping_wikipedia_enriched_audit.csv)

### 2. Limpieza y archivo de entrega

Script:

- [clean_bios_webscrapping_delivery.py](CM/code/clean_bios_webscrapping_delivery.py)

Salida final:

- [Bios_WebScrapping_enriched.csv](CM/data/Bios_WebScrapping_enriched.csv)

## Estado

- el pipeline de Bios ya está implementado
- la salida final ya está formateada según el requerimiento del mail
- la cobertura actual sigue siendo parcial y requiere corridas adicionales para completar todo el universo original
- el archivo de auditoría permite distinguir mejor entre matches conservados y casos que fueron vaciados por baja confianza

## Workspace Activo

Todo el trabajo actual está dentro de:

- [CM](CM)

La documentación detallada del workspace quedó en:

- [CM/README.md](CM/README.md)

## Componentes Secundarios

Además del flujo principal de Bios, el repositorio conserva:

- pipelines para construir bases de autoridades de bancos centrales desde Wikipedia y KOF
- una app visual de mapa en [CM/map-app](CM/map-app)
