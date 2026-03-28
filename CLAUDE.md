# CLAUDE.md — intelli.gente_data_extraction

## Project Overview

ETL pipeline for the **Intelli.gente / IARA Data Science** initiative (MCTI + FAPESP). Automates collection of Brazilian government open-data into a Data Warehouse for indicator calculation.

## Key Commands

```bash
# Setup
python3 -m venv intelienv && source intelienv/bin/activate
pip install -r requirements.txt

# Run extraction (reads sources_to_extract.json)
python main.py [optional_log_path]

# Run individual extractor manually (from test/)
cd InteligenteEtl && python ../test/test1.py
```

> Tests in `test/test1.py` are manual scripts, not automated test suites. Run them by uncommenting the desired function call at the bottom of the file.

## Architecture

```
main.py  →  ExtractorClassesHandler  →  [Extractor classes]
                                              ↓
                                     ScrapperClass (web/API)
                                              ↓
                                     ProcessedDataCollection
                                              ↓
                                         DB Interface
```

**Two-layer webscraping design:**
- `webscrapping/scrapperclasses/` — fetches raw data (HTML/XLS/CSV) from government sites, returns a DataFrame with minimal processing
- `webscrapping/extractorclasses/` — receives a Scrapper via dependency injection, transforms the DataFrame into `ProcessedDataCollection`

**API extractors** (`apiextractors/`) follow the same extractor interface but fetch from government APIs directly (IBGE Agregados, IPEA Violence Map).

## Standard DataFrame Column Names

Internal column names (defined in `etl_config/etl_config.json`):

| Column | Meaning |
|---|---|
| `municipio_cod_ibge` | IBGE municipality code (7 digits) |
| `variavel_sigla` | Variable acronym/identifier |
| `ano` | Year |
| `variavel_valor` | Data value |

CSV outputs rename these to: `codigo_ibge`, `sigla`, `ano`, `variavel_valor`.

## Adding a New Data Source

1. Create a `ScrapperClass` in `webscrapping/scrapperclasses/` extending the abstract base — implement the required abstract methods
2. Create an `ExtractorClass` in `webscrapping/extractorclasses/` extending the abstract base — inject the scrapper and return a `ProcessedDataCollection`
3. Register the extractor in `sources_to_extract.json`
4. Add a test function in `test/test1.py`

## VSCode Import Fix

Auto-complete for the local `InteligenteEtl` package requires adding to `.vscode/settings.json`:

```json
"python.autoComplete.extraPaths": ["${workspaceFolder}/InteligenteEtl"]
```

## Notes

- The project uses Selenium + Chromium for JavaScript-heavy government sites
- `etl_config/etl_config.json` holds global constants (city count, column names, year bounds)
- `citiesinfo/` contains the IBGE CSV with all 5,570 Brazilian municipalities
