# Bogotá TuLlave Smartcard Data Analysis

Analysis of TransMilenio (TM) smartcard validation data for Bogotá's BRT system. The project studies transit usage patterns, focusing on the impact of fare subsidy policies (Incentivo Sisbén / Apoyo Ciudadano) on ridership. The current focus is the April 2017 subsidy reform, analyzed in [`2017-reform/`](2017-reform/).

Two distinct data periods exist due to a card system migration:
- **2016–2019**: numeric card numbers, mixed validation types in single files
- **Since 2020**: alphanumeric card numbers, separate files per validation type (Cable, Dual, Troncal, Zonal)


## Repository structure

| Location | Contents |
| --- | --- |
| root | Data pipeline notebooks (Databricks), one per stage |
| `2017-reform/` | 2017 reform analysis: window cleaning/tags, construction of analysis tables, export to Stata. `DECISIONS_2017.md` is the source of truth for all data decisions in this analysis |
| `old-code/` | Legacy Python/notebook code (gitignored, reference only) |
| `sample-code/` | Legacy Stata code for the 2017 analysis on a sample (gitignored, reference only) |


## Data Storage

### Unity Catalog Volume: `prd_csc_mega.sColom15.vColom15`

Classical file storage (~0.7 TB). Data moves through stages:

```
/Volumes/prd_csc_mega/sColom15/vColom15/
├── Data/                         ← Fetching point (weekly downloads from TM GCloud API)
│   ├── FlotaVinculada/           Fleet data
│   ├── Recargas/                 Recharges (2023–2026, ~116 GB)
│   ├── Salidas/                  Departures (2012–2026, ~9 GB)
│   ├── ValidacionCable/          Cable validations (~799 MB)
│   ├── ValidacionDual/           Dual validations (~8.7 GB)
│   ├── ValidacionTroncal/        Trunk validations (2012–2026, ~329 GB)
│   ├── ValidacionZonal/          Zonal validations (2012–2026)
│   └── *.html                    TM API index/navigation pages
│
├── Documents/                    ← Ingestion point (one-time upload from OneDrive)
│   ├── 2016data/ ... 2019data/   Old validaciones (numeric cards)
│   ├── Dual2020/ ... Dual2023/   Dual validaciones by year
│   ├── Troncal2020/ ... 2023/    Trunk validaciones by year
│   ├── Zonal2020/ ... 2023/      Zonal validaciones by year
│   ├── 2020data_clean/           Pre-cleaned 2020 data
│   ├── Recharges2017-2019/       Old recharges data
│   └── variable_dicts/           Lookup dictionaries
│
├── Workspace/                    ← Organized working data
│   ├── Raw/
│   │   ├── from2016to2019/       Validaciones with NUMERIC cards
│   │   │   ├── ~2,671 loose csv/txt/xls/xlsx files
│   │   │   ├── 2019ER10074/ (zip files)
│   │   │   └── VALTRONCAL_DD-06-2018/ (30 folders with gz files)
│   │   ├── since2020/            Validaciones with ALPHANUMERIC cards
│   │   │   ├── ValidacionCable/  (~738 files: csv + zip)
│   │   │   ├── ValidacionDual/   (~2,362 files: csv + zip)
│   │   │   ├── ValidacionTroncal/ (~2,347 files: csv + zip)
│   │   │   └── ValidacionZonal/  (~2,357 files: csv + zip)
│   │   ├── Recharges/            Recharges data (decompressed)
│   │   └── byheader_dir/         ⚠️ TO REMOVE — legacy physical copy by header
│   ├── Construct/
│   │   ├── export_2017/          ✅ Current — 2017 reform exports: cards_2017.csv, panel_2017.csv, import_2017.do (→ data-export-2017)
│   │   │  Everything below: old workflow outputs — since 2020 only, 1% & 10% samples
│   │   ├── ValidacionDual/, ValidacionTroncal/, ValidacionZonal/  (intermediate subfolders)
│   │   ├── treatment_groups_sample[1|10].csv         → constr-treatment-groups
│   │   ├── panel_with_treatment_sample[1|10].csv     → constr-monthly-panel-treatment
│   │   ├── monthly-valid-subsidy-bycard_sample*.csv  → constr-monthly-panel-treatment
│   │   ├── apoyo_stats_subsidy.csv, apoyo_total.csv  → data-analyze-ScenariosSDM
│   │   ├── df[202311–202406]apoyo.csv                → data-analyze-ScenariosSDM (monthly apoyo cards, Nov 2023–Jun 2024)
│   │   ├── daily_byprofile*.csv, df_clean_relevant_sample.csv → data-sample (from old parquets)
│   │   ├── recharges2017_sample_apoyo10pct.csv               → recharges-clean-and-sample (2017 recharges, apoyo card 10% sample)
│   │   └── apoyo_subsidy_cards_May24.csv, transactions_2025_until2025-04-26.csv
│   ├── Clean/                    ⚠️ TO REMOVE — contains only timing-old-cleaning/ subfolder (legacy timing tests)
│   └── bogota-hdfs/              Parquet files from old pipeline — validaciones only, no recharges
│       ├── parquet_df_raw_2020-2024_withdups   since2020 — ~4.37B rows, raw with duplicates
│       ├── df_clean_relevant                   since2020 — cleaned & filtered for relevant cards (Dec 2019–Oct 2024, ~3.45B rows, alphanumeric cards)
│       ├── df_clean_relevant_sample1           since2020 — 1% sample of df_clean_relevant
│       ├── df_clean_relevant_sample10          since2020 — 10% sample of df_clean_relevant
│       ├── parquet_df_clean_2020-2024_temp     since2020 — partial/temp run
│       ├── intermediate/                       since2020 — card-level aggregates (superswipers, freq, usage_count_day, regular-users-2022-2024)
│       └── sample-will/                        2016–2019 — Aug 2017–May 2018, numeric card IDs; validaciones + treatment vars; produced outside data-clean
│
└── file_to_header/               ⚠️ Legacy folder (can be removed)
```

### Delta Tables: `prd_mega.scolom15`

| Table | Description | Status |
| --- | --- | --- |
| `file_classification_since2020` | Maps each raw file (since 2020) to its per-file classification metadata (encoding, delimiter, archive format, header group, status) | ✅ Active |
| `file_classification_from2016to2019` | Same, for the 2016–2019 raw files | ✅ Populated |
| `bronze_validaciones_from2016to2019` | Unified bronze table for 2016–2019 validaciones (numeric cards) | ✅ Populated |
| `silver_validaciones_oct2016tosep2017` | Clean validaciones restricted to the 2017 analysis window (T1) | ✅ Populated |
| `silver_validaciones_oct2016tosep2017_tags` | Window silver + cleaning tags and constructed columns (T2) | ✅ Populated |
| `monthly_outcomes_2017` | Card × month outcomes for the 2017 analysis (T3) | ✅ Populated |
| `cards_2017` | One row per card: presence, spending, treatment group (T4) | ✅ Populated |
| `panel_2017` | Balanced card × month panel for the analysis sample (T5) | ✅ Populated |
| `recargas_2017to2019_raw` | Raw recharges data 2017–2019 (28 columns) | ✅ Populated |
| `tm_bronze` | Old name for since2020 bronze table, EMPTY | ⚠️ To rename and populate → `bronze_validaciones_since2020` |
| `bronze_raw_staging` | Auxiliary staging table for COPY INTO attempts | ⚠️ Legacy (failed approach, can be dropped) |

Still to create: `bronze_validaciones_since2020` and `silver_validaciones_since2020` (same medallion pattern as 2016–2019).


## Code Structure

All `.py` files are Databricks notebooks. The pipelines follow a medallion architecture: raw files → header classification (Delta control table, no file copying) → bronze (unified schema) → silver (clean, deduplicated) → analysis tables.

### Pipeline: 2016–2019 validaciones (numeric cards)

| Notebook | Purpose | Status |
| --- | --- | --- |
| `data-organize-fromDocuments` | Moved old data from `/Documents/` to `/Workspace/Raw/from2016to2019/` | ✅ Done (ran once) |
| `data-byheader-from2016to2019` | Classifies files by header/format/encoding → `file_classification_from2016to2019` | ✅ Done |
| `data-ingest-bronze-from2016to2019` | Loads files per classification, maps columns to unified schema → `bronze_validaciones_from2016to2019` | ✅ Done |
| `data-clean-silver-from2016to2019` | Bronze → silver: parses dates, casts numerics, maps categoricals, excludes duplicate source files, drops empty rows, dedups. Cleaning and diagnostics run over all of 2016–2019; the saved table keeps only the analysis window → `silver_validaciones_oct2016tosep2017` | ✅ Run for the 2017 window |

### 2017 reform analysis (`2017-reform/`)

All decisions (cleaning rules, tags, window, treatment definitions) are documented in [`2017-reform/DECISIONS_2017.md`](2017-reform/DECISIONS_2017.md) — the source of truth for this analysis.

| Notebook | Purpose | Output |
| --- | --- | --- |
| `data-clean2-silver-2017window` | Adds cleaning tags (tags only — no rows dropped) and constructed columns (time/trip variables, profile group, imputed profile) to the window silver | `silver_validaciones_oct2016tosep2017_tags` |
| `data-construction-2017` | Card classification, fare table, subsidized trips; builds the three analysis tables | `monthly_outcomes_2017`, `cards_2017`, `panel_2017` |
| `data-export-2017` | Exports cards and panel to CSV for Stata, with stable numeric treatment codes, sequential `card_id`, and a generated `import_2017.do` | `/Workspace/Construct/export_2017/` |

Analysis continues locally in Stata from the exported files.

### Pipeline: since 2020 validaciones (alphanumeric cards)

| Notebook | Purpose | Schedule | Status |
| --- | --- | --- | --- |
| `data-fetch` | Downloads newest data from TM GCloud API to `/Data/` | Mondays (job) | ✅ Active |
| `data-organize-fromData` | Moves new downloads from `/Data/` to `/Workspace/Raw/since2020/` | Mondays (job) | ✅ Active |
| `data-byheader-since2020` | Classifies files by header → `file_classification_since2020` (no file copying) | Mondays (job) | ✅ Active |
| `data-ingest-bronze` | Loads files per classification, maps columns to unified schema → `bronze_validaciones_since2020` | — | ⬜ Written, to run |
| `data-clean` | Old workflow cleaning (CSVs → transform → union → dedup → parquet); to be refactored into bronze → silver only | — | ⚠️ Legacy, to refactor |

Still to add: silver, gold/samples.

### Recharges

| Notebook | Purpose | Status |
| --- | --- | --- |
| `recharges-clean-and-sample` | Loads 2017–2019 recharges from `/Documents/Recharges2017-2019/` → `recargas_2017to2019_raw`; samples apoyo cards | ✅ Done |
| `recharges-analyse` | Exploratory analysis of 2025 recharges data (reads directly from `/Data/Recargas/`) | ✅ Done |

### Legacy analysis (old workflow, since2020 samples)

These use the old parquet files and paths; their outputs live in `/Workspace/Construct/`.

| Notebook | Purpose |
| --- | --- |
| `data-sample` | Samples cards (apoyo/subsidy users), creates treatment identifiers |
| `constr-treatment-groups` | Assigns treatment groups (hadlost23, hadlost24, hadkept, gained) per card based on subsidy status across periods |
| `constr-monthly-panel-treatment` | Builds monthly panel: validaciones/trips per card-month, codes 0s, merges treatment status |
| `plot` | Visualizes monthly validaciones with/without coding 0s by treatment group |
| `data-analyze-ScenariosSDM` | Apoyo/subsidy card statistics (Dec 2023–May 2024) for SDM scenarios |

### Exploration

| Notebook | Purpose |
| --- | --- |
| `explore-catalog` | Documents volume structure, lists files and folders, measures storage size |
| `explore-2025-data` | Quick analysis of 2025 data for treatment sample cards |
| `explore-sample-will` | Explores the `bogota-hdfs/sample-will/` legacy sample |

### Utils

Some notebooks reference `%run ./utils/...` (file handling, packages, spark session setup, window functions, the `spark_df_handler` transform class). The `utils/` folder lives in the Databricks workspace and is not tracked in this repo.


## Job: `data-fetch-organize` (ID: 461593863778684)

Runs every Monday at 3:36 AM ET. Tasks:
1. `data-fetch` → 2. `data-organize-fromData` → 3. `data-byheader-since2020`

All tasks use Git source (`github.com/dime-worldbank/ColombiaTransMilenio`, branch `main`) and cluster `ITSDA_DAP_TEAM_colombiaprojecttransmileniorawdata`.


## Questions to ask TM

- Some dates have "UTC" at the end and some don't. Can we assume they are all in UTC? Or in Colombia time?
- Can the same card number, if not used for a while, be later assigned to another person?
