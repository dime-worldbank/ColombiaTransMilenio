# Bogotá TuLlave Smartcard Data Analysis

Analysis of TransMilenio (TM) smartcard validation data for Bogotá's BRT system. The project studies transit usage patterns, focusing on the impact of fare subsidy policies (Incentivo Sisbén / Apoyo Ciudadano) on ridership.

Two distinct data periods exist due to a card system migration:
- **2016–2019**: numeric card numbers, mixed validation types in single files
- **Since 2020**: alphanumeric card numbers, separate files per validation type (Cable, Dual, Troncal, Zonal)


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
│   ├── Construct/                ⚠️ Old workflow outputs — since 2020 only, 1% & 10% samples
│   │   │  Produced by: data-sample · constr-treatment-groups · constr-monthly-panel-treatment · data-analyze-ScenariosSDM
│   │   ├── ValidacionDual/, ValidacionTroncal/, ValidacionZonal/  (intermediate subfolders)
│   │   ├── treatment_groups_sample[1|10].csv         → constr-treatment-groups
│   │   ├── panel_with_treatment_sample[1|10].csv     → constr-monthly-panel-treatment
│   │   ├── monthly-valid-subsidy-bycard_sample*.csv  → constr-monthly-panel-treatment
│   │   ├── apoyo_stats_subsidy.csv, apoyo_total.csv  → data-analyze-ScenariosSDM
│   │   ├── df[202311–202406]apoyo.csv                → data-analyze-ScenariosSDM (monthly apoyo cards, Nov 2023–Jun 2024)
│   │   ├── daily_byprofile*.csv, df_clean_relevant_sample.csv → data-sample (from old parquets)
│   │   ├── recharges2017_sample_apoyo10pct.csv               → recharges-clean-and-sample (2017 recharges, apoyo card 10% sample)
│   │   └── apoyo_subsidy_cards_May24.csv, transactions_2025_until2025-04-26.csv
│   ├── Clean/                    ⚠️ TO REMOVE — contains only timing-old-cleaning/ subfolder
│   │                             Legacy timing tests from old loop-through-files cleaning approach
│   └── bogota-hdfs/              Parquet files from old pipeline — validaciones only, no recharges
│       ├── parquet_df_raw_2020-2024_withdups   ✅ since2020 confirmed — ~4.37B rows, raw with duplicates
│       ├── df_clean_relevant                   ✅ since2020 confirmed — cleaned & filtered for relevant cards (Dec 2019–Oct 2024, ~3.45B rows, alphanumeric cards)
│       ├── df_clean_relevant_sample1           ✅ since2020 confirmed — 1% sample of df_clean_relevant
│       ├── df_clean_relevant_sample10          ✅ since2020 confirmed — 10% sample of df_clean_relevant
│       ├── parquet_df_clean_2020-2024_temp     ✅ since2020 confirmed (by name) — partial/temp run
│       ├── intermediate/                       ✅ since2020 confirmed — card-level aggregates (superswipers, freq, usage_count_day, regular-users-2022-2024)
│       └── sample-will/                        ✅ confirmed 2016–2019 — Aug 2017–May 2018, numeric card IDs; validaciones + treatment vars; produced outside data-clean
│
└── file_to_header/               ⚠️ Legacy folder (can be removed)
```

### Delta Tables: `prd_mega.scolom15`

#### Existing tables

| Table | Description | Status |
| --- | --- | --- |
| `file_to_header_since2020` | Maps each raw file (since 2020) to its header group + broken flag | ✅ Active (rebuilt June 2026) |
| `tm_bronze` | Old name for since2020 bronze table but EMPTY | ⚠️ To rename and populate → `bronze_validaciones_since2020` |
| `bronze_raw_staging` | Auxiliary staging table for COPY INTO attempts | ⚠️ Legacy (failed approach, can be dropped) |
| `recargas_2017to2019_raw` | Raw recharges data 2017–2019 (28 columns) | ✅ Populated |

#### Tables to create

| Table | Description |
| --- | --- | 
| `file_to_header_from2016to2019` | Maps each raw file (2016–2019) to its header group | 
| `bronze_validaciones_since2020` | Unified bronze table for since2020 validaciones (replaces `tm_bronze`) | 
| `bronze_validaciones_from2016to2019` | Unified bronze table for 2016–2019 validaciones (numeric cards) | 
| `silver_validaciones_since2020` | Deduplicated, clean validaciones  | 
| `silver_validaciones_from2016to2019` | Deduplicated, clean validaciones  | 


## Code Structure

### Notebooks

#### Data Pipeline (since 2020)

| Notebook | Purpose | Schedule | Status |
| --- | --- | --- | --- |
| `data-fetch` | Downloads newest data from TM GCloud API to `/Data/` | Mondays (job) | ✅ Active |
| `data-organize-fromDocuments` | Moves old data from `/Documents/` to `/Workspace/Raw/` | Ran once | ✅ Done |
| `data-organize-fromData` | Moves new downloads from `/Data/` to `/Workspace/Raw/since2020/` | Mondays (job) | ✅ Active |
| `data-byheader-since2020` | Classifies files by header → `file_to_header_since2020` table (no file copying) | Mondays (job) | ✅ Active (refactored June 2026) |
| `data-ingest-bronze` | Reads files by header group, applies column mapping, writes to `bronze_since2020` Delta table | To be created | ⬜ TODO |
| `data-clean` | Old: imports CSVs → transforms → unions → dedup → parquet. New role: bronze→silver only | To be refactored | ⚠️ Partially obsolete, to refactor |

**To add: construction, Gold tables, samples**

#### Data Pipeline (from 2016 to 2019)

| Notebook | Purpose | Status |
| --- | --- | --- |
| `data-organize-fromDocuments` | Moved 2016–2019 data from `/Documents/` to `/Workspace/Raw/from2016to2019/` | ✅ Done |
| `data-byheader-from2016to2019` | Classify files by header (headers one–seven defined but never applied) | ⬜ TODO |
| `data-ingest-bronze-from2016to2019` | Ingest into `bronze_from2016to2019` table | ⬜ TODO |



**To add: cleaning, construction, Gold tables, samples**

#### Recharges Pipeline

| Notebook | Purpose | Status |
| --- | --- | --- |
| `recharges-clean-and-sample` | Loads 2017–2019 recharges from `/Documents/Recharges2017-2019/`, writes to `recargas_2017to2019_raw` Delta table | ✅ Done |
| `recharges-analyse` | Exploratory analysis of 2025 recharges data (reads directly from `/Data/Recargas/`) | ✅ Done |

#### Analysis & Construction (old workflow, uses old paths/parquets)

| Notebook | Purpose | Input | Output |
| --- | --- | --- | --- |
| `data-sample` | Samples cards (apoyo/subsidy users), creates treatment identifiers | Old parquets + byheader_dir | `Construct/*.csv` |
| `constr-treatment-groups` | Assigns treatment groups (hadlost23, hadlost24, hadkept, gained) per card based on subsidy status across periods | `Construct/` CSVs | `treatment_groups_sample10.csv` |
| `constr-monthly-panel-treatment` | Builds monthly panel: validaciones/trips per card-month, codes 0s, merges treatment status | `Construct/` CSVs | `panel_with_treatment_sample10.csv` |
| `plot` | Visualizes monthly validaciones with/without coding 0s by treatment group | Panel CSV | Plots |
| `data-analyze-ScenariosSDM` | Apoyo/subsidy card statistics (Dec 2023–May 2024) for SDM scenarios, monthly subsidy trip counts | Raw CSVs | `apoyo_stats_subsidy.csv`, `apoyo_total.csv` |

#### Exploration

| Notebook | Purpose |
| --- | --- |
| `explore-catalog` | Documents volume structure, lists files and folders, measures storage size |
| `explore-2025-data` | Quick analysis of 2025 data for treatment sample cards (reads from byheader_dir) |

### Utils Folder (`/utils/`)

| File | Purpose | Status |
| --- | --- | --- |
| `handle_files` | File utilities: `detect_encoding()`, `unzip_and_rename()`, `dbfs_file_exists()`, `detect_format()` | ✅ Needed (used by `data-byheader-since2020`) |
| `setup` | Spark session config, path definitions, old `spark_df_handler` class | ⚠️ To refactor — spark session setup is unnecessary on Databricks; paths are outdated (`/mnt/DAP/...`). Keep handler class but move to `spark_df_handler` |
| `packages` | Standard pip installs and imports | ⚠️ To refactor — bloated with unused packages; should only list what's actually needed per notebook |
| `windows` | PySpark window function definitions for time-series operations | ✅ Needed (used by analysis notebooks) |
| `generate_variables` | Variable generation logic for analysis (trip detection, transfer tagging) | ✅ Needed (used by `data-clean` and analysis) |
| `spark_df_handler` | Updated `spark_df_handler` class with per-header `transform()` methods mapping raw columns to unified schema | ✅ Needed (core of ingestion logic — will be used by `data-ingest-bronze`) |
| `import_test` | Simple import verification function | ❌ Legacy — can be removed |


## Workflow: OLD (to be replaced)

This workflow was built for **since 2020 data only**. It never processed 2016–2019 validaciones.

The old workflow mixed ingestion, cleaning, sampling, and analysis into a tightly coupled sequence:

1. **`data-fetch`** → downloads weekly data to `/Data/`
2. **`data-organize-fromData`** → moves to `/Workspace/Raw/since2020/`
3. **`data-byheader` (old)** → physically COPIED files into `/byheader_dir/` folders by header type (unnecessary 0.7 TB duplication)
4. **`data-clean`** → read from `/byheader_dir/`, transformed each header group using `spark_df_handler.transform()`, unioned all into one DataFrame (~4.37B rows), removed duplicates (~0.18%), saved as parquet files. Also attempted (failed) to use `COPY INTO` for Delta ingestion.
5. **`data-sample`** → read cleaned parquet, identified apoyo/subsidy cards, took 1% and 10% random samples, saved as CSV
6. **`constr-treatment-groups`** → for sampled cards, determined treatment group (hadlost23, hadlost24, hadkept, gained, adulto) based on monthly subsidy payment patterns across periods
7. **`constr-monthly-panel-treatment`** → built card-month panel with validaciones, trips, subsidy trips, coded 0s for inactive months, merged treatment status
8. **`plot`** → monthly time series of validaciones by treatment group

**Problems with old workflow:**
- Physical file duplication in `/byheader_dir/` (wastes 0.7 TB)
- No Delta tables — everything in parquet files without versioning or incrementality
- Cleaning and ingestion conflated in one notebook
- Not incremental — full reprocessing on every run
- `tm_bronze` table was never actually populated (COPY INTO failed)
- Old path references (`/mnt/DAP/...`, `/dbfs/...`) in analysis notebooks
- Only handles since2020 data — no path for 2016–2019


## Workflow: NEW (in progress)

Medallion architecture with Delta tables:

### Since 2020

1. **`data-fetch`** → downloads from TM API to `/Data/` _(weekly, automated)_
2. **`data-organize-fromData`** → moves to `/Workspace/Raw/since2020/` _(weekly, automated)_
3. **`data-byheader-since2020`** → classifies files by header, records in `file_to_header_since2020` Delta table. No file copying. _(weekly, automated)_
4. ⬜ **`data-ingest-bronze`** → reads `file_to_header_since2020`, loads files from original paths with correct delimiter/encoding per header, maps columns to unified schema, writes to `bronze_since2020` Delta table. Incremental via `input_file` tracking.
5. ⬜ **`data-clean-silver`** → reads bronze, deduplicates, validates, writes to silver Delta table
6. ⬜ **Gold table** → aggregated/sampled data for analysis export

### From 2016 to 2019

1. **`data-organize-fromDocuments`** → moved to `/Workspace/Raw/from2016to2019/` _(done once)_
2. ⬜ **`data-byheader-from2016to2019`** → classify ~2,671 files using headers one–seven, record in `file_to_header_from2016to2019` _(run once)_
3. ⬜ **`data-ingest-bronze-from2016to2019`** → ingest into `bronze_from2016to2019` Delta table _(run once)_
4. ⬜ **Silver / Gold** → same pattern as since2020

### Job: `data-fetch-organize` (ID: 461593863778684)

Runs every Monday at 3:36 AM ET. Tasks:
1. `data-fetch` → 2. `data-organize-fromData` → 3. `data-byheader-since2020`

All tasks use Git source (`github.com/dime-worldbank/ColombiaTransMilenio`, branch `main`) and cluster `ITSDA_DAP_TEAM_colombiaprojecttransmileniorawdata`.


## Status of 2016–2019 Validaciones Data

**Has any processing been done beyond organizing files?**

**Validaciones: No.** The 2016–2019 validaciones data has only been:
- ✅ Moved from `/Documents/2016data`...`2019data` to `/Workspace/Raw/from2016to2019/` (by `data-organize-fromDocuments`)

What has NOT been done:
- ⬜ Header classification (headers one–seven are defined in code but never applied)
- ⬜ Bronze table creation
- ⬜ Any cleaning or analysis

**Recharges: partially.** The `recharges-clean-and-sample` notebook loaded 2017–2019 recharges into `recargas_2017to2019_raw` Delta table.

**Validaciones (bogota-hdfs/sample-will): yes, partially.** `sample-will/` contains Aug 2017–May 2018 validaciones with numeric card IDs, joined with treatment variables. This is the only known prior processing of 2016–2019 validaciones — done ad hoc by Will, outside the main pipeline.

### Known challenges for 2016–2019 data:
- Mixed file formats: csv, txt, xls, xlsx, gz archives, zip files
- Semicolon and comma delimiters mixed across files
- Accented characters in column names (Spanish)
- Some files have date columns in different formats
- 30 subfolders for June 2018 trunk data (gz files per day)
- `2019ER10074` folder with different structure (zip files)


## Immediate Next Steps: 2016–2019 Data

The 2016–2019 validaciones pipeline is the current priority. Files are organized in `/Workspace/Raw/from2016to2019/` (~2,671 files). Headers one–seven are already defined in `utils/spark_df_handler`. Steps in order:

### 1. Create `data-byheader-from2016to2019`
Classify each file by header type, record in `file_to_header_from2016to2019` Delta table (mirrors `data-byheader-since2020`). Challenges:
- Mixed formats: csv, txt, xls, xlsx, gz, zip — need format detection before header read
- 30 `VALTRONCAL_DD-06-2018/` subfolders (each has daily gz files) → handle as batches
- `2019ER10074/` subfolder (zip files) → unzip first or read inside zip
- Semicolon and comma delimiters mixed across files

### 2. Create `data-ingest-bronze-from2016to2019`
Use the header mapping to load files from original paths, apply `spark_df_handler.transform()` per header group, write to `bronze_validaciones_from2016to2019` Delta table.

### 3. Create `data-clean-silver-from2016to2019` (or extend `data-clean`)
Deduplicate, standardize dates and encoding, write to `silver_validaciones_from2016to2019`.

### Open questions before starting
- Verify headers one–seven in `spark_df_handler` still cover all file variants in `from2016to2019`
- Decide how to handle xls/xlsx (different reader than csv/txt)
- Confirm whether gz files inside `VALTRONCAL_*` subfolders need a decompression step before classification


## Questions to ask TM
- Some dates have "UTC" at the end and some don't. Can we assume they are all in UTC? Or in Colombia time?
- Can the same card number, if not used for a while, be later assigned to another person?
