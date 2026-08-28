# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Bronze ingestion — from2016to2019 validaciones
# MAGIC
# MAGIC Reads every file listed in `prd_mega.scolom15.file_classification_from2016to2019` and writes a unified bronze Delta table (`bronze_validaciones_from2016to2019`).  
# MAGIC Only files with `classification_status = 'ready'` and `ingested_at IS NULL` are processed — making the run **incremental and safe to re-run**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Where the read settings come from — `file_classification_since2020`
# MAGIC
# MAGIC The classification notebook (`data-byheader-from2016to2019`) opened every raw file, read its first line, and stored the result in the control table. The ingestion here **must use those per-file values** — never assume or batch across different values. The relevant columns are:
# MAGIC
# MAGIC | Column | What it means | Used for |
# MAGIC |---|---|---|
# MAGIC | `header` | Exact fingerprint of the raw column names (e.g. `header_08`) | Outer loop — files with the same header have the same raw schema and can be loaded together |
# MAGIC | `transform_format` | Mapping recipe from raw columns → bronze schema (`format_6` or `format_7`) | Selects the right `_base_columns` transform |
# MAGIC | `encoding` | Character encoding detected per file (utf-8, ISO-8859-1, ascii, GB2312…) | Passed to `.option("encoding", ...)` — **each encoding gets its own `spark.read` call** |
# MAGIC | `delimiter` | Field separator detected per file (always `,` for since-2020, but read from the table) | Passed to `.option("sep", ...)` |
# MAGIC | `zipped` | 1 if the file is a ZIP archive, 0 otherwise | Determines plain-read vs ZIP-extract path |
# MAGIC | `archive_format` | `zip` or NULL — confirms archive type | Used alongside `zipped` |
# MAGIC | `inner_file_name` | Name of the CSV member inside the ZIP | Used to extract the correct inner file before reading |
# MAGIC | `classification_status` | `ready` / `broken` / `unsupported_empty_header` | Pre-filtered — only `ready` files reach the loop |
# MAGIC | `ingested_at` | NULL until this notebook sets it | Idempotency guard — re-running skips already-ingested files |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Two-level loop structure
# MAGIC
# MAGIC ```
# MAGIC for each header_group:                       ← same raw schema (same column names)
# MAGIC     for each (encoding, delimiter, zipped):  ← same read settings per file
# MAGIC         if zipped == 1:
# MAGIC             extract inner CSV to zip_extracted/ using inner_file_name  ← kept permanently
# MAGIC             spark.read the extracted file with this encoding + delimiter
# MAGIC         else:
# MAGIC             spark.read the plain CSV files with this encoding + delimiter
# MAGIC         apply transform (format_6 or format_7)
# MAGIC         write to bronze
# MAGIC         mark ingested_at in control table  ← only set AFTER bronze write commits
# MAGIC ```
# MAGIC
# MAGIC **Why two levels?**
# MAGIC - `header` groups files that share identical raw column names → safe to batch in one `spark.read.load([file1, file2, ...])`
# MAGIC - `(encoding, delimiter, zipped)` sub-groups within a header batch → Spark applies a **single encoding** to all files in a `.load()` call, so mixing encodings in one batch silently corrupts characters in files whose encoding doesn’t match
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## header vs transform_format — what is the difference?
# MAGIC
# MAGIC - **`header`** = the exact set of column names found in the raw file. Think of it as the file’s schema fingerprint. It determines which files can be read together in one batch (same schema = safe to batch).
# MAGIC - **`transform_format`** = a coarser label grouping headers by how their columns map to the unified bronze schema. Multiple headers can share one transform if their differences are handled by null fallbacks inside `_base_columns`:
# MAGIC   - `format_6` → headers 08, 09, 10, 15: no `Acceso_Estacion` column → `station_access` is NULL
# MAGIC   - `format_7` → headers 11, 12, 13, 14: have `Acceso_Estacion` → `station_access` is populated
# MAGIC   - Optional columns (`Tipo_Tarifa`, `Day_Group_Type`) are filled with NULL when absent in a header group
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## No-duplicate guarantee — how it works and what Delta provides
# MAGIC
# MAGIC ### What Delta Lake gives us for free
# MAGIC
# MAGIC Every `df.write.format("delta").mode("append").saveAsTable()` call is an **ACID transaction**: it either commits fully or not at all. There are no partial writes — if the job is cancelled mid-write, Delta’s transaction log rolls it back and the table is left exactly as it was before. This eliminates the classic “half a file got written” corruption problem.
# MAGIC
# MAGIC ### What Delta does NOT do by default
# MAGIC
# MAGIC Delta’s `mode("append")` does **not** deduplicate. If you call it twice with the same data, you get two copies. Delta has no built-in awareness of whether a source file has already been ingested — that is the application’s responsibility.
# MAGIC
# MAGIC ### Why we don’t use MERGE INTO
# MAGIC
# MAGIC The recommended Delta pattern for idempotent writes is `MERGE INTO` (upsert on a unique key). But transit validation data has no reliable business unique key — the same card, timestamp, machine, and value can legitimately appear twice in the source. Row-level deduplication would require a synthetic key and is out of scope for bronze (bronze = raw, unchanged source data).
# MAGIC
# MAGIC ### Our approach: file-level idempotency via `_source_file`
# MAGIC
# MAGIC Since source files are immutable (a classified CSV or extracted ZIP never changes), the right unit of idempotency is the **file**, not the row. The guarantee we enforce:
# MAGIC
# MAGIC > A source file is written to bronze **at most once**.
# MAGIC
# MAGIC This is implemented in two complementary layers:
# MAGIC
# MAGIC | Layer | Mechanism | Protects against |
# MAGIC |---|---|---|
# MAGIC | **Control table** (`ingested_at`) | Set after every successful write. Cell 6 filters to `ingested_at IS NULL` so already-ingested files never enter the loop. | Normal re-runs |
# MAGIC | **`already_in_bronze` set** | Built at startup by scanning all distinct `_source_file` values in bronze. Checked before every write — if already there, skip the write and only repair `ingested_at`. Updated after every successful write. | Write succeeded but `ingested_at` UPDATE failed; cancellation between write and update |
# MAGIC
# MAGIC The two layers are **independent** — if one fails, the other still catches the duplicate on the next run.
# MAGIC
# MAGIC ### Residual risk
# MAGIC
# MAGIC The one scenario neither layer catches is if `_source_file` in bronze is wrong or missing (e.g. `_metadata.file_path` returned an unexpected value). This would be a Spark bug, not an application bug.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC ## Known linter warnings — do not fix
# MAGIC
# MAGIC | Code | Line(s) | Why it fires | Why it is correct as-is |
# MAGIC |---|---|---|---|
# MAGIC | SCPAP001 | 46, 105 | `dfraw.columns` accessed inside the inner loop triggers one Analyze RPC per sub-batch | Intentional — each sub-batch has its own `dfraw`; the RPC must happen per iteration and is already at the minimum of one call. Cannot be moved outside the loop. |
# MAGIC | SCPAP005 | 23, 59, 137 | Linter sees lazy Spark transforms inside `try/except` and warns exceptions won’t be raised | False positive. Line 23: `.toPandas()` is an action and will raise if the table is missing — exactly what the `except` catches. Line 105: `dfraw.columns` is an eager Analyze RPC. Lines 137+: `df.count()` and `df.write` are actions inside the same `try` block and will raise correctly. |

# COMMAND ----------

# DBTITLE 1,Set Up
# MAGIC %pip install openpyxl xlrd tqdm python-calamine -q
# MAGIC
# MAGIC from pyspark.sql import functions as F
# MAGIC import pandas as pd
# MAGIC import io
# MAGIC import os
# MAGIC import time
# MAGIC import zipfile
# MAGIC import shutil
# MAGIC from pathlib import Path
# MAGIC from tqdm import tqdm

# COMMAND ----------

# DBTITLE 1,Set Catalog and Schema
# MAGIC %sql
# MAGIC USE CATALOG prd_mega;
# MAGIC USE SCHEMA scolom15;
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema;

# COMMAND ----------

# DBTITLE 1,Validate upgraded control table
CONTROL_TABLE = "prd_mega.scolom15.file_classification_from2016to2019"
BRONZE_TABLE  = "prd_mega.scolom15.bronze_validaciones_from2016to2019"

# COMMAND ----------

# DBTITLE 1,Create bronze table
# MAGIC %sql
# MAGIC -- Create bronze table if it does not exist
# MAGIC CREATE TABLE IF NOT EXISTS bronze_validaciones_from2016to2019 (
# MAGIC     fecha_transaccion      STRING     COMMENT 'Raw transaction timestamp string from source (e.g. "2020-01-15 10:30:00 UTC") — parsed and normalized in silver',
# MAGIC     emisor                STRING     COMMENT 'Fare system issuer (Emisor)',
# MAGIC     operator              STRING     COMMENT 'Transport operator (Operador)',
# MAGIC     line                  STRING     COMMENT 'Line or route identifier (Linea)',
# MAGIC     station               STRING     COMMENT 'Station or stop (Estacion_Parada)',
# MAGIC     station_access        STRING     COMMENT 'Access point within the station (Acceso_Estacion). NULL for format_6',
# MAGIC     machine               STRING     COMMENT 'Device / validator machine ID (Dispositivo) — cast to INT in silver',
# MAGIC     phase                 STRING     COMMENT 'Operational phase (Fase)',
# MAGIC     clearing_date         STRING     COMMENT 'Clearing date (Fecha_Clearing)',
# MAGIC     peak_hour             STRING     COMMENT 'Peak hour flag Y/N (Hora_Pico_SN)',
# MAGIC     vehicle_id            STRING     COMMENT 'Vehicle identifier (ID_Vehiculo)',
# MAGIC     route                 STRING     COMMENT 'Route (Ruta)',
# MAGIC     fare_type             STRING     COMMENT 'Fare type (Tipo_Tarifa). NULL for header_15 (column absent in source)',
# MAGIC     card_type             STRING     COMMENT 'Card type (Tipo_Tarjeta)',
# MAGIC     vehicle_type          STRING     COMMENT 'Vehicle type (Tipo_Vehiculo)',
# MAGIC     account_name          STRING     COMMENT 'Profile / account name (Nombre_Perfil)',
# MAGIC     cardnumber            STRING     COMMENT 'Card number — alphanumeric since 2020, kept as STRING',
# MAGIC     balance_before        STRING     COMMENT 'Balance before transaction in COP (Saldo_Previo_a_Transaccion) — cast to INT in silver',
# MAGIC     value                 STRING     COMMENT 'Transaction value in COP (Valor) — cast to INT in silver',
# MAGIC     balance_after         STRING     COMMENT 'Balance after transaction in COP (Saldo_Despues_Transaccion) — cast to INT in silver',
# MAGIC     system                STRING     COMMENT 'Validation system (Sistema)',
# MAGIC     day_group_type        STRING     COMMENT 'Day group type (Day_Group_Type). NULL for format_6 (column absent in source)',
# MAGIC     _source_file          STRING     COMMENT 'Source file path as returned by Spark input_file_name()',
# MAGIC     _header_group         STRING     COMMENT 'Header group from file_classification_since2020',
# MAGIC     _transform_format     STRING     COMMENT 'Transform applied during ingestion (format_6 or format_7)',
# MAGIC     _ingestion_ts         TIMESTAMP  COMMENT 'Timestamp when this batch was written to bronze'
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Profile pending and ingested files
# Load control table as pandas DataFrame for easier manipulation
classification_df = spark.table(CONTROL_TABLE).toPandas()

# Dimensions to group by for file statistics
stats_dims = [
    "classification_status",  # File classification outcome
    "header",                 # Raw schema fingerprint
    "transform_format",       # Mapping recipe to bronze schema
    "file_format",            # File format (csv, etc.)
    "archive_format",         # Archive type (zip or NULL)
    "zipped",                 # ZIP flag (1/0)
    "delimiter",              # Field separator
    "encoding",               # Character encoding
]

# Add ingestion state column: "pending" if not ingested, "already_ingested" otherwise
classification_df["ingestion_state"] = classification_df["ingested_at"].apply(
    lambda x: "pending" if pd.isna(x) else "already_ingested"
)

# Group files by ingestion state and relevant dimensions, count files per group
stats_df = (
    classification_df
    .groupby(["ingestion_state"] + stats_dims, dropna=False)
    .size()
    .reset_index(name="n_files")
)

# Compute total files per ingestion state
stats_df["state_total"] = stats_df.groupby("ingestion_state")["n_files"].transform("sum")

# Compute percentage of files within each state group
stats_df["pct_within_state"] = (100.0 * stats_df["n_files"] / stats_df["state_total"]).round(2)

# Show summary: total files by ingestion state
print("Files by ingestion state:")
display(
    stats_df[["ingestion_state", "n_files"]]
    .groupby("ingestion_state", as_index=False)
    .sum()
)

# Show detailed breakdown: file mix by state, header, zipped, encoding
print("Detailed file mix by state:")
display(
    stats_df
    .sort_values(["ingestion_state", "header", "zipped", "encoding"], na_position="last")
)

# COMMAND ----------

# DBTITLE 1,Load control table — pending files
# Reuse the full table already loaded in classification_df — just filter in pandas
control_df = classification_df[
    (classification_df["classification_status"] == "ready") &
    (classification_df["ingested_at"].isna())
].copy()

print(f"Files pending ingestion: {len(control_df)}")

# Show sample of pending files (10 random or all if fewer)
sample_n = min(10, len(control_df))
if sample_n > 0:
    sample = control_df.sample(n=sample_n, random_state=42)
    print(f"\nSample of {sample_n} pending file(s):")
    for _, row in sample.iterrows():
        print(f"  {row['raw_filepath'].split('/')[-1]}  [{row['header']}, {row['file_format']}, {row['delimiter']}, {row['encoding']}]")

# COMMAND ----------

# DBTITLE 1,Check if pending files are already in bronze
# Validate pending status: check that ALL files marked as pending (ingested_at IS NULL)
# truly have no rows in bronze. If any do, it means the control table is out of sync.
# Also fetches _ingestion_ts so the next cell can fix without re-querying.
from pyspark.sql import functions as F

pending_files = control_df["raw_filepath"].tolist()
print(f"Total files pending ingestion: {len(pending_files)}")
for f in pending_files:
    print(f"  {f.split('/')[-1]}")

print(f"\n{'='*70}")
print("Cross-checking bronze table for rows from ALL pending files...")
print("="*70)

filter_cond = F.lit(False)
for fp in pending_files:
    fname = fp.split('/')[-1]
    filter_cond = filter_cond | F.col("_source_file").contains(fname)

# Single query: count + max ingestion timestamp per file found in bronze
out_of_sync = (
    spark.table(BRONZE_TABLE)
    .filter(filter_cond)
    .groupBy("_source_file")
    .agg(
        F.count("*").alias("row_count"),
        F.max("_ingestion_ts").alias("actual_ingestion_ts"),
    )
    .orderBy("_source_file")
    .collect()
)

if out_of_sync:
    print(f"\n⚠️  {len(out_of_sync)} file(s) ARE already in bronze (control table out of sync!):")
    for row in out_of_sync:
        print(f"  {row['_source_file'].split('/')[-1]}: {row['row_count']:,} rows  (ingested {row['actual_ingestion_ts']})")
    print("\n→ Run the next cell to fix ingested_at in the control table.")
else:
    print(f"\n✅ All {len(pending_files)} pending files have zero rows in bronze — status is correct.")

# COMMAND ----------

# DBTITLE 1,Fix out-of-sync ingested_at from bronze timestamps
# Fix control table for files detected as out-of-sync by the previous cell.
# Uses `out_of_sync` (already has filenames + timestamps) — no re-query needed.

if not out_of_sync:
    print("✅ No out-of-sync files — nothing to fix. Skipping.")
else:
    print(f"⚠️  Fixing {len(out_of_sync)} file(s)...\n")

    for row in out_of_sync:
        fname = row["_source_file"].split("/")[-1]
        ts = row["actual_ingestion_ts"]
        spark.sql(f"""
            UPDATE {CONTROL_TABLE}
            SET ingested_at = TIMESTAMP '{ts}'
            WHERE raw_filepath LIKE '%{fname}'
              AND ingested_at IS NULL
        """)
        print(f"  {fname}: set ingested_at = {ts}")

    print(f"\n✅ Updated ingested_at for {len(out_of_sync)} file(s) in {CONTROL_TABLE}.")
    print(f"\n{'='*70}")
    print("⚠️  NOW RE-RUN CELLS 6 and 7 to refresh classification_df and control_df!")
    print("   The in-memory DataFrames still have stale ingested_at=NULL values.")
    print(f"{'='*70}")

# COMMAND ----------

# Inspect how files are distributed across headers 
tot_file = 0 
for h in classification_df["header"].unique():
    number_with_header = classification_df[classification_df["header"] == h].shape[0]
    print(f"Number of files with header {h}: {number_with_header}")
    tot_file = tot_file + number_with_header

print(f"Total number of files: {tot_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC The headers used are:  
# MAGIC - header_one: 1091
# MAGIC - header_two: 310
# MAGIC - header_three: 309
# MAGIC - header_four: 516
# MAGIC - header_five: 547
# MAGIC - header_six: 179
# MAGIC - header_seven: 29
# MAGIC - header header_17: 8
# MAGIC - header header_18: 62
# MAGIC - header_19: 2
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC |header | columns |
# MAGIC | -------- | -------- |
# MAGIC | 'header_one' | 'Fecha de Liquidación', 'Fecha de Uso', 'Day Group Type', 'Hora Pico S/N', 'Fase', 'Emisor', 'Operador', 'Línea', 'Estación', 'Acceso de Estación', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Número de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transacción', 'Valor', 'Saldo Después de Transacción'|
# MAGIC | 'header_two' | ['Fecha de Clearing;Fecha de Transaccion;Hora Pico SN;Fase;Emisor;Operador;Linea;Ruta;Parada;Tipo Vehiculo;ID Vehiculo;Dispositivo;Tipo Tarjeta;Nombre de Perfil;Numero Tarjeta;Tipo de Tarifa;Saldo Previo a Transaccion;Valor;Saldo Despues de Transaccion',] |
# MAGIC | 'header_three' | ['Fecha de Liquidación', 'Fecha de Uso', 'Day Group Type', 'Hora Pico S/N', 'Fase', 'Emisor', 'Operador', 'Línea', 'Ruta', 'Parada', 'Tipo de Vehículo', 'ID de Vehículo', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Número de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transacción', 'Valor', 'Saldo Después de Transacción'] |
# MAGIC | 'header_four'| ['Fecha de Clearing;Fecha de Transaccion;Day Group Type;Hora Pico SN;Fase;Emisor;Operador;Linea;Estacion;Acceso de Estación;Dispositivo;Tipo de Tarjeta;Nombre de Perfil;Numero de Tarjeta;Tipo de Tarifa;Saldo Previo a Transaccion;Valor;Saldo Despues de Transaccion',] |
# MAGIC | 'header_five' | ['Fecha de Clearing;Fecha de Transaccion;Hora Pico SN;Fase;Emisor;Operador;Linea;Ruta;Parada;Tipo Vehiculo;ID Vehiculo;Dispositivo;Tipo Tarjeta;Nombre de Perfil;Numero Tarjeta;Tipo de Tarifa;Saldo Previo a Transaccion;Valor;Saldo Despues de Transaccion;Ruta_Modificada;Linea_Modificada;Cenefa;Parada_Modificada',] |
# MAGIC | 'header_six' | ['Fecha de Clearing', 'Fecha de Transaccion', 'DAY_GROUP_CD', 'Hora Pico SN', 'Fase', 'Emisor', 'Operador', 'Linea', 'Ruta', 'Parada', 'Tipo Vehiculo', 'ID Vehiculo', 'Dispositivo', 'Tipo Tarjeta', 'Nombre de Perfil', 'Numero Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transaccion', 'Valor', 'Saldo Despues de Transaccion'] |
# MAGIC | 'header_seven' | ['Fecha de Clearing', 'Fecha de Transaccion', 'DAY_GROUP_CD', 'Hora Pico SN', 'Emisor', 'Operador', 'Linea', 'Estacion', 'Acceso de Estación', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Numero de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transaccion', 'Valor', 'Saldo Despues de Transaccion'] |
# MAGIC | 'header_17' | ['fechaclearing', 'fechatransaccion', 'daygrouptype', 'fase', 'emisor', 'operador', 'linea', 'estacion', 'accesoestacion', 'dispositivo', 'tipotarjeta', 'nombreperfil', 'nrotarjeta', 'saldoprevioatransaccion', 'valor', 'saldodespuesdetransaccion'] |
# MAGIC |'header_18' |  ['Fecha de Clearing', 'Fecha de Transaccion', 'Hora Pico SN', 'Emisor', 'Operador', 'Linea', 'Estacion', 'Acceso de Estación', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Numero de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transaccion', 'Valor', 'Saldo Despues de Transaccion'] |
# MAGIC | 'header_19': |  ['', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'] |
# MAGIC

# COMMAND ----------

# DBTITLE 1,Transform definitions — raw columns to bronze schema
# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORM DEFINITIONS — raw column names → unified bronze schema
#
# Each header group has different column names (accented vs plain, spaces vs
# underscores, etc.). This cell defines how to map each header's raw columns
# to the single bronze schema.
#
# Structure:
#   HEADER_MAPPINGS  = {header_name: {bronze_col: raw_col_name_or_None}}
#   apply_mapping()  = helper that builds the .select() expressions
#   TRANSFORMS       = {header_name: function(df) → transformed df}
#                      This is what the ingestion loop (cell 11) uses.
# ─────────────────────────────────────────────────────────────────────────────

# Bronze target columns (order must match the CREATE TABLE in cell 5)
BRONZE_COLS = [
    'fecha_transaccion', 'emisor', 'operator', 'line', 'station',
    'station_access', 'machine', 'phase', 'clearing_date', 'peak_hour',
    'vehicle_id', 'route', 'fare_type', 'card_type', 'vehicle_type',
    'account_name', 'cardnumber', 'balance_before', 'value', 'balance_after',
    'system', 'day_group_type',
]

# ── Per-header column mappings ───────────────────────────────────────────────
# Key   = bronze column name
# Value = raw column name in the source file, or None if that column is absent
#         (absent columns will be filled with NULL)

HEADER_MAPPINGS = {

    # ── header_one (732 files) ─────────────────────────────────────────────
    # Troncal, comma-delimited, accented column names.
    # Has station + access. No vehicle_id, route, vehicle_type, system.
    'header_one': {
        'fecha_transaccion': 'Fecha de Uso',
        'emisor':            'Emisor',
        'operator':          'Operador',
        'line':              'Línea',
        'station':           'Estación',
        'station_access':    'Acceso de Estación',
        'machine':           'Dispositivo',
        'phase':             'Fase',
        'clearing_date':     'Fecha de Liquidación',
        'peak_hour':         'Hora Pico S/N',
        'vehicle_id':        None,
        'route':             None,
        'fare_type':         'Tipo de Tarifa',
        'card_type':         'Tipo de Tarjeta',
        'vehicle_type':      None,
        'account_name':      'Nombre de Perfil',
        'cardnumber':        'Número de Tarjeta',
        'balance_before':    'Saldo Previo a Transacción',
        'value':             'Valor',
        'balance_after':     'Saldo Después de Transacción',
        'system':            None,
        'day_group_type':    'Day Group Type',
    },

    # ── header_two (310 files) ─────────────────────────────────────────────
    # Zonal, semicolon-delimited, no accents.
    # Has route, parada, vehicle. No station_access, day_group_type, system.
    'header_two': {
        'fecha_transaccion': 'Fecha de Transaccion',
        'emisor':            'Emisor',
        'operator':          'Operador',
        'line':              'Linea',
        'station':           'Parada',
        'station_access':    None,
        'machine':           'Dispositivo',
        'phase':             'Fase',
        'clearing_date':     'Fecha de Clearing',
        'peak_hour':         'Hora Pico SN',
        'vehicle_id':        'ID Vehiculo',
        'route':             'Ruta',
        'fare_type':         'Tipo de Tarifa',
        'card_type':         'Tipo Tarjeta',
        'vehicle_type':      'Tipo Vehiculo',
        'account_name':      'Nombre de Perfil',
        'cardnumber':        'Numero Tarjeta',
        'balance_before':    'Saldo Previo a Transaccion',
        'value':             'Valor',
        'balance_after':     'Saldo Despues de Transaccion',
        'system':            None,
        'day_group_type':    None,
    },

    # ── header_three (10 files) ────────────────────────────────────────────
    # Zonal, comma-delimited, accented column names.
    # Has route, parada, vehicle. No station_access, system.
    'header_three': {
        'fecha_transaccion': 'Fecha de Uso',
        'emisor':            'Emisor',
        'operator':          'Operador',
        'line':              'Línea',
        'station':           'Parada',
        'station_access':    None,
        'machine':           'Dispositivo',
        'phase':             'Fase',
        'clearing_date':     'Fecha de Liquidación',
        'peak_hour':         'Hora Pico S/N',
        'vehicle_id':        'ID de Vehículo',
        'route':             'Ruta',
        'fare_type':         'Tipo de Tarifa',
        'card_type':         'Tipo de Tarjeta',
        'vehicle_type':      'Tipo de Vehículo',
        'account_name':      'Nombre de Perfil',
        'cardnumber':        'Número de Tarjeta',
        'balance_before':    'Saldo Previo a Transacción',
        'value':             'Valor',
        'balance_after':     'Saldo Después de Transacción',
        'system':            None,
        'day_group_type':    'Day Group Type',
    },

    # ── header_four (516 files) ────────────────────────────────────────────
    # Troncal, semicolon-delimited, no accents (except 'Estación').
    # Has station + access. No vehicle_id, route, vehicle_type, system.
    'header_four': {
        'fecha_transaccion': 'Fecha de Transaccion',
        'emisor':            'Emisor',
        'operator':          'Operador',
        'line':              'Linea',
        'station':           'Estacion',
        'station_access':    'Acceso de Estación',
        'machine':           'Dispositivo',
        'phase':             'Fase',
        'clearing_date':     'Fecha de Clearing',
        'peak_hour':         'Hora Pico SN',
        'vehicle_id':        None,
        'route':             None,
        'fare_type':         'Tipo de Tarifa',
        'card_type':         'Tipo de Tarjeta',
        'vehicle_type':      None,
        'account_name':      'Nombre de Perfil',
        'cardnumber':        'Numero de Tarjeta',
        'balance_before':    'Saldo Previo a Transaccion',
        'value':             'Valor',
        'balance_after':     'Saldo Despues de Transaccion',
        'system':            None,
        'day_group_type':    'Day Group Type',
    },

    # ── header_five (547 files) ────────────────────────────────────────────
    # Zonal, semicolon-delimited, no accents.
    # Same as header_two + 4 extra columns (Ruta_Modificada, Linea_Modificada,
    # Cenefa, Parada_Modificada) which are dropped during transform.
    # No station_access, day_group_type, system.
    'header_five': {
        'fecha_transaccion': 'Fecha de Transaccion',
        'emisor':            'Emisor',
        'operator':          'Operador',
        'line':              'Linea',
        'station':           'Parada',
        'station_access':    None,
        'machine':           'Dispositivo',
        'phase':             'Fase',
        'clearing_date':     'Fecha de Clearing',
        'peak_hour':         'Hora Pico SN',
        'vehicle_id':        'ID Vehiculo',
        'route':             'Ruta',
        'fare_type':         'Tipo de Tarifa',
        'card_type':         'Tipo Tarjeta',
        'vehicle_type':      'Tipo Vehiculo',
        'account_name':      'Nombre de Perfil',
        'cardnumber':        'Numero Tarjeta',
        'balance_before':    'Saldo Previo a Transaccion',
        'value':             'Valor',
        'balance_after':     'Saldo Despues de Transaccion',
        'system':            None,
        'day_group_type':    None,
    },

    # ── header_six (121 files) ─────────────────────────────────────────────
    # Zonal, comma-delimited, no accents.
    # Has route, parada, vehicle. Column 'DAY_GROUP_CD' instead of 'Day Group Type'.
    # No station_access, system.
    'header_six': {
        'fecha_transaccion': 'Fecha de Transaccion',
        'emisor':            'Emisor',
        'operator':          'Operador',
        'line':              'Linea',
        'station':           'Parada',
        'station_access':    None,
        'machine':           'Dispositivo',
        'phase':             'Fase',
        'clearing_date':     'Fecha de Clearing',
        'peak_hour':         'Hora Pico SN',
        'vehicle_id':        'ID Vehiculo',
        'route':             'Ruta',
        'fare_type':         'Tipo de Tarifa',
        'card_type':         'Tipo Tarjeta',
        'vehicle_type':      'Tipo Vehiculo',
        'account_name':      'Nombre de Perfil',
        'cardnumber':        'Numero Tarjeta',
        'balance_before':    'Saldo Previo a Transaccion',
        'value':             'Valor',
        'balance_after':     'Saldo Despues de Transaccion',
        'system':            None,
        'day_group_type':    'DAY_GROUP_CD',
    },

    # ── header_seven (28 files) ────────────────────────────────────────────
    # Troncal, comma-delimited, no accents (except 'Estación').
    # Has station + access. No Fase, vehicle_id, route, vehicle_type, system.
    'header_seven': {
        'fecha_transaccion': 'Fecha de Transaccion',
        'emisor':            'Emisor',
        'operator':          'Operador',
        'line':              'Linea',
        'station':           'Estacion',
        'station_access':    'Acceso de Estación',
        'machine':           'Dispositivo',
        'phase':             None,
        'clearing_date':     'Fecha de Clearing',
        'peak_hour':         'Hora Pico SN',
        'vehicle_id':        None,
        'route':             None,
        'fare_type':         'Tipo de Tarifa',
        'card_type':         'Tipo de Tarjeta',
        'vehicle_type':      None,
        'account_name':      'Nombre de Perfil',
        'cardnumber':        'Numero de Tarjeta',
        'balance_before':    'Saldo Previo a Transaccion',
        'value':             'Valor',
        'balance_after':     'Saldo Despues de Transaccion',
        'system':            None,
        'day_group_type':    'DAY_GROUP_CD',
    },

    # ── header_17 (8 files) ────────────────────────────────────────────────
    # Troncal, semicolon-delimited, ALL LOWERCASE no spaces.
    # Has station + access. No peak_hour, vehicle_id, route, fare_type,
    # vehicle_type, system.
    'header_17': {
        'fecha_transaccion': 'fechatransaccion',
        'emisor':            'emisor',
        'operator':          'operador',
        'line':              'linea',
        'station':           'estacion',
        'station_access':    'accesoestacion',
        'machine':           'dispositivo',
        'phase':             'fase',
        'clearing_date':     'fechaclearing',
        'peak_hour':         None,
        'vehicle_id':        None,
        'route':             None,
        'fare_type':         None,
        'card_type':         'tipotarjeta',
        'vehicle_type':      None,
        'account_name':      'nombreperfil',
        'cardnumber':        'nrotarjeta',
        'balance_before':    'saldoprevioatransaccion',
        'value':             'valor',
        'balance_after':     'saldodespuesdetransaccion',
        'system':            None,
        'day_group_type':    'daygrouptype',
    },

    # ── header_18 (31 files) ───────────────────────────────────────────────
    # Troncal, comma-delimited, no accents (except 'Estación').
    # Has station + access. No Fase, Day Group Type, vehicle_id, route,
    # vehicle_type, system.
    'header_18': {
        'fecha_transaccion': 'Fecha de Transaccion',
        'emisor':            'Emisor',
        'operator':          'Operador',
        'line':              'Linea',
        'station':           'Estacion',
        'station_access':    'Acceso de Estación',
        'machine':           'Dispositivo',
        'phase':             None,
        'clearing_date':     'Fecha de Clearing',
        'peak_hour':         'Hora Pico SN',
        'vehicle_id':        None,
        'route':             None,
        'fare_type':         'Tipo de Tarifa',
        'card_type':         'Tipo de Tarjeta',
        'vehicle_type':      None,
        'account_name':      'Nombre de Perfil',
        'cardnumber':        'Numero de Tarjeta',
        'balance_before':    'Saldo Previo a Transaccion',
        'value':             'Valor',
        'balance_after':     'Saldo Despues de Transaccion',
        'system':            None,
        'day_group_type':    None,
    },

    # ── header_19 (2 files) ────────────────────────────────────────────────
    # Dual (troncal+zonal combined export), comma-delimited, underscore-separated.
    # Has station, vehicle, route, system. First column is an empty index (dropped).
    # No station_access, fare_type, day_group_type.
    'header_19': {
        'fecha_transaccion': 'Fecha_Transaccion',
        'emisor':            'Emisor',
        'operator':          'Operador',
        'line':              'Linea',
        'station':           'Estacion_Parada',
        'station_access':    None,
        'machine':           'Dispositivo',
        'phase':             'Fase',
        'clearing_date':     'Fecha_Clearing',
        'peak_hour':         'Hora_Pico_SN',
        'vehicle_id':        'ID_Vehiculo',
        'route':             'Ruta',
        'fare_type':         None,
        'card_type':         'Tipo_Tarjeta',
        'vehicle_type':      'Tipo_Vehiculo',
        'account_name':      'Nombre_Perfil',
        'cardnumber':        'Numero_Tarjeta',
        'balance_before':    'Saldo_Previo_a_Transaccion',
        'value':             'Valor',
        'balance_after':     'Saldo_Despues_Transaccion',
        'system':            'Sistema',
        'day_group_type':    None,
    },
}


# ── Helper function: convert a mapping dict into Spark select expressions ────
def _apply_mapping(df, mapping):
    """
    Given a DataFrame and a {bronze_col: raw_col} mapping, produce a new
    DataFrame with the bronze schema. Columns mapped to None become NULL.
    """
    exprs = []
    for bronze_col in BRONZE_COLS:
        raw_col = mapping.get(bronze_col)
        if raw_col is None:
            # Column doesn't exist in this header → fill with NULL
            exprs.append(F.lit(None).cast('string').alias(bronze_col))
        else:
            # Map raw column name → bronze alias (cast to string for uniformity)
            exprs.append(F.col(f'`{raw_col}`').cast('string').alias(bronze_col))
    return df.select(exprs)


# ── Build the TRANSFORMS dict used by the ingestion loop (cell 11) ───────────
# Key   = transform_format value from the classification table
#         (in from2016to2019, transform_format == header name)
# Value = function(df, df_cols) → transformed DataFrame
TRANSFORMS = {
    header_name: (lambda mapping: lambda df, df_cols: _apply_mapping(df, mapping))(mapping)
    for header_name, mapping in HEADER_MAPPINGS.items()
}

print(f"Defined transforms for {len(TRANSFORMS)} header groups:")
for k in sorted(TRANSFORMS.keys()):
    n_nulls = sum(1 for v in HEADER_MAPPINGS[k].values() if v is None)
    print(f"  {k}: {22 - n_nulls} mapped cols, {n_nulls} NULLs")

# COMMAND ----------

# DBTITLE 1,Ingestion loop — read, transform, write to bronze
# ─────────────────────────────────────────────────────────────────────────────
# INGESTION LOOP
#
# For each header group + encoding/delimiter/zipped combination:
#   1. Resolve file paths (extract zips if needed, pass gz/csv directly)
#   2. Filter out files already in bronze (duplicate guard)
#   3. Read with Spark CSV reader (handles .gz decompression automatically)
#   4. Apply the column mapping transform
#   5. Write to bronze Delta table
#   6. Mark files as ingested in the control table
# ─────────────────────────────────────────────────────────────────────────────

# ── Encoding fallback map ────────────────────────────────────────────────────
# Some encodings detected by chardet are not recognized by Spark/Java.
# Map them to safe alternatives that cover the same character range.
ENCODING_FALLBACK = {
    'TIS-620':    'ISO-8859-1',   # Thai encoding → latin-1 (preserves raw bytes)
    'windows-874': 'ISO-8859-1',  # Thai Windows variant
    'MacCyrillic': 'ISO-8859-5',  # Mac Cyrillic → ISO Cyrillic
}

def _resolve_encoding(enc):
    """Return a Spark-compatible encoding, falling back if needed."""
    if enc in ENCODING_FALLBACK:
        print(f"      ⚠ Encoding '{enc}' not supported by Spark → using '{ENCODING_FALLBACK[enc]}'")
        return ENCODING_FALLBACK[enc]
    # UTF-8-SIG (UTF-8 with BOM): Spark handles BOM automatically with 'utf-8'
    if enc and str(enc).upper() == 'UTF-8-SIG':
        return 'utf-8'
    return enc


# ── Permanent cache for ZIP-extracted CSVs ───────────────────────────────────
# Files are kept across runs so re-ingestion (e.g. after a bronze drop) skips
# re-extraction.
ZIP_EXTRACT_ROOT = "/Volumes/prd_csc_mega/sColom15/vColom15/Workspace/zip_extracted"
os.makedirs(ZIP_EXTRACT_ROOT, exist_ok=True)

ingestion_log = []
run_start = time.time()

# ── Duplicate guard ──────────────────────────────────────────────────────────
# Load all _source_file paths already written to bronze.
# Protects against: write succeeded but ingested_at UPDATE failed, or
# mid-write cancellation. Updated in-memory after every successful write.
try:
    _bronze_files = (
        spark.table(BRONZE_TABLE)
        .select("_source_file").distinct()
        .toPandas()["_source_file"].tolist()
    )
    already_in_bronze = set(_bronze_files)
    print(f"Bronze already contains {len(already_in_bronze):,} distinct source file paths")
except Exception:
    already_in_bronze = set()
    print("Bronze table is empty or does not exist yet — will be created on first write")


# ── Main loop: one iteration per header group ────────────────────────────────
for header_group in sorted(control_df['header'].dropna().unique()):
    group = control_df[control_df['header'] == header_group].copy()

    # Validate: each header group should have exactly one transform_format
    assert group['transform_format'].nunique(dropna=False) == 1, (
        f"[{header_group}] mixed transform_format values: {group['transform_format'].unique().tolist()}"
    )
    transform_fmt = group['transform_format'].iloc[0]

    # Skip header groups with no transform defined
    if transform_fmt not in TRANSFORMS:
        print(f"Skipping {header_group}: no transform defined for '{transform_fmt}'")
        ingestion_log.append({'header': header_group, 'encoding': None, 'zipped': None,
                              'files': len(group), 'rows': 0, 'status': 'skipped_no_transform'})
        continue

    print(f"\n{'='*60}")
    print(f"[{header_group}] {len(group)} files | transform={transform_fmt}")

    # ── Sub-group by (encoding, delimiter, zipped) ───────────────────────
    # Spark applies a SINGLE encoding to all files in one .load() call, so
    # we cannot mix encodings in one batch.
    for (encoding, delimiter, zipped), subgroup in group.groupby(
        ['encoding', 'delimiter', 'zipped'], dropna=False
    ):
        original_paths = subgroup['raw_filepath'].tolist()
        inner_names = (
            subgroup['inner_file_name'].tolist()
            if 'inner_file_name' in subgroup.columns
            else [None] * len(subgroup)
        )
        n_files = len(original_paths)
        zipped_int = int(zipped) if pd.notna(zipped) else 0
        spark_encoding = _resolve_encoding(encoding)

        print(f"  [{spark_encoding}] zipped={zipped_int} | {n_files} files | sep='{delimiter}'")

        # ── Step 1: Resolve read paths ───────────────────────────────────
        if zipped_int == 1:
            # Extract inner CSV from each zip to permanent cache
            extracted_paths = []
            n_extracted = 0
            n_cached = 0
            try:
                for zip_path, inner_name in tqdm(
                    zip(original_paths, inner_names),
                    total=n_files,
                    desc=f"    {header_group} [{spark_encoding}]",
                    unit="zip",
                ):
                    local_zip_path = '/' + zip_path.lstrip('/')
                    target_name = f"{Path(zip_path).stem}__{Path(inner_name or 'inner.csv').name}"
                    target_path = os.path.join(ZIP_EXTRACT_ROOT, target_name)
                    if os.path.exists(target_path):
                        n_cached += 1
                    else:
                        with zipfile.ZipFile(local_zip_path, 'r') as zf:
                            members = zf.namelist()
                            chosen_member = inner_name if inner_name in members else members[0]
                            with zf.open(chosen_member) as src, open(target_path, 'wb') as dst:
                                shutil.copyfileobj(src, dst)
                        n_extracted += 1
                    extracted_paths.append(target_path)
                print(f"    {n_extracted} extracted, {n_cached} already cached")
                read_paths = extracted_paths
            except Exception as e:
                print(f"    → ZIP EXTRACT ERROR: {e}")
                ingestion_log.append({'header': header_group, 'encoding': encoding,
                                      'zipped': zipped_int, 'files': n_files, 'rows': 0,
                                      'status': f'error_zip: {e}'})
                continue
        else:
            # Plain CSV/TXT/GZ files: Spark reads them directly.
            # .gz files are decompressed transparently by Spark's CSV reader.
            read_paths = ['/' + p.lstrip('/') for p in original_paths]

        # ── Step 2: Duplicate guard (per-file, not per-batch) ────────────
        # FIX: The old code skipped the ENTIRE batch if ANY file was already
        # in bronze. Now we filter out only the overlapping files and proceed
        # with the rest. This prevents data loss when a batch was partially
        # ingested (e.g. due to a crash mid-write).
        overlap_mask = [p in already_in_bronze for p in read_paths]
        n_overlap = sum(overlap_mask)

        if n_overlap == n_files:
            # ALL files already in bronze → skip entirely, just repair ingested_at
            print(f"    → All {n_files} files already in bronze — repairing ingested_at only")
            processed_df = spark.createDataFrame(
                [(p,) for p in original_paths], ["raw_filepath"]
            )
            batch_suffix = f"{header_group}_{encoding}_{zipped_int}".replace('-', '_').replace('.', '_')
            batch_view = f"_ingested_{batch_suffix}"
            processed_df.createOrReplaceTempView(batch_view)
            spark.sql(f"""
                UPDATE {CONTROL_TABLE}
                SET ingested_at = current_timestamp()
                WHERE raw_filepath IN (SELECT raw_filepath FROM {batch_view})
            """)
            ingestion_log.append({'header': header_group, 'encoding': encoding,
                                  'zipped': zipped_int, 'files': n_files,
                                  'rows': 0, 'status': 'already_in_bronze'})
            continue

        if n_overlap > 0:
            # PARTIAL overlap: filter out already-ingested files, keep the new ones
            print(f"    → {n_overlap}/{n_files} already in bronze — ingesting remaining {n_files - n_overlap}")
            # Filter read_paths and original_paths in parallel
            read_paths = [p for p, dup in zip(read_paths, overlap_mask) if not dup]
            original_paths = [p for p, dup in zip(original_paths, overlap_mask) if not dup]
            n_files = len(read_paths)

        # ── Step 3: Read raw files ─────────────────────────────────────────
        # Determine if this sub-group contains Excel files.
        # Excel files have encoding=None and delimiter=None in the control table.
        file_formats_in_batch = subgroup['file_format'].unique().tolist()
        is_excel_batch = any(fmt in ('xls', 'xlsx') for fmt in file_formats_in_batch)

        if is_excel_batch:
            # ── Excel path: read each file, transform, write IMMEDIATELY ──
            # Spark's native CSV reader cannot handle binary Excel formats.
            # These files can be very large (100MB+), so we CANNOT accumulate
            # them in memory — that causes Java heap space OOM errors.
            #
            # Approach: read one file at a time with pandas, convert to Spark,
            # apply the transform, and write directly to bronze. This keeps
            # only one file in memory at any given time.
            excel_total_rows = 0
            excel_files_ok = 0
            excel_errors = []

            for fpath in tqdm(read_paths, desc=f"    {header_group} [excel]", unit="file"):
                try:
                    local_path = '/' + fpath.lstrip('/')
                    pdf = pd.read_excel(local_path, dtype=str, engine='calamine')
                    row_count_one = len(pdf)
                    dfraw_one = spark.createDataFrame(pdf)
                    df_one = (
                        TRANSFORMS[transform_fmt](dfraw_one, dfraw_one.columns)
                        .withColumns({
                            "_source_file": F.lit(fpath),
                            "_header_group": F.lit(header_group),
                            "_transform_format": F.lit(transform_fmt),
                            "_ingestion_ts": F.current_timestamp(),
                        })
                    )
                    # Write this single file to bronze immediately
                    df_one.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
                    excel_total_rows += row_count_one
                    excel_files_ok += 1
                    already_in_bronze.add(fpath)
                except Exception as e:
                    print(f"\n      ⚠ Failed: {os.path.basename(fpath)} — {e}")
                    excel_errors.append((fpath, str(e)))

            # Mark successfully ingested files in control table
            ingested_paths = [p for p in original_paths
                              if '/' + p.lstrip('/') in already_in_bronze
                              or p in already_in_bronze]
            if ingested_paths:
                processed_df = spark.createDataFrame(
                    [(p,) for p in ingested_paths], ["raw_filepath"]
                )
                batch_suffix = (
                    f"{header_group}_excel_{zipped_int}"
                    .replace('-', '_').replace('.', '_')
                )
                batch_view = f"_ingested_{batch_suffix}"
                processed_df.createOrReplaceTempView(batch_view)
                spark.sql(f"""
                    UPDATE {CONTROL_TABLE}
                    SET ingested_at = current_timestamp()
                    WHERE raw_filepath IN (SELECT raw_filepath FROM {batch_view})
                """)

            elapsed = (time.time() - run_start) / 60
            print(
                f"    → {excel_files_ok}/{n_files} files OK | "
                f"{excel_total_rows:,} rows | {len(excel_errors)} errors | "
                f"elapsed {elapsed:.1f} min"
            )
            status = 'ok' if not excel_errors else f'partial ({len(excel_errors)} errors)'
            ingestion_log.append({'header': header_group, 'encoding': encoding,
                                  'zipped': zipped_int, 'files': n_files,
                                  'rows': excel_total_rows, 'status': status})
            # Skip the shared Step 5 below (we already wrote inside the loop)
            continue

        else:
            # ── CSV/TXT/GZ path: Spark reads them directly ──
            # Spark automatically handles:
            #   - .gz files: gzip decompression (not splittable, 1 task per file)
            #   - .csv/.txt: direct read
            #   - extracted zips: plain CSV after extraction
            dfraw = (
                spark.read.format("csv")
                .option("header", "true")
                .option("sep", delimiter)
                .option("encoding", spark_encoding)
                .load(read_paths)
            )

            # Validate that the read succeeded by triggering an action
            try:
                dfraw_cols = dfraw.columns  # Analyze RPC on Spark Connect
            except Exception as e:
                print(f"    → READ ERROR: {e}")
                ingestion_log.append({'header': header_group, 'encoding': encoding,
                                      'zipped': zipped_int, 'files': n_files,
                                      'rows': 0, 'status': f'error_read: {e}'})
                continue

            # ── Step 4: Apply column mapping transform ─────────────────
            df = (
                TRANSFORMS[transform_fmt](dfraw, dfraw_cols)
                .withColumns({
                    "_source_file": F.col("_metadata.file_path"),
                    "_header_group": F.lit(header_group),
                    "_transform_format": F.lit(transform_fmt),
                    "_ingestion_ts": F.current_timestamp(),
                })
            )

        # ── Step 5: Write to bronze + update control table ───────────────
        try:
            t0 = time.time()
            row_count = df.count()  # action — triggers actual read + transform
            t_count = time.time() - t0

            t0 = time.time()
            (
                df.write
                .format("delta")
                .mode("append")
                .saveAsTable(BRONZE_TABLE)
            )
            t_write = time.time() - t0

            # Update duplicate guard for remaining batches in this run
            already_in_bronze.update(read_paths)

            # ── Step 6: Mark files as ingested in control table ──────────
            processed_df = spark.createDataFrame(
                [(p,) for p in original_paths], ["raw_filepath"]
            )
            batch_suffix = (
                f"{header_group}_{encoding}_{zipped_int}"
                .replace('-', '_').replace('.', '_')
            )
            batch_view = f"_ingested_{batch_suffix}"
            processed_df.createOrReplaceTempView(batch_view)
            spark.sql(f"""
                UPDATE {CONTROL_TABLE}
                SET ingested_at = current_timestamp()
                WHERE raw_filepath IN (SELECT raw_filepath FROM {batch_view})
            """)

            elapsed = (time.time() - run_start) / 60
            print(
                f"    → {row_count:,} rows | count {t_count:.0f}s | "
                f"write {t_write:.0f}s | total elapsed {elapsed:.1f} min"
            )
            ingestion_log.append({'header': header_group, 'encoding': encoding,
                                  'zipped': zipped_int, 'files': n_files,
                                  'rows': row_count, 'status': 'ok'})

        except Exception as e:
            print(f"    → WRITE ERROR: {e}")
            ingestion_log.append({'header': header_group, 'encoding': encoding,
                                  'zipped': zipped_int, 'files': n_files,
                                  'rows': 0, 'status': f'error_write: {e}'})


# ── Summary ──────────────────────────────────────────────────────────────────
total_min = (time.time() - run_start) / 60
print(f"\n{'='*60}")
print(f"=== Ingestion complete — {total_min:.1f} min total ===")
display(pd.DataFrame(ingestion_log))

# COMMAND ----------

# DBTITLE 1,Spot-check — random rows per sub-batch
# Explore bronze data by header group — covers ALL ingested data (old + new runs).
# For each header group, shows row count, ingestion batch history, and 5 random sample rows.
# Inspect for: garbled characters (encoding bug), NULLs where values expected,
# unexpected station/emisor values

# Columns to check in sample output for validation
check_cols = [
    "fecha_transaccion", "emisor", "operator", "line",
    "station", "station_access", "fare_type", "day_group_type",
    "cardnumber", "value", "_header_group", "_transform_format",
    "_ingestion_ts", "_source_file",
]

# Identify header groups ingested in the current session (if cell 9 was run)
new_this_run = set()
try:
    # Collect header groups with successful ingestion in this run
    new_this_run = {b['header'] for b in ingestion_log if b['status'] == 'ok'}
except NameError:
    pass  # ingestion_log not in scope — cell 9 was not run this session

# Load bronze table as Spark DataFrame
bronze = spark.table(BRONZE_TABLE)

# List all distinct header groups present in bronze table
all_header_groups = sorted(
    bronze.select("_header_group").distinct().toPandas()["_header_group"].dropna().tolist()
)

print(f"Header groups in bronze  : {len(all_header_groups)}")
print(f"New this session         : {sorted(new_this_run) if new_this_run else '(none / cell 9 not run)'}\n")

# Loop through each header group to summarize and sample data
for hg in all_header_groups:
    tag = "  ← NEW this run" if hg in new_this_run else ""

    # Aggregate summary statistics for the header group
    summary = (
        bronze
        .filter(F.col("_header_group") == hg)
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("_ingestion_ts").alias("ingestion_batches"),
            F.min("_ingestion_ts").alias("first_ingested"),
            F.max("_ingestion_ts").alias("last_ingested"),
        )
        .toPandas()
        .iloc[0]
    )

    # Print summary for the header group
    print(
        f"\n── {hg}{tag} | {int(summary['rows']):,} rows "
        f"| {int(summary['ingestion_batches'])} batch(es) "
        f"| first={summary['first_ingested']} | last={summary['last_ingested']} ──"
    )

    # Display 5 random sample rows for the header group for inspection
    sample = (
        bronze
        .filter(F.col("_header_group") == hg)
        .orderBy(F.rand(seed=42))
        .limit(5)
        .select(*check_cols)
        .toPandas()
    )
    display(sample)

# COMMAND ----------

# DBTITLE 1,Validate bronze table
bronze_df = spark.table(BRONZE_TABLE)
print(f"Total rows in {BRONZE_TABLE}: {bronze_df.count():,}")

display(
    bronze_df
    .groupBy("_header_group", "_transform_format")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("cardnumber").alias("unique_cards"),
        F.min("fecha_transaccion").alias("earliest_ts"),
        F.max("fecha_transaccion").alias("latest_ts")
    )
    .orderBy("_header_group")
)
