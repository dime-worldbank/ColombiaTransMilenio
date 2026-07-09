# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Inspect and define control schema
# MAGIC %md
# MAGIC # Bronze ingestion — since 2020 validaciones
# MAGIC
# MAGIC Reads every file listed in `prd_mega.scolom15.file_classification_since2020` and writes a unified bronze Delta table (`bronze_validaciones_since2020`).  
# MAGIC Only files with `classification_status = 'ready'` and `ingested_at IS NULL` are processed — making the run **incremental and safe to re-run**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Where the read settings come from — `file_classification_since2020`
# MAGIC
# MAGIC The classification notebook (`data-byheader-since2020`) opened every raw file, read its first line, and stored the result in the control table. The ingestion here **must use those per-file values** — never assume or batch across different values. The relevant columns are:
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
# MAGIC ## ⚠️ Critical bugs fixed — do not regress
# MAGIC
# MAGIC 1. **Encoding bug**: early versions took `group['encoding'].iloc[0]` — one encoding for the whole header group. Within `header_08` alone there are files in GB2312, ISO-8859-1, ascii, and utf-8. Mixing them in a single `spark.read` call silently garbles characters in every file whose encoding is not the one picked. **Fix**: inner groupby on `(encoding, delimiter, zipped)` so each `spark.read` call uses exactly the encoding the classifier detected for those files.
# MAGIC
# MAGIC 2. **ZIP bug**: early versions passed all file paths (plain and ZIP) to `spark.read.format("csv").load()`. Spark cannot read ZIP archives as CSV — it reads binary ZIP bytes as text, which poisons the schema and causes `RESOURCE_EXHAUSTED` errors. **Fix**: split on `zipped` flag; extract the inner CSV to `zip_extracted/` using `inner_file_name`, then Spark reads the extracted plain CSV.
# MAGIC
# MAGIC 3. **Path scheme bug**: `raw_filepath` values are stored with a double leading slash (`//Volumes/...`). Spark’s Hadoop path parser treats `//` as an authority-based URI with no scheme, raising `No FileSystem for scheme "null"`. **Fix**: normalize with `'/' + p.lstrip('/')` before passing to `.load()`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Known linter warnings — do not fix
# MAGIC
# MAGIC | Code | Line(s) | Why it fires | Why it is correct as-is |
# MAGIC |---|---|---|---|
# MAGIC | SCPAP001 | 46, 105 | `dfraw.columns` accessed inside the inner loop triggers one Analyze RPC per sub-batch | Intentional — each sub-batch has its own `dfraw`; the RPC must happen per iteration and is already at the minimum of one call. Cannot be moved outside the loop. |
# MAGIC | SCPAP005 | 23, 59, 137 | Linter sees lazy Spark transforms inside `try/except` and warns exceptions won’t be raised | False positive. Line 23: `.toPandas()` is an action and will raise if the table is missing — exactly what the `except` catches. Line 105: `dfraw.columns` is an eager Analyze RPC. Lines 137+: `df.count()` and `df.write` are actions inside the same `try` block and will raise correctly. |

# COMMAND ----------

# DBTITLE 1,Alter control table
from pyspark.sql import functions as F
import pandas as pd
import io
import os
import time
import zipfile
import shutil
from pathlib import Path
from tqdm import tqdm  

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG prd_mega;
# MAGIC USE SCHEMA scolom15;
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema;

# COMMAND ----------

# DBTITLE 1,Validate upgraded control table
CONTROL_TABLE = "prd_mega.scolom15.file_classification_since2020"
BRONZE_TABLE  = "prd_mega.scolom15.bronze_validaciones_since2020"

# COMMAND ----------

# DBTITLE 1,Create bronze table
# MAGIC %sql
# MAGIC -- Create bronze table if it does not exist
# MAGIC CREATE TABLE IF NOT EXISTS bronze_validaciones_since2020 (
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
classification_df = spark.table(CONTROL_TABLE).toPandas()

stats_dims = [
    "classification_status",
    "header",
    "transform_format",
    "file_format",
    "archive_format",
    "zipped",
    "delimiter",
    "encoding",
]

classification_df["ingestion_state"] = classification_df["ingested_at"].apply(
    lambda x: "pending" if pd.isna(x) else "already_ingested"
)

stats_df = (
    classification_df
    .groupby(["ingestion_state"] + stats_dims, dropna=False)
    .size()
    .reset_index(name="n_files")
)
stats_df["state_total"] = stats_df.groupby("ingestion_state")["n_files"].transform("sum")
stats_df["pct_within_state"] = (100.0 * stats_df["n_files"] / stats_df["state_total"]).round(2)

print("Files by ingestion state:")
display(
    stats_df[["ingestion_state", "n_files"]]
    .groupby("ingestion_state", as_index=False)
    .sum()
)

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


# COMMAND ----------

# DBTITLE 1,Define transform functions
# Maps transform_format label → column selection.
# format_6: headers 08, 09, 10, 15 — station_access mirrors station (no separate access column)
# format_7: headers 11, 12, 13, 14 — station_access from Acceso_Estacion

def _base_columns(dfraw_cols, station_access_col):
    # Columns absent in some header groups — fall back to NULL
    station_access_expr = (
        F.col(station_access_col) if station_access_col
        else F.lit(None).cast('string')
    )
    fare_type_expr = (
        F.col('Tipo_Tarifa') if 'Tipo_Tarifa' in dfraw_cols
        else F.lit(None).cast('string')
    )
    day_group_type_expr = (
        F.col('Day_Group_Type') if 'Day_Group_Type' in dfraw_cols
        else F.lit(None).cast('string')
    )
    return [
        F.col('Fecha_Transaccion').alias('fecha_transaccion'),
        F.col('Emisor').alias('emisor'),
        F.col('Operador').alias('operator'),
        F.col('Linea').alias('line'),
        F.col('Estacion_Parada').alias('station'),
        station_access_expr.alias('station_access'),
        F.col('Dispositivo').alias('machine'),
        F.col('Fase').alias('phase'),
        F.col('Fecha_Clearing').alias('clearing_date'),
        F.col('Hora_Pico_SN').alias('peak_hour'),
        F.col('ID_Vehiculo').alias('vehicle_id'),
        F.col('Ruta').alias('route'),
        fare_type_expr.alias('fare_type'),
        F.col('Tipo_Tarjeta').alias('card_type'),
        F.col('Tipo_Vehiculo').alias('vehicle_type'),
        F.col('Nombre_Perfil').alias('account_name'),
        F.col('Numero_Tarjeta').alias('cardnumber'),
        F.col('Saldo_Previo_a_Transaccion').alias('balance_before'),
        F.col('Valor').alias('value'),
        F.col('Saldo_Despues_Transaccion').alias('balance_after'),
        F.col('Sistema').alias('system'),
        day_group_type_expr.alias('day_group_type'),
    ]

TRANSFORMS = {
    'format_6': lambda dfraw, dfraw_cols: dfraw.select(_base_columns(dfraw_cols, None)),
    'format_7': lambda dfraw, dfraw_cols: dfraw.select(_base_columns(dfraw_cols, 'Acceso_Estacion')),
}

# COMMAND ----------

# DBTITLE 1,Ingest — per header group
# Permanent cache for ZIP-extracted CSVs.
# Files are kept across runs so re-ingestion (e.g. after a bronze drop) skips re-extraction.
ZIP_EXTRACT_ROOT = "/Volumes/prd_csc_mega/sColom15/vColom15/Workspace/zip_extracted"
os.makedirs(ZIP_EXTRACT_ROOT, exist_ok=True)

ingestion_log = []
run_start = time.time()

# Duplicate guard: load all _source_file paths already written to bronze.
# Protects against: write succeeded but ingested_at UPDATE failed, or mid-write cancellation.
# For each batch we check this set BEFORE writing — if paths already present, skip the write
# and only repair ingested_at. Updated in-memory after every successful write.
try:
    already_in_bronze = set(
        spark.table(BRONZE_TABLE)
        .select("_source_file").distinct()
        .toPandas()["_source_file"].tolist()
    )
    print(f"Bronze already contains {len(already_in_bronze):,} distinct source file paths")
except Exception:
    already_in_bronze = set()
    print("Bronze table is empty or does not exist yet — will be created on first write")

for header_group in sorted(control_df['header'].dropna().unique()):
    group         = control_df[control_df['header'] == header_group].copy()
    assert group['transform_format'].nunique(dropna=False) == 1, (
        f"[{header_group}] mixed transform_format values: {group['transform_format'].unique().tolist()}"
    )
    transform_fmt = group['transform_format'].iloc[0]

    if transform_fmt not in TRANSFORMS:
        print(f"Skipping {header_group}: no transform defined for '{transform_fmt}'")
        ingestion_log.append({'header': header_group, 'encoding': None, 'zipped': None, 'files': len(group), 'rows': 0, 'status': 'skipped_no_transform'})
        continue

    print(f"\n[{header_group}] {len(group)} files | transform={transform_fmt}")

    # Sub-group by per-file read metadata captured during classification.
    for (encoding, delimiter, zipped), subgroup in group.groupby(['encoding', 'delimiter', 'zipped'], dropna=False):
        original_paths = subgroup['raw_filepath'].tolist()
        inner_names    = subgroup['inner_file_name'].tolist() if 'inner_file_name' in subgroup.columns else [None] * len(subgroup)
        n_files        = len(original_paths)

        zipped_int = int(zipped) if pd.notna(zipped) else 0
        print(f"  [{encoding}] zipped={zipped_int} | {n_files} files | sep='{delimiter}'")

        if zipped_int == 1:
            extracted_paths = []
            n_extracted = 0
            n_cached    = 0
            try:
                for zip_path, inner_name in tqdm(zip(original_paths, inner_names), total=n_files, desc=f"    {header_group} [{encoding}]", unit="zip"):
                    local_zip_path = '/' + zip_path.lstrip('/')  # /Volumes/... accessible directly on serverless
                    target_name    = f"{Path(zip_path).stem}__{Path(inner_name or 'inner.csv').name}"
                    target_path    = os.path.join(ZIP_EXTRACT_ROOT, target_name)
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
                print(f"    → ERROR: {e}")
                ingestion_log.append({'header': header_group, 'encoding': encoding, 'zipped': zipped_int, 'files': n_files, 'rows': 0, 'status': f'error: {e}'})
                continue
        else:
            read_paths = ['/' + p.lstrip('/') for p in original_paths]

        # --- Duplicate guard ---
        overlap = [p for p in read_paths if p in already_in_bronze]
        if overlap:
            print(f"    → {len(overlap)}/{n_files} source files already in bronze — skipping write, repairing ingested_at")
            processed_df = spark.createDataFrame([(p,) for p in original_paths], ["raw_filepath"])
            batch_suffix  = f"{header_group}_{encoding}_{zipped_int}".replace('-', '_').replace('.', '_')
            batch_view    = f"_ingested_{batch_suffix}"
            processed_df.createOrReplaceTempView(batch_view)
            spark.sql(f"""
                UPDATE {CONTROL_TABLE}
                SET ingested_at = current_timestamp()
                WHERE raw_filepath IN (SELECT raw_filepath FROM {batch_view})
            """)
            ingestion_log.append({'header': header_group, 'encoding': encoding, 'zipped': zipped_int,
                                   'files': n_files, 'rows': len(overlap), 'status': 'already_in_bronze'})
            continue

        dfraw = (
            spark.read.format("csv")
            .option("header", "true")
            .option("sep", delimiter)
            .option("encoding", encoding)
            .load(read_paths)
        )

        try:
            dfraw_cols = dfraw.columns
        except Exception as e:
            print(f"    → ERROR: {e}")
            ingestion_log.append({'header': header_group, 'encoding': encoding, 'zipped': zipped_int, 'files': n_files, 'rows': 0, 'status': f'error: {e}'})
            continue

        df = (
            TRANSFORMS[transform_fmt](dfraw, dfraw_cols)
            .withColumns({
                "_source_file": F.col("_metadata.file_path"),
                "_header_group": F.lit(header_group),
                "_transform_format": F.lit(transform_fmt),
                "_ingestion_ts": F.current_timestamp(),
            })
        )

        try:
            t0 = time.time()
            row_count = df.count()
            t_count = time.time() - t0

            t0 = time.time()
            (
                df.write
                .format("delta")
                .mode("append")
                .saveAsTable(BRONZE_TABLE)
            )
            t_write = time.time() - t0
            already_in_bronze.update(read_paths)  # keep duplicate guard current for remaining batches

            processed_df = spark.createDataFrame([(p,) for p in original_paths], ["raw_filepath"])
            batch_suffix = f"{header_group}_{encoding}_{zipped_int}".replace('-', '_').replace('.', '_')
            batch_view = f"_ingested_{batch_suffix}"
            processed_df.createOrReplaceTempView(batch_view)
            spark.sql(f"""
                UPDATE {CONTROL_TABLE}
                SET ingested_at = current_timestamp()
                WHERE raw_filepath IN (SELECT raw_filepath FROM {batch_view})
            """)

            elapsed = (time.time() - run_start) / 60
            print(f"    → {row_count:,} rows | count {t_count:.0f}s | write {t_write:.0f}s | total elapsed {elapsed:.1f} min")
            ingestion_log.append({'header': header_group, 'encoding': encoding, 'zipped': zipped_int, 'files': n_files, 'rows': row_count, 'status': 'ok'})

        except Exception as e:
            print(f"    → ERROR: {e}")
            ingestion_log.append({'header': header_group, 'encoding': encoding, 'zipped': zipped_int, 'files': n_files, 'rows': 0, 'status': f'error: {e}'})

total_min = (time.time() - run_start) / 60
print(f"\n=== Ingestion complete — {total_min:.1f} min total ===")
display(pd.DataFrame(ingestion_log))

# COMMAND ----------

# DBTITLE 1,Spot-check — random rows per sub-batch
# Explore bronze data by header group — covers ALL ingested data (old + new runs).
# For each header group, shows row count, ingestion batch history, and 5 random sample rows.
# Inspect for: garbled characters (encoding bug), NULLs where values expected,
# unexpected station/emisor values, correct station_access for format_7.

check_cols = [
    "fecha_transaccion", "emisor", "operator", "line",
    "station", "station_access", "fare_type", "day_group_type",
    "cardnumber", "value", "_header_group", "_transform_format",
    "_ingestion_ts", "_source_file",
]

# Mark header groups ingested in the current session (if cell 9 was run)
new_this_run = set()
try:
    new_this_run = {b['header'] for b in ingestion_log if b['status'] == 'ok'}
except NameError:
    pass  # ingestion_log not in scope — cell 9 was not run this session

bronze = spark.table(BRONZE_TABLE)

all_header_groups = sorted(
    bronze.select("_header_group").distinct().toPandas()["_header_group"].dropna().tolist()
)

print(f"Header groups in bronze  : {len(all_header_groups)}")
print(f"New this session         : {sorted(new_this_run) if new_this_run else '(none / cell 9 not run)'}\n")

for hg in all_header_groups:
    tag = "  ← NEW this run" if hg in new_this_run else ""

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

    print(
        f"\n── {hg}{tag} | {int(summary['rows']):,} rows "
        f"| {int(summary['ingestion_batches'])} batch(es) "
        f"| first={summary['first_ingested']} | last={summary['last_ingested']} ──"
    )

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
