# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Fix Plan
# MAGIC %md
# MAGIC ## Fixing Oct 2016 – Sep 2017 Data Issues
# MAGIC
# MAGIC **Problem identified:**
# MAGIC - 8 files (Enero, Mar–Sep 2017) were ingested TWICE into bronze → all rows for these files must be deleted and re-ingested once.
# MAGIC - Oct 2016 file is broken (0-byte) in both Documents and Raw → must be cleaned from Raw and control table, then re-processed after the user re-uploads.
# MAGIC
# MAGIC **This notebook performs (in order):**
# MAGIC 1. Delete ALL rows from bronze for the 8 double-ingested files
# MAGIC 2. Reset `ingested_at` → NULL in the control table for those 8 files
# MAGIC 3. Delete the 0-byte Oct 2016 file from the Raw volume
# MAGIC 4. Delete the Oct 2016 row from the control table
# MAGIC 5. Re-ingest the 8 cleaned files (calls data-ingest-bronze-from2016to2019)
# MAGIC
# MAGIC **POSTPONED** (until user confirms new Oct 2016 file uploaded to Documents):
# MAGIC - Re-run data-organize-fromDocuments
# MAGIC - Re-run data-byheader-from2016to2019 (re-classify)
# MAGIC - Re-run data-ingest-bronze-from2016to2019 (ingest Oct 2016)

# COMMAND ----------

# DBTITLE 1,Setup and constants
from pyspark.sql import functions as F
import os

CONTROL_TABLE = "prd_mega.scolom15.file_classification_from2016to2019"
BRONZE_TABLE  = "prd_mega.scolom15.bronze_validaciones_from2016to2019"

# The 8 files that were ingested twice
DOUBLE_INGESTED_FILES = [
    '01_ValidacionesEnero2017.csv',
    '03_ValidacionesMar2017.csv',
    '04_ValidacionesAbr2017.csv',
    '05_ValidacionesMay2017.csv',
    '06_ValidacionesJun2017.csv',
    '07_ValidacionesJul2017.csv',
    '08_ValidacionesAgo2017.csv',
    '09_ValidacionesSept2017.csv',
]

# Oct 2016 broken file
OCT2016_RAW_PATH = "/Volumes/prd_csc_mega/sColom15/vColom15/Workspace/Raw/from2016to2019/10_ValidacionesOct2016.csv"

# COMMAND ----------

# DBTITLE 1,Step 1
# MAGIC %md
# MAGIC ### Step 1: Delete ALL rows from bronze for the 8 double-ingested files
# MAGIC This removes ~1.77 billion rows. After this, these files will have 0 rows in bronze.

# COMMAND ----------

# DBTITLE 1,Delete doubled files from bronze
# Build a filter condition: _source_file LIKE '%filename%' for each of the 8 files
filter_condition = F.lit(False)
for fname in DOUBLE_INGESTED_FILES:
    filter_condition = filter_condition | F.col("_source_file").contains(fname)

# Count rows to be deleted (sanity check)
rows_to_delete = spark.table(BRONZE_TABLE).filter(filter_condition).count()
print(f"Rows to delete from bronze: {rows_to_delete:,}")
print(f"Files affected: {len(DOUBLE_INGESTED_FILES)}")
print("\n⚠️  Review the count above before running the next cell!")

# COMMAND ----------

# DBTITLE 1,Execute bronze deletion
# MAGIC %sql
# MAGIC -- STEP 1: Delete ALL rows for the 8 double-ingested files from bronze
# MAGIC -- This is irreversible! Make sure the count above looks correct.
# MAGIC DELETE FROM prd_mega.scolom15.bronze_validaciones_from2016to2019
# MAGIC WHERE _source_file LIKE '%01_ValidacionesEnero2017.csv%'
# MAGIC    OR _source_file LIKE '%03_ValidacionesMar2017.csv%'
# MAGIC    OR _source_file LIKE '%04_ValidacionesAbr2017.csv%'
# MAGIC    OR _source_file LIKE '%05_ValidacionesMay2017.csv%'
# MAGIC    OR _source_file LIKE '%06_ValidacionesJun2017.csv%'
# MAGIC    OR _source_file LIKE '%07_ValidacionesJul2017.csv%'
# MAGIC    OR _source_file LIKE '%08_ValidacionesAgo2017.csv%'
# MAGIC    OR _source_file LIKE '%09_ValidacionesSept2017.csv%'

# COMMAND ----------

# DBTITLE 1,Step 2
# MAGIC %md
# MAGIC ### Step 2: Reset `ingested_at` in control table for the 8 files
# MAGIC This marks them as "classified & ready, not yet ingested" so the ingestion notebook picks them up on the next run.

# COMMAND ----------

# DBTITLE 1,Reset ingested_at for 8 files
# MAGIC %sql
# MAGIC -- STEP 2: Reset ingested_at to NULL for the 8 double-ingested files
# MAGIC UPDATE prd_mega.scolom15.file_classification_from2016to2019
# MAGIC SET ingested_at = NULL
# MAGIC WHERE raw_filepath LIKE '%01_ValidacionesEnero2017.csv%'
# MAGIC    OR raw_filepath LIKE '%03_ValidacionesMar2017.csv%'
# MAGIC    OR raw_filepath LIKE '%04_ValidacionesAbr2017.csv%'
# MAGIC    OR raw_filepath LIKE '%05_ValidacionesMay2017.csv%'
# MAGIC    OR raw_filepath LIKE '%06_ValidacionesJun2017.csv%'
# MAGIC    OR raw_filepath LIKE '%07_ValidacionesJul2017.csv%'
# MAGIC    OR raw_filepath LIKE '%08_ValidacionesAgo2017.csv%'
# MAGIC    OR raw_filepath LIKE '%09_ValidacionesSept2017.csv%'

# COMMAND ----------

# DBTITLE 1,Step 3
# MAGIC %md
# MAGIC ### Step 3: Delete Oct 2016 broken file from Raw volume
# MAGIC The file at the Raw path is 0-byte (broken copy from the original Documents folder). Remove it so data-organize-fromDocuments can place a fresh copy later.

# COMMAND ----------

# DBTITLE 1,Delete Oct2016 from Raw volume
# STEP 3: Delete the 0-byte Oct 2016 file from the Raw folder
if os.path.exists(OCT2016_RAW_PATH):
    file_size = os.path.getsize(OCT2016_RAW_PATH)
    print(f"File exists: {OCT2016_RAW_PATH}")
    print(f"File size: {file_size} bytes")
    os.remove(OCT2016_RAW_PATH)
    print("✅ File deleted successfully.")
else:
    print(f"⚠️  File not found (may already be deleted): {OCT2016_RAW_PATH}")

# COMMAND ----------

# DBTITLE 1,Step 4
# MAGIC %md
# MAGIC ### Step 4: Delete Oct 2016 row from control table
# MAGIC Remove the classification record entirely so the file can be re-classified fresh after the new upload.

# COMMAND ----------

# DBTITLE 1,Delete Oct2016 from control table
# MAGIC %sql
# MAGIC -- STEP 4: Delete the Oct 2016 row from the control table
# MAGIC DELETE FROM prd_mega.scolom15.file_classification_from2016to2019
# MAGIC WHERE raw_filepath LIKE '%Oct2016%'

# COMMAND ----------

# DBTITLE 1,Verify cleanup
# VERIFICATION: Confirm cleanup was successful
print("=" * 70)
print("VERIFICATION: Bronze row counts after deletion")
print("=" * 70)
for fname in DOUBLE_INGESTED_FILES:
    rc = spark.table(BRONZE_TABLE).filter(F.col("_source_file").contains(fname)).count()
    status = "✅ clean" if rc == 0 else f"❌ still has {rc:,} rows!"
    print(f"  {fname}: {status}")

print(f"\n{'=' * 70}")
print("VERIFICATION: Control table ingested_at status")
print("=" * 70)
for fname in DOUBLE_INGESTED_FILES:
    row = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("raw_filepath").contains(fname))
        .select("raw_filepath", "ingested_at", "classification_status")
        .collect()
    )
    if row:
        ing = row[0]["ingested_at"]
        status = "✅ NULL (ready for re-ingest)" if ing is None else f"❌ still set: {ing}"
        print(f"  {fname}: {status}")
    else:
        print(f"  {fname}: ⚠️  not found in control table")

print(f"\n{'=' * 70}")
print("VERIFICATION: Oct 2016")
print("=" * 70)
oct_exists = os.path.exists(OCT2016_RAW_PATH)
print(f"  Raw file exists: {'❌ still there!' if oct_exists else '✅ deleted'}")
oct_in_ctrl = spark.table(CONTROL_TABLE).filter(F.col("raw_filepath").contains("Oct2016")).count()
print(f"  Control table row: {'❌ still there!' if oct_in_ctrl > 0 else '✅ deleted'}")

# COMMAND ----------

# DBTITLE 1,Step 5
# MAGIC %md
# MAGIC ### Step 5: Re-ingest the 8 files (single pass)
# MAGIC After steps 1–2, the control table has these 8 files as `ingested_at = NULL` with `classification_status = 'ready'`.
# MAGIC Run the ingestion notebook to pick them up:
# MAGIC
# MAGIC ```
# MAGIC %run ./data-ingest-bronze-from2016to2019
# MAGIC ```
# MAGIC
# MAGIC Or open and run [data-ingest-bronze-from2016to2019](/editor/notebooks/954539715547077) manually.

# COMMAND ----------

# DBTITLE 1,Steps 5-7: Process Oct 2016 and re-ingest all
# MAGIC %md
# MAGIC ---
# MAGIC ### Steps 5–7: Process Oct 2016 and re-ingest all 9 files
# MAGIC
# MAGIC Oct 2016 has been re-uploaded. Run these in order:
# MAGIC
# MAGIC 1. **Step 5 — Re-organize**: Run [data-organize-fromDocuments](/editor/notebooks/921996174464178) — moves Oct 2016 from Documents → Raw
# MAGIC 2. **Step 6 — Re-classify**: Run [data-byheader-from2016to2019](/editor/notebooks/954539715547076) — detects header/encoding/delimiter, writes `ready` row to control table
# MAGIC 3. **Step 7 — Ingest all**: Run [data-ingest-bronze-from2016to2019](/editor/notebooks/954539715547077) — picks up all 9 files (8 reset + Oct 2016) in one pass
