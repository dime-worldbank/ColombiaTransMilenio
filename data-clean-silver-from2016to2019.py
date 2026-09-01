# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Index
# MAGIC %md
# MAGIC ## Index
# MAGIC
# MAGIC 1. **Setup** — Import packages and select catalog/schema
# MAGIC 2. **Define Bronze Table** — Inspect columns of `bronze_validaciones_from2016to2019`
# MAGIC 3. **Ingestion Diagnostics**
# MAGIC    - Control table completeness (missing or broken files)
# MAGIC    - Character encoding issues in `fecha_transaccion`
# MAGIC    - Duplicate ingestion check
# MAGIC    - Raw row count vs bronze
# MAGIC 4. **Transaction Dates** — Format mapping and cleaning (`fecha_transaccion`, `clearing_date`)
# MAGIC 5. **Exploratory Visualizations**
# MAGIC    - Day-of-month heatmap (month × day) using `clearing_date`
# MAGIC    - Daily transaction line plots (both date columns)
# MAGIC 6. **Numeric Variables** — Validation and casting of `cardnumber`, `balance_before`, `value`, `balance_after`
# MAGIC 7. **Categorical Variables** — Frequency counts for `card_type`
# MAGIC 8. **Consolidation** — Chain all transformations into `df_silver`

# COMMAND ----------

# DBTITLE 1,Section 1: Setup
# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Setup

# COMMAND ----------

# DBTITLE 1,Import Packages
from pyspark.sql import functions as F
import pandas as pd
import numpy as np

import io
import os
import time
import zipfile
import shutil
from pathlib import Path
from tqdm import tqdm

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import matplotlib.dates as mdates

import calendar


# COMMAND ----------

# DBTITLE 1,Select catalog
# MAGIC %sql
# MAGIC USE CATALOG prd_mega;
# MAGIC USE SCHEMA scolom15;
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema;

# COMMAND ----------

# DBTITLE 1,Section 2: Define Bronze Table
# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Define Bronze Table

# COMMAND ----------

# DBTITLE 1,Define Bronze Table and Inspect Columns
BRONZE_TABLE  = "prd_mega.scolom15.bronze_validaciones_from2016to2019"
# Inspect Columns of BRONZE_TABLE
spark.table(BRONZE_TABLE).columns

# COMMAND ----------

# DBTITLE 1,Section 3: Ingestion Diagnostics
# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Ingestion Diagnostics

# COMMAND ----------

# DBTITLE 1,Ingestion diagnostics: control table completeness
# ══════════════════════════════════════════════════════════════════════════════
# INGESTION DIAGNOSTICS
# Verify that all source files made it into bronze correctly
# ══════════════════════════════════════════════════════════════════════════════

CONTROL_TABLE = "prd_mega.scolom15.file_classification_from2016to2019"

# ── 1. Control table vs bronze file counts ────────────────────────────────────
in_bronze = spark.table(BRONZE_TABLE).select("_source_file").distinct().count()
in_control = spark.table(CONTROL_TABLE).select("raw_filepath").distinct().count()
df_broken = (
    spark.table(CONTROL_TABLE)
    .filter(F.col("classification_status") == "broken")

)
in_control_broken = df_broken.count()

print(f"Files in CONTROL_TABLE : {in_control}")
print(f"  └─ broken            : {in_control_broken}")
print(f"Files in BRONZE_TABLE  : {in_bronze}")
print(f"  └─ expected match    : {in_control - in_control_broken}")

if in_bronze == in_control - in_control_broken:
    print("✅ All non-broken files are in bronze.")
else:
    print(f"⚠️  Mismatch! {in_control - in_control_broken - in_bronze} files missing from bronze.")

# Show broken file reasons
print("\n── Broken files breakdown by reason ──")
display(
    df_broken
    .groupBy("detection_notes")
    .count()
    .orderBy(F.col("count").desc())
)

# List each broken file with its reason
print("\n── Broken files list ──")
display(
    df_broken
    .select("raw_filepath", "detection_notes")
    .orderBy("raw_filepath")
)


# COMMAND ----------

# DBTITLE 1,Ingestion diagnostics: character encoding issues
# ──  Rows with non-normal characters in fecha_transaccion ───────────────────
# Detect letters other than 'UTC' — indicates encoding corruption

df_bronze = spark.table(BRONZE_TABLE)

# Remove 'UTC' then check for remaining letters — filter once and cache
df_weird = (
    df_bronze
    .filter(
        F.regexp_replace(F.col("fecha_transaccion"), "(?i)UTC", "")
         .rlike("[A-Za-z]")
    )

)
weird_char_count = df_weird.count()

print(f"Rows with non-normal characters in fecha_transaccion: {weird_char_count:,}")
if weird_char_count > 0:
    print("Sample of affected rows:")
    display(
        df_weird
        .select("_source_file", "fecha_transaccion")
        .distinct()
        .limit(20)
    )
    # List affected files with row counts
    print("\n── Affected files ──")
    display(
        df_weird
        .groupBy("_source_file")
        .count()
        .orderBy(F.col("count").desc())
    )
else:
    print("✅ No encoding issues detected.")


# COMMAND ----------

# DBTITLE 1,Ingestion diagnostics: duplicate ingestion check
# ──  Check for duplicate ingestion (same content loaded twice) ──────────────
# ⚠️ This is expensive (countDistinct per file across all content columns).
# Set to False to skip.
RUN_DUPLICATE_CHECK = False

if RUN_DUPLICATE_CHECK:
    print("Checking for duplicate rows within each source file...")
    print("(comparing content columns only, excluding metadata)\n")

    content_cols = [c for c in df_bronze.columns 
                    if c not in ("_source_file", "_header_group", "_transform_format", "_ingestion_ts")]

    # Aggregate: total rows vs distinct content rows per file
    dup_check = (
        df_bronze
        .groupBy("_source_file")
        .agg(
            F.count("*").alias("total_rows"),
            F.countDistinct(*content_cols).alias("distinct_rows")
        )
        .withColumn("duplicates", F.col("total_rows") - F.col("distinct_rows"))
        .filter(F.col("duplicates") > 0)
        .orderBy(F.col("duplicates").desc())
    )

    dup_count = dup_check.count()
    if dup_count == 0:
        print("✅ No duplicate rows detected in any source file.")
    else:
        print(f"⚠️  {dup_count} files have duplicate rows:")
        display(dup_check)
else:
    print("ℹ️  Duplicate check skipped (RUN_DUPLICATE_CHECK = False).")

# COMMAND ----------

# DBTITLE 1,Ingestion diagnostics: raw row count vs bronze
# ── Raw file row counts vs bronze (for specified files) ────────────────────
# EDIT THIS LIST to check specific files of interest:
files_to_verify = []

if files_to_verify:
    import zipfile, io

    # Get metadata from control table
    file_meta = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("raw_filepath").rlike("|".join(files_to_verify)))
        .select("raw_filepath", "encoding", "delimiter", "zipped", "header_row")
        .toPandas()
    )

    print(f"{'File':<42} {'Raw Rows':>10} {'Bronze Rows':>12} {'Match?'}")
    print("─" * 80)

    for _, row in file_meta.iterrows():
        fpath = row["raw_filepath"]
        fname = fpath.split("/")[-1]
        enc = row["encoding"] or "utf-8"
        is_zipped = row["zipped"] == 1

        try:
            if is_zipped:
                with open(fpath, "rb") as fh:
                    with zipfile.ZipFile(io.BytesIO(fh.read())) as zf:
                        inner = zf.namelist()[0]
                        with zf.open(inner) as inner_fh:
                            raw_count = sum(1 for _ in inner_fh) - 1
            else:
                with open(fpath, "r", encoding=enc, errors="replace") as fh:
                    raw_count = sum(1 for _ in fh) - 1
        except Exception as e:
            raw_count = f"ERROR: {e}"

        bronze_count = (
            spark.table(BRONZE_TABLE)
            .filter(F.col("_source_file").contains(fname))
            .count()
        )

        if isinstance(raw_count, int):
            match = "✅" if raw_count == bronze_count else f"⚠️  diff={bronze_count - raw_count:+,}"
        else:
            match = "—"

        print(f"  {fname:<40} {str(raw_count):>10} {bronze_count:>12,} {match}")
else:
    print("ℹ️  No files specified in files_to_verify — skipping raw vs bronze check.")

# COMMAND ----------

# DBTITLE 1,Section 4: Transaction Dates
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Transaction Dates

# COMMAND ----------

# DBTITLE 1,Transaction Dates: Classification, Parsing & Diagnostics
# ══════════════════════════════════════════════════════════════════════════════
# TRANSACTION DATES: Format classification, parsing & diagnostics
# Consolidates mapping + cleaning into a single pass.
# Produces df_with_parsed_dates (used by downstream cells).
# NOTE: clearing_date is date-only in the source — no timestamp generated for it.
#
# Renames:
#   fecha_transaccion  → fecha_transaccion_string   (original string preserved)
#   clearing_date      → clearing_date_string        (original string preserved)
#
# New columns (3):
#   fecha_transaccion           (date)       — parsed date
#   clearing_date               (date)       — parsed date
#   fecha_transaccion_timestamp (timestamp)  — parsed timestamp (only fecha_transaccion has time)
# ══════════════════════════════════════════════════════════════════════════════

date_vars = ["fecha_transaccion", "clearing_date"]

# ── Format classifier ─────────────────────────────────────────────────────────
def _classify_date_format(col_name):
    return (
        F.when(F.col(col_name).rlike(r"^(2016|2017|2018|2019)\d{10}$"), F.lit("YYYYMMDDHHmmss"))
        .when(F.col(col_name).rlike(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$"), F.lit("YYYY/MM/DD HH:mm:ss"))
        .when(F.col(col_name).rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"), F.lit("YYYY-MM-DD HH:mm:ss"))
        .when(F.col(col_name).rlike(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} (UTC)$"), F.lit("YYYY/MM/DD HH:mm:ss UTC"))
        .when(F.col(col_name).rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} (UTC)$"), F.lit("YYYY-MM-DD HH:mm:ss UTC"))
        .when(F.col(col_name).rlike(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$"), F.lit("DD/MM/YYYY HH:mm:ss"))
        .when(F.col(col_name).rlike(r"^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$"), F.lit("DD-MM-YYYY HH:mm:ss"))
        .when(F.col(col_name).rlike(r"^(2016|2017|2018|2019)\d{4}$"), F.lit("YYYYMMDD"))
        .when(F.col(col_name).rlike(r"^\d{4}/\d{2}/\d{2}$"), F.lit("YYYY/MM/DD"))
        .when(F.col(col_name).rlike(r"^\d{2}/\d{2}/\d{4}$"), F.lit("DD/MM/YYYY"))
        .when(F.col(col_name).rlike(r"^\d{4}-\d{2}-\d{2}$"), F.lit("YYYY-MM-DD"))
        .when(F.col(col_name).rlike(r"^\d{2}-\d{2}-\d{4}$"), F.lit("DD-MM-YYYY"))
        .otherwise(F.lit("unknown"))
    )

# ── Spark format strings for parsing ─────────────────────────────────────────
_time_formats = {
    "YYYYMMDDHHmmss":          "yyyyMMddHHmmss",
    "YYYY/MM/DD HH:mm:ss":     "yyyy/MM/dd HH:mm:ss",
    "YYYY-MM-DD HH:mm:ss":     "yyyy-MM-dd HH:mm:ss",
    "YYYY/MM/DD HH:mm:ss UTC": "yyyy/MM/dd HH:mm:ss z",
    "YYYY-MM-DD HH:mm:ss UTC": "yyyy-MM-dd HH:mm:ss z",
    "DD/MM/YYYY HH:mm:ss":     "dd/MM/yyyy HH:mm:ss",
    "DD-MM-YYYY HH:mm:ss":     "dd-MM-yyyy HH:mm:ss",
}
_date_only_formats = {
    "YYYYMMDD":    "yyyyMMdd",
    "YYYY/MM/DD":  "yyyy/MM/dd",
    "YYYY-MM-DD":  "yyyy-MM-dd",
    "DD-MM-YYYY":  "dd-MM-yyyy",
    "DD/MM/YYYY":  "dd/MM/yyyy",
}

def _build_ts_expr(string_col, type_col):
    """Timestamp expression — only for time-bearing formats."""
    expr = F.lit(None).cast("timestamp")
    for label, fmt in _time_formats.items():
        expr = F.when(F.col(type_col) == label,
                      F.to_timestamp(F.col(string_col), fmt)).otherwise(expr)
    return expr

def _build_dt_expr(string_col, type_col):
    """Date expression — populated for all known formats."""
    expr = F.lit(None).cast("date")
    for label, fmt in {**_time_formats, **_date_only_formats}.items():
        expr = F.when(F.col(type_col) == label,
                      F.to_date(F.col(string_col), fmt)).otherwise(expr)
    return expr

# ── Step 1: Trim + classify formats ──────────────────────────────────────────
df_classified = (
    spark.table(BRONZE_TABLE)
    .withColumns({var: F.trim(F.col(var)) for var in date_vars})
    .withColumns({f"type_{var}": _classify_date_format(var) for var in date_vars})
)

# ── Step 2: Diagnostics — format distribution & unknowns ─────────────────────
for var in date_vars:
    print(f"\n{'='*60}\nVariable: {var}\n{'='*60}")
    display(
        df_classified
        .groupBy(f"type_{var}")
        .count()
        .orderBy(F.col("count").desc())
    )

    unknown_count = df_classified.filter(F.col(f"type_{var}") == "unknown").count()
    print(f"Unknown format rows: {unknown_count}")

    if unknown_count > 0:
        display(
            df_classified
            .select(var)
            .where(F.col(f"type_{var}") == "unknown")
            .orderBy(F.col(var).desc())
            .limit(10)
        )

# ── Step 3: Parse all dates in a single withColumns pass ─────────────────────
# All expressions evaluate against the original schema, so reading var (string)
# and overwriting var (parsed date) in the same dict is safe.
df_with_parsed_dates = df_classified.withColumns({
    # Preserve original strings
    **{f"{var}_string": F.col(var) for var in date_vars},
    # Timestamp only for fecha_transaccion (clearing_date is date-only in source)
    "fecha_transaccion_timestamp": _build_ts_expr("fecha_transaccion", "type_fecha_transaccion"),
    # Parsed dates (overwrites original string columns with proper date type)
    **{var: _build_dt_expr(var, f"type_{var}") for var in date_vars},
})

# ── Step 4: Sanity check — null counts ───────────────────────────────────────
total = df_with_parsed_dates.count()
for var in date_vars:
    types = df_with_parsed_dates.select(f"type_{var}").distinct().collect()
    print(f"\n{'='*60}\n{var} — formats found: {[r[0] for r in types]}\n{'='*60}")
    null_dt = df_with_parsed_dates.filter(F.col(var).isNull()).count()
    print(f"Total rows          : {total:,}")
    print(f"Null {var} (date)    : {null_dt:,}  ({100*null_dt/total:.1f}%)  ← should be 0 or only 'unknown' rows")
    if var == "fecha_transaccion":
        null_ts = df_with_parsed_dates.filter(F.col("fecha_transaccion_timestamp").isNull()).count()
        print(f"Null {var}_timestamp : {null_ts:,}  ({100*null_ts/total:.1f}%)  ← only 'unknown' rows expected")


# COMMAND ----------

# DBTITLE 1,Source files covering Oct 2016 – Sep 2017 (completeness check)
# ── List source files with clearing_date in the analysis window ──────────────
# Goal: verify completeness of the Oct 2016 – Sep 2017 window by listing which
# raw files contribute rows to it (check this list against the expected files).
# Uses the parsed clearing_date from df_with_parsed_dates (Section 4).
WINDOW_START = "2016-10-01"
WINDOW_END   = "2017-09-30"   # inclusive

df_window = df_with_parsed_dates.filter(
    F.col("clearing_date").between(WINDOW_START, WINDOW_END)
)

files_window = (
    df_window
    .groupBy("_source_file")
    .agg(
        F.count("*").alias("rows_in_window"),
        F.min("clearing_date").alias("min_clearing_date"),
        F.max("clearing_date").alias("max_clearing_date"),
        F.countDistinct("clearing_date").alias("distinct_days"),
    )
    .orderBy("min_clearing_date", "_source_file")
)

print(f"Source files with clearing_date in [{WINDOW_START}, {WINDOW_END}]: {files_window.count()}")
display(files_window)

# Days in the window with no rows at all (quick completeness check)
days_present = df_window.select("clearing_date").distinct()
all_days = spark.sql(
    f"SELECT explode(sequence(to_date('{WINDOW_START}'), to_date('{WINDOW_END}'), interval 1 day)) AS clearing_date"
)
missing_days = all_days.join(days_present, "clearing_date", "left_anti").orderBy("clearing_date")
n_missing = missing_days.count()
print(f"Days in window with zero rows: {n_missing}")
if n_missing > 0:
    display(missing_days)

# COMMAND ----------

# DBTITLE 1,Window files: expected monthly files vs extra files (duplicate check)
# ══════════════════════════════════════════════════════════════════════════════
# The Oct 2016 – Sep 2017 window is expected to come from 12 monthly files.
# Other files (e.g. dated Sep 30 2017) also contribute rows with clearing_date
# in the window. This cell checks whether those extra rows are duplicates of
# the monthly files' content or genuinely new data.
# How to read the results:
#   - overlap pct ≈ 100%  → the extra file duplicates the monthly files: exclude it.
#   - overlap pct ≈ 0%    → new data (e.g. late-clearing validations): investigate
#                           before excluding — dropping it would lose real trips.
#   - partial overlap     → mixed; look at the daily-volume table to see which days.
# ══════════════════════════════════════════════════════════════════════════════

MONTHLY_FILES = [
    "10_ValidacionesOct2016.csv",
    "11_ValidacionesNov2016.csv",
    "12_ValidacionesDic2016.csv",
    "01_ValidacionesEnero2017.csv",
    "02_ValidacionesFeb2017.csv",
    "03_ValidacionesMar2017.csv",
    "04_ValidacionesAbr2017.csv",
    "05_ValidacionesMay2017.csv",
    "06_ValidacionesJun2017.csv",
    "07_ValidacionesJul2017.csv",
    "08_ValidacionesAgo2017.csv",
    "09_ValidacionesSept2017.csv",
]

is_monthly = F.lit(False)
for fname in MONTHLY_FILES:
    is_monthly = is_monthly | F.col("_source_file").contains(fname)

df_monthly_win = df_window.filter(is_monthly)
df_extra_win   = df_window.filter(~is_monthly)

# ── 1. Sanity: all 12 monthly files present in bronze? ───────────────────────
found_monthly = [r[0] for r in df_monthly_win.select("_source_file").distinct().collect()]
for fname in MONTHLY_FILES:
    if not any(fname in f for f in found_monthly):
        print(f"⚠️  Expected monthly file NOT found in window: {fname}")
print(f"Monthly files found: {len(found_monthly)} / {len(MONTHLY_FILES)}")

# ── 2. Extra files: who are they, what do they cover ─────────────────────────
print("\n── Extra files with rows in the window ──")
display(
    df_extra_win
    .groupBy("_source_file")
    .agg(
        F.count("*").alias("rows_in_window"),
        F.min("clearing_date").alias("min_clearing"),
        F.max("clearing_date").alias("max_clearing"),
        F.min("fecha_transaccion").alias("min_tx_date"),
        F.max("fecha_transaccion").alias("max_tx_date"),
    )
    .orderBy("_source_file")
)

# ── 3. Daily volume: monthly files vs extra files side by side ───────────────
print("\n── Daily rows: monthly vs extra files (only days where extra files contribute) ──")
display(
    df_window
    .groupBy("clearing_date")
    .agg(
        F.sum(F.when(is_monthly, 1).otherwise(0)).alias("rows_monthly_files"),
        F.sum(F.when(~is_monthly, 1).otherwise(0)).alias("rows_extra_files"),
    )
    .filter(F.col("rows_extra_files") > 0)
    .orderBy("clearing_date")
)

# ── 4. Exact-content overlap: are the extra rows already in the monthly files? ─
# Compare on content columns only (exclude ingestion metadata).
content_cols = [c for c in spark.table(BRONZE_TABLE).columns
                if c not in ("_source_file", "_header_group", "_transform_format", "_ingestion_ts")]

extra_content   = df_extra_win.select("_source_file", *content_cols)
monthly_content = df_monthly_win.select(*content_cols).dropDuplicates()

overlap = (
    extra_content
    .join(monthly_content, on=content_cols, how="left_semi")
    .groupBy("_source_file")
    .agg(F.count("*").alias("rows_also_in_monthly_files"))
)

totals = df_extra_win.groupBy("_source_file").agg(F.count("*").alias("rows_in_window"))

print("\n── Overlap of extra files with monthly files (exact content match) ──")
display(
    totals
    .join(overlap, "_source_file", "left")
    .fillna(0, subset=["rows_also_in_monthly_files"])
    .withColumn("overlap_pct", F.round(100 * F.col("rows_also_in_monthly_files") / F.col("rows_in_window"), 2))
    .orderBy("_source_file")
)

# COMMAND ----------

# DBTITLE 1,Section 5: Exploratory Visualizations
# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Exploratory Visualizations

# COMMAND ----------

# DBTITLE 1,Heatmap of Days
# ── 1. Aggregate to daily counts ─────────────────────────────────────────────
daily_pd = (
    df_with_parsed_dates
    .filter(F.col("clearing_date").isNotNull())
    .groupBy("clearing_date")
    .count()
    .withColumn("year",  F.year("clearing_date"))
    .withColumn("month", F.month("clearing_date"))
    .withColumn("day",   F.dayofmonth("clearing_date"))
    .toPandas()
)

years = sorted(daily_pd["year"].unique())
MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun",
                "Jul","Aug","Sep","Oct","Nov","Dec"]

fig, axes = plt.subplots(len(years), 1, figsize=(22, 4 * len(years)))
if len(years) == 1:
    axes = [axes]

for ax, year in zip(axes, years):
    yd = daily_pd[daily_pd["year"] == year]

    # ── 2. Build 12 × 31 count grid ──────────────────────────────────────────
    # NaN  → invalid calendar day (e.g. Feb-30)  → white
    # 0    → valid day with no data              → red
    # n>0  → n transactions                      → gray gradient
    grid = np.full((12, 31), np.nan)
    for m in range(1, 13):
        n_days = calendar.monthrange(year, m)[1]
        grid[m - 1, :n_days] = 0           # valid days start as "no data"
    for _, row in yd.iterrows():
        grid[int(row["month"]) - 1, int(row["day"]) - 1] = row["count"]

    max_count = yd["count"].max() if not yd.empty else 1

    # ── 3. Gray layer: valid days with data ───────────────────────────────────
    gray_data = np.ma.masked_where((np.isnan(grid)) | (grid == 0), grid)
    gray_cmap = plt.cm.Greys
    ax.imshow(gray_data, aspect="auto", cmap=gray_cmap,
              vmin=0, vmax=max_count, interpolation="nearest")

    # ── 4. Red overlay: valid days with no data ───────────────────────────────
    red_rgba = np.zeros((12, 31, 4))
    red_rgba[(~np.isnan(grid)) & (grid == 0)] = [1, 0, 0, 1]
    ax.imshow(red_rgba, aspect="auto", interpolation="nearest")

    # ── 5. Colorbar ───────────────────────────────────────────────────────────
    sm = plt.cm.ScalarMappable(
        cmap="Greys",
        norm=mcolors.Normalize(vmin=0, vmax=max_count)
    )
    cbar = plt.colorbar(sm, ax=ax, label="Transactions", fraction=0.015, pad=0.01)
    cbar.ax.yaxis.set_label_position("left")

    # ── 6. Axes, grid lines, labels ───────────────────────────────────────────
    ax.set_yticks(range(12))
    ax.set_yticklabels(MONTH_LABELS, fontsize=9)
    ax.set_xticks(range(31))
    ax.set_xticklabels(range(1, 32), fontsize=8)
    ax.set_xlabel("Day of month", fontsize=10)
    ax.set_title(str(year), fontsize=13, fontweight="bold", pad=8)

    ax.set_xticks(np.arange(-0.5, 31, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, 12, 1), minor=True)
    ax.grid(which="minor", color="lightgray", linewidth=0.4)
    ax.tick_params(which="minor", bottom=False, left=False)

    red_p   = mpatches.Patch(color="red",  label="No data (valid day)")
    white_p = mpatches.Patch(facecolor="white", edgecolor="lightgray", label="Invalid date")
    ax.legend(handles=[red_p, white_p], loc="lower right", fontsize=8, framealpha=0.85)

plt.suptitle("Daily Rows Heatmap  (month × day-of-month using CLEARING data)"
             "The goal is to identify missing files only",
             fontsize=14, fontweight="bold", y=1.01)
plt.tight_layout()
plt.show()


# COMMAND ----------

# DBTITLE 1,Daily transactions line plots (clearing_date + fecha_transaccion)

# ── 1. Build full calendar for clearing_date ──────────────────────────────────
daily_pd["clearing_date"] = pd.to_datetime(
    daily_pd[["year", "month", "day"]].rename(columns={"year": "year", "month": "month", "day": "day"})
)

full_range = pd.date_range(start=daily_pd["clearing_date"].min(), end=daily_pd["clearing_date"].max(), freq="D")
full_df = pd.DataFrame({"date": full_range}).merge(
    daily_pd[["clearing_date", "count"]].rename(columns={"clearing_date": "date"}),
    on="date", how="left"
)

# ── 2. Build full calendar for fecha_transaccion ──────────────────────────────
daily_tx_pd = (
    df_with_parsed_dates
    .filter(F.col("fecha_transaccion").isNotNull())
    .groupBy("fecha_transaccion")
    .count()
    .toPandas()
)
daily_tx_pd["fecha_transaccion"] = pd.to_datetime(daily_tx_pd["fecha_transaccion"])

full_range_tx = pd.date_range(start=daily_tx_pd["fecha_transaccion"].min(), end=daily_tx_pd["fecha_transaccion"].max(), freq="D")
full_tx_df = pd.DataFrame({"date": full_range_tx}).merge(
    daily_tx_pd.rename(columns={"fecha_transaccion": "date"}),
    on="date", how="left"
)

# ── 3. Two subplots stacked ───────────────────────────────────────────────────
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 9), sharex=True)

for ax, df_plot, title_var in [(ax1, full_df, "clearing_date"), (ax2, full_tx_df, "fecha_transaccion")]:
    ax.plot(df_plot["date"], df_plot["count"], color="steelblue", linewidth=0.7, label="Daily transactions")
    ax.axvline(x=pd.Timestamp("2017-04-01"), color="red", linewidth=1.2, linestyle="--", label="1 Apr 2017")
    ax.axhline(y=1_000_000, color="orange", linewidth=1.0, linestyle="--", label="1M")
    ax.axhline(y=4_500_000, color="orange", linewidth=1.0, linestyle="--", label="4.5M")
    ax.set_xlim(df_plot["date"].min(), df_plot["date"].max())
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.tick_params(axis="both", labelsize=11)
    ax.set_ylabel("Observations", fontsize=12)
    ax.set_title(f"Daily observations by {title_var} - NOT CLEANED", fontsize=14, fontweight="bold")
    ax.legend(loc="upper right", fontsize=10)

plt.xticks(rotation=45, ha="right")
plt.xlabel("Day", fontsize=12)
plt.tight_layout()
plt.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Section 6: Numeric Variables
# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Numeric Variables

# COMMAND ----------

# DBTITLE 1,Cell 17
numvars = ['cardnumber', 'balance_before', 'value', 'balance_after']

# ── Parseability check ────────────────────────────────────────────────────────
# For each numvar: count rows whose raw value is non-null/non-empty but cannot
# be parsed to a number, and show what those raw values look like.
for numvar in numvars:
    bad = (
        df_with_parsed_dates
        .filter(F.col(numvar).isNotNull() & (F.trim(F.col(numvar)) != ""))
        .filter(F.col(numvar).cast("double").isNull())
    )
    n_bad = bad.count()
    print(f"── {numvar}: non-parseable non-null values: {n_bad:,}")
    if n_bad > 0:
        display(bad.groupBy(numvar).count().orderBy(F.col("count").desc()).limit(20))

# ── value distribution ────────────────────────────────────────────────────────
# value has low cardinality (a handful of fares), so the full distribution is
# worth eyeballing — it doubles as an early look for the fare×period table (B3/E5).
display(
    df_with_parsed_dates
    .withColumn("value_double", F.col("value").cast("double"))
    .groupBy("value", "value_double")
    .count()
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

# DBTITLE 1,Consolidated numeric casts (A3)
# ── Numeric cast expressions, applied in the consolidation (Section 8) ────────
# cardnumber → long via decimal (a double would lose precision on long card ids;
# the decimal step tolerates values like "123.0"). value/balances → double.
# Garbage values (letters etc.) become NULL — counted in the consolidation checks.
_numeric_casts = {
    "cardnumber":     F.col("cardnumber").cast("decimal(38,0)").cast("long"),
    "balance_before": F.col("balance_before").cast("double"),
    "value":          F.col("value").cast("double"),
    "balance_after":  F.col("balance_after").cast("double"),
}

# COMMAND ----------

# DBTITLE 1,Section 7: Categorical Variables
# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Categorical Variables

# COMMAND ----------

# Count how many time each value appears
spark.table(BRONZE_TABLE).select('card_type').groupBy('card_type').count().display()


# COMMAND ----------


# ── 1. Issuer ─────────────────────────────────────────────────────────────────
_issuer_map = {
    "(1900001)": "(1900001) Angelcom Card",
    "(3200401)": "(3200401) Colpatria",
    "(3101000)": "(3101000) Bogota Card (Citizen)",
    "(3200201)": "(3200201) AV Villas",
    "(3200101)": "(3200101) Bancolombia",
    "(3200501)": "(3200501) Codensa (Master Card)",
    "(3200301)": "(3200301) Davivienda",
    "(3200502)": "(3200502) Codensa (Sin Franquicia)",
    "(3200701)": "(3200701) RappiPay-Daviplata",
    "(3200601)": "(3200601) Itau",
}
issuer_expr = F.lit(None).cast("string")
for prefix, canonical in _issuer_map.items():
    issuer_expr = F.when(F.col("emisor").contains(prefix), F.lit(canonical)).otherwise(issuer_expr)

# ── 2. Operator ───────────────────────────────────────────────────────────────
_operator_map = {
    "(001)": "(001) CONSORCIO EXPRESS USAQUEN",
    "(002)": "(002) MASIVO CAPITAL SUBA ORIENTAL",
    "(004)": "(004) ESTE ES MI BUS CALLE 80",
    "(005)": "(005) GMOVIL",
    "(007)": "(007) ETIB",
    "(008)": "(008) SUMA",
    "(009)": "(009) TRANZIT",
    "(011)": "(011) CONSORCIO EXPRESS SAN CRISTOBAL",
    "(012)": "(012) MASIVO CAPITAL KENNEDY",
    "(014)": "(014) ESTE ES MI BUS TINTAL ZONA FRANCA",
    "(201)": "(201) Trunk agency",
    "(2054)": "(2054) HABITUAL_2015-11-09",
}
operator_expr = F.lit(None).cast("string")
for prefix, canonical in _operator_map.items():
    operator_expr = F.when(F.col("operator").contains(prefix), F.lit(canonical)).otherwise(operator_expr)

# ── 3. Card type ──────────────────────────────────────────────────────────────
_card_type_map = {
    "Plus": "TuLlave Plus",
    "sica": "TuLlave Basica",
    "Angel": "Angelcom",
}
card_type_expr = F.lit(None).cast("string")
for prefix, canonical in _card_type_map.items():
    card_type_expr = F.when(F.col("card_type").contains(prefix), F.lit(canonical)).otherwise(card_type_expr)

# COMMAND ----------

# DBTITLE 1,Card profile (account_name) canonical map — A4
# ── 4. Card profile (account_name) ────────────────────────────────────────────
# 1-to-1 cleaning map (see CLEANING_DECISIONS_2017.md, A4 notes): keys are the
# exact TRIMMED raw strings found in bronze (distinct values checked 2026-08-31).
# It only fixes encoding corruption and truncated values — no detail is lost;
# the analytical grouping of profiles happens later (E10 profile_group).
# NOTE: the parenthesis code is AMBIGUOUS for this variable ((001), (003), (006),
# (029) each cover two different profiles), so the map keys on the FULL string.
# Unmapped values fall to NULL and are counted in the consolidation checks.

_profile_canonical = [
    "(001) Adulto",
    "(001) Anonymous",
    "(002) Adulto Mayor",
    "(003) Capital",
    "(003) Estudiantil",
    "(004) Menor de Edad",
    "(005) Discapacidad",
    "(006) Apoyo Ciudadano",
    "(006) Discapacitados",
    "(008) Étnico",
    "(014) Usuario frecuente",
    "(017) Discapacitado Monedero",
    "(018) Universitaria",
    "(021) Tarjeta Ciudadana",
    "(022) Empresarial TM",
    "(023) Empresarial Davivienda",
    "(024) Empresarial Colsubsidio",
    "(025) Empresarial Compensar",
    "(026) Empresarial AV Villas",
    "(027) Club Universitario",
    "(029) Empresarial Banco de Bogotá",
    "(030) Capital monedero",
    "(032) Empresarial Daviplata",
    "(033) Empresarial People Pass",
    "(035) Empresarial AV Villas Crédito",
    "(036) Empresarial Colpatria",
    "(041) Empresarial Cercanos",
    "(044) Empresarial CIS",
    "(101) Adulto PV",
]

_profile_variants = {
    # encoding-corrupted strings → same canonical as their clean versions
    "(029) Empresarial Banco de BogotÃ¡":   "(029) Empresarial Banco de Bogotá",
    "(029) Empresarial Banco de Bogot‡":    "(029) Empresarial Banco de Bogotá",
    "(035) Empresarial AV Villas CrÃ©dito": "(035) Empresarial AV Villas Crédito",
    # truncated code-only values → the unique profile matching that code
    "(005)": "(005) Discapacidad",
    "(004)": "(004) Menor de Edad",
    "(101)": "(101) Adulto PV",
    # "(000)", "1" have no matching profile: left unmapped → NULL
}

_profile_map = {**{v: v for v in _profile_canonical}, **_profile_variants}

card_profile_expr = F.lit(None).cast("string")
for raw, canonical in _profile_map.items():
    card_profile_expr = F.when(F.trim(F.col("account_name")) == raw, F.lit(canonical)).otherwise(card_profile_expr)

# COMMAND ----------

# DBTITLE 1,Categorical map coverage check
# ── For each mapped categorical ──────────────────────────────────────────────
# (1) full table raw value × mapped result (low cardinality, readable in full;
#     rows where the map returns NULL are sorted first), and
# (2) count of unmapped non-null raw values → candidates to add to the map.
# line/station/etc. only get trim (thousands of values, no dictionary) — not checked here.
cat_maps = {
    "emisor":       ("issuer_id", issuer_expr),
    "operator":     ("operator_id", operator_expr),
    "card_type":    ("card_type_id", card_type_expr),
    "account_name": ("card_profile", card_profile_expr),
}

for raw_col, (clean_name, expr) in cat_maps.items():
    print(f"\n{'='*70}\n{raw_col} → {clean_name}\n{'='*70}")
    df_tmp = df_with_parsed_dates.withColumn(clean_name, expr)
    display(
        df_tmp
        .groupBy(raw_col, clean_name)
        .count()
        .orderBy(F.col(clean_name).isNull().desc(), F.col("count").desc())
    )
    n_unmapped = (
        df_tmp
        .filter(F.col(clean_name).isNull() & F.col(raw_col).isNotNull() & (F.trim(F.col(raw_col)) != ""))
        .count()
    )
    print(f"Unmapped non-null raw values: {n_unmapped:,} rows")

# COMMAND ----------

# DBTITLE 1,Section 8: Consolidation
# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Consolidation

# COMMAND ----------

# DBTITLE 1,Missing values per row (B5)
# ── % of missing content columns per row ─────────────────────────────────────
# Old-code handled this ad hoc at ingestion (probed rows with null `value` per
# schema, dropped empty Excel rows / any-null rows of one corrupted file).
# Systematic version: share of missing content columns per row.
#   - rows 100% missing → dropped in the consolidation below
#   - rows with many (but not all) missing → inspect here, decide case by case
_content_cols = [c for c in spark.table(BRONZE_TABLE).columns
                 if c not in ("_source_file", "_header_group", "_transform_format", "_ingestion_ts")]

_missing_count = sum(
    F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)
    for c in _content_cols
)
pct_missing_expr = (_missing_count / F.lit(len(_content_cols)))

# Distribution of per-row missing share
display(
    df_with_parsed_dates
    .withColumn("_pct_missing", F.round(pct_missing_expr, 2))
    .groupBy("_pct_missing")
    .count()
    .orderBy("_pct_missing")
)

# Sample of high-missing (but not fully empty) rows, with their source file
display(
    df_with_parsed_dates
    .withColumn("_pct_missing", pct_missing_expr)
    .filter((F.col("_pct_missing") >= 0.5) & (F.col("_pct_missing") < 1.0))
    .select("_source_file", "_pct_missing", *_content_cols)
    .limit(50)
)

# COMMAND ----------

# DBTITLE 1,Exclude duplicate re-delivery files (H10)
# ── Files confirmed as duplicate re-deliveries ────────────────────────────────
# The ~10 files dated Sep 30 2017 duplicate the monthly files' content:
# ~100% of their rows match a monthly-file row on card + exact timestamp
# (see CLEANING_DECISIONS_2017.md, H10). They are excluded from silver entirely.
# TODO: paste the file names from the "Window files" diagnostic cell output.
EXCLUDED_SOURCE_FILES = [
    # "example_file_dated_2017-09-30.csv",
]

if EXCLUDED_SOURCE_FILES:
    _is_excluded = F.lit(False)
    for fname in EXCLUDED_SOURCE_FILES:
        _is_excluded = _is_excluded | F.col("_source_file").contains(fname)
    n_excluded = df_with_parsed_dates.filter(_is_excluded).count()
    print(f"Excluding {len(EXCLUDED_SOURCE_FILES)} files, {n_excluded:,} rows.")
    df_for_silver = df_with_parsed_dates.filter(~_is_excluded)
else:
    print("⚠️  EXCLUDED_SOURCE_FILES is empty — no files excluded yet (paste the Sep-30 file names).")
    df_for_silver = df_with_parsed_dates

# ── Drop fully-empty rows (B5: 100% of content columns missing) ──────────────
n_empty = df_for_silver.filter(pct_missing_expr >= 1.0).count()
print(f"Fully-empty rows dropped (100% missing content columns): {n_empty:,}")
df_for_silver = df_for_silver.filter(pct_missing_expr < 1.0)

# COMMAND ----------

# DBTITLE 1,Consolidate all cleaning into df_silver
# ══════════════════════════════════════════════════════════════════════════════
# CONSOLIDATION: chain ALL cleaning transformations into a single df_silver
# ══════════════════════════════════════════════════════════════════════════════


# ── 4. Build df_silver with all transformations ───────────────────────────────
df_silver = (
    df_for_silver
    # Categorical mappings
    .withColumn("issuer_id", issuer_expr)
    .withColumn("operator_id", operator_expr)
    .withColumn("card_type_id", card_type_expr)
    # Card profile canonical map (A4) — keep raw account_name alongside
    .withColumn("card_profile", card_profile_expr)
    # Numeric casts (A3) — overwrite in place; raw strings remain in bronze
    .withColumns(_numeric_casts)
    # Trim + remove trailing slashes
    .withColumn("line_id", F.regexp_replace(F.trim(F.col("line")), r"/$", ""))
    .withColumn("station_id", F.regexp_replace(F.trim(F.col("station")), r"/$", ""))
    .withColumn("station_access_id", F.regexp_replace(F.trim(F.col("station_access")), r"/$", ""))
    .withColumn("machine_id", F.regexp_replace(F.trim(F.col("machine")), r"/$", ""))
    .withColumn("vehicle_id_clean", F.regexp_replace(F.trim(F.col("vehicle_id")), r"/$", ""))
    .withColumn("route_id", F.regexp_replace(F.trim(F.col("route")), r"/$", ""))
    .withColumn("fare_type_id", F.regexp_replace(F.trim(F.col("fare_type")), r"/$", ""))
    # Conditional mappings
    .withColumn("phase_id", F.when(F.col("phase").contains("3"), F.lit("Phase 3")).otherwise(F.lit(None)))
    .withColumn("peak_hour_id", 
        F.when(F.col("peak_hour") == "Peak Time", F.lit("Peak Time"))
         .when(F.col("peak_hour") == "Non Peak Time", F.lit("Non Peak Time"))
         .otherwise(F.lit(None)))
    .withColumn("vehicle_type_id", 
        F.when(F.col("vehicle_type") == "(02) Urbano", F.lit("(02) Urbano")).otherwise(F.lit(None)))
    # Select only clean columns for silver
    .select(
        # Date columns (already cleaned)
        "fecha_transaccion_timestamp",
        "fecha_transaccion",
        "clearing_date",
        # Cleaned categorical columns
        "issuer_id",
        "operator_id",
        "line_id",
        "station_id",
        "station_access_id",
        "machine_id",
        "phase_id",
        "peak_hour_id",
        "vehicle_id_clean",
        "route_id",
        "fare_type_id",
        "card_type_id",
        "vehicle_type_id",
        # Card profile: canonical (A4 map) + raw string preserved
        "card_profile",
        "account_name",
        # Numeric columns (cast in A3: cardnumber long, others double)
        "cardnumber",
        "balance_before",
        "value",
        "balance_after",
        # Pass-through columns
        "system",
        "day_group_type",
        # Metadata
        "_source_file",
        "_header_group",
        "_transform_format",
        "_ingestion_ts",
    )
)

# ── 5. Summary ────────────────────────────────────────────────────────────────
print(f"df_silver columns: {len(df_silver.columns)}")
print(f"Schema:")
df_silver.printSchema()
print(f"\nRow count: {df_silver.count():,}")

# COMMAND ----------

# DBTITLE 1,Consolidation checks: NULLs introduced by casts and maps
# ── Rows where the raw value existed but the clean version is NULL ────────────
# For numerics: garbage that failed the cast (checked on the pre-cast dataframe,
# since df_silver overwrites the columns in place). For card_profile: raw values
# not covered by the A4 map (expected: "(000)" ~35 rows, "1" 1 row — anything
# else means a new unmapped value appeared and the map needs updating).
for raw_col, cast_expr in _numeric_casts.items():
    n = (
        df_for_silver
        .filter(cast_expr.isNull() & F.col(raw_col).isNotNull() & (F.trim(F.col(raw_col)) != ""))
        .count()
    )
    print(f"{raw_col:<16} cast to NULL with non-null raw: {n:,}")

n_profile = (
    df_silver
    .filter(F.col("card_profile").isNull() & F.col("account_name").isNotNull() & (F.trim(F.col("account_name")) != ""))
    .count()
)
print(f"{'card_profile':<16} NULL with non-null raw (account_name): {n_profile:,}")

# Show the unmapped raw profiles, if any
print("\n── Unmapped account_name values (should be only (000) and '1') ──")
display(
    df_silver
    .filter(F.col("card_profile").isNull() & F.col("account_name").isNotNull())
    .groupBy("account_name")
    .count()
    .orderBy(F.col("count").desc())
)
