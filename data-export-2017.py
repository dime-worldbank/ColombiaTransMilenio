# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///

# DBTITLE 1,Index
# MAGIC %md
# MAGIC ## Index
# MAGIC
# MAGIC Exports the analysis tables to CSV files for Stata, plus a generated `import_2017.do` that reads the CSVs, applies value labels and saves `.dta` files. The panel is already restricted to the analysis sample; the cards file covers ALL cards in the window, with `in_sample` (membership in the panel, computed here — Stata only converts formats and applies labels) plus the presence flags, superswiper tag and treatment that explain who is in and out.
# MAGIC
# MAGIC `treatment` is exported as stable numeric codes defined explicitly below, so the codes never change across re-runs. Each card gets a sequential integer `card_id` (the panel key in Stata); the original `cardnumber` is kept only in the cards file, as a string, for the Sisbén crosswalk merge.
# MAGIC
# MAGIC 1. **Setup** — packages, catalog, parameters, code maps
# MAGIC 2. **Read tables** — input checks
# MAGIC 3. **Card ids** — sequential `card_id` per card
# MAGIC 4. **Cards export** — `cards_2017.csv`
# MAGIC 5. **Panel export** — `panel_2017.csv`
# MAGIC 6. **Stata import do-file** — `import_2017.do`
# MAGIC 7. **Checks** — re-read the CSVs and compare against the tables

# COMMAND ----------

# DBTITLE 1,Section 1: Setup
# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Setup

# COMMAND ----------

# DBTITLE 1,Import packages
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Select catalog
# MAGIC %sql
# MAGIC USE CATALOG prd_mega;
# MAGIC USE SCHEMA scolom15;
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema;

# COMMAND ----------

# DBTITLE 1,Parameters
# Input tables
T4_TABLE = "prd_mega.scolom15.cards_2017"
T5_TABLE = "prd_mega.scolom15.panel_2017"

# Output folder on the volume
EXPORT_DIR = "/Volumes/prd_csc_mega/sColom15/Workspace/Construct/export_2017/"

# Stable numeric codes for `treatment` (never reassigned: new values must be
# added here explicitly, existing codes are never changed)
TREATMENT_CODES = {
    "never":      0,
    "apoyo_kept": 1,
    "apoyo_lost": 2,
    "apoyo_gain": 3,
    "mayor_kept": 4,
    "mayor_lost": 5,
    "mayor_gain": 6,
}


# Numeric code from a string column: values not in the map stay NULL
def code_expr(colname, codes):
    expr = F.lit(None).cast("int")
    for value, code in codes.items():
        expr = F.when(F.col(colname) == value, F.lit(code)).otherwise(expr)
    return expr


# Write a dataframe as a single named CSV file on the volume
def write_single_csv(df, filename):
    tmp_dir = EXPORT_DIR + "_tmp_" + filename
    out_path = EXPORT_DIR + filename
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)
    part = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.startswith("part-")][0]
    dbutils.fs.cp(part, out_path)
    dbutils.fs.rm(tmp_dir, recurse=True)
    size_mb = dbutils.fs.ls(out_path)[0].size / 1024**2
    print(f"✅ Wrote {out_path} ({size_mb:,.0f} MB)")

# COMMAND ----------

# DBTITLE 1,Section 2: Read tables
# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Read tables

# COMMAND ----------

# DBTITLE 1,Read and check inputs
df_t4 = spark.table(T4_TABLE)
df_t5 = spark.table(T5_TABLE)

n_t4 = df_t4.count()
n_t5 = df_t5.count()
print(f"{T4_TABLE}: {n_t4:,} cards")
print(f"{T5_TABLE}: {n_t5:,} card-months")

# Every treatment value in the data must have a code in the map
unmapped = (
    df_t4.filter(F.col("treatment").isNotNull() & ~F.col("treatment").isin(list(TREATMENT_CODES)))
    .groupBy("treatment").count().collect()
)
if unmapped:
    print(f"⚠️  Values of `treatment` without a numeric code: {[(r[0], r[1]) for r in unmapped]}")
else:
    print("✅ All non-null values of `treatment` are covered by the code map.")

# COMMAND ----------

# DBTITLE 1,Section 3: Card ids
# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Card ids
# MAGIC
# MAGIC One sequential integer `card_id` per card, assigned by `cardnumber` order over ALL cards in `cards_2017`. Deterministic given the same card universe, and shared by both exports so they merge on `card_id` in Stata.

# COMMAND ----------

# DBTITLE 1,Sequential card_id per card
df_ids = df_t4.select("cardnumber").withColumn(
    "card_id", F.row_number().over(Window.orderBy("cardnumber"))
)

# COMMAND ----------

# DBTITLE 1,Section 4: Cards export
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Cards export → `cards_2017.csv`
# MAGIC
# MAGIC ALL cards in the window. `in_sample` marks membership in `panel_2017` itself (not a re-application of the sample filter, so it can never drift from the actual panel); the columns behind the filter — presence flags, `tag_superswiper`, `treatment` — ride along so *why* a card is out is visible in Stata, plus `tag_infrequent` (the robustness flag), the apoyo subsidized-month counts and the pre/post spending totals. `cardnumber` is a string column (long ids lose precision as doubles), kept for the Sisbén crosswalk merge.

# COMMAND ----------

# DBTITLE 1,Build and write cards CSV
df_sample_cards = df_t5.select("cardnumber").distinct().withColumn("in_sample", F.lit(1))

df_cards_out = (
    df_t4
    .join(df_ids, "cardnumber")
    .join(df_sample_cards, "cardnumber", "left")
    .withColumn("in_sample", F.coalesce(F.col("in_sample"), F.lit(0)))
    .withColumn("treatment", code_expr("treatment", TREATMENT_CODES))
    .withColumn("cardnumber", F.col("cardnumber").cast("string"))
    # card_id first, cardnumber second (the do-file imports column 2 as string)
    .select(
        "card_id", "cardnumber", "in_sample", "treatment",
        "tag_infrequent", "tag_superswiper",
        "in_6m_bef", "in_6m_aft",
        "apoyo_m_in_6m_bef", "apoyo_m_in_6m_aft",
        "tot_value_no_tr_6bef", "tot_value_no_tr_6aft",
    )
)

write_single_csv(df_cards_out, "cards_2017.csv")

# COMMAND ----------

# DBTITLE 1,Section 5: Panel export
# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Panel export → `panel_2017.csv`
# MAGIC
# MAGIC The balanced panel keyed by `card_id` × `ymonth`; `cardnumber` is dropped (card-level variables merge from the cards file on `card_id` at analysis time).

# COMMAND ----------

# DBTITLE 1,Build and write panel CSV
df_panel_out = (
    df_t5
    .join(df_ids, "cardnumber")
    .drop("cardnumber", "avg_daily_trips")
)
panel_cols = ["card_id", "ymonth"] + [c for c in df_panel_out.columns if c not in ("card_id", "ymonth")]
df_panel_out = df_panel_out.select(panel_cols)

n_panel_out = df_panel_out.count()
if n_panel_out == n_t5:
    print(f"✅ Panel keeps all {n_t5:,} card-months after the card_id join.")
else:
    print(f"⚠️  Panel rows changed after the card_id join: {n_t5:,} → {n_panel_out:,}")

write_single_csv(df_panel_out, "panel_2017.csv")

# COMMAND ----------

# DBTITLE 1,Section 6: Stata import do-file
# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Stata import do-file → `import_2017.do`
# MAGIC
# MAGIC Generated from the same code maps as the CSVs, so the value labels can never drift from the exported codes. The do-file imports both CSVs, converts the month strings to Stata monthly dates, applies the value labels and saves compressed `.dta` files.

# COMMAND ----------

# DBTITLE 1,Generate and write the do-file
def label_define(name, codes):
    pairs = " ".join(f'{code} "{value}"' for value, code in sorted(codes.items(), key=lambda kv: kv[1]))
    return f"label define {name} {pairs}"


do_file = f"""* import_2017.do — GENERATED FILE, do not edit by hand.
* This do-file and the data that come with it (cards_2017.csv, panel_2017.csv)
* are generated by data-export-2017.py in the repo:
*   https://github.com/dime-worldbank/ColombiaTransMilenio
* To change columns, codes or labels, edit that notebook and re-run it there.
* Run from the folder containing the CSVs.

* ---- Cards (one row per card, ALL cards in the window) ----
import delimited using "cards_2017.csv", clear stringcols(2) case(preserve)

{label_define("treatment", TREATMENT_CODES)}
label values treatment treatment

compress
save "cards_2017.dta", replace

* ---- Panel (balanced card × month, analysis sample only) ----
import delimited using "panel_2017.csv", clear case(preserve)

gen mdate = monthly(ymonth, "YM")
format mdate %tm
drop ymonth

compress
save "panel_2017.dta", replace

* Panel setup after merging card-level variables (the panel's cards are
* exactly the cards with in_sample==1):
*   use panel_2017, clear
*   merge m:1 card_id using cards_2017, keep(match) nogen
*   xtset card_id mdate
"""

with open(EXPORT_DIR + "import_2017.do", "w", newline="\n") as f:
    f.write(do_file)
print(f"✅ Wrote {EXPORT_DIR}import_2017.do")
print(do_file)

# COMMAND ----------

# DBTITLE 1,Section 7: Checks
# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Checks

# COMMAND ----------

# DBTITLE 1,Re-read the CSVs and compare against the tables
chk_cards = spark.read.option("header", True).csv(EXPORT_DIR + "cards_2017.csv")
chk_panel = spark.read.option("header", True).csv(EXPORT_DIR + "panel_2017.csv")

n_chk_cards = chk_cards.count()
n_chk_panel = chk_panel.count()
print(f"cards_2017.csv: {n_chk_cards:,} rows {'✅' if n_chk_cards == n_t4 else f'⚠️ table has {n_t4:,}'}")
print(f"panel_2017.csv: {n_chk_panel:,} rows {'✅' if n_chk_panel == n_t5 else f'⚠️ table has {n_t5:,}'}")

n_ids_panel = chk_panel.select("card_id").distinct().count()
n_months = chk_panel.select("ymonth").distinct().count()
if n_chk_panel == n_ids_panel * n_months:
    print(f"✅ Balanced: {n_ids_panel:,} cards × {n_months} months.")
else:
    print(f"⚠️  Not balanced: {n_ids_panel:,} cards × {n_months} months ≠ {n_chk_panel:,} rows.")

# in_sample in the cards CSV must mark exactly the panel's cards
n_sample_csv = chk_cards.filter(F.col("in_sample") == "1").count()
if n_sample_csv == n_ids_panel:
    print(f"✅ in_sample marks exactly the panel's {n_ids_panel:,} cards.")
else:
    print(f"⚠️  in_sample marks {n_sample_csv:,} cards; the panel has {n_ids_panel:,}.")

print("── Cards by treatment code (label = code map) ──")
display(chk_cards.groupBy("treatment").count().orderBy("treatment"))
