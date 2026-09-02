# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Index
# MAGIC %md
# MAGIC ## Index
# MAGIC
# MAGIC Builds `silver_validaciones_2017window` from the silver table: restricts to the analysis window and adds cleaning tags and constructed columns. Tags only — no rows are dropped; any drop is decided at analysis time.
# MAGIC
# MAGIC 1. **Setup** — packages, catalog, parameters
# MAGIC 2. **Window filter** — keep the 12 monthly files of the analysis window
# MAGIC 3. **Card activity tags** — infrequent users and superswipers
# MAGIC 4. **Row tags** — implausible balance, impossible fare, early zero
# MAGIC 5. **Time and trip variables** — month/week/day/hour…, trip/transfer
# MAGIC 6. **Profile group** — analytical grouping of card profiles
# MAGIC 7. **Station fix** — use the access station for zonal operators
# MAGIC 8. **Profile switches** — implausible/plausible switch tags and imputed profile (baseline for classification)
# MAGIC 9. **Build T2** — consolidate all columns and write the table
# MAGIC 10. **Status figures** — tag incidence, profiles by month, balance dates, days per card

# COMMAND ----------

# DBTITLE 1,Section 1: Setup
# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Setup

# COMMAND ----------

# DBTITLE 1,Import packages
from pyspark.sql import functions as F
from pyspark.sql import Window
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# COMMAND ----------

# DBTITLE 1,Select catalog
# MAGIC %sql
# MAGIC USE CATALOG prd_mega;
# MAGIC USE SCHEMA scolom15;
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema;

# COMMAND ----------

# DBTITLE 1,Parameters
# Input and output tables
T1_TABLE = "prd_mega.scolom15.silver_validaciones_oct2016tosep2017"
T2_TABLE = "prd_mega.scolom15.silver_validaciones_oct2016tosep2017_tags"

# Analysis window and reform date
WINDOW_START = "2016-10-01"
WINDOW_END   = "2017-09-30"   # inclusive
REFORM_DATE  = "2017-04-01"

# The window is defined by these 12 monthly files, not by clearing_date
# (filtering by clearing_date pulled in duplicate files).
WINDOW_FILES = [
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

# Infrequent user: active on fewer than this many distinct days in the window
MIN_DISTINCT_DAYS = 12

# Superswiper: more than SWIPES_DAY_MAX transactions in one day, or more than
# SWIPES_DAY_HIGH per day on more than DAYS_HIGH_MAX days
SWIPES_DAY_MAX  = 100
SWIPES_DAY_HIGH = 20
DAYS_HIGH_MAX   = 2

# Implausible balance: above the maximum rechargeable amount (COP)
BALANCE_MAX = 1_000_000

# Fare values that could not exist under the scheme of their period.
# Pre-reform: post-reform fares (200 transfer, 1450/1650 SISBEN, 2200 full TM)
# Post-reform: pre-reform fares (700/1000 discounts, 1550 senior SITP, 1700 full SITP)
# Always: never a valid fare in either scheme (1600 labelled "Error?!" in old code)
IMPOSSIBLE_FARES_BY_PERIOD = [
    (WINDOW_START, "2017-03-31", [200, 1450, 1650, 2200]),
    (REFORM_DATE,  WINDOW_END,   [700, 1000, 1550, 1700]),
]
IMPOSSIBLE_FARES_ALWAYS = [900, 1600]

# A trip pays more than this; anything at or below is a transfer
TRIP_MIN_VALUE = 300

# The trunk operator (all other operators are zonal)
TRUNK_OPERATOR = "(201) Trunk agency"

# COMMAND ----------

# DBTITLE 1,Section 2: Window filter
# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Window filter

# COMMAND ----------

# DBTITLE 1,Validate monthly files and filter by transaction date
df_t1 = spark.table(T1_TABLE)

# --- Step 1: Validate source files (all expected, no extras) ---
found_paths = [r[0] for r in df_t1.select("_source_file").distinct().collect()]
found_basenames = {p.rsplit("/", 1)[-1] for p in found_paths}
expected = set(WINDOW_FILES)

missing = sorted(expected - found_basenames)
extra   = sorted(found_basenames - expected)

if missing:
    print(f"⚠️  Missing files: {missing}")
else:
    print(f"✅ All {len(WINDOW_FILES)} expected monthly files found.")
if extra:
    print(f"⚠️  Extra files not in WINDOW_FILES: {extra}")
else:
    print(f"✅ No extra files beyond the expected {len(WINDOW_FILES)}.")

# --- Step 2: Filter by transaction date ---
df_win = df_t1.filter(
    F.col("fecha_transaccion").between(WINDOW_START, WINDOW_END)
)

n_t1  = df_t1.count()
n_win = df_win.count()
print(f"\nSilver rows                             : {n_t1:,}")
print(f"Window rows (fecha_transaccion filter)  : {n_win:,}  ({100*n_win/n_t1:.1f}%)")

# Rows and date range per source file
display(
    df_win
    .groupBy("_source_file")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.min("fecha_transaccion").alias("min_fecha"),
        F.max("fecha_transaccion").alias("max_fecha"),
    )
    .orderBy("min_fecha")
)

# COMMAND ----------

# DBTITLE 1,Section 3: Card activity tags
# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Card activity tags
# MAGIC
# MAGIC `tag_infrequent`: the card was active on fewer than 12 distinct days in the window. `tag_superswiper`: more than 100 transactions in one day, or more than 20 per day on more than 2 days. Both are card-level: every row of a tagged card carries the tag.

# COMMAND ----------

# DBTITLE 1,Distinct days and daily peaks per card
# Count transactions per card per day, then aggregate to one row per card.
_daily_card = (
    df_win
    .filter(F.col("cardnumber").isNotNull() & F.col("fecha_transaccion").isNotNull())
    .groupBy("cardnumber", "fecha_transaccion")
    .agg(F.count(F.lit(1)).alias("n_tx_day"))
)

df_card_activity = (
    _daily_card
    .groupBy("cardnumber")
    .agg(
        F.count(F.lit(1)).alias("n_days_active"),
        F.max("n_tx_day").alias("max_daily_tx"),
        F.sum(F.when(F.col("n_tx_day") > SWIPES_DAY_HIGH, 1).otherwise(0)).alias("n_days_high"),
    )
    .withColumn("tag_infrequent", (F.col("n_days_active") < MIN_DISTINCT_DAYS).cast("int"))
    .withColumn(
        "tag_superswiper",
        ((F.col("max_daily_tx") > SWIPES_DAY_MAX) | (F.col("n_days_high") > DAYS_HIGH_MAX)).cast("int"),
    )
)

display(
    df_card_activity.agg(
        F.count(F.lit(1)).alias("cards"),
        F.sum("tag_infrequent").alias("cards_infrequent"),
        F.sum("tag_superswiper").alias("cards_superswiper"),
    )
)

# COMMAND ----------

# DBTITLE 1,Section 4: Row tags
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Row tags
# MAGIC
# MAGIC `tag_high_balance`: balance before the trip above the maximum rechargeable. `tag_impossible_fare`: value that did not exist under the fare scheme of its period. `tag_early_zero`: value 0 before the reform, when free transfers did not exist yet.

# COMMAND ----------

# DBTITLE 1,Tag expressions and sizing
tag_high_balance_expr = (F.col("balance_before") > BALANCE_MAX).cast("int")

# Value in the impossible list for its period, or always-impossible
_impossible = F.col("value").isin(IMPOSSIBLE_FARES_ALWAYS) & F.col("value").isNotNull()
for p_start, p_end, bad_fares in IMPOSSIBLE_FARES_BY_PERIOD:
    _impossible = _impossible | (
        F.col("fecha_transaccion").between(p_start, p_end)
        & F.col("value").isin(bad_fares)
    )
tag_impossible_fare_expr = _impossible.cast("int")

tag_early_zero_expr = ((F.col("value") == 0) & (F.col("fecha_transaccion") < F.lit(REFORM_DATE))).cast("int")

# How many rows each tag catches
df_row_tags = (
    df_win
    .withColumn("tag_high_balance", tag_high_balance_expr)
    .withColumn("tag_impossible_fare", tag_impossible_fare_expr)
    .withColumn("tag_early_zero", tag_early_zero_expr)
)
display(
    df_row_tags.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("tag_high_balance").alias("rows_high_balance"),
        F.sum("tag_impossible_fare").alias("rows_impossible_fare"),
        F.sum("tag_early_zero").alias("rows_early_zero"),
    )
)

# Which values get tagged as impossible, by period
for p_start, p_end, bad_fares in IMPOSSIBLE_FARES_BY_PERIOD:
    print(f"\nPeriod {p_start} → {p_end} (impossible: {sorted(bad_fares)}) — tagged values:")
    display(
        df_row_tags
        .filter(F.col("fecha_transaccion").between(p_start, p_end) & (F.col("tag_impossible_fare") == 1))
        .groupBy("value").count().orderBy(F.col("count").desc()).limit(30)
    )

# COMMAND ----------

# DBTITLE 1,Section 5: Time and trip variables
# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Time and trip variables

# COMMAND ----------

# DBTITLE 1,Expressions
# Calendar parts of the transaction timestamp
time_cols = {
    "ymonth":    F.date_format("fecha_transaccion_timestamp", "yyyy-MM"),
    "month":     F.month("fecha_transaccion_timestamp"),
    "week":      F.weekofyear("fecha_transaccion_timestamp"),
    "day":       F.dayofmonth("fecha_transaccion_timestamp"),
    "dayofweek": F.dayofweek("fecha_transaccion_timestamp"),
    "hour":      F.hour("fecha_transaccion_timestamp"),
    "minute":    F.minute("fecha_transaccion_timestamp"),
    "second":    F.second("fecha_transaccion_timestamp"),
}

# A trip pays more than TRIP_MIN_VALUE; anything else is a transfer
trip_cols = {
    "trip":     (F.col("value") > TRIP_MIN_VALUE).cast("int"),
    "transfer": (F.col("value") <= TRIP_MIN_VALUE).cast("int"),
}

# COMMAND ----------

# DBTITLE 1,Section 6: Profile group
# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Profile group
# MAGIC
# MAGIC Groups the card profiles for analysis. `Adulto PV` (PV = personalización virtual) is an adulto card and maps to `adulto`; `frecuente` stays separate on purpose: it cannot serve as comparison cards. A NULL profile stays NULL.

# COMMAND ----------

# DBTITLE 1,Map card_profile → profile_group
_profile_group_map = {
    "(001) Adulto":                        "adulto",
    "(001) Anonymous":                     "anonymous",
    "(002) Adulto Mayor":                  "mayor",
    "(006) Apoyo Ciudadano":               "apoyo",
    "(101) Adulto PV":                     "adulto",
    "(014) Usuario frecuente":             "frecuente",
    "(004) Menor de Edad":                 "menor",
    "(003) Estudiantil":                   "estudiantil",
    "(018) Universitaria":                 "estudiantil",
    "(027) Club Universitario":            "estudiantil",
    "(005) Discapacidad":                  "discapacidad",
    "(006) Discapacitados":                "discapacidad",
    "(017) Discapacitado Monedero":        "discapacidad",
    "(022) Empresarial TM":                "empresarial",
    "(023) Empresarial Davivienda":        "empresarial",
    "(024) Empresarial Colsubsidio":       "empresarial",
    "(025) Empresarial Compensar":         "empresarial",
    "(026) Empresarial AV Villas":         "empresarial",
    "(029) Empresarial Banco de Bogotá":   "empresarial",
    "(032) Empresarial Daviplata":         "empresarial",
    "(033) Empresarial People Pass":       "empresarial",
    "(035) Empresarial AV Villas Crédito": "empresarial",
    "(036) Empresarial Colpatria":         "empresarial",
    "(041) Empresarial Cercanos":          "empresarial",
    "(044) Empresarial CIS":               "empresarial",
    "(003) Capital":                       "other",
    "(008) Étnico":                        "other",
    "(021) Tarjeta Ciudadana":             "other",
    "(030) Capital monedero":              "other",
}

profile_group_expr = F.lit(None).cast("string")
for canonical, group in _profile_group_map.items():
    profile_group_expr = F.when(F.col("card_profile") == canonical, F.lit(group)).otherwise(profile_group_expr)

# Every non-null profile in the window must be grouped
_unmapped = (
    df_win
    .withColumn("profile_group", profile_group_expr)
    .filter(F.col("profile_group").isNull() & F.col("card_profile").isNotNull())
    .groupBy("card_profile").count().orderBy(F.col("count").desc())
)
n_unmapped_pg = _unmapped.count()
if n_unmapped_pg == 0:
    print("✅ Every non-null card_profile maps to a profile_group.")
else:
    print(f"⚠️  {n_unmapped_pg} card_profile values have no profile_group — add them to the map:")
    display(_unmapped)

# COMMAND ----------

# DBTITLE 1,Section 7: Station fix
# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Station fix
# MAGIC
# MAGIC For zonal operators the meaningful location is the access station, not `station`. Deeper station cleaning (name standardization, geography) is pending and will plug in here.

# COMMAND ----------

# DBTITLE 1,Expressions
is_trunk_expr = (F.col("operator_id") == TRUNK_OPERATOR)

station_fixed_expr = F.when(
    ~is_trunk_expr & F.col("station_access_id").isNotNull() & (F.trim(F.col("station_access_id")) != ""),
    F.col("station_access_id"),
).otherwise(F.col("station_id"))

# COMMAND ----------

# DBTITLE 1,Section 8: Profile switches
# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Profile switches
# MAGIC
# MAGIC A card can plausibly go anonymous → personalized (bought anonymous, then registered), but never personalized → anonymous: those records are trunk devices mis-recording the profile. Here we tag the switching cards and build the imputed profile, which is the baseline profile for card classification; the original profile is kept for robustness checks.

# COMMAND ----------

# DBTITLE 1,Switch tags per card
# For each card, order its transactions in time and look at each pair of
# consecutive profiles:
#   personalized → anonymous : impossible in real life → tag_implausible_switch
#   anonymous → personalized with no impossible switch : tag_plausible_switch
_is_anon = F.col("card_profile") == "(001) Anonymous"

_w_card = Window.partitionBy("cardnumber").orderBy("fecha_transaccion_timestamp")

_df_transitions = (
    df_win
    .filter(F.col("cardnumber").isNotNull() & F.col("fecha_transaccion_timestamp").isNotNull() & F.col("card_profile").isNotNull())
    .withColumn("_is_anon", _is_anon.cast("int"))
    .withColumn("_prev_anon", F.lag("_is_anon").over(_w_card))
)

df_card_switches = (
    _df_transitions
    .groupBy("cardnumber")
    .agg(
        F.max(F.when((F.col("_prev_anon") == 0) & (F.col("_is_anon") == 1), 1).otherwise(0)).alias("_pers_to_anon"),
        F.max(F.when((F.col("_prev_anon") == 1) & (F.col("_is_anon") == 0), 1).otherwise(0)).alias("_anon_to_pers"),
    )
    .withColumn("tag_implausible_switch", F.col("_pers_to_anon"))
    .withColumn(
        "tag_plausible_switch",
        ((F.col("_anon_to_pers") == 1) & (F.col("_pers_to_anon") == 0)).cast("int"),
    )
    .select("cardnumber", "tag_implausible_switch", "tag_plausible_switch")
)

_switch_stats = df_card_switches.agg(
    F.count(F.lit(1)).alias("cards"),
    F.sum("tag_implausible_switch").alias("cards_implausible_switch"),
    F.sum("tag_plausible_switch").alias("cards_plausible_switch"),
).collect()[0]

_n_cards = _switch_stats["cards"]
_n_impl  = _switch_stats["cards_implausible_switch"]
_n_plaus = _switch_stats["cards_plausible_switch"]

print(f"Total cards                : {_n_cards:,}")
print(f"Cards implausible switch   : {_n_impl:,}  ({100*_n_impl/_n_cards:.2f}%)")
print(f"Cards plausible switch     : {_n_plaus:,}  ({100*_n_plaus/_n_cards:.2f}%)")

# COMMAND ----------

# DBTITLE 1,Implausible switches: month distribution
# Month distribution of all personalized → anonymous transition events
_transitions_impl = (
    _df_transitions
    .filter((F.col("_prev_anon") == 0) & (F.col("_is_anon") == 1))  # pers → anon
)

print("Implausible (pers→anon) transition events by month:")
display(
    _transitions_impl
    .groupBy(F.date_format("fecha_transaccion_timestamp", "yyyy-MM").alias("month"))
    .agg(
        F.count(F.lit(1)).alias("transition_events"),
        F.countDistinct("cardnumber").alias("distinct_cards"),
    )
    .orderBy("month")
)

# COMMAND ----------

# DBTITLE 1,Diagnose: why is implausible switch so high?
# --- 1. How many pers→anon events per flagged card? ---
# If most cards have just 1 event, the window-order may be producing
# false transitions (e.g. ties in timestamp).
_impl_events_per_card = (
    _df_transitions
    .filter((F.col("_prev_anon") == 0) & (F.col("_is_anon") == 1))
    .groupBy("cardnumber")
    .agg(F.count(F.lit(1)).alias("n_impl_events"))
)
print("Distribution of implausible-event count per flagged card:")
display(
    _impl_events_per_card
    .groupBy("n_impl_events")
    .agg(F.count(F.lit(1)).alias("cards"))
    .orderBy("n_impl_events")
    .limit(20)
)

# --- 2. Do tied timestamps exist? ---
# Two rows on the same card with the exact same timestamp but different
# profiles would create a spurious transition in either direction.
_ties = (
    _df_transitions
    .groupBy("cardnumber", "fecha_transaccion_timestamp")
    .agg(
        F.countDistinct("card_profile").alias("n_profiles"),
        F.count(F.lit(1)).alias("n_rows"),
    )
    .filter(F.col("n_profiles") > 1)
)
n_ties = _ties.count()
print(f"\nCard-timestamp pairs with >1 distinct profile (ties): {n_ties:,}")
if n_ties > 0:
    print("Sample ties:")
    display(_ties.limit(10))

# --- 3. Show the actual profile sequence for 5 flagged cards ---
_sample_cards = (
    df_card_switches
    .filter(F.col("tag_implausible_switch") == 1)
    .select("cardnumber")
    .limit(5)
)
print("\nProfile sequence for 5 implausible-switch cards:")
display(
    _df_transitions
    .join(_sample_cards, "cardnumber", "inner")
    .select("cardnumber", "fecha_transaccion_timestamp", "card_profile",
            "value", "_is_anon", "_prev_anon", "operator_id")
    .orderBy("cardnumber", "fecha_transaccion_timestamp")
)



# COMMAND ----------

# DBTITLE 1,Implausible switches: trunk vs zonal + transfer check
# --- 4. Do implausible transitions happen on trunk, zonal, or both? ---
# For each pers→anon event, check whether the CURRENT row (the one that
# flipped to anonymous) was recorded by a trunk or zonal operator.
print("\nImplausible transitions: operator type of the anonymous row:")
display(
    _df_transitions
    .filter((F.col("_prev_anon") == 0) & (F.col("_is_anon") == 1))
    .withColumn("operator_type", F.when(F.col("operator_id") == TRUNK_OPERATOR, "trunk").otherwise("zonal"))
    .groupBy("operator_type")
    .agg(
        F.count(F.lit(1)).alias("transition_events"),
        F.countDistinct("cardnumber").alias("distinct_cards"),
    )
)

# And which specific operators produce these anonymous readings?
print("\nImplausible transitions by operator_id:")
display(
    _df_transitions
    .filter((F.col("_prev_anon") == 0) & (F.col("_is_anon") == 1))
    .groupBy("operator_id")
    .agg(
        F.count(F.lit(1)).alias("transition_events"),
        F.countDistinct("cardnumber").alias("distinct_cards"),
    )
    .orderBy(F.col("transition_events").desc())
)

# --- 5. Do these mis-recorded anonymous rows show transfers after reform? ---
# A transfer is value <= TRIP_MIN_VALUE (covers both 0 and 300).
_impl_zonal = (
    _df_transitions
    .filter((F.col("_prev_anon") == 0) & (F.col("_is_anon") == 1))
    .filter(F.col("fecha_transaccion") >= F.lit(REFORM_DATE))
)
_n_impl_post = _impl_zonal.count()
_n_impl_transfer = _impl_zonal.filter(F.col("value") <= TRIP_MIN_VALUE).count()
_n_impl_trip     = _n_impl_post - _n_impl_transfer

print(f"\nImplausible transitions after reform ({REFORM_DATE}):")
print(f"  Total events       : {_n_impl_post:,}")
print(f"  Transfers (≤{TRIP_MIN_VALUE})  : {_n_impl_transfer:,}  ({100*_n_impl_transfer/max(_n_impl_post,1):.2f}%)")
print(f"  Trips (>{TRIP_MIN_VALUE})       : {_n_impl_trip:,}  ({100*_n_impl_trip/max(_n_impl_post,1):.2f}%)")

# Breakdown by value for these rows
print("\nValue distribution of post-reform implausible transitions:")
display(
    _impl_zonal
    .groupBy("value")
    .agg(F.count(F.lit(1)).alias("events"))
    .orderBy(F.col("events").desc())
    .limit(15)
)

# COMMAND ----------

# DBTITLE 1,Check: adulto cards with an implausible switch
# The comparison group will be adulto cards, so we need to know how many of
# them show the impossible personalized → anonymous pattern.
_adulto_cards = (
    df_win
    .withColumn("profile_group", profile_group_expr)
    .filter(F.col("profile_group").isNotNull() & (F.col("profile_group") != "anonymous"))
    .groupBy("cardnumber")
    .agg(F.collect_set("profile_group").alias("groups"))
    .filter(F.array_contains("groups", "adulto"))
)
n_adulto = _adulto_cards.count()
n_adulto_implausible = (
    _adulto_cards
    .join(df_card_switches.filter(F.col("tag_implausible_switch") == 1), "cardnumber", "inner")
    .count()
)
print(f"Cards with adulto among their profiles : {n_adulto:,}")
print(f"  └─ with an implausible switch        : {n_adulto_implausible:,} ({100*n_adulto_implausible/max(n_adulto,1):.2f}%)")

# COMMAND ----------

# DBTITLE 1,Imputed profile (baseline for classification)
# A transaction is imputed as anonymous if the card has ANY anonymous
# transaction from that moment on: everything up to the card's last anonymous
# transaction becomes anonymous. Card classification uses this imputed
# profile; the original profile is kept for robustness.
_w_future = Window.partitionBy("cardnumber").orderBy("fecha_transaccion_timestamp") \
                  .rowsBetween(Window.currentRow, Window.unboundedFollowing)

card_profile_imputed_expr = F.when(
    F.max(_is_anon.cast("int")).over(_w_future) == 1, F.lit("(001) Anonymous")
).otherwise(F.col("card_profile"))

# Imputation only ever turns a profile into anonymous, so the imputed group
# follows directly from the original group
profile_group_imputed_expr = F.when(
    F.col("card_profile_imputed") == "(001) Anonymous", F.lit("anonymous")
).otherwise(F.col("profile_group"))



# COMMAND ----------

# DBTITLE 1,Section 9: Build T2
# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Build T2

# COMMAND ----------

# DBTITLE 1,Consolidate all columns
df_t2 = (
    df_win
    # Time and trip variables
    .withColumns(time_cols)
    .withColumns(trip_cols)
    # Profile group
    .withColumn("profile_group", profile_group_expr)
    # Station fix
    .withColumn("is_trunk", is_trunk_expr.cast("int"))
    .withColumn("station_fixed", station_fixed_expr)
    # Imputed profile (baseline for classification)
    .withColumn("card_profile_imputed", card_profile_imputed_expr)
    .withColumn("profile_group_imputed", profile_group_imputed_expr)
    # Row tags
    .withColumn("tag_high_balance", tag_high_balance_expr)
    .withColumn("tag_impossible_fare", tag_impossible_fare_expr)
    .withColumn("tag_early_zero", tag_early_zero_expr)
    # Card-level tags
    .join(
        df_card_activity.select("cardnumber", "n_days_active", "tag_infrequent", "tag_superswiper"),
        "cardnumber", "left",
    )
    .join(df_card_switches, "cardnumber", "left")
)

print(f"df_t2 columns: {len(df_t2.columns)}")
df_t2.printSchema()

# COMMAND ----------

# DBTITLE 1,Write T2 to the catalog
(
    df_t2.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T2_TABLE)
)

n_t2 = spark.table(T2_TABLE).count()
print(f"✅ Wrote {T2_TABLE}: {n_t2:,} rows")
if n_t2 == n_win:
    print("✅ Row count matches the window filter (tags drop no rows).")
else:
    print(f"⚠️  Row count differs from the window filter ({n_win:,}) — the joins must not add or drop rows, investigate.")

# COMMAND ----------

# DBTITLE 1,Section 10: Status figures
# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Status figures
# MAGIC
# MAGIC 1. Incidence of each tag: % of cards and % of transactions flagged
# MAGIC 2. Transactions by profile group per month
# MAGIC 3. Rows with balance above 1M by date
# MAGIC 4. Distinct days per card, with the infrequent threshold marked

# COMMAND ----------

# DBTITLE 1,Figure 1: tag incidence
df_t2_tbl = spark.table(T2_TABLE)

_tag_cols = ["tag_infrequent", "tag_superswiper", "tag_high_balance", "tag_impossible_fare", "tag_early_zero"]
_tag_labels = {
    "tag_infrequent":      f"infrequent\n(<{MIN_DISTINCT_DAYS} days)",
    "tag_superswiper":     "superswiper",
    "tag_high_balance":    "balance > 1M",
    "tag_impossible_fare": "impossible fare",
    "tag_early_zero":      "early zero",
}

tot_tx    = df_t2_tbl.count()
tot_cards = df_t2_tbl.select("cardnumber").distinct().count()

# % of transactions: share of rows flagged. % of cards: share of cards with
# at least one flagged row.
_tx_flagged = df_t2_tbl.agg(*[F.sum(F.coalesce(F.col(t), F.lit(0))).alias(t) for t in _tag_cols]).collect()[0]
_cards_flagged = (
    df_t2_tbl
    .groupBy("cardnumber")
    .agg(*[F.max(F.coalesce(F.col(t), F.lit(0))).alias(t) for t in _tag_cols])
    .agg(*[F.sum(t).alias(t) for t in _tag_cols])
    .collect()[0]
)

incidence_pd = pd.DataFrame({
    "tag": [_tag_labels[t] for t in _tag_cols],
    "pct_cards":        [100 * _cards_flagged[t] / tot_cards for t in _tag_cols],
    "pct_transactions": [100 * _tx_flagged[t] / tot_tx for t in _tag_cols],
})
print(f"Window totals — transactions: {tot_tx:,} | cards: {tot_cards:,}")
display(incidence_pd)

fig, ax = plt.subplots(figsize=(11, 5))
x = np.arange(len(_tag_cols))
w = 0.4
ax.bar(x - w / 2, incidence_pd["pct_cards"], width=w, color="steelblue", label="% of cards")
ax.bar(x + w / 2, incidence_pd["pct_transactions"], width=w, color="indianred", label="% of transactions")
for xi, (pc, pt) in enumerate(zip(incidence_pd["pct_cards"], incidence_pd["pct_transactions"])):
    ax.text(xi - w / 2, pc, f"{pc:.2f}", ha="center", va="bottom", fontsize=9)
    ax.text(xi + w / 2, pt, f"{pt:.2f}", ha="center", va="bottom", fontsize=9)
ax.set_xticks(x)
ax.set_xticklabels([_tag_labels[t] for t in _tag_cols], fontsize=10)
ax.set_ylabel("%", fontsize=12)
ax.set_title("Tag incidence: how much each cleaning decision weighs", fontsize=13, fontweight="bold")
ax.legend(fontsize=10)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure 2: transactions by profile group per month
pg_month_pd = (
    df_t2_tbl
    .filter(F.col("profile_group").isNotNull())
    .groupBy("ymonth", "profile_group")
    .count()
    .orderBy("ymonth")
    .toPandas()
    .pivot(index="ymonth", columns="profile_group", values="count")
    .fillna(0)
)

fig, ax = plt.subplots(figsize=(13, 6))
for group in sorted(pg_month_pd.columns, key=lambda g: -pg_month_pd[g].sum()):
    ax.plot(pg_month_pd.index, pg_month_pd[group], marker="o", markersize=3, linewidth=1.2, label=group)
ax.set_yscale("log")
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax.set_ylabel("Transactions (log scale)", fontsize=12)
ax.set_xlabel("Month", fontsize=12)
ax.tick_params(axis="x", rotation=45)
ax.set_title("Transactions by profile group per month", fontsize=13, fontweight="bold")
ax.legend(fontsize=9, ncol=2)
plt.tight_layout()
plt.show()

# Size of frecuente in the window (transactions and cards by month)
_check_pd = (
    df_t2_tbl
    .filter(F.col("profile_group") == "frecuente")
    .groupBy("ymonth")
    .agg(
        F.count(F.lit(1)).alias("transactions"),
        F.countDistinct("cardnumber").alias("distinct_cards"),
    )
    .orderBy("ymonth")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(14, 4.5))
for ax, var, title in [(axes[0], "transactions", "Transactions"), (axes[1], "distinct_cards", "Distinct cards")]:
    ax.plot(_check_pd["ymonth"], _check_pd[var], marker="o", linewidth=1.3, color="indianred")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.tick_params(axis="x", rotation=45)
    ax.set_title(title, fontsize=12, fontweight="bold")
plt.suptitle("Size of frecuente in the window, by month", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure 3: balance above 1M
# How many rows have balance_before above the threshold, and on which days?
_high_bal = df_t2_tbl.filter(F.col("tag_high_balance") == 1)
_n_high = _high_bal.count()

if _n_high == 0:
    print(f"No rows with balance_before above {BALANCE_MAX:,} COP in the window.")
else:
    _days_affected = (
        _high_bal
        .groupBy("fecha_transaccion")
        .agg(F.count(F.lit(1)).alias("flagged"))
        .orderBy("fecha_transaccion")
    )
    # Total transactions on each affected day
    _daily_totals = (
        df_t2_tbl
        .groupBy("fecha_transaccion")
        .agg(F.count(F.lit(1)).alias("total"))
    )
    _summary = (
        _days_affected
        .join(_daily_totals, "fecha_transaccion")
        .withColumn("pct", F.format_string("%.6f%%", 100 * F.col("flagged rows") / F.col("total rows")))
        .orderBy("fecha_transaccion")
    )
    print(f"Balance above {BALANCE_MAX:,} COP: {_n_high:,} row(s) on {_summary.count()} day(s).")
    display(_summary)

# COMMAND ----------

# DBTITLE 1,Figure 4: distinct days per card
days_pd = (
    df_t2_tbl
    .select("cardnumber", "n_days_active")
    .filter(F.col("n_days_active").isNotNull())
    .distinct()
    .groupBy("n_days_active")
    .count()
    .orderBy("n_days_active")
    .toPandas()
)

_n_cards_total = days_pd["count"].sum()
_n_below = days_pd.loc[days_pd["n_days_active"] < MIN_DISTINCT_DAYS, "count"].sum()
print(f"Cards below the {MIN_DISTINCT_DAYS}-day threshold: {_n_below:,} of {_n_cards_total:,} ({100*_n_below/_n_cards_total:.1f}%)")

fig, ax = plt.subplots(figsize=(13, 5))
colors = ["indianred" if d < MIN_DISTINCT_DAYS else "steelblue" for d in days_pd["n_days_active"]]
ax.bar(days_pd["n_days_active"], days_pd["count"], width=1.0, color=colors)
ax.axvline(MIN_DISTINCT_DAYS - 0.5, color="black", linewidth=1.5, linestyle="--",
           label=f"Threshold: {MIN_DISTINCT_DAYS} distinct days")
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax.set_xlabel("Distinct days active in the window", fontsize=12)
ax.set_ylabel("Cards", fontsize=12)
ax.set_title(f"Distinct days per card in the window (red = tagged infrequent, {100*_n_below/_n_cards_total:.1f}% of cards)",
             fontsize=13, fontweight="bold")
ax.legend(fontsize=10)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure 5: adulto cards before and after anonymous imputation
# Distinct adulto cards per day: original profile vs imputed profile
_adulto_orig = (
    df_t2_tbl
    .filter(F.col("profile_group") == "adulto")
    .groupBy("fecha_transaccion")
    .agg(F.countDistinct("cardnumber").alias("cards_original"))
)

_adulto_imputed = (
    df_t2_tbl
    .filter(F.col("profile_group_imputed") == "adulto")
    .groupBy("fecha_transaccion")
    .agg(F.countDistinct("cardnumber").alias("cards_imputed"))
)

imputation_pd = (
    _adulto_orig
    .join(_adulto_imputed, "fecha_transaccion", "outer")
    .orderBy("fecha_transaccion")
    .toPandas()
)
imputation_pd["fecha_transaccion"] = pd.to_datetime(imputation_pd["fecha_transaccion"])
imputation_pd = imputation_pd.fillna(0)

fig, ax = plt.subplots(figsize=(13, 5))
ax.plot(imputation_pd["fecha_transaccion"], imputation_pd["cards_original"],
        linewidth=1.2, color="steelblue", label="Original profile", alpha=0.8)
ax.plot(imputation_pd["fecha_transaccion"], imputation_pd["cards_imputed"],
        linewidth=1.2, color="indianred", label="Imputed profile", alpha=0.8)
ax.fill_between(imputation_pd["fecha_transaccion"],
                imputation_pd["cards_imputed"], imputation_pd["cards_original"],
                alpha=0.2, color="steelblue", label="Reclassified as anonymous")
ax.axvline(pd.Timestamp(REFORM_DATE), color="red", linewidth=1.2, linestyle="--",
           label=f"Reform ({REFORM_DATE})")
ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax.tick_params(axis="x", rotation=45)
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax.set_xlabel("Transaction date (daily)", fontsize=12)
ax.set_ylabel("Distinct adulto cards", fontsize=12)
ax.set_title("Adulto cards per day: original vs imputed profile\n"
             "(shaded area = reclassified as anonymous by imputation)",
             fontsize=13, fontweight="bold")
ax.legend(fontsize=10)
plt.tight_layout()
plt.show()

_total_orig = imputation_pd["cards_original"].sum()
_total_imp  = imputation_pd["cards_imputed"].sum()
print(f"Total adulto card-days (original) : {_total_orig:,.0f}")
print(f"Total adulto card-days (imputed)  : {_total_imp:,.0f}")
print(f"Reclassified to anonymous         : {_total_orig - _total_imp:,.0f} "
      f"({100*(_total_orig - _total_imp)/_total_orig:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Figure 6: anonymous cards before and after imputation
# Distinct anonymous cards per day: original profile vs imputed profile
_anon_orig = (
    df_t2_tbl
    .filter(F.col("profile_group") == "anonymous")
    .groupBy("fecha_transaccion")
    .agg(F.countDistinct("cardnumber").alias("cards_original"))
)

_anon_imputed = (
    df_t2_tbl
    .filter(F.col("profile_group_imputed") == "anonymous")
    .groupBy("fecha_transaccion")
    .agg(F.countDistinct("cardnumber").alias("cards_imputed"))
)

anon_pd = (
    _anon_orig
    .join(_anon_imputed, "fecha_transaccion", "outer")
    .orderBy("fecha_transaccion")
    .toPandas()
)
anon_pd["fecha_transaccion"] = pd.to_datetime(anon_pd["fecha_transaccion"])
anon_pd = anon_pd.fillna(0)

fig, ax = plt.subplots(figsize=(13, 5))
ax.plot(anon_pd["fecha_transaccion"], anon_pd["cards_original"],
        linewidth=1.2, color="steelblue", label="Original profile", alpha=0.8)
ax.plot(anon_pd["fecha_transaccion"], anon_pd["cards_imputed"],
        linewidth=1.2, color="indianred", label="Imputed profile", alpha=0.8)
ax.fill_between(anon_pd["fecha_transaccion"],
                anon_pd["cards_original"], anon_pd["cards_imputed"],
                alpha=0.2, color="indianred", label="Gained from other profiles")
ax.axvline(pd.Timestamp(REFORM_DATE), color="red", linewidth=1.2, linestyle="--",
           label=f"Reform ({REFORM_DATE})")
ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax.tick_params(axis="x", rotation=45)
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax.set_xlabel("Transaction date (daily)", fontsize=12)
ax.set_ylabel("Distinct anonymous cards", fontsize=12)
ax.set_title("Anonymous cards per day: original vs imputed profile\n"
             "(shaded area = reclassified from other profiles by imputation)",
             fontsize=13, fontweight="bold")
ax.legend(fontsize=10)
plt.tight_layout()
plt.show()

_total_orig = anon_pd["cards_original"].sum()
_total_imp  = anon_pd["cards_imputed"].sum()
print(f"Total anonymous card-days (original) : {_total_orig:,.0f}")
print(f"Total anonymous card-days (imputed)  : {_total_imp:,.0f}")
print(f"Gained from other profiles           : {_total_imp - _total_orig:,.0f} "
      f"({100*(_total_imp - _total_orig)/max(_total_orig,1):.1f}%)")

# COMMAND ----------

# DBTITLE 1,Figure 7: apoyo cards before and after anonymous imputation
# Distinct apoyo cards per day: original profile vs imputed profile
_apoyo_orig = (
    df_t2_tbl
    .filter(F.col("profile_group") == "apoyo")
    .groupBy("fecha_transaccion")
    .agg(F.countDistinct("cardnumber").alias("cards_original"))
)

_apoyo_imputed = (
    df_t2_tbl
    .filter(F.col("profile_group_imputed") == "apoyo")
    .groupBy("fecha_transaccion")
    .agg(F.countDistinct("cardnumber").alias("cards_imputed"))
)

apoyo_pd = (
    _apoyo_orig
    .join(_apoyo_imputed, "fecha_transaccion", "outer")
    .orderBy("fecha_transaccion")
    .toPandas()
)
apoyo_pd["fecha_transaccion"] = pd.to_datetime(apoyo_pd["fecha_transaccion"])
apoyo_pd = apoyo_pd.fillna(0)

fig, ax = plt.subplots(figsize=(13, 5))
ax.plot(apoyo_pd["fecha_transaccion"], apoyo_pd["cards_original"],
        linewidth=1.2, color="steelblue", label="Original profile", alpha=0.8)
ax.plot(apoyo_pd["fecha_transaccion"], apoyo_pd["cards_imputed"],
        linewidth=1.2, color="indianred", label="Imputed profile", alpha=0.8)
ax.fill_between(apoyo_pd["fecha_transaccion"],
                apoyo_pd["cards_imputed"], apoyo_pd["cards_original"],
                alpha=0.2, color="steelblue", label="Reclassified as anonymous")
ax.axvline(pd.Timestamp(REFORM_DATE), color="red", linewidth=1.2, linestyle="--",
           label=f"Reform ({REFORM_DATE})")
ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax.tick_params(axis="x", rotation=45)
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax.set_xlabel("Transaction date (daily)", fontsize=12)
ax.set_ylabel("Distinct apoyo cards", fontsize=12)
ax.set_title("Apoyo cards per day: original vs imputed profile\n"
             "(shaded area = reclassified as anonymous by imputation)",
             fontsize=13, fontweight="bold")
ax.legend(fontsize=10)
plt.tight_layout()
plt.show()

_total_orig = apoyo_pd["cards_original"].sum()
_total_imp  = apoyo_pd["cards_imputed"].sum()
print(f"Total apoyo card-days (original) : {_total_orig:,.0f}")
print(f"Total apoyo card-days (imputed)  : {_total_imp:,.0f}")
print(f"Reclassified to anonymous        : {_total_orig - _total_imp:,.0f} "
      f"({100*(_total_orig - _total_imp)/max(_total_orig,1):.1f}%)")
