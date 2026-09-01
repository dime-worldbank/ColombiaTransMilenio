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
# MAGIC 8. **Profile switches** — implausible/plausible switch tags and imputed profile
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
T1_TABLE = "prd_mega.scolom15.silver_validaciones_from2016to2019"
T2_TABLE = "prd_mega.scolom15.silver_validaciones_2017window"

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

# Fares that existed in each period. 0 is always allowed here: post-reform
# zeros are legitimate transfers, pre-reform zeros get their own tag below.
FARE_PERIODS = [
    (WINDOW_START, "2017-03-31", [0, 200, 900, 1450, 1600, 1650, 2200]),
    (REFORM_DATE,  WINDOW_END,   [0, 700, 900, 1000, 1550, 1600, 1700]),
]

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

# DBTITLE 1,Keep the 12 monthly files
df_t1 = spark.table(T1_TABLE)

_is_window_file = F.lit(False)
for fname in WINDOW_FILES:
    _is_window_file = _is_window_file | F.col("_source_file").contains(fname)

df_win = df_t1.filter(_is_window_file)

n_t1  = df_t1.count()
n_win = df_win.count()
print(f"Silver rows            : {n_t1:,}")
print(f"Window rows (12 files) : {n_win:,}  ({100*n_win/n_t1:.1f}% of silver)")

# All 12 files present?
found_files = [r[0] for r in df_win.select("_source_file").distinct().collect()]
missing = [f for f in WINDOW_FILES if not any(f in ff for ff in found_files)]
if missing:
    print(f"⚠️  Expected window files NOT found: {missing}")
else:
    print(f"✅ All {len(WINDOW_FILES)} monthly files found.")

# Rows and clearing dates per file
display(
    df_win
    .groupBy("_source_file")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.min("clearing_date").alias("min_clearing"),
        F.max("clearing_date").alias("max_clearing"),
        F.countDistinct("clearing_date").alias("distinct_clearing_days"),
    )
    .orderBy("min_clearing")
)

# Rows whose clearing_date falls outside the window dates. They are kept
# (the filter is by filename); this only sizes them.
n_outside = df_win.filter(
    ~F.col("clearing_date").between(WINDOW_START, WINDOW_END) | F.col("clearing_date").isNull()
).count()
print(f"Rows with clearing_date outside [{WINDOW_START}, {WINDOW_END}] or NULL (kept): {n_outside:,}")

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

# Value not in the allowed list of its period (NULL values are not tagged)
_impossible = F.lit(False)
for p_start, p_end, allowed in FARE_PERIODS:
    _impossible = _impossible | (
        F.col("fecha_transaccion").between(p_start, p_end)
        & ~F.col("value").isin(allowed)
        & F.col("value").isNotNull()
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
for p_start, p_end, allowed in FARE_PERIODS:
    print(f"\nPeriod {p_start} → {p_end} (allowed: {sorted(allowed)}) — tagged values:")
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
# MAGIC Groups the card profiles for analysis. `adultopv` and `frecuente` stay separate from `adulto` on purpose: they cannot serve as comparison cards. A NULL profile stays NULL.

# COMMAND ----------

# DBTITLE 1,Map card_profile → profile_group
_profile_group_map = {
    "(001) Adulto":                        "adulto",
    "(001) Anonymous":                     "anonymous",
    "(002) Adulto Mayor":                  "mayor",
    "(006) Apoyo Ciudadano":               "apoyo",
    "(101) Adulto PV":                     "adultopv",
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
# MAGIC A card can plausibly go anonymous → personalized (bought anonymous, then registered), but never personalized → anonymous: those records are trunk devices mis-recording the profile. The baseline keeps the original profile; here we only tag the switching cards and build an imputed profile column for robustness checks.

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

display(
    df_card_switches.agg(
        F.count(F.lit(1)).alias("cards"),
        F.sum("tag_implausible_switch").alias("cards_implausible_switch"),
        F.sum("tag_plausible_switch").alias("cards_plausible_switch"),
    )
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

# DBTITLE 1,Imputed profile (robustness column)
# A transaction is imputed as anonymous if the card has ANY anonymous
# transaction from that moment on: everything up to the card's last anonymous
# transaction becomes anonymous. The baseline analysis ignores this column.
_w_future = Window.partitionBy("cardnumber").orderBy("fecha_transaccion_timestamp") \
                  .rowsBetween(Window.currentRow, Window.unboundedFollowing)

card_profile_imputed_expr = F.when(
    F.max(_is_anon.cast("int")).over(_w_future) == 1, F.lit("(001) Anonymous")
).otherwise(F.col("card_profile"))

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
    # Imputed profile (robustness)
    .withColumn("card_profile_imputed", card_profile_imputed_expr)
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

# Size of adultopv and frecuente in the window (transactions and cards by month)
_check_pd = (
    df_t2_tbl
    .filter(F.col("profile_group").isin("adultopv", "frecuente"))
    .groupBy("ymonth", "profile_group")
    .agg(
        F.count(F.lit(1)).alias("transactions"),
        F.countDistinct("cardnumber").alias("distinct_cards"),
    )
    .orderBy("ymonth")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(14, 4.5))
for ax, var, title in [(axes[0], "transactions", "Transactions"), (axes[1], "distinct_cards", "Distinct cards")]:
    for group, color in [("adultopv", "steelblue"), ("frecuente", "indianred")]:
        pdf = _check_pd[_check_pd["profile_group"] == group]
        ax.plot(pdf["ymonth"], pdf[var], marker="o", linewidth=1.3, color=color, label=group)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.tick_params(axis="x", rotation=45)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
plt.suptitle("Size of adultopv and frecuente in the window, by month", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure 3: balance above 1M by date
bal_pd = (
    df_t2_tbl
    .filter(F.col("tag_high_balance") == 1)
    .groupBy("fecha_transaccion")
    .count()
    .orderBy("fecha_transaccion")
    .toPandas()
)

if bal_pd.empty:
    print("No rows with balance_before above 1M in the window — nothing to plot.")
else:
    bal_pd["fecha_transaccion"] = pd.to_datetime(bal_pd["fecha_transaccion"])
    fig, ax = plt.subplots(figsize=(13, 4.5))
    ax.bar(bal_pd["fecha_transaccion"], bal_pd["count"], width=1.0, color="indianred")
    ax.axvline(pd.Timestamp(REFORM_DATE), color="red", linewidth=1.2, linestyle="--", label=f"Reform ({REFORM_DATE})")
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.tick_params(axis="x", rotation=45)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.set_ylabel("Rows with balance_before > 1M", fontsize=12)
    ax.set_title("Rows with balance above 1M by transaction date (do they concentrate on specific dates?)",
                 fontsize=13, fontweight="bold")
    ax.legend(fontsize=10)
    plt.tight_layout()
    plt.show()

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
