# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Index
# MAGIC %md
# MAGIC ## Index
# MAGIC
# MAGIC Builds the three analysis tables from `silver_validaciones_2017window`:
# MAGIC `monthly_outcomes_2017` (card × month), `cards_2017` (one row per card) and `panel_2017` (balanced card × month panel for the analysis sample).
# MAGIC
# MAGIC 1. **Setup** — packages, catalog, parameters
# MAGIC 2. **Read window table** — input checks
# MAGIC 3. **Card classification** — one profile group per card (imputed profile), `ever_*` and `always_adulto` flags
# MAGIC 4. **Fare table** — modal fares by profile group × fare period
# MAGIC 5. **Subsidized trips** — `apoyo_trip` / `mayor_trip` per transaction
# MAGIC 6. **Monthly outcomes** — trips and subsidized months per card × month → `monthly_outcomes_2017`
# MAGIC 7. **Card-level dataset** — presence, spending, subsidized months, treatment group → `cards_2017`
# MAGIC 8. **Balanced panel** — card × month grid for the analysis sample → `panel_2017`
# MAGIC 9. **Status figures** — fare table, subsidized share by month, sample funnel, treatment groups, pre-trends

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
T2_TABLE = "prd_mega.scolom15.silver_validaciones_2017window"
T3_TABLE = "prd_mega.scolom15.monthly_outcomes_2017"
T4_TABLE = "prd_mega.scolom15.cards_2017"
T5_TABLE = "prd_mega.scolom15.panel_2017"

# Reform month and analysis window (dist_months = months since the reform)
REFORM_MONTH = "2017-04"
WINDOW_MONTHS = [str(p) for p in pd.period_range("2016-10", "2017-09", freq="M")]

# Presence and subsidy windows, in months relative to the reform
PRE_RANGE      = (-6, -1)   # 6 months before
POST_RANGE     = (0, 5)     # 6 months after
SUB_POST_RANGE = (0, 17)    # window to count subsidized months after (clipped by the data)

# Fare periods inside the window (the fare scheme changed with the reform)
FARE_PERIODS_M = [
    ("oct16-mar17", "2016-10", "2017-03"),
    ("apr17-sep17", "2017-04", "2017-09"),
]

# The three profile groups that define treatment and comparison cards
TREATMENT_GROUPS = ["adulto", "apoyo", "mayor"]

# Modal fares are computed over each card's first trips of the month, up to this rank
MAX_TRIPS_RANK = 30

# A month is subsidized if it has at least this many subsidized trips,
# or if more than this share of its trips is subsidized
SUB_MONTH_TRIPS = 30
SUB_MONTH_SHARE = 0.5

# A card counts as subsidized in a window if it has at least this many subsidized months
MIN_SUB_MONTHS = 1

# In this month every apoyo holder got the subsidy by mistake: apoyo_trip is
# set to missing there
GLITCH_MONTH = "2017-08"

# COMMAND ----------

# DBTITLE 1,Section 2: Read window table
# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Read window table

# COMMAND ----------

# DBTITLE 1,Read and check months
df_t2 = spark.table(T2_TABLE)

n_t2 = df_t2.count()
print(f"Window table rows: {n_t2:,}")

months_found = sorted(r[0] for r in df_t2.select("ymonth").distinct().collect() if r[0] is not None)
missing_months = [m for m in WINDOW_MONTHS if m not in months_found]
extra_months   = [m for m in months_found if m not in WINDOW_MONTHS]
print(f"Months found: {months_found}")
if missing_months:
    print(f"⚠️  Expected months with no rows: {missing_months}")
if extra_months:
    print(f"⚠️  Months outside the window (from timestamps at file borders): {extra_months}")
if not missing_months and not extra_months:
    print("✅ Exactly the 12 window months.")

# Months since the reform, used everywhere below
dist_months_expr = F.months_between(
    F.to_date(F.concat(F.col("ymonth"), F.lit("-01"))),
    F.to_date(F.lit(f"{REFORM_MONTH}-01")),
).cast("int")

# COMMAND ----------

# DBTITLE 1,Section 3: Card classification
# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Card classification
# MAGIC
# MAGIC Classification runs on the imputed profile (everything up to a card's last anonymous transaction counts as anonymous). A card gets a group only if all its non-anonymous records belong to exactly ONE profile group; anonymous records never break this for the treatment groups. Cards mixing two or more non-anonymous groups get no group — they cannot be classified and will get no treatment group. The comparison group is stricter: `always_adulto` marks cards that are adulto in EVERY transaction, with no anonymous records at all.

# COMMAND ----------

# DBTITLE 1,One profile group per card + ever_* and always_adulto flags
_non_anon_group = F.when(
    F.col("profile_group_imputed").isNotNull() & (F.col("profile_group_imputed") != "anonymous"),
    F.col("profile_group_imputed"),
)

df_cards_profile = (
    df_t2
    .filter(F.col("cardnumber").isNotNull())
    .groupBy("cardnumber")
    .agg(
        F.collect_set(_non_anon_group).alias("_groups"),
        F.max((F.col("profile_group_imputed") == "anonymous").cast("int")).alias("has_anonymous"),
    )
    .withColumn("n_profile_groups", F.size("_groups"))
    .withColumn("profile_groups", F.concat_ws("+", F.array_sort("_groups")))
    .withColumn("card_group", F.when(F.col("n_profile_groups") == 1, F.element_at("_groups", 1)))
    .withColumn("single_profile", (F.col("n_profile_groups") == 1).cast("int"))
    .withColumn("ever_adulto", F.when(F.col("n_profile_groups") == 1, (F.col("card_group") == "adulto").cast("int")))
    .withColumn("ever_apoyo",  F.when(F.col("n_profile_groups") == 1, (F.col("card_group") == "apoyo").cast("int")))
    .withColumn("ever_mayor",  F.when(F.col("n_profile_groups") == 1, (F.col("card_group") == "mayor").cast("int")))
    .withColumn(
        "always_adulto",
        F.when(F.col("n_profile_groups") == 1, ((F.col("card_group") == "adulto") & (F.col("has_anonymous") == 0)).cast("int")),
    )
    .drop("_groups")
)

print("── Cards by classification outcome ──")
display(
    df_cards_profile.agg(
        F.count(F.lit(1)).alias("cards"),
        F.sum("single_profile").alias("single_group"),
        F.sum("always_adulto").alias("always_adulto"),
        F.sum(F.when(F.col("n_profile_groups") > 1, 1).otherwise(0)).alias("mixed_groups"),
        F.sum(F.when((F.col("n_profile_groups") == 0) & (F.col("has_anonymous") == 1), 1).otherwise(0)).alias("anonymous_only"),
        F.sum(F.when((F.col("n_profile_groups") == 0) & (F.col("has_anonymous") == 0), 1).otherwise(0)).alias("no_profile_at_all"),
    )
)

print("── Most common profile-group combinations ──")
display(
    df_cards_profile
    .groupBy("profile_groups", "has_anonymous")
    .count()
    .orderBy(F.col("count").desc())
    .limit(25)
)

# COMMAND ----------

# DBTITLE 1,Section 4: Fare table
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Fare table
# MAGIC
# MAGIC For each profile group and fare period, the two most frequent trip values are the zonal (lower) and troncal (higher) fares. Computed over each card's first 30 trips of the month, so heavy travelers do not dominate the mode.

# COMMAND ----------

# DBTITLE 1,Modal fares by group × period
_fare_period_expr = F.lit(None).cast("string")
for label, m_start, m_end in FARE_PERIODS_M:
    _fare_period_expr = F.when(F.col("ymonth").between(m_start, m_end), F.lit(label)).otherwise(_fare_period_expr)

_w_trip_rank = Window.partitionBy("cardnumber", "ymonth").orderBy("fecha_transaccion_timestamp")

df_ranked_trips = (
    df_t2
    .join(df_cards_profile.select("cardnumber", "card_group"), "cardnumber")
    .filter(
        F.col("card_group").isin(*TREATMENT_GROUPS)
        & (F.col("trip") == 1)
        & F.col("fecha_transaccion_timestamp").isNotNull()
    )
    .withColumn("fare_period", _fare_period_expr)
    .filter(F.col("fare_period").isNotNull())
    .withColumn("trip_rank", F.row_number().over(_w_trip_rank))
    .filter(F.col("trip_rank") <= MAX_TRIPS_RANK)
)

_w_mode = Window.partitionBy("card_group", "fare_period").orderBy(F.col("count").desc())

fare_modes_pd = (
    df_ranked_trips
    .groupBy("card_group", "fare_period", "value")
    .count()
    .withColumn("mode_rank", F.row_number().over(_w_mode))
    .filter(F.col("mode_rank") <= 2)
    .toPandas()
)

# One row per group × period: zonal = lower fare, troncal = higher fare
fare_table_pd = (
    fare_modes_pd
    .groupby(["card_group", "fare_period"])
    .apply(lambda g: pd.Series({
        "fare_zonal":    g["value"].min(),
        "freq_zonal":    g.loc[g["value"].idxmin(), "count"],
        "fare_troncal":  g["value"].max(),
        "freq_troncal":  g.loc[g["value"].idxmax(), "count"],
    }))
    .reset_index()
)
print("── Fare table: modal fares by profile group × fare period ──")
display(fare_table_pd)

# Dictionary used to tag subsidized trips: (group, period) → [zonal, troncal]
FARES = {
    (r["card_group"], r["fare_period"]): [r["fare_zonal"], r["fare_troncal"]]
    for _, r in fare_table_pd.iterrows()
}

# COMMAND ----------

# DBTITLE 1,Section 5: Subsidized trips
# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Subsidized trips
# MAGIC
# MAGIC A transaction is a subsidized trip if it pays exactly the subsidized fare (zonal or troncal) of its period AND the card belongs to that group. The card's group is used, not the transaction's recorded profile, so trips mis-recorded as anonymous still count.

# COMMAND ----------

# DBTITLE 1,Tabulation: who pays the subsidized fares, by month
# Before constructing the variable: how many cards pay exactly the apoyo/mayor
# subsidized fare each month, among the cards of that group.
def _fare_match_expr(group):
    match = F.lit(False)
    for label, m_start, m_end in FARE_PERIODS_M:
        match = match | (F.col("ymonth").between(m_start, m_end) & F.col("value").isin(FARES[(group, label)]))
    return match

df_tx = df_t2.join(
    df_cards_profile.select("cardnumber", "card_group", "ever_adulto", "ever_apoyo", "ever_mayor"),
    "cardnumber", "left",
)

_pretab_pd = {}
for group, ever_col in [("apoyo", "ever_apoyo"), ("mayor", "ever_mayor")]:
    _pretab_pd[group] = (
        df_tx
        .filter((F.col(ever_col) == 1) & (F.col("trip") == 1))
        .groupBy("ymonth")
        .agg(
            F.countDistinct("cardnumber").alias("cards_total"),
            F.countDistinct(F.when(_fare_match_expr(group), F.col("cardnumber"))).alias("cards_at_sub_fare"),
        )
        .orderBy("ymonth")
        .toPandas()
    )

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
for ax, group in zip(axes, ["apoyo", "mayor"]):
    pdf = _pretab_pd[group]
    ax.plot(pdf["ymonth"], pdf["cards_total"], marker="o", color="dimgray", linewidth=1.3, label="Cards traveling")
    ax.plot(pdf["ymonth"], pdf["cards_at_sub_fare"], marker="o", color="indianred", linewidth=1.3, label="Cards at the subsidized fare")
    ax.axvline(REFORM_MONTH, color="red", linewidth=1.2, linestyle="--", label=f"Reform ({REFORM_MONTH})")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.tick_params(axis="x", rotation=45)
    ax.set_ylabel("Cards per month", fontsize=11)
    ax.set_title(group, fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
plt.suptitle("Cards at the subsidized fare, by month", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Glitch check: apoyo cards at the subsidized fare, per day
# Distinct apoyo cards paying the apoyo subsidized fare each day. If the glitch
# is real, the daily count jumps during the glitch month and drops back after —
# this shows its exact start and end dates.
glitch_daily_pd = (
    df_tx
    .filter((F.col("ever_apoyo") == 1) & (F.col("trip") == 1) & F.col("fecha_transaccion").isNotNull())
    .groupBy("fecha_transaccion")
    .agg(
        F.countDistinct("cardnumber").alias("cards_total"),
        F.countDistinct(F.when(_fare_match_expr("apoyo"), F.col("cardnumber"))).alias("cards_at_sub_fare"),
    )
    .orderBy("fecha_transaccion")
    .toPandas()
)
glitch_daily_pd["fecha_transaccion"] = pd.to_datetime(glitch_daily_pd["fecha_transaccion"])
glitch_daily_pd["pct_at_sub_fare"] = 100 * glitch_daily_pd["cards_at_sub_fare"] / glitch_daily_pd["cards_total"]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
ax1.plot(glitch_daily_pd["fecha_transaccion"], glitch_daily_pd["cards_total"], color="dimgray", linewidth=0.8, label="Apoyo cards traveling")
ax1.plot(glitch_daily_pd["fecha_transaccion"], glitch_daily_pd["cards_at_sub_fare"], color="indianred", linewidth=0.9, label="Apoyo cards at the subsidized fare")
ax1.set_ylabel("Cards per day", fontsize=12)
ax1.legend(fontsize=9, loc="upper left")
ax2.plot(glitch_daily_pd["fecha_transaccion"], glitch_daily_pd["pct_at_sub_fare"], color="steelblue", linewidth=0.9)
ax2.set_ylabel("% of traveling apoyo cards\nat the subsidized fare", fontsize=11)
ax2.set_ylim(0, 100)
for ax in (ax1, ax2):
    ax.axvline(pd.Timestamp(f"{REFORM_MONTH}-01"), color="red", linewidth=1.2, linestyle="--")
    ax.axvspan(pd.Timestamp(f"{GLITCH_MONTH}-01"), pd.Timestamp(f"{GLITCH_MONTH}-01") + pd.offsets.MonthEnd(1), color="orange", alpha=0.15)
ax1.set_title("Glitch check — apoyo cards at the subsidized fare per day (red line = reform, orange band = glitch month)",
              fontsize=13, fontweight="bold")
ax2.set_xlabel("Day", fontsize=12)
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()

# Zoom on the months around the glitch, to pin down its exact start and end days
_zoom = glitch_daily_pd[
    (glitch_daily_pd["fecha_transaccion"] >= "2017-07-01")
    & (glitch_daily_pd["fecha_transaccion"] <= "2017-09-30")
]
fig, ax = plt.subplots(figsize=(13, 4))
ax.plot(_zoom["fecha_transaccion"], _zoom["pct_at_sub_fare"], marker="o", markersize=3, color="steelblue", linewidth=1.0)
ax.axvspan(pd.Timestamp(f"{GLITCH_MONTH}-01"), pd.Timestamp(f"{GLITCH_MONTH}-01") + pd.offsets.MonthEnd(1), color="orange", alpha=0.15)
ax.xaxis.set_major_locator(mdates.DayLocator(interval=3))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b-%d"))
ax.tick_params(axis="x", rotation=45)
ax.set_ylim(0, 100)
ax.set_ylabel("% of traveling apoyo cards\nat the subsidized fare", fontsize=11)
ax.set_title("Zoom Jul–Sep 2017: exact start and end of the glitch", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,apoyo_trip and mayor_trip
# apoyo_trip is missing in the glitch month (the subsidy was wrongly given to
# every apoyo holder, so that month says nothing about true eligibility).
apoyo_trip_expr = F.when(F.col("ymonth") == GLITCH_MONTH, F.lit(None).cast("int")).otherwise(
    ((F.col("ever_apoyo") == 1) & _fare_match_expr("apoyo")).cast("int")
)
mayor_trip_expr = ((F.col("ever_mayor") == 1) & _fare_match_expr("mayor")).cast("int")

df_tx = (
    df_tx
    .withColumn("apoyo_trip", apoyo_trip_expr)
    .withColumn("mayor_trip", mayor_trip_expr)
)

display(
    df_tx.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("apoyo_trip").alias("apoyo_trips"),
        F.sum("mayor_trip").alias("mayor_trips"),
        F.sum(F.when(F.col("apoyo_trip").isNull(), 1).otherwise(0)).alias("apoyo_trip_missing_glitch_month"),
    )
)

# COMMAND ----------

# DBTITLE 1,Section 6: Monthly outcomes
# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Monthly outcomes → `monthly_outcomes_2017`
# MAGIC
# MAGIC One row per card × month with at least one transaction. A subsidized month has at least 30 subsidized trips or more than half of its trips subsidized.

# COMMAND ----------

# DBTITLE 1,Build and write T3
# Trips per card per day, then averaged over the card's active days in the month
_daily = (
    df_tx
    .filter(F.col("fecha_transaccion").isNotNull())
    .groupBy("cardnumber", "ymonth", "fecha_transaccion")
    .agg(F.sum("trip").alias("n_trips_day"))
)
_avg_daily = (
    _daily
    .groupBy("cardnumber", "ymonth")
    .agg(
        F.avg("n_trips_day").alias("avg_daily_trips"),
        F.count(F.lit(1)).alias("days_active_month"),
    )
)

df_t3 = (
    df_tx
    .groupBy("cardnumber", "ymonth")
    .agg(
        F.count(F.lit(1)).alias("n_tx"),
        F.sum("trip").alias("n_trips"),
        F.sum(F.when(F.col("trip") == 1, F.col("value"))).alias("tot_value_trips"),
        F.coalesce(F.sum("apoyo_trip"), F.lit(0)).alias("apoyo_trips_month"),
        F.coalesce(F.sum("mayor_trip"), F.lit(0)).alias("mayor_trips_month"),
    )
    .join(_avg_daily, ["cardnumber", "ymonth"], "left")
    .withColumn("has_trips", (F.col("n_trips") > 0).cast("int"))
    .withColumn(
        "apoyo_month",
        (
            (F.col("apoyo_trips_month") >= SUB_MONTH_TRIPS)
            | ((F.col("n_trips") > 0) & (F.col("apoyo_trips_month") / F.col("n_trips") > SUB_MONTH_SHARE))
        ).cast("int"),
    )
    .withColumn(
        "mayor_month",
        (
            (F.col("mayor_trips_month") >= SUB_MONTH_TRIPS)
            | ((F.col("n_trips") > 0) & (F.col("mayor_trips_month") / F.col("n_trips") > SUB_MONTH_SHARE))
        ).cast("int"),
    )
)

(
    df_t3.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T3_TABLE)
)

n_t3 = spark.table(T3_TABLE).count()
print(f"✅ Wrote {T3_TABLE}: {n_t3:,} card-months")
display(
    spark.table(T3_TABLE)
    .groupBy("ymonth")
    .agg(
        F.count(F.lit(1)).alias("card_months"),
        F.sum("n_trips").alias("trips"),
        F.sum("apoyo_month").alias("apoyo_months"),
        F.sum("mayor_month").alias("mayor_months"),
    )
    .orderBy("ymonth")
)

# COMMAND ----------

# DBTITLE 1,Section 7: Card-level dataset
# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Card-level dataset → `cards_2017`
# MAGIC
# MAGIC One row per card, for ALL cards in the window: profile classification, presence before/after the reform, spending, subsidized-month counts, treatment group, cleaning tags. Nothing is dropped here, so we can account for who ends up in or out of the sample.

# COMMAND ----------

# DBTITLE 1,Aggregate monthly outcomes to card level
df_t3_dist = spark.table(T3_TABLE).withColumn("dist_months", dist_months_expr)

_in_pre  = F.col("dist_months").between(*PRE_RANGE)
_in_post = F.col("dist_months").between(*POST_RANGE)
_in_sub_post = F.col("dist_months").between(*SUB_POST_RANGE)

df_cards_monthly = (
    df_t3_dist
    .groupBy("cardnumber")
    .agg(
        # Presence in the months around the reform
        F.max(_in_pre.cast("int")).alias("in_6m_bef"),
        F.max(_in_post.cast("int")).alias("in_6m_aft"),
        # Subsidized months before and after
        F.sum(F.when(_in_pre, F.col("apoyo_month")).otherwise(0)).alias("apoyo_m_in_6m_bef"),
        F.sum(F.when(_in_sub_post, F.col("apoyo_month")).otherwise(0)).alias("apoyo_m_in_18m_aft"),
        F.sum(F.when(_in_pre, F.col("mayor_month")).otherwise(0)).alias("mayor_m_in_6m_bef"),
        F.sum(F.when(_in_sub_post, F.col("mayor_month")).otherwise(0)).alias("mayor_m_in_18m_aft"),
        # Spending on trips (no transfers) before and after
        F.sum(F.when(_in_pre, F.col("tot_value_trips")).otherwise(0)).alias("tot_value_no_tr_6bef"),
        F.sum(F.when(_in_post, F.col("tot_value_trips")).otherwise(0)).alias("tot_value_no_tr_6aft"),
        # Activity
        F.min("ymonth").alias("first_active_month"),
        F.count(F.lit(1)).alias("n_months_active"),
        F.sum("n_tx").alias("n_tx_total"),
        F.sum("n_trips").alias("n_trips_total"),
    )
)

# COMMAND ----------

# DBTITLE 1,Aggregate cleaning tags to card level
df_cards_tags = (
    df_t2
    .filter(F.col("cardnumber").isNotNull())
    .groupBy("cardnumber")
    .agg(
        F.max("n_days_active").alias("n_days_active"),
        F.max("tag_infrequent").alias("tag_infrequent"),
        F.max("tag_superswiper").alias("tag_superswiper"),
        F.max("tag_implausible_switch").alias("tag_implausible_switch"),
        F.max("tag_plausible_switch").alias("tag_plausible_switch"),
        F.sum("tag_high_balance").alias("n_high_balance"),
        F.sum("tag_impossible_fare").alias("n_impossible_fare"),
        F.sum("tag_early_zero").alias("n_early_zero"),
    )
)

# COMMAND ----------

# DBTITLE 1,Treatment group and write T4
# kept = subsidized before and after | lost = only before | gain = only after.
# never = an always-adulto card (adulto in every transaction, never anonymous)
# with no subsidized month at all (the comparison group).
_z = lambda c: F.coalesce(F.col(c), F.lit(0))

treatment_expr = F.lit(None).cast("string")
for g in ["apoyo", "mayor"]:
    treatment_expr = (
        F.when((_z(f"{g}_m_in_6m_bef") >= MIN_SUB_MONTHS) & (_z(f"{g}_m_in_18m_aft") >= MIN_SUB_MONTHS) & (F.col(f"ever_{g}") == 1), F.lit(f"{g}_kept"))
        .when((_z(f"{g}_m_in_6m_bef") >= MIN_SUB_MONTHS) & (_z(f"{g}_m_in_18m_aft") == 0) & (F.col(f"ever_{g}") == 1), F.lit(f"{g}_lost"))
        .when((_z(f"{g}_m_in_6m_bef") == 0) & (_z(f"{g}_m_in_18m_aft") >= MIN_SUB_MONTHS) & (F.col(f"ever_{g}") == 1), F.lit(f"{g}_gain"))
        .otherwise(treatment_expr)
    )
treatment_expr = F.when(
    (_z("apoyo_m_in_6m_bef") == 0) & (_z("apoyo_m_in_18m_aft") == 0)
    & (_z("mayor_m_in_6m_bef") == 0) & (_z("mayor_m_in_18m_aft") == 0)
    & (F.col("always_adulto") == 1),
    F.lit("never"),
).otherwise(treatment_expr)

df_t4 = (
    df_cards_profile
    .join(df_cards_monthly, "cardnumber", "left")
    .join(df_cards_tags, "cardnumber", "left")
    .withColumn("apoyo_in_6m_bef",  (_z("apoyo_m_in_6m_bef")  >= MIN_SUB_MONTHS).cast("int"))
    .withColumn("apoyo_in_18m_aft", (_z("apoyo_m_in_18m_aft") >= MIN_SUB_MONTHS).cast("int"))
    .withColumn("mayor_in_6m_bef",  (_z("mayor_m_in_6m_bef")  >= MIN_SUB_MONTHS).cast("int"))
    .withColumn("mayor_in_18m_aft", (_z("mayor_m_in_18m_aft") >= MIN_SUB_MONTHS).cast("int"))
    .withColumn("treatment", treatment_expr)
)

(
    df_t4.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T4_TABLE)
)

n_t4 = spark.table(T4_TABLE).count()
print(f"✅ Wrote {T4_TABLE}: {n_t4:,} cards")

print("── Cards by treatment group ──")
display(
    spark.table(T4_TABLE)
    .groupBy("treatment")
    .count()
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

# DBTITLE 1,Section 8: Balanced panel
# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Balanced panel → `panel_2017`
# MAGIC
# MAGIC The analysis sample: cards present before and after the reform, not superswipers, not infrequent, with a single profile group and an assigned treatment group. Each of them gets one row per window month; months without transactions are coded as zero trips. Card-level variables are NOT here — they live in `cards_2017` and merge at analysis time.

# COMMAND ----------

# DBTITLE 1,Build and write T5
df_sample_cards = (
    spark.table(T4_TABLE)
    .filter(
        (F.col("in_6m_bef") == 1)
        & (F.col("in_6m_aft") == 1)
        & (F.coalesce(F.col("tag_superswiper"), F.lit(0)) == 0)
        & (F.coalesce(F.col("tag_infrequent"), F.lit(0)) == 0)
        & (F.col("single_profile") == 1)
        & F.col("treatment").isNotNull()
    )
    .select("cardnumber")
)
n_sample = df_sample_cards.count()
print(f"Cards in the analysis sample: {n_sample:,}")

df_months = spark.createDataFrame([(m,) for m in WINDOW_MONTHS], ["ymonth"])

df_t5 = (
    df_sample_cards
    .crossJoin(df_months)
    .join(
        spark.table(T3_TABLE).select("cardnumber", "ymonth", "n_trips", "has_trips", "avg_daily_trips"),
        ["cardnumber", "ymonth"], "left",
    )
    # Months without transactions are real zeros
    .fillna(0, subset=["n_trips", "has_trips", "avg_daily_trips"])
    # Month-level variables, deterministic from ymonth
    .withColumn("dist_months", dist_months_expr)
    .withColumn("before", (F.col("dist_months") < 0).cast("int"))
    .withColumn("after",  (F.col("dist_months") >= 0).cast("int"))
    .withColumn("period", F.floor(F.col("dist_months") / 6).cast("int"))
)

(
    df_t5.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T5_TABLE)
)

n_t5 = spark.table(T5_TABLE).count()
print(f"✅ Wrote {T5_TABLE}: {n_t5:,} rows")
if n_t5 == n_sample * len(WINDOW_MONTHS):
    print(f"✅ Balanced: {n_sample:,} cards × {len(WINDOW_MONTHS)} months.")
else:
    print(f"⚠️  Not balanced: expected {n_sample * len(WINDOW_MONTHS):,} rows — investigate.")

# COMMAND ----------

# DBTITLE 1,Section 9: Status figures
# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Status figures
# MAGIC
# MAGIC 1. The fare table: modal fares by profile group × period with their frequencies
# MAGIC 2. Share of apoyo cards' trips at the subsidized fare, by month (the reform and the glitch must be visible)
# MAGIC 3. Sample funnel: from all cards to the analysis sample
# MAGIC 4. Cards per treatment group
# MAGIC 5. Mean monthly trips by treatment group (raw pre-trends)

# COMMAND ----------

# DBTITLE 1,Figure 1: fare table
_ft = fare_table_pd.sort_values(["card_group", "fare_period"]).reset_index(drop=True)
_labels = [f"{r['card_group']}\n{r['fare_period']}" for _, r in _ft.iterrows()]

fig, ax = plt.subplots(figsize=(12, 5.5))
x = np.arange(len(_ft))
w = 0.4
b1 = ax.bar(x - w / 2, _ft["fare_zonal"], width=w, color="steelblue", label="Zonal (lower mode)")
b2 = ax.bar(x + w / 2, _ft["fare_troncal"], width=w, color="indianred", label="Troncal (higher mode)")
for xi, r in _ft.iterrows():
    ax.text(xi - w / 2, r["fare_zonal"], f"${int(r['fare_zonal'])}\n({int(r['freq_zonal']):,})", ha="center", va="bottom", fontsize=8)
    ax.text(xi + w / 2, r["fare_troncal"], f"${int(r['fare_troncal'])}\n({int(r['freq_troncal']):,})", ha="center", va="bottom", fontsize=8)
ax.set_xticks(x)
ax.set_xticklabels(_labels, fontsize=10)
ax.set_ylim(0, _ft["fare_troncal"].max() * 1.25)
ax.set_ylabel("Fare (COP)", fontsize=12)
ax.set_title("Modal fares by profile group × fare period (frequency in parentheses)", fontsize=13, fontweight="bold")
ax.legend(fontsize=10)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure 2: share of apoyo trips at the subsidized fare, by month
# Computed directly from the fare match (not from apoyo_trip), so the glitch
# month is visible instead of missing.
share_pd = (
    df_tx
    .filter((F.col("ever_apoyo") == 1) & (F.col("trip") == 1))
    .groupBy("ymonth")
    .agg((100 * F.avg(_fare_match_expr("apoyo").cast("int"))).alias("pct_sub"))
    .orderBy("ymonth")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(share_pd["ymonth"], share_pd["pct_sub"], marker="o", color="steelblue", linewidth=1.5)
ax.axvline(REFORM_MONTH, color="red", linewidth=1.2, linestyle="--", label=f"Reform ({REFORM_MONTH})")
ax.axvline(GLITCH_MONTH, color="orange", linewidth=1.2, linestyle="--", label=f"Subsidy glitch ({GLITCH_MONTH})")
ax.set_ylabel("% of apoyo cards' trips at the subsidized fare", fontsize=12)
ax.set_xlabel("Month", fontsize=12)
ax.tick_params(axis="x", rotation=45)
ax.set_ylim(0, 100)
ax.set_title("Share of apoyo cards' trips at the subsidized fare, by month", fontsize=13, fontweight="bold")
ax.legend(fontsize=10)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure 3: sample funnel
df_t4_tbl = spark.table(T4_TABLE)

_f1 = df_t4_tbl
_f2 = _f1.filter((F.col("in_6m_bef") == 1) & (F.col("in_6m_aft") == 1))
_f3 = _f2.filter((F.coalesce(F.col("tag_superswiper"), F.lit(0)) == 0) & (F.coalesce(F.col("tag_infrequent"), F.lit(0)) == 0))
_f4 = _f3.filter(F.col("single_profile") == 1)
_f5 = _f4.filter(F.col("treatment").isNotNull())

funnel = [
    ("All cards\nin window",        _f1.count()),
    ("Present before\nand after",   _f2.count()),
    ("Not superswiper,\nnot infrequent", _f3.count()),
    ("Single profile\ngroup",       _f4.count()),
    ("Assigned to a\ntreatment group", _f5.count()),
]

fig, ax = plt.subplots(figsize=(11, 5.5))
_labels = [s[0] for s in funnel]
_vals   = [s[1] for s in funnel]
bars = ax.bar(_labels, _vals, color=["dimgray"] + ["steelblue"] * 3 + ["seagreen"])
for i, (b, v) in enumerate(zip(bars, _vals)):
    txt = f"{v:,}"
    if i > 0:
        txt += f"\n({100 * v / _vals[0]:.1f}% of all)"
    ax.text(b.get_x() + b.get_width() / 2, v, txt, ha="center", va="bottom", fontsize=10)
ax.set_ylim(0, max(_vals) * 1.15)
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax.set_ylabel("Cards", fontsize=12)
ax.set_title("Sample funnel: how many cards are in or out", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure 4: cards per treatment group
treat_pd = (
    _f5.groupBy("treatment").count().toPandas()
    .set_index("treatment")
    .reindex(["apoyo_kept", "apoyo_lost", "apoyo_gain", "mayor_kept", "mayor_lost", "mayor_gain", "never"])
    .fillna(0)
    .reset_index()
)

fig, ax = plt.subplots(figsize=(11, 5))
colors = ["indianred"] * 3 + ["steelblue"] * 3 + ["dimgray"]
bars = ax.bar(treat_pd["treatment"], treat_pd["count"], color=colors)
for b, v in zip(bars, treat_pd["count"]):
    ax.text(b.get_x() + b.get_width() / 2, v, f"{int(v):,}", ha="center", va="bottom", fontsize=10)
ax.set_ylim(0, treat_pd["count"].max() * 1.15)
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax.set_ylabel("Cards", fontsize=12)
ax.tick_params(axis="x", rotation=30)
ax.set_title("Cards per treatment group (analysis sample)", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure 5: mean monthly trips by treatment group
trends_pd = (
    spark.table(T5_TABLE)
    .join(spark.table(T4_TABLE).select("cardnumber", "treatment"), "cardnumber")
    .groupBy("ymonth", "treatment")
    .agg(F.avg("n_trips").alias("mean_trips"))
    .orderBy("ymonth")
    .toPandas()
    .pivot(index="ymonth", columns="treatment", values="mean_trips")
)

fig, ax = plt.subplots(figsize=(12, 6))
for col in trends_pd.columns:
    style = "--" if col == "never" else "-"
    ax.plot(trends_pd.index, trends_pd[col], style, marker="o", markersize=3, linewidth=1.4, label=col)
ax.axvline(REFORM_MONTH, color="red", linewidth=1.2, linestyle="--", label=f"Reform ({REFORM_MONTH})")
ax.set_ylabel("Mean monthly trips per card", fontsize=12)
ax.set_xlabel("Month", fontsize=12)
ax.tick_params(axis="x", rotation=45)
ax.set_title("Mean monthly trips by treatment group (raw pre-trends)", fontsize=13, fontweight="bold")
ax.legend(fontsize=9, ncol=2)
plt.tight_layout()
plt.show()
