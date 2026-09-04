# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Index
# MAGIC %md
# MAGIC ## Index
# MAGIC
# MAGIC Builds the price inputs for the fare elasticities, restricted to the analysis sample (the cards in `panel_2017`: `never`, `apoyo_kept`, `apoyo_lost`, present before and after the reform). Reads the window table, `cards_2017` and `panel_2017`; the tables built upstream are not touched.
# MAGIC
# MAGIC Three tables: `fares_2017` (fares by group × fare period × type, including transfers), `basket_2017` (card × month trip basket: zonal and troncal trips, transfers by type, amount paid) and `prices_2017` (one row per card: price paid per trip before the reform and the price the same basket would cost after it, for two pre windows).
# MAGIC
# MAGIC 1. **Setup** — packages, catalog, parameters
# MAGIC 2. **Read tables** — sample cards and their transactions
# MAGIC 3. **Transfer type** — where each transfer comes from (zonal or troncal) and where it goes, within the transfer time window
# MAGIC 4. **Fare table** — modal fares by group × period for trips and for each transfer type → `fares_2017`
# MAGIC 5. **Monthly basket** — trips, transfers and spending per card × month → `basket_2017`
# MAGIC 6. **Prices per card** — price paid per trip before, price of the same basket after → `prices_2017`
# MAGIC 7. **Status figures** — fare table, basket composition by month, mean prices by group

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

# COMMAND ----------

# DBTITLE 1,Select catalog
# MAGIC %sql
# MAGIC USE CATALOG prd_mega;
# MAGIC USE SCHEMA scolom15;
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema;

# COMMAND ----------

# DBTITLE 1,Parameters
# Input tables
T2_TABLE = "prd_mega.scolom15.silver_validaciones_oct2016tosep2017_tags"
T4_TABLE = "prd_mega.scolom15.cards_2017"
T5_TABLE = "prd_mega.scolom15.panel_2017"

# Output tables
FARES_TABLE  = "prd_mega.scolom15.fares_2017"
BASKET_TABLE = "prd_mega.scolom15.basket_2017"
PRICES_TABLE = "prd_mega.scolom15.prices_2017"

# Reform month and analysis window (dist_months = months since the reform)
REFORM_MONTH = "2017-04"
WINDOW_MONTHS = [str(p) for p in pd.period_range("2016-10", "2017-09", freq="M")]

# Fare periods inside the window (the fare scheme changed with the reform)
FARE_PERIODS_M = [
    ("oct16-mar17", "2016-10", "2017-03"),
    ("apr17-sep17", "2017-04", "2017-09"),
]
PRE_PERIOD  = "oct16-mar17"
POST_PERIOD = "apr17-sep17"

# Treatment groups in the sample and the fare group each one pays after the reform
SAMPLE_TREATMENTS = ["never", "apoyo_kept", "apoyo_lost"]
POST_FARE_GROUP = {
    "apoyo_kept": "apoyo",
    "apoyo_lost": "always_adulto",
    "never":      "always_adulto",
}

# Modal fares are computed over each card's first trips of the month, up to this rank
MAX_TRIPS_RANK = 30

# A validation at or below this value is a transfer (same cut as the window table)
TRIP_MIN_VALUE = 300

# Maximum minutes since the previous validation for a transfer to be classified
# (95 minutes was the rule at the April 2017 reform; checked against the data below)
TRANSFER_WINDOW_MIN = 95

# Transfer types by origin → destination leg (z = zonal, t = troncal); transfers
# with no previous validation or outside the time window are "unknown"
TRANSFER_TYPES = ["zz", "zt", "tz", "tt"]

# Unknown transfers are priced after the reform at this transfer type's fare
UNKNOWN_TRANSFER_AS = "zz"

# Pre-reform windows for the basket and the price paid, in months relative to the reform
PRICE_WINDOWS = {
    "6m": (-6, -1),
    "3m": (-3, -1),
}

# Post-reform months for the observed price paid; the glitch month is excluded
# (every apoyo holder got the subsidy that month)
POST_OBS_RANGE = (0, 5)
GLITCH_MONTH = "2017-08"

# COMMAND ----------

# DBTITLE 1,Section 2: Read tables
# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Read tables
# MAGIC
# MAGIC The sample is the set of cards in `panel_2017`. Their treatment and analysis group come from `cards_2017`; their transactions are the window table restricted to those cards (whole cards are kept, so the transaction sequence used below is complete).

# COMMAND ----------

# DBTITLE 1,Sample cards and their transactions
df_sample_cards = (
    spark.table(T5_TABLE)
    .select("cardnumber").distinct()
    .join(spark.table(T4_TABLE).select("cardnumber", "card_group", "treatment"), "cardnumber", "left")
)

n_sample = df_sample_cards.count()
print(f"Cards in the sample: {n_sample:,}")

_unexpected = df_sample_cards.filter(~F.col("treatment").isin(SAMPLE_TREATMENTS) | F.col("treatment").isNull())
n_unexpected = _unexpected.count()
if n_unexpected == 0:
    print(f"✅ Every sample card is in {SAMPLE_TREATMENTS}.")
else:
    print(f"⚠️  {n_unexpected:,} sample cards outside {SAMPLE_TREATMENTS} — investigate:")
    display(_unexpected.groupBy("treatment").count())

print("── Sample cards by treatment ──")
display(df_sample_cards.groupBy("treatment", "card_group").count().orderBy("treatment"))

df_tx = (
    spark.table(T2_TABLE)
    .select("cardnumber", "ymonth", "fecha_transaccion", "fecha_transaccion_timestamp",
            "value", "trip", "transfer", "is_trunk")
    .join(df_sample_cards, "cardnumber")
)

n_tx = df_tx.count()
print(f"Transactions of sample cards: {n_tx:,}")

# Fare period of each transaction, used throughout
_fare_period_expr = F.lit(None).cast("string")
for label, m_start, m_end in FARE_PERIODS_M:
    _fare_period_expr = F.when(F.col("ymonth").between(m_start, m_end), F.lit(label)).otherwise(_fare_period_expr)

# Months since the reform, used for the windows
dist_months_expr = F.months_between(
    F.to_date(F.concat(F.col("ymonth"), F.lit("-01"))),
    F.to_date(F.lit(f"{REFORM_MONTH}-01")),
).cast("int")

df_tx = df_tx.withColumn("fare_period", _fare_period_expr)

# COMMAND ----------

# DBTITLE 1,Section 3: Transfer type
# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Transfer type
# MAGIC
# MAGIC A transfer is priced by where the card comes from and where it goes: after the reform, zonal → troncal transfers pay 200 and every other transfer is free, while before the reform every transfer paid 300. The origin leg is the previous validation of the same card (of any kind: the transfer chain is trip → transfer → transfer), and the destination leg is the transfer's own operator. Transfers with no previous validation in the window, or more than the transfer time window after it, stay unclassified.

# COMMAND ----------

# DBTITLE 1,Previous validation and transfer type
_w_card = Window.partitionBy("cardnumber").orderBy("fecha_transaccion_timestamp")

df_tx = (
    df_tx
    .withColumn("_prev_ts",    F.lag("fecha_transaccion_timestamp").over(_w_card))
    .withColumn("_prev_trunk", F.lag("is_trunk").over(_w_card))
    .withColumn(
        "gap_min",
        F.when(
            F.col("_prev_ts").isNotNull() & F.col("fecha_transaccion_timestamp").isNotNull(),
            (F.unix_timestamp("fecha_transaccion_timestamp") - F.unix_timestamp("_prev_ts")) / 60,
        ),
    )
)

_leg = lambda c: F.when(F.col(c) == 1, F.lit("t")).otherwise(F.lit("z"))
_classifiable = F.coalesce(
    (F.col("gap_min") <= TRANSFER_WINDOW_MIN) & F.col("_prev_trunk").isNotNull() & F.col("is_trunk").isNotNull(),
    F.lit(False),
)

transfer_type_expr = (
    F.when(F.col("transfer") != 1, F.lit(None).cast("string"))
    .when(~_classifiable, F.lit("unknown"))
    .otherwise(F.concat(_leg("_prev_trunk"), _leg("is_trunk")))
)

df_tx = df_tx.withColumn("transfer_type", transfer_type_expr)

print("── Transfers by type and fare period ──")
display(
    df_tx
    .filter(F.col("transfer") == 1)
    .groupBy("fare_period")
    .pivot("transfer_type", TRANSFER_TYPES + ["unknown"])
    .count()
    .orderBy("fare_period")
)

# Why transfers end up unknown: no previous validation, or previous validation too long ago
print("── Unknown transfers: reason ──")
display(
    df_tx
    .filter((F.col("transfer") == 1) & (F.col("transfer_type") == "unknown"))
    .withColumn(
        "reason",
        F.when(F.col("_prev_ts").isNull(), "no previous validation")
         .when(F.col("gap_min") > TRANSFER_WINDOW_MIN, f"more than {TRANSFER_WINDOW_MIN} min after the previous one")
         .otherwise("operator type missing"),
    )
    .groupBy("fare_period", "reason")
    .count()
    .orderBy("fare_period", "reason")
)

# COMMAND ----------

# DBTITLE 1,Check: minutes since the previous validation, for transfers, by period
# If the time window was different before the reform, the pre-period histogram
# shows mass beyond the line: adjust TRANSFER_WINDOW_MIN per period in that case.
BIN_MIN = 5
_gap_pd = (
    df_tx
    .filter((F.col("transfer") == 1) & F.col("gap_min").isNotNull() & (F.col("gap_min") <= 240))
    .withColumn("gap_bin", (F.floor(F.col("gap_min") / BIN_MIN) * BIN_MIN).cast("int"))
    .groupBy("fare_period", "gap_bin")
    .count()
    .orderBy("fare_period", "gap_bin")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(14, 4.5), sharey=False)
for ax, (label, _, _) in zip(axes, FARE_PERIODS_M):
    pdf = _gap_pd[_gap_pd["fare_period"] == label]
    ax.bar(pdf["gap_bin"], pdf["count"], width=BIN_MIN, align="edge", color="steelblue")
    ax.axvline(TRANSFER_WINDOW_MIN, color="red", linewidth=1.2, linestyle="--", label=f"Transfer window ({TRANSFER_WINDOW_MIN} min)")
    ax.set_xlabel("Minutes since the previous validation", fontsize=10)
    ax.set_ylabel("Transfers", fontsize=10)
    ax.set_title(label, fontsize=12, fontweight="bold")
    ax.legend(fontsize=8)
plt.suptitle("Transfers by time since the previous validation (capped at 240 min)", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# Share of transfers beyond the window, per period
display(
    df_tx
    .filter((F.col("transfer") == 1) & F.col("gap_min").isNotNull())
    .groupBy("fare_period")
    .agg(
        F.count(F.lit(1)).alias("transfers"),
        F.sum((F.col("gap_min") > TRANSFER_WINDOW_MIN).cast("int")).alias("beyond_window"),
    )
    .withColumn("pct_beyond_window", F.round(100 * F.col("beyond_window") / F.col("transfers"), 2))
    .orderBy("fare_period")
)

# COMMAND ----------

# DBTITLE 1,Section 4: Fare table
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Fare table → `fares_2017`
# MAGIC
# MAGIC For trips, the two most frequent values by analysis group and fare period are the zonal (lower) and troncal (higher) fares, computed over each card's first 30 trips of the month so heavy travelers do not dominate the mode.
# MAGIC For transfers, the most frequent value by group, period and transfer type. The full value distribution of transfers is displayed first: it is the check of the transfer pricing rule (300 before; after, 200 for zonal → troncal and 0 otherwise).

# COMMAND ----------

# DBTITLE 1,Modal trip fares by group × period
_w_trip_rank = Window.partitionBy("cardnumber", "ymonth").orderBy("fecha_transaccion_timestamp")

df_ranked_trips = (
    df_tx
    .filter((F.col("trip") == 1) & F.col("fecha_transaccion_timestamp").isNotNull() & F.col("fare_period").isNotNull())
    .withColumn("trip_rank", F.row_number().over(_w_trip_rank))
    .filter(F.col("trip_rank") <= MAX_TRIPS_RANK)
)

_w_mode = Window.partitionBy("card_group", "fare_period").orderBy(F.col("count").desc())

_trip_modes_pd = (
    df_ranked_trips
    .groupBy("card_group", "fare_period", "value")
    .count()
    .withColumn("mode_rank", F.row_number().over(_w_mode))
    .filter(F.col("mode_rank") <= 2)
    .toPandas()
)
_trip_totals_pd = df_ranked_trips.groupBy("card_group", "fare_period").count().toPandas().rename(columns={"count": "total"})

trip_fares_pd = (
    _trip_modes_pd
    .merge(_trip_totals_pd, on=["card_group", "fare_period"])
    .assign(fare_type=lambda d: np.where(
        d["value"] == d.groupby(["card_group", "fare_period"])["value"].transform("min"), "zonal", "troncal"
    ))
    .rename(columns={"value": "fare", "count": "freq"})
    .assign(share=lambda d: d["freq"] / d["total"])
    [["card_group", "fare_period", "fare_type", "fare", "freq", "share"]]
)
print("── Modal trip fares by group × period (share = among the ranked trips of the group-period) ──")
display(trip_fares_pd.sort_values(["card_group", "fare_period", "fare_type"]))

# COMMAND ----------

# DBTITLE 1,Check: value distribution of transfers by group × period × type
_w_tr = Window.partitionBy("card_group", "fare_period", "transfer_type")

df_transfer_values = (
    df_tx
    .filter((F.col("transfer") == 1) & F.col("fare_period").isNotNull())
    .groupBy("card_group", "fare_period", "transfer_type", "value")
    .count()
    .withColumn("share", F.round(F.col("count") / F.sum("count").over(_w_tr), 4))
    .withColumn("mode_rank", F.row_number().over(_w_tr.orderBy(F.col("count").desc())))
)

print("── Transfer values by group × period × type (every value with share ≥ 1%) ──")
display(
    df_transfer_values
    .filter(F.col("share") >= 0.01)
    .orderBy("card_group", "fare_period", "transfer_type", F.col("count").desc())
)

# COMMAND ----------

# DBTITLE 1,Modal transfer fares and the fare table
transfer_fares_pd = (
    df_transfer_values
    .filter((F.col("mode_rank") == 1) & F.col("transfer_type").isin(TRANSFER_TYPES))
    .withColumn("fare_type", F.concat(F.lit("tr_"), F.col("transfer_type")))
    .select("card_group", "fare_period", "fare_type", F.col("value").alias("fare"), F.col("count").alias("freq"), "share")
    .toPandas()
)

fares_pd = (
    pd.concat([trip_fares_pd, transfer_fares_pd], ignore_index=True)
    .astype({"fare": "float64", "freq": "int64", "share": "float64"})
    .assign(share=lambda d: d["share"].mul(100).round(1))
    .rename(columns={"share": "pct_at_fare"})
    .sort_values(["card_group", "fare_period", "fare_type"])
    .reset_index(drop=True)
)

# Every group × period must have a fare for every type; a transfer type absent
# from a group-period gets that period's base transfer fare and is reported
_groups  = sorted(df_sample_cards.select("card_group").distinct().toPandas()["card_group"].dropna())
_periods = [p[0] for p in FARE_PERIODS_M]
_types   = ["zonal", "troncal"] + [f"tr_{t}" for t in TRANSFER_TYPES]
_filled = []
for g in _groups:
    for p in _periods:
        present = set(fares_pd[(fares_pd["card_group"] == g) & (fares_pd["fare_period"] == p)]["fare_type"])
        for t in _types:
            if t in present:
                continue
            if t in ("zonal", "troncal"):
                raise ValueError(f"No {t} fare for {g} × {p}")
            base = fares_pd[(fares_pd["card_group"] == g) & (fares_pd["fare_period"] == p) & (fares_pd["fare_type"] == f"tr_{UNKNOWN_TRANSFER_AS}")]
            fares_pd = pd.concat([fares_pd, pd.DataFrame([{
                "card_group": g, "fare_period": p, "fare_type": t,
                "fare": float(base["fare"].iloc[0]), "freq": 0, "pct_at_fare": np.nan,
            }])], ignore_index=True)
            _filled.append((g, p, t))
if _filled:
    print(f"⚠️  Transfer types with no transfers, priced at the base transfer fare: {_filled}")
else:
    print("✅ Every group × period has a fare for every type.")

_period_order = {p[0]: i for i, p in enumerate(FARE_PERIODS_M)}
fares_pd = fares_pd.sort_values(
    ["fare_type", "card_group", "fare_period"],
    key=lambda col: col.map(_period_order) if col.name == "fare_period" else col,
).reset_index(drop=True)
print("── Fare table (pct_at_fare = % of transactions in the group × period × type paying exactly that fare) ──")
display(fares_pd)

(
    spark.createDataFrame(fares_pd)
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(FARES_TABLE)
)
print(f"✅ Wrote {FARES_TABLE}: {len(fares_pd)} rows")

# Wide post-reform fares per fare group, for the counterfactual price below
fares_post_wide_pd = (
    fares_pd[fares_pd["fare_period"] == POST_PERIOD]
    .pivot(index="card_group", columns="fare_type", values="fare")
    .reset_index()
    .rename(columns={"card_group": "fare_group_post"})
)
fares_post_wide_pd.columns.name = None
df_fares_post = spark.createDataFrame(fares_post_wide_pd)

# COMMAND ----------

# DBTITLE 1,Section 5: Monthly basket
# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Monthly basket → `basket_2017`
# MAGIC
# MAGIC One row per card × month with at least one transaction: trips split into zonal and troncal, transfers by type, and the amount paid on trips, on transfers and in total. Values are what the card actually paid, with no adjustment for tagged rows.

# COMMAND ----------

# DBTITLE 1,Build and write the basket
_n_tr = lambda t: F.sum(F.when((F.col("transfer") == 1) & (F.col("transfer_type") == t), 1).otherwise(0)).alias(f"n_tr_{t}")

df_basket = (
    df_tx
    .groupBy("cardnumber", "ymonth")
    .agg(
        F.count(F.lit(1)).alias("n_tx"),
        F.sum("trip").alias("n_trips"),
        F.sum(F.when((F.col("trip") == 1) & (F.col("is_trunk") == 0), 1).otherwise(0)).alias("n_zonal"),
        F.sum(F.when((F.col("trip") == 1) & (F.col("is_trunk") == 1), 1).otherwise(0)).alias("n_troncal"),
        F.sum("transfer").alias("n_transfers"),
        *[_n_tr(t) for t in TRANSFER_TYPES + ["unknown"]],
        F.coalesce(F.sum(F.when(F.col("trip") == 1, F.col("value"))), F.lit(0.0)).alias("tot_value_trips"),
        F.coalesce(F.sum(F.when(F.col("transfer") == 1, F.col("value"))), F.lit(0.0)).alias("tot_value_transfers"),
        F.coalesce(F.sum("value"), F.lit(0.0)).alias("tot_value_all"),
    )
)

(
    df_basket.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BASKET_TABLE)
)

n_basket = spark.table(BASKET_TABLE).count()
print(f"✅ Wrote {BASKET_TABLE}: {n_basket:,} card-months")

# Trips with no operator type would fall out of both zonal and troncal
_n_no_type = df_tx.filter((F.col("trip") == 1) & F.col("is_trunk").isNull()).count()
print(f"Trips with no operator type (neither zonal nor troncal): {_n_no_type:,}")

# COMMAND ----------

# DBTITLE 1,Check: trips per card-month match the panel
_cmp = (
    spark.table(BASKET_TABLE).select("cardnumber", "ymonth", F.col("n_trips").alias("n_trips_basket"))
    .join(spark.table(T5_TABLE).select("cardnumber", "ymonth", "n_trips"), ["cardnumber", "ymonth"], "inner")
)
n_mismatch = _cmp.filter(F.col("n_trips_basket") != F.col("n_trips")).count()
if n_mismatch == 0:
    print("✅ n_trips in the basket matches panel_2017 for every card-month present in both.")
else:
    print(f"⚠️  {n_mismatch:,} card-months where n_trips differs from panel_2017 — investigate.")

# COMMAND ----------

# DBTITLE 1,Section 6: Prices per card
# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Prices per card → `prices_2017`
# MAGIC
# MAGIC For each pre-reform window (6 months and 3 months before the reform): 
# MAGIC * the card's basket over the window
# MAGIC * the price paid per trip (`p_pre` = total paid, transfers included, over trips)
# MAGIC * the price the same basket would cost at post-reform fares (`p_post_cf`), using the fares of the group the card pays after the reform: 
# MAGIC     * apoyo fares for `apoyo_kept`, 
# MAGIC     * adulto fares for `apoyo_lost` and `never`.
# MAGIC     * Unknown transfers are priced at $0
# MAGIC     * The observed price paid after the reform (`p_post_obs`, months 0–5 without the glitch month) is a diagnostic: its gap with `p_post_cf` is the change in the basket itself.

# COMMAND ----------

# DBTITLE 1,Basket per window and prices
df_basket_dist = spark.table(BASKET_TABLE).withColumn("dist_months", dist_months_expr)

_basket_cols = ["n_trips", "n_zonal", "n_troncal", "n_transfers"] + [f"n_tr_{t}" for t in TRANSFER_TYPES + ["unknown"]] \
    + ["tot_value_trips", "tot_value_transfers", "tot_value_all"]

def window_basket(lo, hi, suffix, exclude_month=None):
    cond = F.col("dist_months").between(lo, hi)
    if exclude_month is not None:
        cond = cond & (F.col("ymonth") != exclude_month)
    return (
        df_basket_dist
        .filter(cond)
        .groupBy("cardnumber")
        .agg(*[F.sum(c).alias(f"{c}_{suffix}") for c in _basket_cols])
    )

df_prices = df_sample_cards.withColumn("fare_group_post", F.lit(None).cast("string"))
for treatment, fare_group in POST_FARE_GROUP.items():
    df_prices = df_prices.withColumn(
        "fare_group_post",
        F.when(F.col("treatment") == treatment, F.lit(fare_group)).otherwise(F.col("fare_group_post")),
    )
df_prices = df_prices.join(df_fares_post, "fare_group_post", "left")

for label, (lo, hi) in PRICE_WINDOWS.items():
    sfx = f"pre_{label}"
    df_prices = (
        df_prices
        .join(window_basket(lo, hi, sfx), "cardnumber", "left")
        .fillna(0, subset=[f"{c}_{sfx}" for c in _basket_cols])
        .withColumn(
            f"p_pre_{label}",
            F.when(F.col(f"n_trips_{sfx}") > 0, F.col(f"tot_value_all_{sfx}") / F.col(f"n_trips_{sfx}")),
        )
        .withColumn(
            f"p_post_cf_{label}",
            F.when(
                F.col(f"n_trips_{sfx}") > 0,
                (
                    F.col(f"n_zonal_{sfx}")   * F.col("zonal")
                    + F.col(f"n_troncal_{sfx}") * F.col("troncal")
                    + sum(F.col(f"n_tr_{t}_{sfx}") * F.col(f"tr_{t}") for t in TRANSFER_TYPES)
                    + F.col(f"n_tr_unknown_{sfx}") * F.col(f"tr_{UNKNOWN_TRANSFER_AS}")
                ) / F.col(f"n_trips_{sfx}"),
            ),
        )
    )

df_prices = (
    df_prices
    .join(window_basket(*POST_OBS_RANGE, "post_obs", exclude_month=GLITCH_MONTH), "cardnumber", "left")
    .fillna(0, subset=[f"{c}_post_obs" for c in _basket_cols])
    .withColumn("p_post_obs", F.when(F.col("n_trips_post_obs") > 0, F.col("tot_value_all_post_obs") / F.col("n_trips_post_obs")))
    # The post fares ride along for reference, prefixed
    .withColumnRenamed("zonal", "fare_post_zonal")
    .withColumnRenamed("troncal", "fare_post_troncal")
)
for t in TRANSFER_TYPES:
    df_prices = df_prices.withColumnRenamed(f"tr_{t}", f"fare_post_tr_{t}")

(
    df_prices.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(PRICES_TABLE)
)

n_prices = spark.table(PRICES_TABLE).count()
print(f"✅ Wrote {PRICES_TABLE}: {n_prices:,} cards")
if n_prices == n_sample:
    print("✅ One row per sample card.")
else:
    print(f"⚠️  Expected {n_sample:,} rows — the joins must not add or drop cards, investigate.")

# COMMAND ----------

# DBTITLE 1,Check: cards without a price, and mean prices by treatment
# A card present in the 6 pre months may have no trips in the 3-month window
df_prices_tbl = spark.table(PRICES_TABLE)

_summary_cols = []
for label in PRICE_WINDOWS:
    _summary_cols += [
        F.sum(F.col(f"p_pre_{label}").isNull().cast("int")).alias(f"no_price_{label}"),
        F.round(F.avg(f"p_pre_{label}"), 1).alias(f"mean_p_pre_{label}"),
        F.round(F.avg(f"p_post_cf_{label}"), 1).alias(f"mean_p_post_cf_{label}"),
    ]
_summary_cols += [
    F.sum(F.col("p_post_obs").isNull().cast("int")).alias("no_price_post_obs"),
    F.round(F.avg("p_post_obs"), 1).alias("mean_p_post_obs"),
]

print("── Cards without a price and mean prices per trip, by treatment (means over cards) ──")
display(
    df_prices_tbl
    .groupBy("treatment")
    .agg(F.count(F.lit(1)).alias("cards"), *_summary_cols)
    .orderBy("treatment")
)

# Basket composition by treatment, 6-month window: shares of zonal trips and transfers per trip
print("── Basket composition in the 6 months before the reform, by treatment (% of total validations) ──")
display(
    df_prices_tbl
    .withColumn("n_tx_pre_6m", F.col("n_trips_pre_6m") + F.col("n_transfers_pre_6m"))
    .groupBy("treatment")
    .agg(
        F.round(100 * F.sum("n_zonal_pre_6m") / F.sum("n_tx_pre_6m"), 1).alias("pct_zonal"),
        F.round(100 * F.sum("n_troncal_pre_6m") / F.sum("n_tx_pre_6m"), 1).alias("pct_troncal"),
        F.round(100 * F.sum("n_transfers_pre_6m") / F.sum("n_tx_pre_6m"), 1).alias("pct_transfers"),
        F.round(100 * F.sum("n_tr_zt_pre_6m") / F.sum("n_tx_pre_6m"), 1).alias("pct_tr_zt"),
    )
    .orderBy("treatment")
)

# COMMAND ----------

# DBTITLE 1,Section 7: Status figures
# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Status figures
# MAGIC
# MAGIC 1. The fare table: trip and transfer fares by group × period
# MAGIC 2. Basket composition by month and treatment group (the reform must show up in transfers)
# MAGIC 3. Mean price per trip by treatment group: paid before, same basket after, observed after

# COMMAND ----------

# DBTITLE 1,Figure 1: fare table
_ft = fares_pd.copy()
_period_order = {p[0]: i for i, p in enumerate(FARE_PERIODS_M)}
_periods = sorted(_ft["fare_period"].unique(), key=lambda p: _period_order.get(p, 0))
_groups = sorted(_ft["card_group"].unique())

fig, axes = plt.subplots(2, 1, figsize=(14, 10))
for ax, types, title in [
    (axes[0], ["zonal", "troncal"], "Trip fares"),
    (axes[1], [f"tr_{t}" for t in TRANSFER_TYPES], "Transfer fares (origin → destination leg)"),
]:
    x_labels = [f"{g}\n{t}" for g in _groups for t in types]
    x = np.arange(len(x_labels))
    w = 0.8 / len(_periods)
    for i, p in enumerate(_periods):
        vals = [_ft[(_ft["card_group"] == g) & (_ft["fare_period"] == p) & (_ft["fare_type"] == t)]["fare"].iloc[0]
                for g in _groups for t in types]
        bars = ax.bar(x + (i - (len(_periods) - 1) / 2) * w, vals, width=w, label=p)
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v, f"${int(v)}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, fontsize=9)
    ax.set_ylabel("Fare (COP)", fontsize=11)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
plt.suptitle("Fare table of the analysis sample, by group × fare period", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure 2: basket composition by month and treatment group
import matplotlib.ticker as mticker

_comp_pd = (
    spark.table(BASKET_TABLE)
    .join(df_sample_cards.select("cardnumber", "treatment"), "cardnumber")
    .groupBy("ymonth", "treatment")
    .agg(
        (F.sum("n_zonal") / F.sum("n_trips")).alias("share_zonal"),
        (F.sum("n_transfers") / F.sum("n_trips")).alias("transfers_per_trip"),
        (F.sum("n_tr_zt") / F.sum("n_trips")).alias("tr_zt_per_trip"),
        (F.sum("tot_value_all") / F.sum("n_trips")).alias("paid_per_trip"),
    )
    .orderBy("ymonth")
    .toPandas()
)

fig, axes = plt.subplots(2, 2, figsize=(14, 9), sharex=True)
for ax, col, title, as_pct in [
    (axes[0, 0], "share_zonal",        "Share of zonal trips", True),
    (axes[0, 1], "transfers_per_trip", "Transfers per trip", True),
    (axes[1, 0], "tr_zt_per_trip",     "Zonal → troncal transfers per trip", True),
    (axes[1, 1], "paid_per_trip",      "Amount paid per trip (COP, transfers included)", False),
]:
    piv = _comp_pd.pivot(index="ymonth", columns="treatment", values=col).reindex(WINDOW_MONTHS)
    for t in SAMPLE_TREATMENTS:
        if t in piv.columns:
            ax.plot(piv.index, piv[t], "--" if t == "never" else "-", marker="o", markersize=3, linewidth=1.4, label=t)
    ax.axvline(str(pd.Period(REFORM_MONTH, freq="M") - 1), color="red", linewidth=1.2, linestyle="--")
    ax.axvline(GLITCH_MONTH, color="orange", linewidth=1.0, linestyle=":")
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.tick_params(axis="x", rotation=45)
    ax.set_ylim(bottom=0)
    if as_pct:
        ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0, decimals=0))
    ax.legend(fontsize=8)
plt.suptitle("Basket composition by month and treatment group (red = reform, orange = glitch month)", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure 3: mean price per trip by treatment group
_price_cols = []
for label in PRICE_WINDOWS:
    _price_cols += [f"p_pre_{label}", f"p_post_cf_{label}"]
_price_cols.append("p_post_obs")

_price_pd = (
    df_prices_tbl
    .groupBy("treatment")
    .agg(*[F.avg(c).alias(c) for c in _price_cols])
    .toPandas()
    .set_index("treatment")
    .reindex(SAMPLE_TREATMENTS)
)

# Average of zonal + troncal fares per treatment for reference
_avg_fares = (
    fares_pd[fares_pd["fare_type"].isin(["zonal", "troncal"])]
    .groupby(["card_group", "fare_period"])["fare"]
    .mean()
)
_pre_fg = {"never": "always_adulto", "apoyo_kept": "apoyo", "apoyo_lost": "apoyo"}
_price_pd["avg_fare_pre"] = [_avg_fares[(_pre_fg[t], PRE_PERIOD)] for t in SAMPLE_TREATMENTS]
_price_pd["avg_fare_post"] = [_avg_fares[(POST_FARE_GROUP[t], POST_PERIOD)] for t in SAMPLE_TREATMENTS]

fig, axes = plt.subplots(len(PRICE_WINDOWS), 1, figsize=(11, 7))
if len(PRICE_WINDOWS) == 1:
    axes = [axes]
for ax, label in zip(axes, PRICE_WINDOWS):
    series = [
        (f"p_pre_{label}",     "Pre (observed)",       "tab:blue",     None),
        ("avg_fare_pre",        "Avg fare (pre)",       "lightskyblue", "//"),
        (f"p_post_cf_{label}", "Post (counterfactual)", "tab:orange",   None),
        ("avg_fare_post",       "Avg fare (post)",      "navajowhite",  "//"),
        ("p_post_obs",         "Post (observed)",       "grey",         None),
    ]
    x = np.arange(len(SAMPLE_TREATMENTS))
    w = 0.8 / len(series)
    for i, (c, lbl, color, hatch) in enumerate(series):
        bars = ax.bar(x + (i - (len(series) - 1) / 2) * w, _price_pd[c], width=w, label=lbl, color=color, hatch=hatch)
        for b, v in zip(bars, _price_pd[c]):
            if pd.notna(v):
                ax.text(b.get_x() + b.get_width() / 2, v, f"{v:,.0f}", ha="center", va="bottom", fontsize=7)
    ax.set_xticks(x)
    ax.set_xticklabels(SAMPLE_TREATMENTS, fontsize=11)
    ax.set_ylabel("COP per trip (mean over cards)", fontsize=11)
    ax.set_title(f"Window: {label} before reform", fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
plt.suptitle("Price per trip by treatment group: paid before, same basket at post fares, observed after", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure 4: % change in price per trip by measure
fig, axes = plt.subplots(len(PRICE_WINDOWS), 1, figsize=(11, 5.5))
if len(PRICE_WINDOWS) == 1:
    axes = [axes]
for ax, label in zip(axes, PRICE_WINDOWS):
    pct_avg = (
        (_price_pd["avg_fare_post"] - _price_pd["avg_fare_pre"])
        / _price_pd["avg_fare_pre"] * 100
    )
    pct_cf = (
        (_price_pd[f"p_post_cf_{label}"] - _price_pd[f"p_pre_{label}"])
        / _price_pd[f"p_pre_{label}"] * 100
    )
    pct_obs = (
        (_price_pd["p_post_obs"] - _price_pd[f"p_pre_{label}"])
        / _price_pd[f"p_pre_{label}"] * 100
    )

    measures = [
        (pct_avg, "Avg fares (post vs pre)",                     "#2ca02c"),
        (pct_cf,  "Pre observed → Post counterfactual",          "#d62728"),
        (pct_obs, "Pre observed → Post observed",                "#9467bd"),
    ]
    x = np.arange(len(SAMPLE_TREATMENTS))
    w = 0.8 / len(measures)
    for i, (vals, lbl, color) in enumerate(measures):
        bars = ax.bar(x + (i - 1) * w, vals, width=w, label=lbl, color=color)
        for b, v in zip(bars, vals):
            if pd.notna(v):
                va = "bottom" if v >= 0 else "top"
                ax.text(b.get_x() + b.get_width() / 2, v, f"{v:+.1f}%",
                        ha="center", va=va, fontsize=8)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(SAMPLE_TREATMENTS, fontsize=11)
    ax.set_ylabel("% change in price per trip", fontsize=11)
    ax.set_title(f"Window: {label} before reform", fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
plt.suptitle(
    "Variation in price per trip by treatment group: three measures",
    fontsize=13, fontweight="bold",
)
plt.tight_layout()
plt.show()
