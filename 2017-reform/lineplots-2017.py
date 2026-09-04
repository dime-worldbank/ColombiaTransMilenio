# Databricks notebook source
"""
Status line-plot figures (5–11) for the 2017 TransMilenio reform analysis.

All figures read from the persisted Delta tables cards_2017 (T4) and
panel_2017 (T5).  Run after data-construction-2017 has built those tables.
"""

# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# Tables
T4_TABLE = "prd_mega.scolom15.cards_2017"
T5_TABLE = "prd_mega.scolom15.panel_2017"

LAST_PRE_REFORM = "2017-03"

# COMMAND ----------
# Helper: standard line-plot formatting

def _trend_plot(pdf, ylabel, title, ylim_bottom=0, ylim_top=None):
    fig, ax = plt.subplots(figsize=(12, 6))
    for col in pdf.columns:
        style = "--" if col == "never" else "-"
        ax.plot(pdf.index, pdf[col], style, marker="o", markersize=3, linewidth=1.4, label=col)
    ax.axvline(LAST_PRE_REFORM, color="red", linewidth=1.2, linestyle="--",
              label=f"Last pre-reform month ({LAST_PRE_REFORM})")
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_xlabel("Month", fontsize=12)
    ax.tick_params(axis="x", rotation=45)
    ax.set_ylim(bottom=ylim_bottom)
    if ylim_top is not None:
        ax.set_ylim(top=ylim_top)
    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.legend(fontsize=9, ncol=2)
    plt.tight_layout()
    plt.show()

# COMMAND ----------
# Read tables once

_t5 = spark.table(T5_TABLE)
_t4 = spark.table(T4_TABLE)

# COMMAND ----------
# Figure 5: mean monthly trips (0s always)

trends_pd = (
    _t5.join(_t4.select("cardnumber", "treatment"), "cardnumber")
    .groupBy("ymonth", "treatment")
    .agg(F.avg("n_trips").alias("mean_trips"))
    .orderBy("ymonth")
    .toPandas()
    .pivot(index="ymonth", columns="treatment", values="mean_trips")
)
_trend_plot(trends_pd, "Mean monthly trips per card",
            "Figure 5 \u2014 Mean monthly trips by treatment group (0s always)")

# COMMAND ----------
# Figure 6: mean monthly trips (excl. infrequent)

trends_freq_pd = (
    _t5.join(_t4.select("cardnumber", "treatment", "tag_infrequent"), "cardnumber")
    .filter(F.coalesce(F.col("tag_infrequent"), F.lit(0)) == 0)
    .groupBy("ymonth", "treatment")
    .agg(F.avg("n_trips").alias("mean_trips"))
    .orderBy("ymonth")
    .toPandas()
    .pivot(index="ymonth", columns="treatment", values="mean_trips")
)
_trend_plot(trends_freq_pd, "Mean monthly trips per card",
            "Figure 6 \u2014 Mean monthly trips (excl. infrequent users)")

# COMMAND ----------
# Figure 7: mean monthly trips (conditional on traveling — intensive margin)

trends_cond_pd = (
    _t5.join(_t4.select("cardnumber", "treatment"), "cardnumber")
    .filter(F.col("n_trips") > 0)
    .groupBy("ymonth", "treatment")
    .agg(F.avg("n_trips").alias("mean_trips"))
    .orderBy("ymonth")
    .toPandas()
    .pivot(index="ymonth", columns="treatment", values="mean_trips")
)
_trend_plot(trends_cond_pd, "Mean monthly trips per card",
            "Figure 7 \u2014 Mean monthly trips (conditional on traveling)")

# COMMAND ----------
# Figure 8: mean monthly trips (missing before first appearance)

trends_entry_pd = (
    _t5.join(_t4.select("cardnumber", "treatment", "first_active_month"), "cardnumber")
    .filter(F.col("ymonth") >= F.col("first_active_month"))
    .groupBy("ymonth", "treatment")
    .agg(F.avg("n_trips").alias("mean_trips"))
    .orderBy("ymonth")
    .toPandas()
    .pivot(index="ymonth", columns="treatment", values="mean_trips")
)
_trend_plot(trends_entry_pd, "Mean monthly trips per card",
            "Figure 8 \u2014 Mean monthly trips (missing before first appearance)")

# COMMAND ----------
# Figure 9: share of cards traveling (extensive margin, 0s always)

share_traveling_pd = (
    _t5.join(_t4.select("cardnumber", "treatment"), "cardnumber")
    .groupBy("ymonth", "treatment")
    .agg((100 * F.avg(F.col("has_trips").cast("int"))).alias("pct_traveling"))
    .orderBy("ymonth")
    .toPandas()
    .pivot(index="ymonth", columns="treatment", values="pct_traveling")
)
_trend_plot(share_traveling_pd, "% of cards traveling",
            "Figure 9 \u2014 Share of cards traveling each month (0s always)",
            ylim_top=100)

# COMMAND ----------
# Figure 10: share traveling (missing before first appearance)

share_traveling_entry_pd = (
    _t5.join(_t4.select("cardnumber", "treatment", "first_active_month"), "cardnumber")
    .filter(F.col("ymonth") >= F.col("first_active_month"))
    .groupBy("ymonth", "treatment")
    .agg((100 * F.avg(F.col("has_trips").cast("int"))).alias("pct_traveling"))
    .orderBy("ymonth")
    .toPandas()
    .pivot(index="ymonth", columns="treatment", values="pct_traveling")
)
_trend_plot(share_traveling_entry_pd, "% of cards traveling",
            "Figure 10 \u2014 Share traveling (missing before first appearance)",
            ylim_top=100)

# COMMAND ----------
# Figure 11: share traveling (missing before first appearance, excl. infrequent)

share_trav_entry_freq_pd = (
    _t5.join(
        _t4.select("cardnumber", "treatment", "first_active_month", "tag_infrequent"),
        "cardnumber",
    )
    .filter(
        (F.col("ymonth") >= F.col("first_active_month"))
        & (F.coalesce(F.col("tag_infrequent"), F.lit(0)) == 0)
    )
    .groupBy("ymonth", "treatment")
    .agg((100 * F.avg(F.col("has_trips").cast("int"))).alias("pct_traveling"))
    .orderBy("ymonth")
    .toPandas()
    .pivot(index="ymonth", columns="treatment", values="pct_traveling")
)
_trend_plot(share_trav_entry_freq_pd, "% of cards traveling",
            "Figure 11 \u2014 Share traveling (missing before first appearance, excl. infrequent)",
            ylim_top=100)

