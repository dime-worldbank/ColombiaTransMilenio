# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup and load all 3 parquets
import os
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

# Base path
V_DIR = "/Volumes/prd_csc_mega/sColom15/vColom15"
sample_will_path = os.path.join(V_DIR, "Workspace/bogota-hdfs/sample-will")

# Load all 3 parquet files
files = ["parquet_df_clean_joined", "parquet_df_raw_new_data", "parquet_df_vars_joined"]
dfs = {}

for f in files:
    dfs[f] = spark.read.parquet(os.path.join(sample_will_path, f))
    
    # Print schema (structure only, not full column list)
    n_cols = len(dfs[f].columns)
    n_rows = dfs[f].count()
    print(f"\n{'='*60}")
    print(f"  {f}")
    print(f"{'='*60}")
    print(f"  Columns: {n_cols}")
    print(f"  Rows:    {n_rows:,}")
    print(f"  Column names: {dfs[f].columns}")
    
    # Print time span (assumes transaction_timestamp exists)
    if "transaction_timestamp" in dfs[f].columns:
        span = dfs[f].select(
            F.min("transaction_timestamp").alias("earliest"),
            F.max("transaction_timestamp").alias("latest")
        ).first()
        print(f"  Time span: {span['earliest']} → {span['latest']}")
    else:
        # Try other timestamp-like columns
        ts_cols = [c for c, t in dfs[f].dtypes if t == "timestamp"]
        if ts_cols:
            span = dfs[f].select(
                F.min(ts_cols[0]).alias("earliest"),
                F.max(ts_cols[0]).alias("latest")
            ).first()
            print(f"  Time span ({ts_cols[0]}): {span['earliest']} → {span['latest']}")
        else:
            print("  No timestamp column found")

# COMMAND ----------

# DBTITLE 1,Plot daily transactions for all 3 files
# Plot daily transactions for each parquet file
fig, axes = plt.subplots(3, 1, figsize=(16, 12), sharex=False)
fig.suptitle("Daily Transactions - sample-will parquets", fontsize=14)

for i, (name, df) in enumerate(dfs.items()):
    # Determine the date column
    if "day" in df.columns:
        day_col = "day"
    elif "transaction_timestamp" in df.columns:
        day_col = None  # we'll derive it
    else:
        ts_cols = [c for c, t in df.dtypes if t == "timestamp"]
        day_col = ts_cols[0] if ts_cols else None
    
    if day_col == "day":
        daily = df.groupBy("day").count().orderBy("day").toPandas()
        daily["day"] = pd.to_datetime(daily["day"])
    elif "transaction_timestamp" in df.columns:
        daily = (df
            .withColumn("day", F.to_date("transaction_timestamp"))
            .groupBy("day").count()
            .orderBy("day")
            .toPandas())
        daily["day"] = pd.to_datetime(daily["day"])
    else:
        axes[i].text(0.5, 0.5, "No timestamp column", transform=axes[i].transAxes, ha="center")
        axes[i].set_title(name)
        continue
    
    axes[i].plot(daily["day"], daily["count"] / 1000, linewidth=0.8)
    axes[i].set_title(name)
    axes[i].set_ylabel("Transactions (thousands)")
    axes[i].grid(True, alpha=0.3)

axes[-1].set_xlabel("Date")
plt.tight_layout()
plt.show()
