# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Inspect and define control schema
# MAGIC %md
# MAGIC # Bronze ingestion — since 2020 validaciones
# MAGIC
# MAGIC Reads files listed in `file_classification_since2020` and writes a unified bronze Delta table.
# MAGIC
# MAGIC - **Incremental**: only processes files where `classification_status = 'ready'` and `ingested_at IS NULL`
# MAGIC - **Per-file metadata detected** during classification (encoding, delimiter, archive format) drives the read options — no assumptions
# MAGIC - **Per header group**: files are read in batches by header group (consistent schema per batch)
# MAGIC - **Updates `ingested_at`** in the control table after each successful batch write

# COMMAND ----------

# DBTITLE 1,Alter control table
from pyspark.sql import functions as F
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Backfill control metadata
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
# MAGIC     transaction_timestamp TIMESTAMP  COMMENT 'Transaction timestamp parsed from Fecha_Transaccion (UTC stripped)',
# MAGIC     emisor                STRING     COMMENT 'Fare system issuer',
# MAGIC     operator              STRING     COMMENT 'Transport operator',
# MAGIC     line                  STRING     COMMENT 'Line or route identifier',
# MAGIC     station               STRING     COMMENT 'Station or stop (Estacion_Parada)',
# MAGIC     station_access        STRING     COMMENT 'Access point: same as station for format_6, Acceso_Estacion for format_7',
# MAGIC     machine               INT        COMMENT 'Device / validator machine ID',
# MAGIC     card_type             STRING     COMMENT 'Card type (Tipo_Tarjeta)',
# MAGIC     account_name          STRING     COMMENT 'Profile name (Nombre_Perfil)',
# MAGIC     cardnumber            STRING     COMMENT 'Card number — alphanumeric since 2020, kept as STRING',
# MAGIC     balance_before        INT        COMMENT 'Balance before transaction in COP',
# MAGIC     value                 INT        COMMENT 'Transaction value in COP',
# MAGIC     balance_after         INT        COMMENT 'Balance after transaction in COP',
# MAGIC     system                STRING     COMMENT 'Validation system (Sistema)',
# MAGIC     _source_file          STRING     COMMENT 'Source file path as returned by Spark input_file_name()',
# MAGIC     _header_group         STRING     COMMENT 'Header group from file_classification_since2020',
# MAGIC     _transform_format     STRING     COMMENT 'Transform applied during ingestion (format_6 or format_7)',
# MAGIC     _ingestion_ts         TIMESTAMP  COMMENT 'Timestamp when this batch was written to bronze'
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Load control table — pending files
# Load only files that are ready and not yet ingested
control_df = (
    spark.table(CONTROL_TABLE)
    .filter(
        (F.col("classification_status") == "ready") &
        F.col("ingested_at").isNull()
    )
    .toPandas()
)

print(f"Files pending ingestion: {len(control_df)}")
display(
    control_df.groupby(["header", "transform_format", "delimiter", "encoding"])
    .size()
    .reset_index(name="n_files")
)

# COMMAND ----------

# DBTITLE 1,Define transform functions
# Maps transform_format label → column selection.
# format_6: headers 08, 09, 10, 15 — station_access mirrors station (no separate access column)
# format_7: headers 11, 12, 13, 14 — station_access from Acceso_Estacion

def _base_columns(dfraw, station_access_col):
    return [
        F.to_timestamp(
            F.regexp_replace(F.col('Fecha_Transaccion'), ' UTC', ''),
            'yyyy-MM-dd HH:mm:ss'
        ).alias('transaction_timestamp'),
        F.col('Emisor').alias('emisor'),
        F.col('Operador').alias('operator'),
        F.col('Linea').alias('line'),
        F.col('Estacion_Parada').alias('station'),
        F.col(station_access_col).alias('station_access'),
        F.col('Dispositivo').cast('int').alias('machine'),
        F.col('Tipo_Tarjeta').alias('card_type'),
        F.col('Nombre_Perfil').alias('account_name'),
        F.col('Numero_Tarjeta').alias('cardnumber'),
        F.trim(F.col('Saldo_Previo_a_Transaccion')).cast('int').alias('balance_before'),
        F.trim(F.col('Valor')).cast('int').alias('value'),
        F.trim(F.col('Saldo_Despues_Transaccion')).cast('int').alias('balance_after'),
        F.col('Sistema').alias('system'),
    ]

TRANSFORMS = {
    'format_6': lambda dfraw: dfraw.select(_base_columns(dfraw, 'Estacion_Parada')),
    'format_7': lambda dfraw: dfraw.select(_base_columns(dfraw, 'Acceso_Estacion')),
}

# COMMAND ----------

# DBTITLE 1,Ingest — per header group
ingestion_log = []

for header_group in sorted(control_df['header'].unique()):
    group         = control_df[control_df['header'] == header_group].copy()
    transform_fmt = group['transform_format'].iloc[0]
    delimiter     = group['delimiter'].iloc[0]
    encoding      = group['encoding'].iloc[0]
    file_paths    = group['raw_filepath'].tolist()
    n_files       = len(file_paths)

    if transform_fmt not in TRANSFORMS:
        print(f"Skipping {header_group}: no transform defined for '{transform_fmt}'")
        ingestion_log.append({'header': header_group, 'files': n_files, 'rows': 0, 'status': 'skipped_no_transform'})
        continue

    print(f"\n[{header_group}] {n_files} files | transform={transform_fmt} | enc={encoding}")

    try:
        dfraw = (
            spark.read.format("csv")
            .option("header",   "true")
            .option("sep",      delimiter)
            .option("encoding", encoding)
            .load(file_paths)
        )

        df = (
            TRANSFORMS[transform_fmt](dfraw)
            .withColumn("_source_file",     F.input_file_name())
            .withColumn("_header_group",     F.lit(header_group))
            .withColumn("_transform_format", F.lit(transform_fmt))
            .withColumn("_ingestion_ts",     F.current_timestamp())
        )

        row_count = df.count()

        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable(BRONZE_TABLE)
        )

        # Mark files as ingested in the control table
        processed_df = spark.createDataFrame([(p,) for p in file_paths], ["raw_filepath"])
        processed_df.createOrReplaceTempView("_ingested_batch")
        spark.sql(f"""
            UPDATE {CONTROL_TABLE}
            SET ingested_at = current_timestamp()
            WHERE raw_filepath IN (SELECT raw_filepath FROM _ingested_batch)
        """)

        print(f"  → {row_count:,} rows written, ingested_at updated")
        ingestion_log.append({'header': header_group, 'files': n_files, 'rows': row_count, 'status': 'ok'})

    except Exception as e:
        print(f"  → ERROR: {e}")
        ingestion_log.append({'header': header_group, 'files': n_files, 'rows': 0, 'status': f'error: {e}'})

print("\n=== Ingestion complete ===")
display(pd.DataFrame(ingestion_log))

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
        F.min("transaction_timestamp").alias("earliest_ts"),
        F.max("transaction_timestamp").alias("latest_ts")
    )
    .orderBy("_header_group")
)
