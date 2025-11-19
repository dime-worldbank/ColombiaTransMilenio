# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

import os 
import pandas as pd 
import numpy as np

# Directories
V_DIR   = '/Volumes/prd_csc_mega/sColom15/vColom15/'

documents        = V_DIR + "/Documents/Recharges2017-2019"
decompressed_dir = V_DIR + "/Workspace/Raw/Recharges/decompressed"
construct_dir    = V_DIR + "/Workspace/Construct"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data to delta table 

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Set default catalog and schema */
# MAGIC
# MAGIC USE CATALOG prd_mega;
# MAGIC USE SCHEMA scolom15;
# MAGIC
# MAGIC SELECT
# MAGIC   current_catalog() as current_catalog,
# MAGIC   current_schema()  as current_schema;

# COMMAND ----------



# COMMAND ----------

rename_dict = {
    "Fecha de Clearing": "fecha_clearing",
    "Fecha de Transaccion": "fecha_transaccion",
    "Tipo Agente": "tipo_agente",
    "Número de Agencia": "numero_agencia",
    "Nombre Agente": "nombre_agente",
    "Nombre Estacion": "nombre_estacion",
    "Acceso Estacion": "acceso_estacion",
    "ID de Agente": "id_agente",
    "ID de POS": "id_pos",
    "ID SAM": "id_sam",
    "Num Trx SAM": "num_trx_sam",
    "Numero Tarjeta": "numero_tarjeta",
    "Num Trx Tarjeta": "num_trx_tarjeta",
    "Tipo Tarjeta": "tipo_tarjeta",
    "Nombre Perfil": "nombre_perfil",
    "Tipo Pago": "tipo_pago",
    "Tipo Transaccion": "tipo_transaccion",
    "Tipo Recarga": "tipo_recarga",
    "Saldo Tarjeta Antes Recarga": "saldo_tarjeta_antes_recarga",
    "Valor de Recarga": "valor_de_recarga",
    "Saldo Tarjeta Despues Recarga": "saldo_tarjeta_despues_recarga",
    "Saldo LSAM Antes Recarga": "saldo_lsam_antes_recarga",
    "Saldo LSAM Despues Recarga": "saldo_lsam_despues_recarga",
    "Saldo Negativo": "saldo_negativo",
    "TERC_ID": "terc_id",
    "Sucursal Representante": "sucursal_representante",
    "Codigo_Representante": "codigo_representante",
    "year": "year"
}

# COMMAND ----------

path_2017 = f"{decompressed_dir}/2017"

df_2017 = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .option("delimiter", ";")
         .csv(path_2017)
         .withColumn("year", F.lit(2017))
)


# COMMAND ----------

df_2017_renamed = df_2017
for old, new in rename_dict.items():
    df_2017_renamed = df_2017_renamed.withColumnRenamed(old, new)
df_2017_renamed.printSchema()

# COMMAND ----------

df_2017_renamed.write.format("delta").mode("overwrite").saveAsTable("recargas_2017to2019_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample

# COMMAND ----------

cards = pd.read_stata(documents + "/10pct_apoyo_cardnumbers.dta")
cards = cards.cardnumber.tolist()
assert len(cards) == len(set(cards))
# to spark
cards_df = spark.createDataFrame([(int(x),) for x in cards], ["numero_tarjeta"])


# COMMAND ----------

df = spark.table("recargas_2017to2019_raw")


# COMMAND ----------

df_filt = df.join(F.broadcast(cards_df), on="numero_tarjeta", how="inner")

# COMMAND ----------

df_filt.count()

# COMMAND ----------

df_filt_pd = df_filt.toPandas()

# COMMAND ----------

df_filt_pd.to_csv(construct_dir + "/recharges2017_sample_apoyo10pct.csv", index=False)
