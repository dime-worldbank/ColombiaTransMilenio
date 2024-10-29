# Databricks notebook source
# Pip install non-standard packages
!pip install tqdm

# Import modules
import os 
import pandas as pd 
import numpy as np
from tqdm import tqdm
from pyspark.sql import functions as F

# COMMAND ----------

pathdb  = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/'
path = '/dbfs/' + pathdb
user = os.listdir('/Workspace/Repos')[0]
git = '/Workspace/Repos/' +user+ '/ColombiaTransMilenio'

cleandir  =  '/Workspace/Clean/timing-old-cleaning/'
rawdir    =  '/Workspace/Raw/since2020/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create necessary directories

# COMMAND ----------

dbutils.fs.mkdirs(pathdb + cleandir)
dbutils.fs.mkdirs(pathdb + cleandir + "/monthly-files-card-level/")

# COMMAND ----------

# MAGIC %md
# MAGIC # (1) Parse columns and remove duplicates
# MAGIC Output: individual files by date and transaction type

# COMMAND ----------

# MAGIC %md
# MAGIC ## Columns to use

# COMMAND ----------

## SET COLUMN NAMES
# ZONAL (AND DUAL) COLUMNS
zonalcols  = ['Emisor', 'Estacion_Parada',
                 'Fecha_Clearing', 'Fecha_Transaccion', 'Linea', 
                 'Nombre_Perfil', 'Numero_Tarjeta', 'Ruta',
                'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion',
                 'Tipo_Tarjeta', 'Valor'] # current names
new_zonalcols = [ 'emisor', 'estacionparada',
                  'fechaclearing', 'fechatransaccion', 'linea',
                  'nombreperfil', 'nrotarjeta', 'ruta',
                  'saldodespuesdetransaccion', 'saldoprevioatransaccion',
                  'tipotarjeta','valor'] # homogeneous names
zonaltypes = ['category', 'category',
                'str', 'str', 'category', 
                'category', 'str', 'category',
               'float64', 'float64',
                'category', 'float64'] # coltypes
zonalcoltypes = dict(zip(zonalcols, zonaltypes))


# TRONCAL COLUMNS
troncalcols  = ['Acceso_Estacion', 'Emisor', 'Estacion_Parada',
                      'Fecha_Clearing', 'Fecha_Transaccion', 'Linea', 
                      'Nombre_Perfil', 'Numero_Tarjeta', 'Ruta',
                     'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion',
                      'Tipo_Tarjeta', 'Valor'] # current names
new_troncalcols = ['accesoestacion', 'emisor', 'estacionparada',
                  'fechaclearing', 'fechatransaccion', 'linea', 
                  'nombreperfil', 'nrotarjeta', 'ruta',
                  'saldodespuesdetransaccion', 'saldoprevioatransaccion',
                  'tipotarjeta','valor'] #homogeneous names
troncaltypes = ['category', 'category', 'category',
                      'str', 'str', 'category', 
                      'category', 'str', 'category',
                     'float64', 'float64',
                      'category', 'float64'] # coltypes
troncalcoltypes = dict(zip(troncalcols, troncaltypes))

# COMMAND ----------

# MAGIC %md
# MAGIC ## List files

# COMMAND ----------

# Create a list of non-duplicated files 
# All files should be named like ValidacionDual20230301.csv

# check if each file has the valid type in its name   
for valid_type in ['Dual', 'Troncal', 'Zonal' ]:

    valid_type_files = os.listdir(f'{path}/{rawdir}/Validacion{valid_type}/')
    print(set([valid_type in f for f in valid_type_files]))

# they do, so we can save a unique list of files
raw_files = []
for valid_type in ['Dual', 'Troncal', 'Zonal' ]:
    valid_type_files = os.listdir(f'{path}/{rawdir}/Validacion{valid_type}/')
    raw_files += [ f'/Validacion{valid_type}/{f}' for f in valid_type_files]

# check whether the date is in the right position
wrong_date_position = [f for f in raw_files if f[-12:-9] != '202']
print(wrong_date_position)

# check that these are duplicated and we can skip them
print([f for f in raw_files if 'validacionZonal20220628' in f])
print([f for f in raw_files if 'validacionDual20230301'  in f])

# they are, so let's remove them
raw_files = [f for f in raw_files if f not in wrong_date_position]


# COMMAND ----------

cleaned_files = os.listdir(path + cleandir)
print(len(raw_files), len(cleaned_files))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read, remove duplicates, transform columns, save

# COMMAND ----------

for f in tqdm(raw_files):

    if ('Zonal' in f) :
        valid_type = 'Zonal'
        cols       = zonalcols
        newcols    = new_zonalcols
        coltypes   = zonalcoltypes

    if ('Troncal' in f) :
        valid_type = 'Troncal'
        cols       = troncalcols
        newcols    = new_troncalcols
        coltypes   = troncalcoltypes

    if ('Dual' in f) :
        valid_type = 'Dual'
        cols       = zonalcols
        newcols    = new_zonalcols
        coltypes   = zonalcoltypes
    
    clean_name = valid_type + f[-12:-4] +"_clean.csv"

    if clean_name not in cleaned_files:

        try:
            # read
            df   = pd.read_csv( path + rawdir + f, usecols = cols, dtype  = coltypes)

            # remove duplicates
            df['fecha']   = df.Fecha_Transaccion.str[:19]
            df.drop_duplicates(subset = ['Numero_Tarjeta','fecha'], inplace = True)

            # add typevalid and rename the other columns
            df['typevalid'] = valid_type
            df.rename(columns=dict(zip(cols, newcols)), inplace=True)

            # save clean data to folder
            df.to_csv( path + cleandir + clean_name, index = False)

        except Exception as e:
                print(f"Error while processing {f}: {e}")



# COMMAND ----------

# MAGIC %md
# MAGIC No columns to parse from files:
# MAGIC - /ValidacionDual/validacionDual20230630.csv
# MAGIC - /ValidacionZonal/validacionZonal20200601.csv
# MAGIC - /ValidacionTroncal/validacionTroncal20200725.csv
# MAGIC - /ValidacionZonal/validacionZonal20220628.cs

# COMMAND ----------

# MAGIC %md
# MAGIC # (2) Monthly datasets by card
# MAGIC To remove infrequent users and super swipers, and decide on the IDs to sample

# COMMAND ----------

price_subsidy_18 = [1575, 1725] # hasta 2021
price_subsidy_22 = [1650, 1800] # ambos
price_subsidy_23 = [2250, 2500] # desde 2022
sisben_subsidy_profiles = ["(006) Apoyo Ciudadano", "(009) Apoyo Ciudadano Reexpedición"]

# COMMAND ----------

cleaned_files = os.listdir(path + cleandir)
ymonths = [x [-18:-12] for x in cleaned_files]
ymonths = np.unique(ymonths)


# COMMAND ----------


# Loop for each month
for ym in tqdm(ymonths):

   # Relevant subsidy value depends on the month
    if ym[:4] in ['2020', '2021']:
        price_subsidy = price_subsidy_18 + price_subsidy_22 
    if ym[:4] in ['2022', '2023', '2024']:
        price_subsidy = price_subsidy_23 + price_subsidy_22 

    # Define the path and file pattern
    pattern = f"*{ym}*.csv"  # This pattern will match files containing '2023' in their name with '.csv' extension

    # Load the matching files into a Spark DataFrame
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("charset", "utf-8") \
        .load(os.path.join(pathdb + cleandir , pattern))

    # Select relevant columns
    df = df.select(df['fechaclearing'].alias('clearing_timestamp'),
               df['nombreperfil'].alias('account_name'),
               df['nrotarjeta'].alias('cardnumber'),
               F.trim(df['valor']).cast('int').alias('value'))

    # construct variables
    df = df.withColumn("sisbensubsidy", # transaction with Sisbén subsidy
              F.when( (df.value.isin(price_subsidy)) & (df.account_name.isin(sisben_subsidy_profiles)), 1).otherwise(0))
    df = df.withColumn("transfer", # transfer transaction
              F.when( df.value < 500 , 1).otherwise(0))
    df = df.withColumn("valid",  F.lit(1)) # for counting transactions

    # group by card number
    monthdf = df.groupBy("cardnumber").agg(
        F.countDistinct("clearing_timestamp").alias("num_days"),     # number of days
        F.countDistinct("account_name").alias("num_unique_profiles"),    # unique profiles
        F.sum("valid").alias("total_valid"),                       # total valid in the month
        F.sum("transfer").alias("total_transfer"),                 # total transfers in the month
        F.sum("sisbensubsidy").alias("total_sisbensubsidy"))        # total transactions with sisben subsidy in the month
    
    # tag DAILY super swipers here
    usage_count_day = df.groupby("cardnumber", "clearing_timestamp").count()
    daily_outlier_accounts = usage_count_day.where(usage_count_day['count'] > 100).select('cardnumber').distinct()
    daily_outlier_accounts = daily_outlier_accounts.withColumnRenamed("cardnumber", "cardnumber_out1")

    daily_outlier_accounts_repeated = usage_count_day.where(usage_count_day['count'] > 20)\
            .groupby('cardnumber').count().filter(F.col('count') > 2).select('cardnumber').distinct()
    daily_outlier_accounts_repeated = daily_outlier_accounts_repeated.withColumnRenamed("cardnumber", "cardnumber_out2")

    monthdf = monthdf.join(daily_outlier_accounts, 
        monthdf['cardnumber'] == daily_outlier_accounts['cardnumber_out1'], 
        how='left')

    monthdf = monthdf.join(daily_outlier_accounts_repeated, 
        monthdf['cardnumber'] == daily_outlier_accounts_repeated['cardnumber_out2'], 
        how='left')

    monthdf = monthdf.withColumn("is_outlier_account_1", 
        F.when(monthdf['cardnumber_out1'].isNotNull(), 1).otherwise(0))
    monthdf = monthdf.drop("cardnumber_out1")

    monthdf = monthdf.withColumn("is_outlier_account_2", 
        F.when(monthdf['cardnumber_out2'].isNotNull(), 1).otherwise(0))
    monthdf = monthdf.drop("cardnumber_out2")

    # save
    monthdf.toPandas().to_csv(path + cleandir + "/monthly-files-card-level/bycard" + ym + ".csv", index = False)




# COMMAND ----------

os.listdir(path + cleandir + "/monthly-files-card-level/")

# COMMAND ----------

"bycard202410.csv"[6:12]

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # (aux) Checking card types

# COMMAND ----------

os.listdir(path + cleandir)

# COMMAND ----------

df202310 = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("charset", "utf-8") \
    .load(os.path.join(pathdb + cleandir , "*202310*"))  

# COMMAND ----------

# MAGIC %md
# MAGIC # NOTE PROBLEM WHEN IMPORTING DATA

# COMMAND ----------

# Perform value_counts on a specific column (e.g., 'column_name')
perfil_counts=df202310.select("linea")
# Show the result
perfil_counts.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # (3) Dataset for the whole period, aggregating monthly info

# COMMAND ----------

# Load the matching files into a Spark DataFrame, parse format
# Each file is at the card level, by month
df  = spark.read.format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("charset", "utf-8") \
        .load(os.path.join(pathdb + cleandir + "/monthly-files-card-level/" , "*")) \
    .withColumn("source_file", F.input_file_name())\
    .withColumn("month", F.expr("substring(input_file_name(), 123, 6)").cast("int"))\
    .drop("source_file") \
    .withColumn("year", F.expr("substring(cast(month as string), 1, 4)").cast("smallint")) \
    .withColumn("num_days", F.col("num_days").cast("smallint")) \
    .withColumn("num_unique_profiles", F.col("num_unique_profiles").cast("smallint")) 
df  = df.withColumn("one_profile",
                F.when( df["num_unique_profiles"] == 1 , 1).otherwise(0)) \
    .withColumn("two_profiles",
                F.when( df["num_unique_profiles"] == 2 , 1).otherwise(0))\
    .withColumn("total_valid", F.col("total_valid").cast("smallint")) \
    .withColumn("total_transfer", F.col("total_transfer").cast("smallint")) \
    .withColumn("total_sisbensubsidy", F.col("total_sisbensubsidy").cast("smallint")) \
    .withColumn("is_outlier_account_1", F.col("is_outlier_account_1").cast("smallint")) \
    .withColumn("is_outlier_account_2", F.col("is_outlier_account_2").cast("smallint"))  

# COMMAND ----------

df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Infrequent users and super swipers

# COMMAND ----------

distinct_cardnumber_count = df.select("cardnumber").distinct().count()
print(distinct_cardnumber_count)

# COMMAND ----------

# by card and year: filter to relevant months
bycardyear = df.filter((df.month  >= 202201) & (df.month <= 202403)) \
        .groupBy(["cardnumber", "year"]).agg(
                        F.sum("num_days").alias("num_days_year"),
                        F.sum("total_valid").alias("total_valid_year"),
                        F.max("is_outlier_account_1").alias("is_outlier_account_1"),
                        F.max("is_outlier_account_2").alias("is_outlier_account_2"))
bycardyear = bycardyear.withColumn("is_infrequent_account_1",
                    F.when( bycardyear["total_valid_year"] < 12 , 1).otherwise(0)) \
                        .withColumn("is_infrequent_account_2",
                    F.when( bycardyear["num_days_year"] < 12 , 1).otherwise(0)) 
accounts_to_remove = bycardyear.groupBy("cardnumber").agg(
        F.max("is_outlier_account_1").alias("is_outlier_account_1"),
        F.max("is_outlier_account_2").alias("is_outlier_account_2"),
        F.max("is_infrequent_account_1").alias("is_infrequent_account_1"),
        F.max("is_infrequent_account_2").alias("is_infrequent_account_2"))

# COMMAND ----------

distinct_cardnumber_count_filtered = accounts_to_remove.count()
distinct_cardnumber_count_filtered

# COMMAND ----------

# Sum all rows for each column in the DataFrame
accounts_to_remove_sums = accounts_to_remove.agg(*[F.sum(col).alias(col) for col in accounts_to_remove.columns])

# Show the result
accounts_to_remove_sums.show()

# COMMAND ----------


