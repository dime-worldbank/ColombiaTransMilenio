# Databricks notebook source
# MAGIC %md
# MAGIC # Sample data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up

# COMMAND ----------

# Pip install non-standard packages
!pip install rarfile
!pip install findspark
!pip install pyspark
!pip install plotly
!pip install pyspark_dist_explore
!pip install geopandas
!pip install seaborn
!pip install folium
!pip install editdistance
!pip install scikit-mobility
!pip install chart_studio
!pip install tqdm
!pip install pyunpack
!pip install patool

import shutil
import sys
import os


# COMMAND ----------

# Directories
pathdb  = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/'
path = '/dbfs/' + pathdb
user = os.listdir('/Workspace/Repos')[0]
git = '/Workspace/Repos/' +user+ '/ColombiaTransMilenio/'
git2 = '/Workspace/Repos/' +user+ '/Colombia-BRT_IE-temp/'
## Important sub-directories for this notebook
byheader_dir = path + '/Workspace/Raw/byheader_dir/'

# COMMAND ----------

# MAGIC
%run ./utils/import_test.py
%run ./utils/packages.py
%run ./utils/setup.py
%run ./utils/utilities.py


## Note that these won't work if there are errors in the code. Make sure first that everything is OK!

# Functions
import_test_function("Running hola.py works fine :)")
import_test_packages("Running packages.py works fine :)")
import_test_setup("Running setup.py works fine :)")
import_test_utilities("Running utilities.py works fine :)")

# Classes and others
objects_in_dir = dir()
print('ImportTestClass'    in objects_in_dir, 
      'spark_df_handler'   in objects_in_dir,
      'user_window'        in objects_in_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Things that did not import

# COMMAND ----------

# setup
## Just a test to see if importing from the main notebook works
def import_test_setup(x):
    print(x)

## Initiate Spark
## Class to handle spark and df in session


## Set up spark
# Check which computer this is running on
if multiprocessing.cpu_count() == 6:
    on_server = False
else:
    on_server = True

# start spark session according to computer
if on_server:
    spark = SparkSession \
        .builder \
        .master("local[75]") \
        .config("spark.driver.memory", "200g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config('spark.local.dir', '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Workspace/') \
        .config("spark.sql.execution.arrow.enabled", "true")\
        .getOrCreate()
else:
    spark = SparkSession.builder.master("local[*]") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.execution.arrow.enabled", "true")\
    .getOrCreate()

# Set paths
if on_server:
    print("OK: ON SERVER")
    path = '/dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData'
    user = os.listdir('/Workspace/Repos')[0]
    git = f'/Workspace/Repos/{user}/Colombia-BRT-IE-temp'
else:
    print("Not on server - no path defined")

## Class to handle spark and df in session

class spark_df_handler:

    """Class to collect spark connection and catch the df in memory.

    Attributes
    ----------
    spark : an initialised spark connection
    df : a spark dataframe that holds the raw data
    on_server : whether

    Methods
    -------
    load(path, pickle = True)
        Loads a pickle or csv

    generate_variables()
        generates additional variables after raw import

    transform()

    memorize(df)
        Catch the df in memory
    """

    def __init__(self,
                spark = spark,
                on_server = on_server):
        self.spark = spark
        self.on_server = on_server


    def load(self, 
             path =  path, 
             type = 'parquet', 
             file = 'parquet_df', 
             delimiter = ';', 
             encoding = "utf-8"):
        
        if type =='parquet':
            self.df = spark.read.format("parquet").load(os.path.join(path, file)) # changed
            

        elif type == 'pickle':
            name = os.path.join(path, file)
            pickleRdd = self.spark.sparkContext.pickleFile(name = name).collect()
            self.df = self.spark.createDataFrame(pickleRdd)


                
        elif type =='new_data':
            self.dfraw = self.spark.read.format("csv").option("header", "true")\
                                        .option("delimiter", delimiter)\
                                        .option("charset", encoding)\
                                            .load(os.path.join(path,"*")) # reads all files in the path

        else:
            self.dfraw = self.spark.read.format("csv").option("header", "true")\
                                        .option("delimiter", ";")\
                                        .option("charset", "utf-8")\
                                        .load(path) # changed -- reads all files in the path

        

            #self.clean()
            #self.gen_vars()
            
    def transform(self, header_format):
        if header_format == 'format_one':
            self.df = self.dfraw.select(F.to_timestamp(self.dfraw['Fecha de Transaccion'],'yyyyMMddHHmmss')\
                            .alias('transaction_timestamp'),
                            self.dfraw['Emisor'].alias('emisor'),
                            self.dfraw['Operador'].alias('operator'),
                            self.dfraw['Linea'].alias('line'),
                            self.dfraw['Estacion'].alias('station'),
                            self.dfraw['Acceso de Estación'].alias('station_access'),
                            self.dfraw['Dispositivo'].cast('int').alias('machine'),
                            self.dfraw['Tipo de Tarjeta'].alias('card_type'),
                            self.dfraw['Nombre de Perfil'].alias('account_name'),
                            self.dfraw['Numero de Tarjeta'].cast('long').alias('cardnumber'),
                            F.trim(self.dfraw['Saldo Previo a Transaccion']).cast('int')\
                            .alias('balance_before'),
                            F.trim(self.dfraw['Valor']).cast('int').alias('value'),
                            F.trim(self.dfraw['Saldo Despues de Transaccion']).cast('int')\
                            .alias('balance_after'))

        elif header_format == 'format_two':
            self.df = self.dfraw.select(F.to_timestamp(self.dfraw['Fecha de Uso'],'dd-MM-yyyy HH:mm:ss')\
                            .alias('transaction_timestamp'),
                            self.dfraw['Emisor'].alias('emisor'),
                            self.dfraw['Operador'].alias('operator'),
                            self.dfraw['Línea'].alias('line'),
                            self.dfraw['Estación'].alias('station'),
                            self.dfraw['Acceso de Estación'].alias('station_access'),
                            self.dfraw['Dispositivo'].cast('int').alias('machine'),
                            self.dfraw['Tipo de Tarjeta'].alias('card_type'),
                            self.dfraw['Nombre de Perfil'].alias('account_name'),
                            self.dfraw['Número de Tarjeta'].cast('long').alias('cardnumber'),
                            F.trim(self.dfraw['Saldo Previo a Transacción']).cast('int')\
                            .alias('balance_before'),
                            F.trim(self.dfraw['Valor']).cast('int').alias('value'),
                            F.trim(self.dfraw['Saldo Después de Transacción']).cast('int')\
                            .alias('balance_after'))
            
        elif header_format == 'format_three':
            self.df = self.dfraw.select(F.to_timestamp(self.dfraw['Fecha de Transaccion'],'yyyy/MM/dd HH:mm:ss')\
                            .alias('transaction_timestamp'),
                            self.dfraw['Emisor'].alias('emisor'),
                            self.dfraw['Operador'].alias('operator'),
                            self.dfraw['Linea'].alias('line'),
                            self.dfraw['Parada'].alias('station'),
                            self.dfraw['Parada'].alias('station_access'),
                            self.dfraw['Dispositivo'].cast('int').alias('machine'),
                            self.dfraw['Tipo Tarjeta'].alias('card_type'),
                            self.dfraw['Nombre de Perfil'].alias('account_name'),
                            F.trim(self.dfraw['Numero Tarjeta']).cast('long').alias('cardnumber'),
                            F.trim(self.dfraw['Saldo Previo a Transaccion']).cast('int')\
                            .alias('balance_before'),
                            F.trim(self.dfraw['Valor']).cast('int').alias('value'),
                            F.trim(self.dfraw['Saldo Despues de Transaccion']).cast('int')\
                            .alias('balance_after'))
            
        elif header_format == 'format_four':
            self.df = self.dfraw.select(F.to_timestamp(self.dfraw['Fecha de Transaccion'],'yyyyMMddHHmmss')\
                            .alias('transaction_timestamp'),
                            self.dfraw['Emisor'].alias('emisor'),
                            self.dfraw['Operador'].alias('operator'),
                            self.dfraw['Linea'].alias('line'),
                            self.dfraw['Parada'].alias('station'),
                            self.dfraw['Parada'].alias('station_access'),
                            self.dfraw['Dispositivo'].cast('int').alias('machine'),
                            self.dfraw['Tipo Tarjeta'].alias('card_type'),
                            self.dfraw['Nombre de Perfil'].alias('account_name'),
                            F.trim(self.dfraw['Numero Tarjeta']).cast('long').alias('cardnumber'),
                            F.trim(self.dfraw['Saldo Previo a Transaccion']).cast('int')\
                            .alias('balance_before'),
                            F.trim(self.dfraw['Valor']).cast('int').alias('value'),
                            F.trim(self.dfraw['Saldo Despues de Transaccion']).cast('int')\
                            .alias('balance_after'))
            
        elif header_format == 'format_five':
            self.df = self.dfraw.select(F.to_timestamp(self.dfraw['Fecha de Uso'],'dd-MM-yyyy HH:mm:ss')\
                            .alias('transaction_timestamp'),
                            self.dfraw['Emisor'].alias('emisor'),
                            self.dfraw['Operador'].alias('operator'),
                            self.dfraw['Línea'].alias('line'),
                            self.dfraw['Parada'].alias('station'),
                            self.dfraw['Parada'].alias('station_access'),
                            self.dfraw['Dispositivo'].cast('int').alias('machine'),
                            self.dfraw['Tipo de Tarjeta'].alias('card_type'),
                            self.dfraw['Nombre de Perfil'].alias('account_name'),
                            self.dfraw['Número de Tarjeta'].cast('long').alias('cardnumber'),
                            F.trim(self.dfraw['Saldo Previo a Transacción']).cast('int')\
                            .alias('balance_before'),
                            F.trim(self.dfraw['Valor']).cast('int').alias('value'),
                            F.trim(self.dfraw['Saldo Después de Transacción']).cast('int')\
                            .alias('balance_after'))
            
            # new formats Wendy  


            ## this goes with header_08, header_09, header_10, header_15
        elif header_format == 'format_6':
            self.df = self.dfraw.select( 
                F.to_timestamp( F.regexp_replace(self.dfraw['Fecha_Transaccion'], ' UTC', ''),
                                'yyyy-MM-dd HH:mm:ss').alias('transaction_timestamp'),
            self.dfraw['Emisor'].alias('emisor'),
            self.dfraw['Operador'].alias('operator'),
            self.dfraw['Linea'].alias('line'),
            self.dfraw['Estacion_Parada'].alias('station'),
            self.dfraw['Estacion_Parada'].alias('station_access'),
            self.dfraw['Dispositivo'].cast('int').alias('machine'),
            self.dfraw['Tipo_Tarjeta'].alias('card_type'),
            self.dfraw['Nombre_Perfil'].alias('account_name'),
            self.dfraw['Numero_Tarjeta'].alias('cardnumber'),
            F.trim(self.dfraw['Saldo_Previo_a_Transaccion']).cast('int')\
                .alias('balance_before'),
            F.trim(self.dfraw['Valor']).cast('int').alias('value'),
            F.trim(self.dfraw['Saldo_Despues_Transaccion']).cast('int')\
                .alias('balance_after'),
            self.dfraw['Sistema'].alias('system'))
            
            ## this goes with header_11, header_12, header_13, header_14
        elif header_format == 'format_7':
                self.df = self.dfraw.select( 
                    F.to_timestamp( F.regexp_replace(self.dfraw['Fecha_Transaccion'], ' UTC', ''),
                    'yyyy-MM-dd HH:mm:ss').alias('transaction_timestamp'),
                self.dfraw['Emisor'].alias('emisor'),
                self.dfraw['Operador'].alias('operator'),
                self.dfraw['Linea'].alias('line'),
                self.dfraw['Estacion_Parada'].alias('station'),
                self.dfraw['Acceso_Estacion'].alias('station_access'),
                self.dfraw['Dispositivo'].cast('int').alias('machine'),
                self.dfraw['Tipo_Tarjeta'].alias('card_type'),
                self.dfraw['Nombre_Perfil'].alias('account_name'),
                self.dfraw['Numero_Tarjeta'].alias('cardnumber'),
                F.trim(self.dfraw['Saldo_Previo_a_Transaccion']).cast('int')\
                    .alias('balance_before'),
                F.trim(self.dfraw['Valor']).cast('int').alias('value'),
                F.trim(self.dfraw['Saldo_Despues_Transaccion']).cast('int')\
                    .alias('balance_after'),
                self.dfraw['Sistema'].alias('system'))

    def memorize(self):
        # Register as table to run SQL queries
        self.df.createOrReplaceTempView("df_table")
        self.spark.sql('CACHE TABLE df_table').collect()

        return self.df
    



# COMMAND ----------

# MAGIC %md
# MAGIC ##  1. Identify apoyo or subsidy users
# MAGIC _This section can be run independently._

# COMMAND ----------

# load data

## regular users
sh = spark_df_handler()
sh.load(type = 'parquet', 
        path = pathdb + '/Workspace/bogota-hdfs/intermediate/', 
        file = 'df_regular-users-2022-2024')

df_regular_users = sh.df
df_regular_users.cache()

## type of profiles
account_name_dict = pd.read_csv(path + '/Workspace/variable_dicts/account_name_dict_dict.csv')
account_name_dict[["account_name", "account_name_id"]]

# COMMAND ----------

## Info to tag people paying subsidy values
price_subsidy_18 = [1575, 1725]
price_subsidy_22 = [1650, 1800] # same since Feb 2019
price_subsidy_23 = [2250, 2500] # same for 2024, though since Feb tariff unified to 2500
timestamp_threshold = dt.datetime(2022, 1, 31, tzinfo= timezone)  # Adjust timezone as needed

# COMMAND ----------

# Analize user's profiles
df_regular_users = df_regular_users.withColumn("profile-apoyo",   
                           F.when( F.col('account_name_id').isin([3, 4]), 1).otherwise(0)) \
                .withColumn("profile-adulto",   
                           F.when( F.col('account_name_id').isin([0, 5, 7, 8]), 1).otherwise(0)) \
                .withColumn("profile-anonymous",   
                           F.when( F.col('account_name_id') == 1, 1).otherwise(0)) \
                .withColumn("sisben-subsidy-value",
                            F.when(    ( (F.col('day') <= timestamp_threshold) & (F.col('value').isin(price_subsidy_18 + price_subsidy_22) ) ) |
                                       ( (F.col('day') > timestamp_threshold)  & (F.col('value').isin(price_subsidy_22 + price_subsidy_23) ) ) \
                                   , 1).otherwise(0))         


# COMMAND ----------

card_types = df_regular_users.groupby("cardnumber").agg(
    F.max("profile-apoyo").alias("profile-apoyo-any"),
    F.max("profile-adulto").alias("profile-adulto-any"),
    F.max("profile-anonymous").alias("profile-anonymous-any"),
    F.max("sisben-subsidy-value").alias("sisben-subsidy-value-any"),
    F.mean("profile-apoyo").alias("profile-apoyo-mean"),
    F.mean("profile-adulto").alias("profile-adulto-mean"),
    F.mean("profile-anonymous").alias("profile-anonymous-mean"),
    F.mean("sisben-subsidy-value").alias("sisben-subsidy-value-mean")
)
card_types = card_types.toPandas()
card_types.shape

# COMMAND ----------

# combined categories
card_types['apoyo-sisben-subsidy-value-any'] = card_types['sisben-subsidy-value-any'] * card_types['profile-apoyo-any']
card_types['profile-apoyo-adulto-any'] = card_types['profile-adulto-any'] * card_types['profile-apoyo-any']
card_types['profile-apoyo-anon-any'] = card_types['profile-anonymous-any'] * card_types['profile-apoyo-any']
card_types['profile-adulto-anon-any'] = card_types['profile-adulto-any'] * card_types['profile-anonymous-any']

print(card_types['profile-apoyo-any'].mean() * 100)
print(card_types['profile-adulto-any'].mean() * 100)
print(card_types['profile-anonymous-any'].mean() * 100)

# COMMAND ----------

pd.options.display.float_format = '{:.1f}'.format 
print(card_types.loc[ list(card_types['profile-apoyo-any'] == 1),
                      'profile-apoyo-anon-any'].mean(axis=0)  * 100)
print(card_types.loc[ list(card_types['profile-adulto-any'] == 1),
                      'profile-adulto-anon-any'].mean(axis=0)  * 100)
print(card_types['profile-apoyo-adulto-any'].sum())

# COMMAND ----------

# MAGIC %md
# MAGIC The ones that are both apoyo and anonymous should be considered apoyo; the ones that are both adulto and anonymous should be considered adulto.

# COMMAND ----------

card_types['profile_final'] = ''
card_types.loc[card_types['profile-anonymous-any']          == 1, 'profile_final'] = 'anonymous'
card_types.loc[card_types['profile-adulto-any']             == 1, 'profile_final'] = 'adulto'   # overrides anon
card_types.loc[card_types['apoyo-sisben-subsidy-value-any'] == 1, 'profile_final'] = 'apoyo_subsidyvalue' # overrides anon
card_types.loc[card_types['profile-apoyo-adulto-any'] == 1, 'profile_final'] = '' # do not consider those that are apoyo AND adulto (just 4 cards) 

card_types['profile_final']  = card_types['profile_final'].astype("category")
card_types.profile_final.value_counts()

# COMMAND ----------

card_types.to_csv(os.path.join(path, 'Workspace/bogota-hdfs/intermediate/card_types.csv'), index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC These are the stats on profiles, among 10,183,766 regular users in 2022 - july 2024:
# MAGIC - 7 % of cards are apoyo at some point
# MAGIC   - 6% are apoyo AND pay Sisbén subsidy values at some point
# MAGIC - 41% are Adulto cards (personalized) at some point
# MAGIC - 55% are Anonymous at some point
# MAGIC
# MAGIC As we know, anonymous cards overlap with Adulto or Apoyo. Specifically:
# MAGIC - Among apoyo, 7% were anonymous at some point
# MAGIC - Among adulto, 25% were anonymous at some point
# MAGIC - For some reason there are 4 (four units, not percent) cards were both apoyo and adulto, which shouldn't happen because they need to change the card
# MAGIC
# MAGIC The ones that are both apoyo and anonymous should be considered apoyo; the ones that are both adulto and anonymous should be considered adulto; the ones that are apoyo and adulto should be excluded. Then, we have:
# MAGIC - always anonymous cards:  4,538,986
# MAGIC - anytime adulto cards:    4,172,023
# MAGIC - any tipe apoyo and paying subsidy value cards: 617,906
# MAGIC - other cards:   854,851
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sampling
# MAGIC _This section can be run independently_

# COMMAND ----------

card_types = pd.read_csv(os.path.join(path, 'Workspace/bogota-hdfs/intermediate/card_types.csv'))
card_types.columns

# COMMAND ----------

card_types.shape

# COMMAND ----------

# Random sample for each type of card
unique_ids       = list(card_types.cardnumber[card_types[f'profile_final']    == 'apoyo_subsidyvalue'] )
sample_size_full = int(len(unique_ids) / 100) # 1% sample
seed(9)
sample_ids       = sample(unique_ids, sample_size_full)
card_types['insample'] = card_types.cardnumber.isin(sample_ids) * 1



for profile in ['adulto', 'anonymous']:
    unique_ids       = list(card_types.cardnumber[card_types[f'profile_final']    == profile] )
    sample_size_full = int(len(unique_ids) / 1000) # 0.1% sample
 
    seed(9)
    sample_ids = sample(unique_ids, sample_size_full)
    card_types.loc[card_types.cardnumber.isin(sample_ids),
                   'insample'] = 1



# COMMAND ----------

card_types.groupby('profile_final')['insample'].sum()

# COMMAND ----------

card_types[["cardnumber","profile_final", "insample"]].to_csv(os.path.join(path, 'Workspace/bogota-hdfs/intermediate/card_types_sampleids.csv'), index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Getting sample transactions
# MAGIC Creating 2 datasets:
# MAGIC - `clean_relevant`
# MAGIC - `clean_relevant_sample_1`
# MAGIC
# MAGIC _This section can be run independently_

# COMMAND ----------

# import dataset by card, with final profile and whether it is in the sample
cards = pd.read_csv(os.path.join(path, 'Workspace/bogota-hdfs/intermediate/card_types_sampleids.csv'))
cards = spark.createDataFrame(cards)

# COMMAND ----------

# import full clean data (I.E. NO DUPLICATES, BUT THERE WILL BE IRREGULAR USERS; REGULAR USERS ARE JUST DEFINED FOR THE RELEVANT PERIOD FROM WHICH WE SAMPLE PEOPLE)
sh = spark_df_handler()
sh.load(type = 'parquet', path = pathdb + '/Workspace/bogota-hdfs/', file = 'parquet_df_clean_2020-2024_temp')

df_clean = sh.df
df_clean.cache()


# COMMAND ----------

# Merge transaction with card type and whether in sample
df_clean = df_clean.join(cards , on="cardnumber", how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC Dataset `df_clean_relevant` keeps just transactions for relevant cards:
# MAGIC - were present in the relevant period: January 2022 - July 2024
# MAGIC - are regular users (i.e., excluding super swipers and infrequent users)
# MAGIC - have a relevant profile (adulto, anonymous, or apoyo and paid a subsidy value at some point in the relevant period)
# MAGIC
# MAGIC Dataset `df_clean_relevant_sample` takes just the observations selected for the sample.

# COMMAND ----------

# 2 hs to save!

# Keep just transactions for relevant cards and save:
df_clean = df_clean.filter(df_clean.profile_final.isNotNull()) 
df_clean.write.mode('overwrite').parquet(os.path.join(pathdb, 
                                                      'Workspace/bogota-hdfs/df_clean_relevant'))
print(df_clean.count())

# COMMAND ----------

print(df_clean_sample.count())
df_clean_sample = df_clean.filter(df_clean.insample == 1)
df_clean_sample.write.mode('overwrite').parquet(os.path.join(pathdb, 
                                                      'Workspace/bogota-hdfs/df_clean_relevant_sample'))

# COMMAND ----------

df_clean_sample_pd = df_clean_sample.toPandas()
df_clean_sample_pd.to_csv(os.path.join(path,
                                       'Workspace/Construct/df_clean_relevant_sample.csv'), index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Plots to compare sample and full data

# COMMAND ----------

df = spark.read.format("parquet").load(pathdb + '/Workspace/bogota-hdfs/df_clean_relevant')
df.cache()

# Aggregate by day and profile
daily_byprofile = df.groupBy("day", "profile_final").agg(
    F.countDistinct("cardnumber").alias("unique_cards"), 
    F.count("*").alias("total_transactions")     
    )  
daily_byprofile = daily_byprofile.withColumn(
    "transactions_by_card", 
    F.col("total_transactions") / F.col("unique_cards")
    )


# COMMAND ----------

daily_byprofile_pd = daily_byprofile.toPandas()

# COMMAND ----------

daily_byprofile_pd.to_csv(os.path.join(path, 'Workspace/Construct/daily_byprofile.csv'), index = False)

# COMMAND ----------

# sample data
df = spark.read.format("parquet").load(pathdb + '/Workspace/bogota-hdfs/df_clean_relevant_sample')
df.cache()

# Aggregate by day and profile
daily_byprofile = df.groupBy("day", "profile_final").agg(
    F.countDistinct("cardnumber").alias("unique_cards"), 
    F.count("*").alias("total_transactions")     
    )  
daily_byprofile = daily_byprofile.withColumn(
    "transactions_by_card", 
    F.col("total_transactions") / F.col("unique_cards")
    )

daily_byprofile_sample_pd = daily_byprofile.toPandas()
daily_byprofile_sample_pd.to_csv(os.path.join(path, 'Workspace/Construct/daily_byprofile_sample.csv'), index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC Read daily by profile csv and plot

# COMMAND ----------

# Get days with missing data:
daily_byprofile = pd.read_csv(os.path.join(path, f'Workspace/Construct/daily_byprofile.csv'))
daily_byprofile = daily_byprofile.sort_values("day")
daily_byprofile = daily_byprofile[daily_byprofile.day >= "2020-01-01"]
daily_byprofile = daily_byprofile[daily_byprofile.day < "2024-10-01"]
daily_cards = daily_byprofile.groupby("day")["unique_cards"].sum()
days_missing = np.unique(list(daily_cards.index[daily_cards < 30000]) + ["2022-09-16", "2022-09-17", "2022-09-18", "2022-09-19", "2022-09-20"])
days_missing

# COMMAND ----------

days_missing = ['2022-09-16', '2022-09-17', '2022-09-18', '2022-09-19',
       '2022-09-20', '2023-10-29', '2023-11-26', '2023-12-03',
       '2023-12-24', '2023-12-25', '2024-02-03', '2024-02-06',
       '2024-02-08', '2024-02-09', '2024-02-26']
print(len(days_missing))

# COMMAND ----------

files = ["daily_byprofile.csv", "daily_byprofile_sample.csv"]
titfiles = ["WHOLEDATA", "SAMPLE"]

for file, titfile in zip (files, titfiles):
    daily_byprofile = pd.read_csv(os.path.join(path, f'Workspace/Construct/{file}'))
    daily_byprofile = daily_byprofile.sort_values("day")
    daily_byprofile = daily_byprofile[daily_byprofile.day >= "2020-01-01"]
    daily_byprofile = daily_byprofile[daily_byprofile.day < "2024-10-01"]

    yvars = ['unique_cards', 'total_transactions', 'transactions_by_card']
    ylabs = ['unique cards', 'total transactions', 'transactions by card']

    # there are some days with missing data
    daily_byprofile.loc[daily_byprofile.day.isin(days_missing), 
                        yvars] = np.NaN

    # Plots

    for y, ylab in zip(yvars, ylabs):
        fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (20, 5))
        fig.subplots_adjust(hspace = 0.4)

        for profile in daily_byprofile.profile_final.unique():
            sns.lineplot(x = daily_byprofile.day[daily_byprofile.profile_final == profile] , 
                        y = daily_byprofile.loc[daily_byprofile.profile_final == profile, y]/1_000,
                        label = profile,
                        alpha = 0.8)

        ymin, ymax = axes.get_ylim()
        ydiff = (ymax-ymin)
        ylim = ymax - ydiff * 0.1
        axes.set_ylim(0, ymax)


        axes.axvline(x = "2023-02-01", color ='black')
        axes.text("2023-02-01", ylim, 'Policy change')
        xticks = plt.gca().get_xticks()

        plt.xlabel("Day")
        plt.ylabel(f"{ylab} (thousands)")
        plt.title(f"{titfile} - Daily {ylab} by card profile*")

        plt.figtext(0.5, -0.05, "*Profiles defined based on 2022 - july 2024 data. Adulto or apoyo_subsidyvalue if anytime Adulto or Apoyo paying subsidy values. Anonymous if always anonymous.", ha="center", fontsize=8, color="black")

        plt.legend()
        plt.xticks(xticks[::180]) 
        plt.show()


