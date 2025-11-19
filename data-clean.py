# Databricks notebook source
# MAGIC %md
# MAGIC # Clean data
# MAGIC

# COMMAND ----------

# MAGIC %run ./utils/packages

# COMMAND ----------

# MAGIC %run ./utils/setup

# COMMAND ----------

# MAGIC %run ./utils/windows

# COMMAND ----------

# MAGIC %run ./utils/generate_variables

# COMMAND ----------

# MAGIC %md
# MAGIC # (1) Create raw parquet files
# MAGIC <mark> So far: data since 2020 </mark>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tm_bronze (
# MAGIC   transaction_timestamp TIMESTAMP,    -- up to seconds
# MAGIC   emisor                STRING,       
# MAGIC   operator              STRING,
# MAGIC   line                  STRING,
# MAGIC   station_or_stop       STRING,
# MAGIC   station_access        STRING,
# MAGIC   device                STRING,
# MAGIC   card_type             STRING,
# MAGIC   card_profile          STRING,
# MAGIC   cardnumber            STRING,
# MAGIC   balance_before        INT,
# MAGIC   value                 INT,
# MAGIC   balance_after         INT,
# MAGIC   system                STRING,
# MAGIC   input_file            STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze_raw_staging (
# MAGIC   input_file   STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronze_raw_staging
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC   _metadata.file_path AS input_file 
# MAGIC   FROM '/Volumes/prd_csc_mega/sColom15/vColom15//Workspace/Raw/byheader_dir/'
# MAGIC   )
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = '*'
# MAGIC FORMAT_OPTIONS (
# MAGIC   'header' = 'true',
# MAGIC   'encoding' = 'ISO-8859-1',
# MAGIC   'allowQuotedNewLines'  = 'true',
# MAGIC   'recursiveFileLookup'  = 'true' ,
# MAGIC   'mergeSchema' = 'true',
# MAGIC   'inferSchema' = 'true'
# MAGIC )
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze_raw_staging
# MAGIC LIMIT 20;

# COMMAND ----------

# import depending on header
header_folders = os.listdir(byheader_dir)
print(f"We have {len(header_folders)} folders: {header_folders}")
spark_handlers = [spark_df_handler() for _ in range(len(header_folders))]

# COMMAND ----------

# Link each header to its formats
headers    =  ['header_one', 'header_two', 'header_three', 'header_four', 'header_five', 'header_six', 'header_seven',
               'header_08', 'header_09', 'header_10', 'header_11', 'header_12', 'header_13', 'header_14', 'header_15', 'header_16']
print(f"The following folder should be added to the headers list: {set(header_folders).difference(headers)}. The set SHOULD be empty. ")

delimiters = [','   , ';'   , ','   , ';'  ,
              ';'   , ','   ,  ','  , ','  ,
              ','   , ','   ,  ','  , ','  ,
              ','   , ','   ,  ','  , ','  ]
encodings = ["utf-8" ,"utf-8", "utf-8", "utf-8" ,"latin1", "utf-8" ,"utf-8",
             "utf-8" ,"utf-8", "utf-8" ,"utf-8","utf-8" ,"utf-8","utf-8" ,"utf-8", "utf-8"]

# different headers might have the same or different formats (find the format for headers six and seven, but as for now those folders are missing)
formats =   ['format_two', 'format_four', 'format_five', 'format_one', 'format_three', '', '',
             'format_6' , 'format_6' , 'format_6',
             'format_7' , 'format_7' , 'format_7', 'format_7',
             'format_6']

# COMMAND ----------

# import data & visualize if necessary

for idx, handler in enumerate(spark_handlers):
          
    if idx < 7: # skip the first 7 headers
        continue
    
    h = headers[idx]
    print('\n Schema ' + h)
    handler.load(type = 'new_data',
                 path = os.path.join(pathdb ,"Workspace/Raw/byheader_dir/" , h),
                 delimiter = delimiters[idx], 
                 encoding = encodings[idx])
    # display(handler.dfraw.limit(1).toPandas())    # display is commented to hide dataset content

# COMMAND ----------

# transform data

for idx, handler in enumerate(spark_handlers):
    
    # skip the first 7 headers, use it later to work with full data
    # skip the 16th header, empty data
    if (idx < 7) | (idx == 15): 
        continue
    
    h = headers[idx]
    print('\n Schema ' + h)

    handler.transform(header_format = formats[idx])
    # display(handler.df.limit(1).toPandas())    # display is commented to hide dataset content

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2.b. Handling NAs

# COMMAND ----------

for idx, handler in enumerate(spark_handlers):
    if (idx < 7) | (idx == 15): 
        continue

    h = headers[idx]
    print('\n Schema ' + h)
    handler.df.where(handler.df['value'].isNull()).show()


# COMMAND ----------

# MAGIC %md
# MAGIC - There are some nulls in values, but while other columns are OK. These obs. are OK. The maximum number of rows by schema is 4. So no big issues here.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2.c. Join, count, and save

# COMMAND ----------

# Initialize an empty DataFrame
df = spark_handlers[7].df  # Start with the first DataFrame

for idx, handler in enumerate(spark_handlers):
    
    # skip the first 7 headers, use it later to work with full data
    # skip the 8th which we used to initialize the df
    # skip the 16th header, empty data
    if (idx < 8) | (idx == 15): 
        continue

    # Loop through the rest of the Spark handlers and union their DataFrames
    df = df.union(handler.df)  


# COMMAND ----------

# Create parquet file of raw data
df.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/parquet_df_raw_2020-2024_withdups'))
del spark_handlers, handler

# COMMAND ----------

df.count()

# COMMAND ----------

4367228459/1_000_000

# COMMAND ----------

# MAGIC %md
# MAGIC # (2) Create clean parquet files
# MAGIC
# MAGIC This section is written so it can be run independently from (1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Remove duplicates
# MAGIC

# COMMAND ----------

sh = spark_df_handler()
sh.load(type = 'parquet', path = pathdb + '/Workspace/bogota-hdfs/', file = 'parquet_df_raw_2020-2024_withdups')

# COMMAND ----------

sh.df.cache()

# COMMAND ----------

# Create var that flags consecutive observations that have the same transaction timestamp, line and cardnumber
sh.df = sh.df.withColumn('duplicate',
                   F.when( (F.lag(sh.df['transaction_timestamp']).over(user_window) == sh.df['transaction_timestamp']) \
                         & (F.lag(sh.df['line']).over(user_window) == sh.df['line']), 1).otherwise(0))

# COMMAND ----------

duplicate_mean = sh.df.agg(F.mean('duplicate')).first()[0]
print(f"Percentage of duplicates: {duplicate_mean * 100}")

# COMMAND ----------

4367228459 * (1 - 0.00183) / 1_000_000

# COMMAND ----------

# Tabulate duplicates
df_count_dupes = df_count_dupes.withColumn('year', F.year(df['transaction_timestamp']))
df_count_dupes = df_count_dupes.filter(df_count_dupes['duplicate'] == 1).groupby(df_count_dupes['year'], df_count_dupes['month']).count().toPandas()
df_count_dupes

# COMMAND ----------

# Drop dupes
sh.df  = sh.df.filter(sh.df['duplicate'] == 0)
sh.df  = sh.df.drop('duplicate')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2. Add variables

# COMMAND ----------

# generate new variables
sh.df = generate_variables(sh.df )
sh.df 

# COMMAND ----------

print(os.listdir(path + '/Workspace/variable_dicts'))

# COMMAND ----------

account_name_dict = pd.read_csv(path + '/Workspace/variable_dicts/account_name_dict_dict.csv')
account_name_dict["perc"] = account_name_dict["count"] / account_name_dict["count"].sum() * 100
account_name_dict

# COMMAND ----------

# Commented because running this took 4 hs
# Encode strings
# as for now, lets just do it for account_name
# sh.df, account_name_dict = enumerate_factors(sh.df, 'account_name', old_dict = False, return_dict = True)
# account_name_dict_pd = account_name_dict.toPandas()
# account_name_dict_pd.to_csv(os.path.join(git, "csv_outputs/" + variable + "_dict.csv"))

# COMMAND ----------

# Create parquet file of raw data
sh.df.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/parquet_df_clean_2020-2024_temp'))

# COMMAND ----------

# MAGIC %md
# MAGIC # (3) Regular Users
# MAGIC
# MAGIC Remove super swipers and infrequent users.
# MAGIC
# MAGIC <mark> We are doing this cleaning just for the period of interest: those present from January 2022 to July 2024 </mark>
# MAGIC
# MAGIC
# MAGIC _This section can be run independently._

# COMMAND ----------

# load data
sh = spark_df_handler()
sh.load(type = 'parquet', path = pathdb + '/Workspace/bogota-hdfs/', file = 'parquet_df_clean_2020-2024_temp')

# Correct variables (and save again the dataset with the corrections made)
sh.df = sh.df.withColumn('year'   , F.year(sh.df['transaction_timestamp']))

# COMMAND ----------

sh.df.select("transaction_timestamp", "year", "month", "week", "day").show(5)

# COMMAND ----------

# filter relevant periods and columns
df_filtered = sh.df \
                .where( (F.col('transaction_timestamp') > dt.datetime(2021, 12, 31, tzinfo = timezone)) & \
                        (F.col('transaction_timestamp') < dt.datetime(2024, 7, 1,   tzinfo = timezone)) ) \
                .select('cardnumber', 'day', 'month', 'year', 'account_name_id', 'value', 'transfer')
df_filtered.cache()

# COMMAND ----------

# count transactions by day
usage_count_day = df_filtered.groupby("cardnumber", 'day', 'year').count()
usage_count_day = usage_count_day.withColumn("more100swipes",   
                           F.when( usage_count_day["count"] > 100, 1).otherwise(0))
usage_count_day = usage_count_day.withColumn("more20swipes",   
                           F.when( usage_count_day["count"] > 100, 1).otherwise(0))

# super swipers
superswipers = usage_count_day.groupBy("cardnumber").agg(
    F.sum("more100swipes").alias("days_more100swipes"),
    F.sum("more20swipes").alias("days_more20swipes"))

superswipers = superswipers.withColumn("1day_more100swipes",
                           F.when(superswipers["days_more100swipes"] > 0, 1).otherwise(0))

superswipers = superswipers.withColumn("2days_more20swipes",
                           F.when(superswipers["days_more20swipes"] > 1, 1).otherwise(0))

# frequent users (exclude 2024 for now)
freq = usage_count_day.groupBy("cardnumber", "year").agg(
            F.countDistinct("day").alias("n_days"),
            F.sum("count").alias("n_valid"))


freq = freq.withColumn("year_more12days",
                           F.when( ((F.col("n_days")  >= 12) & (F.col("year") != 2024)) | \
                                   ((F.col("n_days")  >= 6 ) & (F.col("year") == 2024)) , 1).otherwise(0))
freq = freq.withColumn("year_more12valid",
                           F.when( ((F.col("n_valid")  >= 12) & (F.col("year") != 2024)) | \
                                   ((F.col("n_valid")  >= 6 ) & (F.col("year") == 2024)) , 1).otherwise(0))

freq =  freq.groupBy("cardnumber").agg(
    F.max("year_more12days").alias("more12days_any_year"),
    F.max("year_more12valid").alias("more12valid_any_year"))


# COMMAND ----------

# Total cards
superswipers.count()

# COMMAND ----------

# Total cards
freq.count()

# COMMAND ----------

dbutils.fs.mkdirs(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate'))

# Save intermediate
#usage_count_day.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/usage_count_day'))
#superswipers.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/superswipers'))
#freq.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/freq'))

# COMMAND ----------

# maybe save to Pandas (afterwards)
#superswipers.toPandas().to_csv(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/superswipers.csv'), index = False)
#freq.toPandas().to_csv(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/freq.csv'), index = False)


# COMMAND ----------

# Proportion of superswipers
superswipers_perc = superswipers.agg(*[
    F.round(F.mean(F.col(col)) * 100, 6).alias(col + "_percent")
    for col in ["1day_more100swipes", "2days_more20swipes"]
])

superswipers_perc.show()

# COMMAND ----------

# Proportion of unfrequent users
freq_perc = freq.agg(*[
    F.round(F.mean(F.col(col)) * 100, 5).alias(col + "_percent")
    for col in ["more12valid_any_year", "more12days_any_year"]
])

freq_perc.show()

# COMMAND ----------

# Join transactions dataset with superswipers and freq datasets
df_filtered = df_filtered.join(superswipers, on="cardnumber", how="left")
df_filtered = df_filtered.join(freq, on="cardnumber", how="left")
df_filtered.show(2)

# COMMAND ----------

# Proportion of transactions of super swipers and of frequent users
dummy_columns = ["more12valid_any_year", "more12days_any_year", "1day_more100swipes", "2days_more20swipes"] 
percentages = df_filtered.agg(*[
    (F.mean(F.col(col)) * 100).alias(col + "_perc")
    for col in dummy_columns
])
percentages.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Out of 14,802,496 cards present from January 2022 to **July 2024**, I am removing:
# MAGIC - Swiper swipers
# MAGIC   - more than 100 swipes in a day: 5.34E-4 cards and less than 0.01% transactions.
# MAGIC   - more than 20 swipes in two days: 2.3E-4 cards and less than 0.01% transactions.
# MAGIC - Infrequent users
# MAGIC   - Those at are present for less than 12 _different days_ in 2022 and 2023, and less than 6 days in 2024: 31% of cards, but less than 2% of transactions.
# MAGIC   - If I were to use the 12 _transactions_ criteria instead: 22% of cards and less than 1% of transactions. But we prefer the days criteria.

# COMMAND ----------

# Remove super swipers and infrequent users form dataset and save
df_regular_users = df_filtered.filter(
    (F.col('more12days_any_year') == 1) &
    (F.col('1day_more100swipes') == 0) &
    (F.col('2days_more20swipes') == 0)
) \
    .select('cardnumber', 'day', 'month', 'year', 'account_name_id', 'value', 'transfer')

# COMMAND ----------

df_filtered.count()

# COMMAND ----------

df_regular_users.cache()
df_regular_users.count() 

# COMMAND ----------

df_regular_users.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/df_regular-users-2022-2024'))

# COMMAND ----------

# original  Sebastian's relevant code
# usage_count_day = df.groupby("cardnumber", 'day').count()
#    daily_usage_outlier_accounts = usage_count_day.where(usage_count_day['count'] > 100).select('cardnumber').distinct()
#    daily_usage_outlier_accounts_repeated = usage_count_day\
#        .where(usage_count_day['count'] > 20)\
#        .groupby('cardnumber').count().filter(F.col('count') > 2).select('cardnumber').distinct()
#    usage_count_year = df.groupby("cardnumber").count()
#    infrequent_user_accounts = usage_count_year.where(usage_count_year['count'] < 12).select('cardnumber').distinct()


# COMMAND ----------

# MAGIC %md
# MAGIC # Later

# COMMAND ----------

#os.listdir(os.path.join(path, 'Workspace/bogota-hdfs'))
#dbutils.fs.mv(pathdb + 'Workspace/bogota-hdfs/parquet_df_clean_joined',pathdb + 'Workspace/bogota-hdfs/sample-will/parquet_df_clean_joined', recurse=True)
