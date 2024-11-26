# Databricks notebook source
# MAGIC %md
# MAGIC # Sample: getting treatment group for each card
# MAGIC
# MAGIC In the sample, we have regular users at some point in Jan 2022 - July 2024:
# MAGIC - 4538 anonymous cards --> this will be excluded from the analysis as in our linked data we do not have purely anonymous cards 
# MAGIC - 4172 adulto cards --> this are the Never group. The challenge is that we cannot distinguish 
# MAGIC   - Who was never eligible and not vulnerable
# MAGIC   - Who was never eligible and vulnerable 
# MAGIC   - Who was eligible at some point and signed up
# MAGIC - 6179 apoyo that paid subsidy at some point in time [WHICH PERIOD]---> get a dataset on having the subsidy each month and each period
# MAGIC   - hadlost23: 1, 0, 0
# MAGIC   - hadlost24: 1, 1, 0
# MAGIC   - hadkept: 1, 1, 1
# MAGIC   - gained: 0, 0, 1 or 0, 1, 1
# MAGIC
# MAGIC Replicating the steps for our linked data (https://github.com/dime-worldbank/Colombia-BRT-IE/blob/development/Fare%20Experiment%202022%20Project/Data%20Analysis/DataWork/Master%20Data/Jupyters/gcloud_linked_data_TM/constr_SurveyImpact-treatment-groups.ipynb) as closely as possible.
# MAGIC
# MAGIC
# MAGIC The most important difference is that this analysis can be only done at the **card** level, and not at the ID level.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up

# COMMAND ----------

# Directories
import os
pathdb  = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/'
path = '/dbfs/' + pathdb
user = os.listdir('/Workspace/Repos')[0]
git = '/Workspace/Repos/' +user+ '/ColombiaTransMilenio/'
git2 = '/Workspace/Repos/' +user+ '/Colombia-BRT_IE-temp/'
## Important sub-directories for this notebook
byheader_dir = path + '/Workspace/Raw/byheader_dir/'

# COMMAND ----------

# choose sample to use
#samplesize = "_sample10"
samplesize = "_sample1"

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

# MAGIC
%run ./utils/import_test.py
%run ./utils/packages.py

# Functions
import_test_function("Running hola.py works fine :)")
import_test_packages("Running packages.py works fine :)")



# COMMAND ----------

# MAGIC %md
# MAGIC ## Import sample data

# COMMAND ----------


# days with missing data
days_missing = ['2022-09-16', '2022-09-17', '2022-09-18', '2022-09-19',
       '2022-09-20', '2023-10-29', '2023-11-26', '2023-12-03',
       '2023-12-24', '2023-12-25', '2024-02-03', '2024-02-06',
       '2024-02-08', '2024-02-09', '2024-02-26']


# The following include all the values in the data
price_subsidy_18 = [1575, 1725]
price_subsidy_22 = [1650, 1800] # same since Feb 2019
price_subsidy_23 = [2250, 2500] # same for 2024, though since Feb tariff unified to 2500

price_full_17    = [2000] 
price_full_18    = [2100, 2300] 
price_full_19    = [2200, 2400] 
price_full_20    = [2300, 2500] # careful as 2500 is repeated in the subsidy values for 2022
price_full_22    = [2450, 2650]
price_full_23    = [2750, 2950] # same for 2024, though since Feb tariff unified to 2950

# COMMAND ----------

os.listdir(os.path.join(path,f'Workspace/bogota-hdfs/'))

# COMMAND ----------

df = spark.read.format("parquet").load(os.path.join(pathdb,f'Workspace/bogota-hdfs/df_clean_relevant{samplesize}'))
df.cache()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Get subsidy status per card per month
# MAGIC
# MAGIC 1. Keep all data until month when Follow Up 1 survey finished (March 2024)
# MAGIC
# MAGIC 2. Discard Adulto Mayor and Discapacidad cards (already done because of the way the sample was built)
# MAGIC
# MAGIC 3. Discard transfers
# MAGIC
# MAGIC 4. Tag each validacion as being a subsidy transaction or not

# COMMAND ----------

# Filter dates
falldf = df.filter(F.col("day") < "2024-04-01")

# Correct the year variable
falldf = falldf.withColumn("year", F.year(F.col("year").cast("timestamp")))

# Filter out rows with value <= 200
falldf = falldf.filter(F.col("value") > 200)

falldf = falldf.withColumn(
    "subsidy",
    F.when( # After 2022
        (F.col("value").isin(price_subsidy_22 + price_subsidy_23)) &
        (F.col("day") > "2022-01-31"), 1
    ).when( # After 2022
        (F.col("value").isin(price_full_22 + price_full_23)) &
        (F.col("day") > "2022-01-31"), 0
    ).when( # Before 2022
        (F.col("value").isin(price_subsidy_18 + price_subsidy_22)) &
        (F.col("day") <= "2022-01-31"), 1
    ).when( # Before 2022
        (F.col("value").isin(price_full_17 + price_full_18 + price_full_19 +
                            price_full_20 + price_full_22)) &
        (F.col("day") <= "2022-01-31"), 0
    )
)

# mark if they are trips and NOT transfers
falldf = falldf.withColumn("full_trip", (F.col("value") > 500).cast("int"))
falldf = falldf.withColumn(
    "value_full_trip", 
    F.when(F.col("full_trip") == 0, None).otherwise(F.col("value"))
)
# to count all validaciones
falldf = falldf.withColumn("constant_one", F.lit(1))

# COMMAND ----------

# Stats [NOT RUNNING BECAUSE IT TAKES LONG]
# Get the maximum day
max_day = falldf.agg(F.max("day")).collect()[0][0]
print(f"Max day: {max_day}")

# Cards in sample
num_cards = alldf.select("cardnumber").distinct().count()
print(f"Cards in sample: {num_cards}")


# Cards in sample after filter
num_cards_after = falldf.select("cardnumber").distinct().count()
print(f"Cards in sample after filter: {num_cards_after}")

# Total validations
total_validations = falldf.count()
print(f"Total validations: {total_validations}")


# Count null subsidy values
num_null_subsidy = falldf.filter(F.col("subsidy").isNull()).count()
print(f"Number of null subsidy values: {num_null_subsidy}")


# Calculate percentage of null subsidies for anonymous profiles
anonymous_null_percentage = falldf.filter(F.col("profile_final") == "anonymous") \
                                   .filter(F.col("subsidy").isNull()) \
                                   .count() / falldf.filter(F.col("profile_final") == "anonymous").count() * 100
print(f"Percentage of null subsidies for anonymous profiles: {anonymous_null_percentage:.2f}%")

# Show rows with null subsidy values and drop duplicates
falldf.filter(F.col("subsidy").isNull()) \
      .select("year", "value", "system", "profile_final") \
      .distinct() \
      .show()

# COMMAND ----------

# Get number and % of subsidy trips each month
# get total validaciones, total full trips and avg value of full trips by month

dm = falldf.groupBy("cardnumber", "profile_final", "month") \
.agg(
    F.mean("subsidy").alias("subsidy_mean"),
    F.sum("subsidy").alias("subsidy_sum"),
    F.sum("constant_one").alias("n_validaciones"),
    F.sum("full_trip").alias("n_trips"),
    F.mean("value_full_trip").alias("mean_value_trip")
)
dm = dm.toPandas()

# COMMAND ----------

# subsidy that month rule
dm["subsidy_month"] = 0
dm.loc[dm.subsidy_mean >= 0.4, "subsidy_month" ] = 1 # 40% subsidy trips or more
dm.loc[dm.subsidy_sum  == 30,  "subsidy_month" ] = 1 # 30 subsidy trips 
print(dm.columns)
print(dm.subsidy_month.isnull().sum())

# COMMAND ----------

dm.shape

# COMMAND ----------

dm.to_csv(os.path.join(path, 'Workspace/Construct//monthly-valid-subsidy-bycard'+ samplesize + '.csv'), index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subsidy per card per month: plots & stats
# MAGIC - Plots of cards with subsidy per month.
# MAGIC - Having subsidy in any month versus card profile profiles

# COMMAND ----------

subsidy_anymonth = dm[(dm.month > "2021-12-31") & (dm.month < "2024-04-01")].groupby("cardnumber", as_index = False).agg({"subsidy_month" : "max"})
subsidy_anymonth.columns = ["cardnumber", "subsidy_anymonth"]
subsidy_anymonth = dm.merge(subsidy_anymonth,
                            on = "cardnumber",
                            how = "left")
fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (10, 5))
fig.subplots_adjust(hspace = 0.4)

perc_subsidy =  subsidy_anymonth[ (dm.month > "2021-12-31") & (dm.month < "2024-04-01") & (subsidy_anymonth.subsidy_anymonth == 1)].groupby(["month"]).agg({"subsidy_month": lambda x: np.mean(x)*100})
perc_subsidy.subsidy_month.plot(title = f"NON-LINKED DATA  \n Percentage of CARDS with subsidy trips each month \n over CARDS travelling that month that had the subsidy ANY month Jan22-Mar24 \n {samplesize}")
plt.xlabel("Month")
plt.ylabel("%")
plt.ylim(0, 100)
plt.grid()
plt.show()

# COMMAND ----------

subsidy_anymonth = subsidy_anymonth[["cardnumber", "profile_final", "subsidy_anymonth"]].drop_duplicates()
print(subsidy_anymonth.shape)
print(subsidy_anymonth.cardnumber.nunique())

pd.crosstab(subsidy_anymonth.profile_final, subsidy_anymonth.subsidy_anymonth)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Having the subsidy each period and final subsidy group
# MAGIC
# MAGIC Output `by_card`: dataset at the card level, long dataset at the card-period level (missing rows if a card is not present that period), stating whether the card have the subsidy each period and some extra info

# COMMAND ----------

dm = pd.read_csv(os.path.join(path, 'Workspace/Construct//monthly-valid-subsidy-bycard'+ samplesize + '.csv'))

# Tag periods
dm["period"] = np.NaN
dm.loc[(dm.month >= "2022-01-01")   & (dm.month <=  "2023-01-31"), "period"] = 0
dm.loc[(dm.month >=  "2023-03-01")  & (dm.month <=  "2023-12-31"), "period"] = 1
dm.loc[(dm.month >=  "2024-01-01")  & (dm.month <=  "2024-03-31"), "period"] = 2
print(dm.period.value_counts())


# COMMAND ----------

# Tag cards present in each period
tot_cards = dm.cardnumber.nunique()
cards_t0 = set(dm.cardnumber[dm.period == 0])
cards_t1 = set(dm.cardnumber[dm.period == 1])
cards_t2 = set(dm.cardnumber[dm.period == 2])

dm["cards_t0"] = dm.cardnumber.isin(cards_t0)

cards_always = set(cards_t0).intersection(cards_t1).intersection(cards_t2)
cards_after = set(cards_t1).union(cards_t2)
cards_t0_after = cards_after.intersection(cards_t0)
cards_any = cards_after.union(cards_t0)

print( f"Cards present before (in t0): {len(cards_t0)} cards, { round(len(cards_t0) / tot_cards * 100)}%" )
print( f"Cards present after (in either t1 or t2): {len(cards_after)} cards, { round(len(cards_after) / tot_cards * 100)}%" )
print( f"Cards present before and after (in either t1 or t2, and t0): {len(cards_t0_after)} cards, { round(len(cards_t0_after) / tot_cards * 100)}%" )

print( f"Cards present in t1: {len(cards_t1)} cards, { round(len(cards_t1) / tot_cards * 100)}%" )
print( f"Cards present in t2: {len(cards_t2)} cards, { round(len(cards_t2) / tot_cards * 100)}%" )
print( f"Cards present in all three periods: {len(cards_always)}, { round(len(cards_always) / tot_cards * 100)}%" )
print( f"Cards present in any of the periods: {len(cards_any)}, { round(len(cards_any) / tot_cards * 100, 2)}%" )

# COMMAND ----------

# Keep cards in any of the periods
dm_inperiod = dm[dm.period.notnull()].reset_index(drop = True)

# dataset with last month in each period 
last = dm_inperiod.groupby(["period","cardnumber"], as_index = False).tail(1)

# Prepare data for corrections
# % of months 
check =  dm_inperiod.groupby(["period","cardnumber"],
                    as_index = False).agg({"subsidy_month":  ["mean","sum"],
                                           "month": ["nunique", "min", "max"]})
check.columns = ["period", "cardnumber",
                  "mean_m_subsidy", "sum_m_subsidy",
                  "n_months", "min_month", "max_month"]

check["perc_m_subsidy"] = check.mean_m_subsidy * 100

# dataset by card and period with all info
by_card_period = last.merge(check,
                   on = ["period","cardnumber"],
                   how = "left")
del check, last

by_card_period["subsidy_month_corrected"] = by_card_period.subsidy_month

# CORRECTIONS

## t0: based on % of months with or without subsidy in the period

by_card_period_t0 = by_card_period[by_card_period.period == 0].reset_index(drop = True)

print("% of months with subsidy for those WITH subsidy in the last month:") 
print("- People with less than 100%:",
      np.sum (by_card_period_t0.perc_m_subsidy[by_card_period_t0.subsidy_month == 1] < 100),
      np.mean(by_card_period_t0.perc_m_subsidy[by_card_period_t0.subsidy_month == 1] < 100) * 100)
print("- People with less than 50%:",
      np.sum (by_card_period_t0.perc_m_subsidy[by_card_period_t0.subsidy_month == 1] < 50),
      np.mean(by_card_period_t0.perc_m_subsidy[by_card_period_t0.subsidy_month == 1] < 50) * 100)

print("---")
print("% of months with subsidy for those WITHOUT subsidy in the last month:") 
print("- People with any month:",
      round(np.sum (by_card_period_t0.perc_m_subsidy[by_card_period_t0.subsidy_month == 0] > 0)),
      round(np.mean(by_card_period_t0.perc_m_subsidy[by_card_period_t0.subsidy_month == 0] > 0) * 100, 2), "%")
print("- People with more than 50%:",
      round(np.sum (by_card_period_t0.perc_m_subsidy[by_card_period_t0.subsidy_month == 0] > 50), 2),
      round(np.mean(by_card_period_t0.perc_m_subsidy[by_card_period_t0.subsidy_month == 0] > 50)* 100, 2), "%")


# Those without subsidy in the last month before the policy change but more than 50% of months: they will be considered as having the subsidy before the policy change.

by_card_period.loc[ (by_card_period.period == 0) &
                    (by_card_period.perc_m_subsidy > 50) &
                    (by_card_period.subsidy_month == 0), 
          "subsidy_month_corrected" ] = 1

by_card_period = by_card_period[['cardnumber', 'period', 
                                 'subsidy_month', 'subsidy_month_corrected', 
                                                'sum_m_subsidy',  'mean_m_subsidy', 'perc_m_subsidy']] 


pd.crosstab(by_card_period.subsidy_month, by_card_period.subsidy_month_corrected)   

# COMMAND ----------

# Reshaping from long to wide
s = by_card_period.pivot(index="cardnumber", 
                         columns="period", 
                         values="subsidy_month_corrected")

s.reset_index(inplace=True)
s.columns.name = None  # Remove the name 'period' from columns

s.columns = ["cardnumber", "subsidy_t0", "subsidy_t1", "subsidy_t2"]

s["subsidy_any_period"] = ( (s.subsidy_t0 == 1) | (s.subsidy_t1 == 1) | (s.subsidy_t2 == 1) ) *1


# check we haen't loos cards when pivoting
print("OK:", by_card_period.cardnumber.nunique() ==   s.cardnumber.nunique())

# Add dataset with some info at the card level
bycard = dm[["cardnumber", "profile_final", "cards_t0"]].drop_duplicates()
print("OK:",bycard.shape[0] == bycard.cardnumber.nunique())


s = s.merge(bycard,
             on = "cardnumber",
             how = "left")

print(pd.crosstab(s.profile_final, s.cards_t0))
print(pd.crosstab(s.profile_final, s.subsidy_any_period))

del by_card_period, bycard

# COMMAND ----------

# MAGIC %md
# MAGIC We have 83 anonymous with subsidy! However, we will just use the adulto ones as a comparison. Also, we have 288 apoyo with no subsidy at any period among the relevant ones.

# COMMAND ----------

#hadlost23
s['hadlost23'] = (s.subsidy_t0 == 1) & (s.subsidy_t1 == 0) 

s['hadlost23_v2'] = s['hadlost23']
s.loc[(s.subsidy_t0 == 1) & (s.subsidy_t1.isnull())  & (s.subsidy_t2 == 0), 'hadlost23_v2'] = 1


#hadlost24
s['hadlost24'] =  (s.subsidy_t0 == 1) & (s.subsidy_t1 == 1) & (s.subsidy_t2 == 0)

#hadkept
s['hadkept'] = (s.subsidy_t0 == 1) &  (s.subsidy_t1 == 1) & (s.subsidy_t2 == 1)
s['hadkept_v2'] = s['hadkept']
s.loc[(s.subsidy_t0 == 1) & (s.subsidy_t1 == 1) & (s.subsidy_t2.isnull()), 'hadkept_v2']  = True
s.loc[(s.subsidy_t0 == 1) & (s.subsidy_t1.isnull()) & (s.subsidy_t2 == 1), 'hadkept_v2']  = True


# newly
s['gainedhas'] = (s.subsidy_t0 == 0)  & (s.subsidy_t2 == 1) 
s.loc[(s.subsidy_t0 == 0) &  (s.subsidy_t1 == 1) & (s.subsidy_t2.isnull()), 'gainedhas'] = True
s.loc[(s.subsidy_t0.isnull()) &  (s.subsidy_t2 == 1), 'gainedhas'] = True
s.loc[(s.subsidy_t0.isnull()) &  (s.subsidy_t1 == 1) & (s.subsidy_t2.isnull()), 'gainedhas'] = True

# missings
s['missing_after'] =  (s.subsidy_t1.isnull())  & (s.subsidy_t2.isnull()) 

# to dummies instead of booleans and add a categorical variable
categ =  ["hadlost23", "hadlost24", "hadkept", "gainedhas", "missing_after"]
s[categ + ["hadlost23_v2", "hadkept_v2"] ] = s[categ + ["hadlost23_v2", "hadkept_v2"]] * 1

# check whether they are mutually exclusive
print(s[categ].sum())
print(s[categ].sum(axis = 1).unique())

# add a categorical variable
s["treatment"] = ""
for v in categ:
    s.loc[s[v] == 1, "treatment"] = v
s.treatment.value_counts()

# COMMAND ----------


# check whether they are mutually exclusive
categ =  ["hadlost23_v2", "hadlost24", "hadkept_v2", "gainedhas", "missing_after"]

# check whether they are mutually exclusive
print(s[categ].sum())
print(s[categ].sum(axis = 1).unique())

# add a categorical variable
s["treatment_v2"] = ""
for v in categ:
    s.loc[s[v] == 1, "treatment_v2"] = v
s.treatment.value_counts()

# COMMAND ----------

s.drop(columns = categ + ["hadlost23", "hadkept"], inplace = True)

# COMMAND ----------

# no treatment for those anonymous
s.loc[s.profile_final == "anonymous", "treatment"] = "anonymous"
s.loc[s.profile_final == "adulto", "treatment"] = "adulto"
pd.crosstab(s.profile_final, s.treatment)

# COMMAND ----------

s.loc[s.profile_final == "anonymous", "treatment_v2"] = "anonymous"
s.loc[s.profile_final == "adulto", "treatment_v2"] = "adulto"
pd.crosstab(s.profile_final, s.treatment_v2)

# COMMAND ----------

pd.crosstab(s.treatment, s.treatment_v2)

# COMMAND ----------

# Was there before policy change or after
s["cards_t0"]    = s.subsidy_t0.notnull()
s["cards_after"] = (s.subsidy_t1.notnull()) | (s.subsidy_t2.notnull())

# COMMAND ----------

s.to_csv(os.path.join(path, 'Workspace/Construct/treatment_groups'+samplesize+'.csv'), index=False)
