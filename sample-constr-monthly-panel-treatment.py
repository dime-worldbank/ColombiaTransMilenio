# Databricks notebook source
# MAGIC %md
# MAGIC # Sample: constructing panel data with treatment
# MAGIC - Monthly validaciones and trips per month, coding 0s
# MAGIC - Treatment status per month
# MAGIC   - [TBC] Treatment to missing in "inconsistent months" (e.g. month with no subsidy despite having subsidy in the period)
# MAGIC   - Treatment for gained eligibility: since they sign up

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
from operator import attrgetter

# MAGIC
%run ./utils/import_test.py
%run ./utils/packages.py

# Functions
import_test_function("Running hola.py works fine :)")
import_test_packages("Running packages.py works fine :)")



# COMMAND ----------

# MAGIC %md
# MAGIC ## Import data

# COMMAND ----------

# PARAMETERS

# choose sample to use
samplesize = "_sample10"
#samplesize = "_sample1"


days_missing = ['2022-09-16', '2022-09-17', '2022-09-18', '2022-09-19',
       '2022-09-20', '2023-10-29', '2023-11-26', '2023-12-03',
       '2023-12-24', '2023-12-25', '2024-02-03', '2024-02-06',
       '2024-02-08', '2024-02-09', '2024-02-26']

# COMMAND ----------

# Import and merge data

s  = pd.read_csv(os.path.join(path, 'Workspace/Construct/treatment_groups'+samplesize+'.csv'))
df = pd.read_csv(os.path.join(path,  'Workspace/Construct//monthly-valid-subsidy-bycard'+ samplesize + '.csv'))

s.drop(columns= "profile_final", inplace = True)
df = df.merge(s,
         how='left',
         on = 'cardnumber')

# COMMAND ----------

df.columns

# COMMAND ----------

print(df.month.min(), df.month.max())


# COMMAND ----------

# save a copy of all periods
df_all = df

df = df[df.month >= "2022-01-01"].reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gainedhas info

# COMMAND ----------

# Gained has info 
# Gained has before the policy change
gainedhas = set(s.cardnumber[s.treatment == "gainedhas"]) 
aux = df[df.cardnumber.isin(gainedhas)]
aux["aft"] = aux.month >= "2023-02"

gainedhas_2022 = aux.cardnumber[(aux.month < "2023-02") & (aux.month >= "2022-01") ].nunique()
print("Gainedhas in 2022:", gainedhas_2022 , f"- {round(gainedhas_2022 / len(gainedhas) * 100)}%" )

gainedhas_aft = aux.cardnumber[aux.aft].nunique()
print("Gainedhas after policy change:", gainedhas_aft , f"- {round(gainedhas_aft / len(gainedhas) * 100)}%" )


# first month with subsidy after policy change for gainedhas
gainedhas_s0 = df[(df.treatment == "gainedhas") & (df.subsidy_month == 1) & (df.month >= "2023-01-01") ]
gainedhas_s0 = gainedhas_s0.groupby("cardnumber", as_index = False).agg({"month": "min"})
gainedhas_s0.rename(columns = {"month": "month_s0"}, inplace = True)
df  =  df.merge(gainedhas_s0, on = "cardnumber", how = "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inconsistencies [for V2]

# COMMAND ----------

df["treatment_month"] = df.treatment_v2


## Inconsistencies to missing 

# Gainedhas 
print("Gainedhas with some subsidy month before policy change:",
      df.loc[(df.treatment_v2 == "gainedhas") & (df.subsidy_month == 1) & (df.month < "2023-02-01") ].cardnumber.nunique())

print("Gainedhas paying full tarif AFTER signup:",
      df.loc[(df.treatment_v2 == "gainedhas") & (df.subsidy_month == 0) & (df.month > df.month_s0) ].cardnumber.nunique())
df.loc[(      df.treatment_v2 == "gainedhas") & (df.subsidy_month == 0) & (df.month > df.month_s0) , 
       "treatment_month"] = ""


# hadlost without before the policy change and after first month with subsidy

# hadkept before policy change and after first month with subsidy

# hadkept without subsidy some month after

# hadlost with subsidy some month after

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code 0s

# COMMAND ----------

df = df[['month', 'cardnumber', 'n_validaciones', 'n_trips', 'mean_value_trip', 'treatment_v2', 'treatment_month']]

## Code 0s 

# First month in the data for everyone
# SINCE 2021
month0 = df_all[df_all.month >= "2021-01-01"].groupby("cardnumber", as_index = False).agg({"month": min})
month0.rename(columns = {"month": "month0"}, inplace = True)

# fill with 0s when there are no obs for that doc-month
df = df.set_index(["month","cardnumber"]).unstack(fill_value=0).stack().reset_index()


# add first month in the data
df = df.merge(month0,
         on = "cardnumber",
         how = "left")

# check that everyone has their first month
print(df.month0.isnull().sum() == 0 )

# TRIP VARIABLES
# only code 0s since the first time they show up. Otherwise, code missings.
df.loc[(df.month < df.month0), 
       ['n_validaciones', 'n_trips', 'mean_value_trip'] ] = np.NaN # put NAN in trips
       
# check that those present after the policy change have all NaNs in their validaciones before the policy change
df.n_validaciones[(df.month0 >= "2023-02") & (df.month < "2023-02" )].unique()


# TREATMENT VARIABLES: 
# 1. FOWARD FILL OF 0s (they take the value of the last month for each person)
# 2. BACKWARD FILL OF 0s (for values at the beginning)

# turn 0s to NaN
df.treatment_v2    = df.treatment_v2.replace(0, np.NaN)
df.treatment_month = df.treatment_month.replace(0, np.NaN)

# super important to sort values
df = df.sort_values(["cardnumber","month"]).reset_index(drop=True)

df["treatment_month"] = df.groupby("cardnumber").treatment_month.ffill()
df["treatment_month"] = df.groupby("cardnumber").treatment_month.bfill()


df["treatment_v2"] = df.groupby("cardnumber").treatment_v2.ffill()
df["treatment_v2"] = df.groupby("cardnumber").treatment_v2.bfill()

print("% card-month with inconsistences", sum(df.treatment_month == "") / df.shape[0] * 100)
print("% card-month with 0 valid", np.mean(df.n_validaciones == 0) * 100 )

# COMMAND ----------

# MAGIC %md
# MAGIC ##  After variable
# MAGIC
# MAGIC - variable t: months before or after treatment
# MAGIC - gainedhas people are treated since they signed up, the after variable for them is 1 since they first sign up

# COMMAND ----------

# AFTER

# after policy change
df["after"] = (df.month >= "2023-02-01") * 1

# after by person (varies for the gained has)
df = df.merge(gainedhas_s0, # add first month with the subsidy
              on = "cardnumber",
              how = "left")
print(df.month_s0[df.treatment_v2 == "gainedhas"].isnull().sum()) # should be 0

print(df.month_s0[df.treatment_v2 == "gainedhas"].isnull().sum()) # should be 0

# COMMAND ----------

df["after_p"] = df["after"]
df.loc[(df.treatment_v2 == "indata_neverelig") & (df.after == 1) , "after_p"] = 1
df.loc[(df.treatment_v2 == "gainedhas") & (df.month_s0.notnull()) & (df.month < df.month_s0) , "after_p"] = 0

# tab
pd.crosstab(df.after, df.after_p)

# COMMAND ----------

# add period since treatment. For all groups its since march 23 but for the gained, for whom it is since the first time treated
df["ymonth"]    = df.month.astype("period[M]")
df["ymonth_s0"] = df.month_s0.astype("period[M]")
df.loc[df.treatment_v2 != "gainedhas", "ymonth_s0"] = pd.to_datetime("2023-02").to_period("M")

df["t"] = df.ymonth - df.ymonth_s0 
df["t"] =  df.t.apply(attrgetter('n'))

# COMMAND ----------

df.drop(columns = ["month", "month_s0", "ymonth_s0"], inplace = True)

# COMMAND ----------

df = df.merge(s[["cardnumber", "cards_t0", "cards_after"]],
         on= "cardnumber",
         how= "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save

# COMMAND ----------

df.to_csv(f'{path}Workspace/Construct/panel_with_treatment{samplesize}.csv', index = False)

# COMMAND ----------

dbutils.fs.cp(f'{pathdb}/Workspace/Construct/panel_with_treatment{samplesize}.csv', f"/FileStore/my-stuff/panel_with_treatment{samplesize}.csv")
#Download from: https://adb-6102124407836814.14.azuredatabricks.net/files/my-stuff/panel_with_treatment_sample10.csv
#Download from: https://adb-6102124407836814.14.azuredatabricks.net/files/my-stuff/panel_with_treatment_sample1.csv

# COMMAND ----------

dbutils.fs.rm(f"/FileStore/my-stuff/panel_with_treatment{samplesize}.csv")

# COMMAND ----------


