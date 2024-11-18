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

alldf = pd.read_csv(os.path.join(path, 'Workspace/Construct/df_clean_relevant_sample.csv'))
s  = pd.read_csv(os.path.join(path, 'Workspace/Construct/treatment_groups.csv'))
dm = pd.read_csv(os.path.join(path, 'Workspace/Construct/subsidybymonthdoc-2020toMar2024_sample.csv'))

days_missing = ['2022-09-16', '2022-09-17', '2022-09-18', '2022-09-19',
       '2022-09-20', '2023-10-29', '2023-11-26', '2023-12-03',
       '2023-12-24', '2023-12-25', '2024-02-03', '2024-02-06',
       '2024-02-08', '2024-02-09', '2024-02-26']


# COMMAND ----------

# MAGIC %md
# MAGIC ## By card and month

# COMMAND ----------

falldf = alldf[alldf.month >= "2022-01-01"]
falldf = falldf[falldf.month <= "2024-03-31"]
print(falldf.month.min(), falldf.month.max())

# holidays and weekends [TBC]
# mark if they are trips and NOT transfers
falldf["full_trip"] = (falldf.value > 500) * 1


# for all validaciones
df1 = falldf.groupby(["month", "cardnumber"],
                   as_index = False).agg({'transaction_timestamp': 'count'})
df1.rename(columns = {'transaction_timestamp' : 'n_validaciones'}, inplace = True)


# for full trips (removing transfers)
df2 = falldf[falldf.full_trip == 1].reset_index(drop = True)
df2 = df2.groupby(["month", "cardnumber"],
                  as_index = False).agg({'transaction_timestamp': 'count',
                                         'value': 'mean'})

df2.rename(columns = {'transaction_timestamp' : 'n_trips',
                      'value': 'mean_value_trip'}, inplace = True)

# merge
df = df1.merge(df2,
          how = "left",
          on  = ["month", "cardnumber"])
del df1, df2

# fill na for trips with 0s
df = df.fillna(0)

# add subsidy status
df = df.merge(dm[ ["cardnumber", "month", "subsidy_month"]],
         how = "left",
         on = ["cardnumber", "month"])

# COMMAND ----------

# add treatment group
df = df.merge(s[["cardnumber", "profile_final", "treatment", "treatment_v2"]],
              how = "left",
              on = "cardnumber")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gainedhas info

# COMMAND ----------

# Gained has before the policy change
gainedhas = set(s.cardnumber[s.treatment == "gainedhas"]) 
aux = df[df.cardnumber.isin(gainedhas)]
aux["aft"] = aux.month >= "2023-02"

gainedhas_bef = aux.cardnumber[~aux.aft].nunique()
print("Gainedhas before policy change:", gainedhas_bef , f"- {round(gainedhas_bef / len(gainedhas) * 100)}%" )

gainedhas_2022 = aux.cardnumber[(aux.month < "2023-02") & (aux.month >= "2022-01") ].nunique()
print("Gainedhas in 2022:", gainedhas_2022 , f"- {round(gainedhas_2022 / len(gainedhas) * 100)}%" )

gainedhas_aft = aux.cardnumber[aux.aft].nunique()
print("Gainedhas after policy change:", gainedhas_aft , f"- {round(gainedhas_aft / len(gainedhas) * 100)}%" )

# COMMAND ----------

# first month with subsidy after policy change for gainedhas
gainedhas_s0 = df[(df.treatment == "gainedhas") & (df.subsidy_month == 1) & (df.month >= "2023-01-01") ]
gainedhas_s0 = gainedhas_s0.groupby("cardnumber", as_index = False).agg({"month": "min"})
gainedhas_s0.rename(columns = {"month": "month_s0"}, inplace = True)
df  =  df.merge(gainedhas_s0, on = "cardnumber", how = "left")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Inconsistencies [TBC; also, do it for V2 or V1?]

# COMMAND ----------

df["treatment_month"] = df.treatment


# COMMAND ----------

# Inconsistencies to missing 


# Gainedhas 
print("Gainedhas with some subsidy month before policy change:",
      df.loc[(df.treatment == "gainedhas") & (df.subsidy_month == 1) & (df.month < "2023-02-01") ].cardnumber.nunique())

print("Gainedhas paying full tarif AFTER signup:",
      df.loc[(df.treatment == "gainedhas") & (df.subsidy_month == 0) & (df.month > df.month_s0) ].cardnumber.nunique())
df.loc[(      df.treatment == "gainedhas") & (df.subsidy_month == 0) & (df.month > df.month_s0) , 
       "treatment_month"] = ""


# hadlost without before the policy change and after first month with subsidy

# hadkept before policy change and after first month with subsidy

# hadkept without subsidy some month after

# hadlost with subsidy some month after

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code 0s

# COMMAND ----------

df = df[['month', 'cardnumber', 'n_validaciones', 'n_trips', 'mean_value_trip', 'treatment', 'treatment_month']]

# COMMAND ----------

## Code 0s 

# First month in the data for everyone
# SINCE 2021
month0 = alldf[alldf.month >= "2021-01-01"].groupby("cardnumber", as_index = False).agg({"month": min})
month0.rename(columns = {"month": "month0"}, inplace = True)

# fill with 0s when there are no obs for that doc-month
df = df.set_index(["month","cardnumber"]).unstack(fill_value=0).stack().reset_index()


# add first month in the data
df = df.merge(month0,
         on = "cardnumber",
         how = "left")

# check that everyone has their first month
df.month0.isnull().sum()

# COMMAND ----------

# TRIP VARIABLES
# only code 0s since the first time they show up. Otherwise, code missings.
df.loc[(df.month < df.month0), 
       ['n_validaciones', 'n_trips', 'mean_value_trip'] ] = np.NaN # put NAN in trips
       
# check that those present after the policy change have all NaNs in their validaciones before the policy change
df.n_validaciones[(df.month0 >= "2023-02") & (df.month < "2023-02" )].unique()

# COMMAND ----------

# TREATMENT VARIABLES: 
# 1. FOWARD FILL OF 0s (they take the value of the last month for each person)
# 2. BACKWARD FILL OF 0s (for values at the beginning)

# turn 0s to NaN
df.treatment       = df.treatment.replace(0, np.NaN)
df.treatment_month = df.treatment_month.replace(0, np.NaN)

# super important to sort values
df = df.sort_values(["cardnumber","month"]).reset_index(drop=True)

df["treatment_month"] = df.groupby("cardnumber").treatment_month.ffill()
df["treatment_month"] = df.groupby("cardnumber").treatment_month.bfill()

df["treatment"] = df.groupby("cardnumber").treatment.ffill()
df["treatment"] = df.groupby("cardnumber").treatment.bfill()

# COMMAND ----------

print("% card-month with inconsistences", sum(df.treatment_month == "") / df.shape[0] * 100)
print("% card-month with 0 valid", np.mean(df.n_validaciones == 0) * 100 )

# COMMAND ----------

df.drop(columns = ["month0"], inplace = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plot

# COMMAND ----------

# Aggregate by day and profile
monthly_byt = df.groupby(["month", "treatment"], as_index = False).agg(
    {"cardnumber" :  "count",
     "n_validaciones": "mean",
     "n_trips": "mean"} 
    )  

# COMMAND ----------


#yvars = ['unique_cards', 'total_transactions', 'transactions_by_card']
#ylabs = ['unique cards', 'total transactions', 'transactions by card']
# for y, ylab in zip(yvars, ylabs):

yvars = [ 'n_validaciones']
ylabs = [ 'transactions by card']

tgroup =  [ 'gainedhas', 'hadkept', 'hadlost23']
tcolors =  ['green', 'blue', 'red', 'orange']

# Plots
for y, ylab in zip(yvars, ylabs):
    for t, tcolor in zip (tgroup, tcolors):
        fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (12, 5))
        fig.subplots_adjust(hspace = 0.4)

        sns.lineplot(x = monthly_byt.month[monthly_byt.treatment == t].astype("str") , 
                    y = monthly_byt.loc[monthly_byt.treatment == t, y],
                    label = t,
                    alpha = 0.8,
                    color = tcolor)

        sns.lineplot(x = monthly_byt.month[monthly_byt.treatment == 'adulto'].astype("str") , 
                    y = monthly_byt.loc[monthly_byt.treatment == 'adulto', y],
                    label = 'adulto',
                    alpha = 0.8,
                    color = "gray")


        ymin, ymax = axes.get_ylim()
        ydiff = (ymax-ymin)
        ylim = ymax - ydiff * 0.1
        axes.set_ylim(0, ymax)


        axes.axvline(x = "2023-02-01", color ='black')
        axes.text("2023-02-01", ylim, 'Policy change')
        xticks = plt.gca().get_xticks()

        plt.xlabel("Month")
        plt.ylabel(f"{ylab}")
        plt.title(f"WHOLEDATA SAMPLE (1% apoyo) - Monthy {ylab} by treatment (CODING 0s)")

        
        plt.legend()
        plt.grid()
        plt.xticks(xticks[::3]) 
        plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##  After variable
# MAGIC
# MAGIC - [TBC] variable t: months before or after treatment
# MAGIC - gainedhas people are treated since they signed up, the after variable for them is 1 since they first sign up

# COMMAND ----------

# AFTER

# after policy change
df["after"] = (df.month >= "2023-03-01") * 1

# after by person (varies for the gained has)
df = df.merge(gainedhas_s0, # add first month with the subsidy
              on = "cardnumber",
              how = "left")
print(df.month_s0[df.treatment == "gainedhas"].isnull().sum()) # should be 0

print(df.month_s0[df.treatment == "gainedhas"].isnull().sum()) # should be 0

# COMMAND ----------

df["after_p"] = df["after"]
df.loc[(df.treatment == "indata_neverelig") & (df.after == 1) , "after_p"] = 1
df.loc[(df.treatment == "gainedhas") & (df.month_s0.notnull()) & (df.month < df.month_s0) , "after_p"] = 0

# tab
pd.crosstab(df.after, df.after_p)

# COMMAND ----------

df.drop(columns = ["month_s0"], inplace = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save

# COMMAND ----------

df.to_csv(os.path.join(path, 'Workspace/Construct/panel_with_treatment.csv'), index = False)

# COMMAND ----------

dbutils.fs.cp(os.path.join(pathdb, 'Workspace/Construct/panel_with_treatment.csv'), "/FileStore/my-stuff/panel_with_treatment.csv")
#Download from: https://adb-6102124407836814.14.azuredatabricks.net/files/my-stuff/panel_with_treatment.csv

# COMMAND ----------

dbutils.fs.rm("/FileStore/my-stuff/panel_with_treatment.csv")

# COMMAND ----------


