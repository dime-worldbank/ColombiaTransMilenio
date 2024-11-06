# Databricks notebook source
# MAGIC %md
# MAGIC # Sample analysis
# MAGIC
# MAGIC In the sample, we have regular users at some point in Jan 2022 - July 2024:
# MAGIC - 4538 anonymous cards --> this will be excluded from the analysis as in our linked data we do not have purely anonymous cards 
# MAGIC - 4172 adulto cards --> this are the Never group. The challenge is that we cannot distinguish 
# MAGIC   - Who was never eligible and not vulnerable
# MAGIC   - Who was never eligible and vulnerable 
# MAGIC   - Who was eligible at some point and signed up
# MAGIC - 6179 apoyo that paid subsidy at some point in time ---> get a dataset on having the subsidy each month and each period
# MAGIC   - hadlost23: 1, 0, 0
# MAGIC   - hadlost24: 1, 1, 0
# MAGIC   - hadlost
# MAGIC
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

# Functions
import_test_function("Running hola.py works fine :)")
import_test_packages("Running packages.py works fine :)")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Import sample data

# COMMAND ----------


df = pd.read_csv(os.path.join(path, 'Workspace/Construct/df_clean_relevant_sample.csv'))
df.shape


# COMMAND ----------

df.day.max()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Subsidy per card per month
# MAGIC
# MAGIC Replicating the steps of https://github.com/dime-worldbank/Colombia-BRT-IE/blob/development/Fare%20Experiment%202022%20Project/Data%20Analysis/DataWork/Master%20Data/Jupyters/gcloud_linked_data_TM/constr_SurveyImpact-treatment-groups.ipynb as close as possible
# MAGIC
# MAGIC 1. Keep all data until month when Follow Up 1 survey finished (March 2024)
# MAGIC
# MAGIC 2. Discard Adulto Mayor and Discapacidad cards (already done because of the way the sample was built)
# MAGIC
# MAGIC 3. Discard transfers
# MAGIC
# MAGIC 4. Tag each validacion as being a subsidy transaction or not

# COMMAND ----------

df.columns

# COMMAND ----------

falldf = df[df.day < '2024-04-01'].reset_index(drop = True)
falldf.day.max()

# COMMAND ----------

# CORRECT  YEAR VARIABLE
falldf.year = pd.to_datetime(falldf.year).dt.year

# COMMAND ----------

# days with missing data
days_missing = ['2022-09-16', '2022-09-17', '2022-09-18', '2022-09-19',
       '2022-09-20', '2023-10-29', '2023-11-26', '2023-12-03',
       '2023-12-24', '2023-12-25', '2024-02-03', '2024-02-06',
       '2024-02-08', '2024-02-09', '2024-02-26']

# COMMAND ----------

print("Cards in sample:", falldf.cardnumber.nunique())
print("Total validaciones:", falldf.shape[0])
falldf = falldf[falldf.value > 200].reset_index(drop = True)
print("Total validaciones without transfers:", falldf.shape[0])
print("Cards in sample:", falldf.cardnumber.nunique())

# COMMAND ----------

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

# do it separately before and after 2022 because 2500 is repeated for subsidy in 2023 and full in 2020

## after 2022
falldf["subsidy"] = np.NaN
falldf.loc[(falldf.value.isin( price_subsidy_22 + price_subsidy_23) ) & # here we have subsidy 2023
           (falldf.day > "2022-01-31"), "subsidy"] = 1
falldf.loc[(falldf.value.isin( price_full_22    + price_full_23)    ) &
           (falldf.day > "2022-01-31"), "subsidy"] = 0


## before 2022
falldf.loc[(falldf.value.isin(price_subsidy_18 + price_subsidy_22) ) &
           (falldf.day <= "2022-01-31"), "subsidy"] = 1
falldf.loc[(falldf.value.isin(price_full_17 + price_full_18 + price_full_19 +
                              price_full_20 + price_full_22)    ) & # and here full 2020
                 (falldf.day <= "2022-01-31"), "subsidy"] = 0

print(np.sum(falldf.subsidy.isnull()))
print(np.mean(falldf.subsidy.isnull()) * 100)

# COMMAND ----------

# few weird values
falldf.loc[falldf.subsidy.isnull(), ["year", "value", "system", "profile_final"]].drop_duplicates()

# COMMAND ----------

# Get number and % of subsidy trips each month
dm = falldf.groupby(['cardnumber', 'profile_final',
                     'month'], as_index = False).agg({"subsidy": ["mean", "sum"]}).reset_index(drop = True)
dm.columns =  ['cardnumber','profile_final', 'month', 'subsidy_mean', 'subsidy_sum']

# subsidy that month rule
dm["subsidy_month"] = 0
dm.loc[dm.subsidy_mean >= 0.4, "subsidy_month" ] = 1 # 40% subsidy trips or more
dm.loc[dm.subsidy_sum  == 30,  "subsidy_month" ] = 1 # 30 subsidy trips 
print(dm.columns)
print(dm.subsidy_month.isnull().sum())

# COMMAND ----------

perc_subsidy =  dm[(dm.month > "2021-12") & (dm.profile_final == "apoyo_subsidyvalue")].groupby(["month",]).agg({"subsidy_month": lambda x: np.mean(x)*100})
perc_subsidy.subsidy_month.plot(title = "Percentage of Apoyo people with subsidy trips each month")
plt.xlabel("Month")
plt.ylabel("%")
#plt.xticks(np.arange(0, 28, 1))
plt.ylim(0, 100)
plt.grid()
plt.show()
