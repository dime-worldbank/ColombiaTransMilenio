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

# days with missing data
days_missing = ['2022-09-16', '2022-09-17', '2022-09-18', '2022-09-19',
       '2022-09-20', '2023-10-29', '2023-11-26', '2023-12-03',
       '2023-12-24', '2023-12-25', '2024-02-03', '2024-02-06',
       '2024-02-08', '2024-02-09', '2024-02-26']

df = pd.read_csv(os.path.join(path, 'Workspace/Construct/df_clean_relevant_sample.csv'))

print(df.profile_final.unique())
print(df.shape)
print(df.day.max())
print(df.columns)

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

df.columns

# COMMAND ----------

falldf = df[(df.day < '2024-04-01')].reset_index(drop = True)
falldf.day.max()

# COMMAND ----------

# CORRECT  YEAR VARIABLE
falldf.year = pd.to_datetime(falldf.year).dt.year

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

# COMMAND ----------

# few weird values
print(np.mean(falldf.subsidy[falldf.profile_final == "anonymous"].isnull()) * 100)
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

dm.to_csv(os.path.join(path, 'Workspace/Construct/subsidybymonthdoc-2020toMar2024_sample.csv'), index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subsidy in any month versus profiles

# COMMAND ----------

dm = pd.read_csv(os.path.join(path, 'Workspace/Construct/subsidybymonthdoc-2020toMar2024_sample.csv'))

# COMMAND ----------

fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (10, 5))
fig.subplots_adjust(hspace = 0.4)

perc_subsidy =  dm[(dm.month > "2021-12-01") & (dm.profile_final == "apoyo_subsidyvalue")].groupby(["month",]).agg({"subsidy_month": lambda x: np.mean(x)*100})
perc_subsidy.subsidy_month.plot(title = "Percentage of cards with subsidy trips over Apoyo* cards each month (2022 - march 2024)")
plt.xlabel("Month")
plt.ylabel("%")
plt.figtext(0.5, -0.05, "*Apoyo paying subsidy values anytime on 2022 - july 2024", ha="center", fontsize=8, color="black")

plt.ylim(0, 100)
plt.grid()
plt.show()

# COMMAND ----------

subsidy_anymonth = dm[(dm.month > "2021-12-31") & (dm.month < "2024-04-01")].groupby("cardnumber", as_index = False).agg({"subsidy_month" : "max"})
subsidy_anymonth.columns = ["cardnumber", "subsidy_anymonth"]
subsidy_anymonth = dm.merge(subsidy_anymonth,
                            on = "cardnumber",
                            how = "left")
fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (10, 5))
fig.subplots_adjust(hspace = 0.4)

perc_subsidy =  subsidy_anymonth[ (dm.month > "2021-12-31") & (dm.month < "2024-04-01") & (subsidy_anymonth.subsidy_anymonth == 1)].groupby(["month"]).agg({"subsidy_month": lambda x: np.mean(x)*100})
perc_subsidy.subsidy_month.plot(title =  "NON-LINKED DATA  \n Percentage of CARDS with subsidy trips each month \n over CARDS travelling that month that had the subsidy ANY month Jan22-Mar24")
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
# MAGIC ## 2. Having the subsidy each period
# MAGIC
# MAGIC TO DO: clean this code to have better names and be more compact! bydoc is bydocperiod, for instance

# COMMAND ----------

dm = pd.read_csv(os.path.join(path, 'Workspace/Construct/subsidybymonthdoc-2020toMar2024_sample.csv'))

# COMMAND ----------

# People present a year before the policy change (Jan 2022 - Jan 2023)
docsJan22Jan23 = set(dm.cardnumber[(dm.month >= "2022-01")  & (dm.month <= "2023-01")])
len(docsJan22Jan23)
dm["Jan22Jan23"] = dm.cardnumber.isin(docsJan22Jan23 )

# COMMAND ----------

# Tag periods
dm["period"] = np.NaN
dm.loc[(dm.month >= "2022-01-01")  & (dm.month <= "2023-01-31"), "period"] = 0
dm.loc[(dm.month >=  "2023-03-01")  & (dm.month <=  "2023-12-31"), "period"] = 1
dm.loc[(dm.month >=  "2024-01-01")  & (dm.month <=  "2024-03-31"), "period"] = 2
dm.period.value_counts()

# COMMAND ----------

print("Month not considered for the classification")
print(np.sum(dm.period.isnull()))
print(dm.month[dm.period.isnull()].unique())

# COMMAND ----------

print("Cards present a year before the policy change:", 
      dm.cardnumber[dm.period == 0].nunique())
print("Cards present March-Dec 2023:", 
       dm.cardnumber[dm.period == 1].nunique())
print("Cards present Jan-Mar 2024:", 
       dm.cardnumber[dm.period == 2].nunique())
print("Cards present a year before the policy change, not in March-Dec 2023:", 
       len(set(  dm.cardnumber[dm.period == 0] ).difference(dm.cardnumber[dm.period == 1]) ))
print("Cards present a year before the policy change, not in Jan-Mar 2024:", 
       len(set(  dm.cardnumber[dm.period == 0] ).difference(dm.cardnumber[dm.period == 2]) ))

# COMMAND ----------

# dataset with last month in each period 
last = dm.groupby(["period","cardnumber"], as_index = False).tail(1)

# COMMAND ----------

# Prepare data for corrections
# % of months 
check =  dm.groupby(["period","cardnumber"],
                    as_index = False).agg({"subsidy_month":  ["mean","sum"],
                                           "month": ["nunique", "min", "max"]})
check.columns = ["period", "cardnumber",
                  "mean_m_subsidy", "sum_m_subsidy",
                  "n_months", "min_month", "max_month"]

check["perc_m_subsidy"] = check.mean_m_subsidy * 100

# dataset by doc and period with all info
bydoc = last.merge(check,
                   on = ["period","cardnumber"],
                   how = "left")

bydoc["subsidy_month_corrected"] = bydoc.subsidy_month

# COMMAND ----------

# 1.  JAN 22- JAN 23: Based on % of months with or without subsidy in the period

bydocbef = bydoc[bydoc.period == 0].reset_index(drop = True)

print("% of months with subsidy for those WITH subsidy in the last month:") 
print("- People with less than 100%:",
      np.sum(bydocbef.perc_m_subsidy[bydocbef.subsidy_month == 1] < 100),
      np.mean(bydocbef.perc_m_subsidy[bydocbef.subsidy_month == 1] < 100) * 100)
print("- People with less than 50%:",
      np.sum(bydocbef.perc_m_subsidy[bydocbef.subsidy_month == 1] < 50),
      np.mean(bydocbef.perc_m_subsidy[bydocbef.subsidy_month == 1] < 50) * 100)

print("---")
print("% of months with subsidy for those WITHOUT subsidy in the last month:") 
print("- People with any month:",
      np.sum(bydocbef.perc_m_subsidy[bydocbef.subsidy_month == 0] > 0),
      np.mean(bydocbef.perc_m_subsidy[bydocbef.subsidy_month == 0] > 0) * 100)
print("- People with more than 50%:",
      np.sum(bydocbef.perc_m_subsidy[bydocbef.subsidy_month == 0] > 50),
      np.mean(bydocbef.perc_m_subsidy[bydocbef.subsidy_month == 0] > 50)* 100)

# COMMAND ----------

# Those without subsidy in the last month before the policy change but more than 50% of months: they will be considered as having the subsidy before the policy change.

bydoc.loc[ (bydoc.period == 0) &
           (bydoc.perc_m_subsidy > 50) &
           (bydoc.subsidy_month == 0), 
          "subsidy_month_corrected" ] = 1

# COMMAND ----------

bydoc = bydoc[['cardnumber', 'period', 
               'subsidy_month', 'subsidy_month_corrected', 
               'sum_m_subsidy',  'mean_m_subsidy', 'perc_m_subsidy']]

# COMMAND ----------

pd.crosstab(bydoc.subsidy_month, bydoc.subsidy_month_corrected)

# COMMAND ----------

bydoc.to_csv(os.path.join(path, 'Workspace/Construct/subsidybyperioddoc.csv'), index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Final subsidy group

# COMMAND ----------

bydoc = pd.read_csv(os.path.join(path, 'Workspace/Construct/subsidybyperioddoc.csv'))

# COMMAND ----------

# add subsidy in each period
s = bydoc.loc[bydoc.period == 0, ['cardnumber', 'subsidy_month_corrected']]
s.rename(columns = {"subsidy_month_corrected": "subsidy_t0"}, inplace = True)
s = s.merge(bydoc.loc[bydoc.period == 1, ['cardnumber', 'subsidy_month_corrected']],
            how = "left",
            on = "cardnumber")
s.rename(columns = {"subsidy_month_corrected": "subsidy_t1"}, inplace = True)
s = s.merge(bydoc.loc[bydoc.period == 2, ['cardnumber', 'subsidy_month_corrected']],
            how = "left",
            on = "cardnumber")
s.rename(columns = {"subsidy_month_corrected": "subsidy_t2"}, inplace = True)

# COMMAND ----------

#hadlost23
s['hadlost23'] = (s.subsidy_t0 == 1) & (s.subsidy_t1 == 0) 

#hadlost24
s['hadlost24'] =  (s.subsidy_t0 == 1) & (s.subsidy_t1 == 1) & (s.subsidy_t2 == 0)

#hadkept
s['hadkept'] = (s.subsidy_t0 == 1) &  (s.subsidy_t1 == 1) & (s.subsidy_t2 == 1)

# newly
s['gainedhas'] = (s.subsidy_t0 == 0)  & (s.subsidy_t2 == 1) 
s.loc[ (s.subsidy_t0.isnull()) &  (s.subsidy_t2 == 1), 'gainedhas'] = True
s.loc[ (s.subsidy_t0.isnull()) &  (s.subsidy_t1 == 1) & (s.subsidy_t2.isnull()), 'gainedhas'] = True

# missings
s['had_missing'] =  (s.subsidy_t0 == 1)  & (s.subsidy_t1.isnull()) 
s['had_had_missing'] =  (s.subsidy_t0 == 1)  & (s.subsidy_t1 == 1) & (s.subsidy_t2.isnull()) 



# COMMAND ----------

categ =  ["hadlost23", "hadlost24", "hadkept", "gainedhas", "had_missing", "had_had_missing"]
s[categ].sum()

# check whether they are mutually exclusive
s[categ].sum(axis = 1).unique()

# COMMAND ----------

# to dummies instead of booleans and add a categorical variable
s[categ] = s[categ] * 1
s["treatment"] = ""
for v in categ:
    s.loc[s[v] == 1, "treatment"] = v
    s.drop(columns = [v], inplace = True)
s.treatment.value_counts()

# COMMAND ----------

s.columns

# COMMAND ----------

groups = pd.merge(subsidy_anymonth[["cardnumber", "profile_final", "subsidy_anymonth"]],
                  s,
                  on = "cardnumber",
                  how = "left")
                  

# COMMAND ----------

pd.crosstab(groups.profile_final, groups.treatment)

# COMMAND ----------

pd.crosstab(groups.profile_final, groups.subsidy_anymonth)

# COMMAND ----------

# no treatment for those anonymous
groups.loc[groups.profile_final == "anonymous", "treatment"] = "anonymous"
groups.loc[groups.profile_final == "adulto", "treatment"] = "adulto"
pd.crosstab(groups.profile_final, groups.treatment)

# COMMAND ----------

groups.to_csv(os.path.join(path, 'Workspace/Construct/treatment_groups.csv'), index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plot monthly validaciones

# COMMAND ----------

groups = pd.read_csv(os.path.join(path, 'Workspace/Construct/treatment_groups.csv'))

# COMMAND ----------

# days with missing data
days_missing = ['2022-09-16', '2022-09-17', '2022-09-18', '2022-09-19',
       '2022-09-20', '2023-10-29', '2023-11-26', '2023-12-03',
       '2023-12-24', '2023-12-25', '2024-02-03', '2024-02-06',
       '2024-02-08', '2024-02-09', '2024-02-26']

df = pd.read_csv(os.path.join(path, 'Workspace/Construct/df_clean_relevant_sample.csv'))

print(df.profile_final.unique())
print(df.shape)
print(df.day.max())
print(df.columns)

# COMMAND ----------

df = df.merge(groups,
         on = "cardnumber",
         how = "left")


# COMMAND ----------

df.columns

# COMMAND ----------

# Aggregate by day and profile
monthly_byt = df[(df.day >= "2022-01-01") & (df.day <= "2024-03-31")].groupby(["month", "treatment"], as_index = False).agg(
    {"cardnumber" : ["nunique", "count"]} 
    )  
monthly_byt.columns = ["month", "treatment", "unique_cards", "total_transactions"]
monthly_byt[ "transactions_by_card"] = monthly_byt.total_transactions / monthly_byt.unique_cards

# COMMAND ----------

monthly_byt.treatment.unique()

# COMMAND ----------


    yvars = ['unique_cards', 'total_transactions', 'transactions_by_card']
    ylabs = ['unique cards', 'total transactions', 'transactions by card']



    # Plots

    for y, ylab in zip(yvars, ylabs):
        fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (20, 5))
        fig.subplots_adjust(hspace = 0.4)

        for t in [ 'gainedhas', 'hadkept', 'hadlost23', 'hadlost24']:
            sns.lineplot(x = monthly_byt.month[monthly_byt.treatment == t] , 
                         y = monthly_byt.loc[monthly_byt.treatment == t, y],
                         label = t,
                         alpha = 0.8)

        ymin, ymax = axes.get_ylim()
        ydiff = (ymax-ymin)
        ylim = ymax - ydiff * 0.1
        axes.set_ylim(0, ymax)


        axes.axvline(x = "2023-02-01", color ='black')
        axes.text("2023-02-01", ylim, 'Policy change')
        xticks = plt.gca().get_xticks()

        plt.xlabel("Month")
        plt.ylabel(f"{ylab}")
        plt.title(f"Monthy {ylab} by treatment")

        
        plt.legend()
        plt.grid()
        plt.xticks(xticks[::2]) 
        plt.show()


# COMMAND ----------

# monthly validaciones per card
