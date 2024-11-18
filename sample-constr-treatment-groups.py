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

dm.to_csv(os.path.join(path, 'Workspace/Construct/subsidybmonthdoc-2020toMar2024_sample.csv'), index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subsidy per card per month: plots & stats
# MAGIC - Plots of cards with subsidy per month.
# MAGIC - Having subsidy in any month versus card profile profiles

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
# MAGIC ## 2. Having the subsidy each period and final subsidy group
# MAGIC
# MAGIC Output `by_card`: dataset at the card level, long dataset at the card-period level (missing rows if a card is not present that period), stating whether the card have the subsidy each period and some extra info

# COMMAND ----------

dm = pd.read_csv(os.path.join(path, 'Workspace/Construct/subsidybymonthdoc-2020toMar2024_sample.csv'))

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


pd.crosstab(by_card_period.subsidy_month,by_card_period.subsidy_month_corrected)   

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

s.to_csv(os.path.join(path, 'Workspace/Construct/treatment_groups.csv'), index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Plot monthly validaciones (without coding 0s)

# COMMAND ----------

# Import data
df = pd.read_csv(os.path.join(path, 'Workspace/Construct/df_clean_relevant_sample.csv'))
groups = pd.read_csv(os.path.join(path, 'Workspace/Construct/treatment_groups.csv'))
df = df.merge(groups,
         on = "cardnumber",
         how = "left")

# Aggregate by day and profile
monthly_byt = df[(df.day >= "2022-01-01") & (df.day <= "2024-03-31")].groupby(["month", "treatment"], as_index = False).agg(
    {"cardnumber" : ["nunique", "count"]} 
    )  
monthly_byt.columns = ["month", "treatment", "unique_cards", "total_transactions"]
monthly_byt[ "transactions_by_card"] = monthly_byt.total_transactions / monthly_byt.unique_cards


## treatment v2
monthly_byt_v2 = df[(df.day >= "2022-01-01") & (df.day <= "2024-03-31")].groupby(["month", "treatment_v2"], as_index = False).agg(
    {"cardnumber" : ["nunique", "count"]} 
    )  
monthly_byt_v2.columns = ["month", "treatment", "unique_cards", "total_transactions"]
monthly_byt_v2[ "transactions_by_card"] = monthly_byt_v2.total_transactions / monthly_byt_v2.unique_cards  

# COMMAND ----------



yvars = [ 'transactions_by_card']
ylabs = [ 'transactions by card']

tgroup =  [ 'gainedhas', 'hadkept', 'hadlost23']
tcolors =  ['green', 'blue', 'red']

# Plots

tot_adulto_cards = s.cardnumber[s.treatment == 'adulto'].nunique()

for y, ylab in zip(yvars, ylabs):
    for t, tcolor in zip (tgroup, tcolors):
        fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (12, 5))
        fig.subplots_adjust(hspace = 0.4)

        tot_t_cards = s.cardnumber[s.treatment == t].nunique()

        sns.lineplot(x = monthly_byt.month[monthly_byt.treatment == t] , 
                    y = monthly_byt.loc[monthly_byt.treatment == t, y],
                    label = t + " - total cards: " + str(tot_t_cards)  ,
                    alpha = 0.8,
                    color = tcolor)

        sns.lineplot(x = monthly_byt.month[monthly_byt.treatment == 'adulto'] , 
                    y = monthly_byt.loc[monthly_byt.treatment == 'adulto', y],
                    label = f'adulto - total cards: {tot_adulto_cards}',
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
        plt.title(f"WHOLEDATA SAMPLE (1% apoyo) - Monthy {ylab} by treatment")

        
        plt.legend()
        plt.grid()
        plt.xticks(xticks[::3]) 
        plt.show()


# COMMAND ----------


#yvars = ['unique_cards', 'total_transactions', 'transactions_by_card']
#ylabs = ['unique cards', 'total transactions', 'transactions by card']
# for y, ylab in zip(yvars, ylabs):

yvars = [ 'transactions_by_card']
ylabs = [ 'transactions by card']

tgroup =  [  'hadkept_v2', 'hadlost23_v2']
tcolors =  [ '#2986cc', '#cc5199']

# Plots

tot_adulto_cards = s.cardnumber[s.treatment_v2 == 'adulto'].nunique()

for y, ylab in zip(yvars, ylabs):
    for t, tcolor in zip (tgroup, tcolors):
        fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (12, 5))
        fig.subplots_adjust(hspace = 0.4)

        tot_t_cards = s.cardnumber[s.treatment_v2 == t].nunique()

        sns.lineplot(x = monthly_byt_v2.month[monthly_byt_v2.treatment == t] , 
                    y = monthly_byt_v2.loc[monthly_byt_v2.treatment == t, y],
                    label = t + " - total cards: " + str(tot_t_cards)  ,
                    alpha = 0.8,
                    color = tcolor)

        sns.lineplot(x = monthly_byt_v2.month[monthly_byt_v2.treatment == 'adulto'] , 
                    y = monthly_byt_v2.loc[monthly_byt_v2.treatment == 'adulto', y],
                    label = f'adulto - total cards: {tot_adulto_cards}',
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
        plt.title(f"WHOLEDATA SAMPLE (1% apoyo) - Monthy {ylab} by treatment")

        
        plt.legend()
        plt.grid()
        plt.xticks(xticks[::3]) 
        plt.show()
