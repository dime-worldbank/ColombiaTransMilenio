# Databricks notebook source
# MAGIC %md
# MAGIC # Plot
# MAGIC
# MAGIC - Monthly validaciones with and without coding 0s

# COMMAND ----------

# Modules
import os 
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

samplesize = "_sample10"
#samplesize = "_sample1"

# COMMAND ----------

pathdb  = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/'
path = '/dbfs/' + pathdb

# COMMAND ----------

cm = pd.read_csv(f'{path}Workspace/Construct/panel_with_treatment{samplesize}.csv')

# COMMAND ----------

# Variables without 0s
cm["n_validaciones_no0s"] = cm["n_validaciones"]
cm.loc[cm.n_validaciones == 0, "n_validaciones_no0s"] = np.nan 

cm["n_trips_no0s"] = cm["n_trips"]
cm.loc[cm.n_trips == 0, "n_trips_no0s"] = np.nan 

# COMMAND ----------


if samplesize == "_sample10":
    samplesize_lab = "10"

if samplesize == "_sample1":
    samplesize_lab = "1"

tsdef = ["treatment" ]

tgroup =  [ 'gainedhas', 'hadkept', 'hadlost23']
tcolors =  [ 'green', '#2986cc', '#cc5199']

for ts in tsdef:

    tot_adulto_cards = cm.cardnumber[cm[ts] == 'adulto'].nunique()

        # Aggregate by day and profile
    monthly = cm.groupby(["ymonth", ts], as_index = False).agg(
        {"cardnumber" :  "count",
        "n_validaciones": "mean",
        "n_trips": "mean",
        "n_validaciones_no0s": "mean",
        "n_trips_no0s": "mean"} 
        )  

    for t, tcolor in zip (tgroup, tcolors):
        fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (12, 5))
        fig.subplots_adjust(hspace = 0.4)

        tot_t_cards = cm.cardnumber[cm[ts] == t].nunique()

        sns.lineplot(x = monthly.ymonth[monthly[ts] == t] , 
                     y = monthly.loc   [monthly[ts] == t, "n_validaciones_no0s"],
                     label = f"{t} - total cards: {str(tot_t_cards)} - cond. on travelling"   ,
                     alpha = 0.8,
                     color = tcolor)


        sns.lineplot(x = monthly.ymonth[monthly[ts] == t] , 
                     y = monthly.loc   [monthly[ts] == t, "n_validaciones"],
                     label = f"coding 0s"   ,
                     alpha = 0.8,
                     color = tcolor,
                     linestyle="dashed")



        sns.lineplot(x = monthly.ymonth[monthly[ts] == 'adulto'] , 
                     y = monthly.loc   [monthly[ts] == 'adulto',  "n_validaciones_no0s"],
                     label = f'adulto - total cards: {tot_adulto_cards} - cond. on travelling',
                     alpha = 0.8,
                     color = "gray")



        sns.lineplot(x = monthly.ymonth[monthly[ts] == 'adulto'] , 
                     y = monthly.loc   [monthly[ts] == 'adulto',  "n_validaciones"],
                     label = f'coding 0s',
                     alpha = 0.8,
                     color = "gray",
                    linestyle="dashed")

        axes.set_ylim(0, 40)


        axes.axvline(x = "2023-02", color ='black')
        axes.text("2023-02", 3, 'Policy change')
        xticks = plt.gca().get_xticks()

        plt.xlabel("Month")
        plt.ylabel(f"Validaciones")
        
        if ts == "treatment_v2":
            plt.title(f"WHOLEDATA SAMPLE {samplesize_lab}% \n Monthy validaciones by treatment group")

        if ts == "treatment_v3":
            if t == "gainedhas":
                plt.title(f"WHOLEDATA SAMPLE {samplesize_lab}% \n Monthy validaciones by treatment group \n --- GAINED THAT WERE PRESENT BEFORE ---")
            else:
                 plt.title(f"WHOLEDATA SAMPLE {samplesize_lab}% \n Monthy validaciones by treatment group")

        if ts == "treatment_v4":
            if t == "gainedhas":
                plt.title(f"WHOLEDATA SAMPLE {samplesize_lab}% \n Monthy validaciones by treatment group \n --- ADULTO THAT WERE PRESENT BEFORE --- \n --- GAINED THAT WERE PRESENT BEFORE ---")
            else:
                 plt.title(f"WHOLEDATA SAMPLE {samplesize_lab}% \n Monthy validaciones by treatment group  \n --- ADULTO THAT WERE PRESENT BEFORE --- ")

        if ts == "treatment_v5":
            if t == "gainedhas":
                plt.title(f"WHOLEDATA SAMPLE {samplesize_lab}% \n Monthy validaciones by treatment group \n --- ADULTO THAT WERE PRESENT BEFORE & AFTER--- \n --- GAINED THAT WERE PRESENT BEFORE ---")
            else:
                 plt.title(f"WHOLEDATA SAMPLE {samplesize_lab}% \n Monthy validaciones by treatment group  \n --- ADULTO THAT WERE PRESENT BEFORE & AFTER --- ")

        if ts == "treatment_v5":
            if t == "gainedhas":
                plt.title(f"LINKED DATA \n Monthy validaciones by treatment group \n --- ADULTO THAT WERE PRESENT BEFORE & AFTER--- \n --- GAINED THAT WERE PRESENT BEFORE ---")
            else:
                 plt.title(f"LINKED DATA \n Monthy validaciones by treatment group  \n --- ADULTO THAT WERE PRESENT BEFORE & AFTER --- ")
                    
        
        if ts == "treatment_ba_v2":
            if t == "gainedhas":
                plt.title(f"WHOLEDATA SAMPLE {samplesize_lab}% \n Monthy validaciones by treatment group \n --- ADULTO PRESENT BEFORE & AFTER--- \n --- GAINED PRESENT AFTER ---")
            else:
                 plt.title(f"WHOLEDATA SAMPLE {samplesize_lab}% \n Monthy validaciones by treatment group  \n --- CARDS PRESENT BEFORE & AFTER --- ")
                    
        if ts == "treatment_ba":
                 plt.title(f"WHOLEDATA SAMPLE {samplesize_lab}% \n Monthy validaciones by treatment group  \n --- CARDS PRESENT BEFORE & AFTER --- ")

        plt.legend()
        plt.grid()
        plt.xticks(xticks[::2]) 
        plt.show()


# COMMAND ----------


if samplesize == "_sample10":
    samplesize_lab = "10"

if samplesize == "_sample1":
    samplesize_lab = "1"

tsdef = ["treatment" ]  # cards present before and after
tgroup =  [ 'gainedhas', 'hadkept', 'hadlost23']
tcolors =  [ 'green', '#2986cc', '#cc5199']

for ts in tsdef:

    tot_adulto_cards = cm.cardnumber[cm[ts] == 'adulto'].nunique()

        # Aggregate by day and profile
    monthly = cm.groupby(["ymonth", ts], as_index = False).agg(
        {"cardnumber" :  "count",
        "n_validaciones": "mean",
        "n_trips": "mean",
        "n_validaciones_no0s": "mean",
        "n_trips_no0s": "mean"} 
        )  

    for t, tcolor in zip (tgroup, tcolors):
        fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (12, 5))
        fig.subplots_adjust(hspace = 0.4)

        tot_t_cards = cm.cardnumber[cm[ts] == t].nunique()


        sns.lineplot(x = monthly.ymonth[monthly[ts] == t] , 
                     y = monthly.loc   [monthly[ts] == t, "n_trips"],
                     label = f"{t} - total cards: {str(tot_t_cards)} "   ,
                     alpha = 0.8,
                     color = tcolor,
                     linestyle="dashed")


        sns.lineplot(x = monthly.ymonth[monthly[ts] == 'adulto'] , 
                     y = monthly.loc   [monthly[ts] == 'adulto',  "n_trips"],
                     label = f'adulto - total cards: {tot_adulto_cards} ',
                     alpha = 0.8,
                     color = "gray",
                    linestyle="dashed")

        axes.set_ylim(0, 40)


        axes.axvline(x = "2023-02", color ='black')
        axes.text("2023-02", 3, 'Policy change')
        xticks = plt.gca().get_xticks()

        plt.xlabel("Month")
        plt.ylabel(f"Validaciones")
        
     
                    
        if ts == "treatment_ba":
                 plt.title(f"WHOLEDATA SAMPLE {samplesize_lab}% - Monthy trips by treatment group  -coding 0s ")

        plt.legend()
        plt.grid()
        plt.xticks(xticks[::4]) 
        plt.show()

