# Databricks notebook source
import os
from pathlib import Path
from shutil import rmtree
import pandas as pd

!pip install tqdm
from tqdm import tqdm

!pip install pyunpack
!pip install patool
from pyunpack import Archive



# COMMAND ----------

path = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/'

# COMMAND ----------

subsidy_cards = set()

for d in ['ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/']:
    folder = path +  '/Workspace/Raw/since2020/' + d
    files = [f.name for f in dbutils.fs.ls(folder) ]

    files2024 = [f for f in files if '2024' in f[-12:-8]]

    for f in tqdm(files2024):
        df = pd.read_csv("/dbfs/"+ folder + f)
        scards = set(df.Numero_Tarjeta[df.Valor.isin([2250, 2500])])
        subsidy_cards = subsidy_cards.union(scards)

    print("After ", d, ":", len(subsidy_cards), "unique subsidy cards")


# COMMAND ----------


