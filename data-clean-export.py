# Databricks notebook source
# MAGIC %pip install google-cloud-storage tqdm
# MAGIC
# MAGIC import os
# MAGIC from pathlib import Path
# MAGIC from shutil import rmtree
# MAGIC from tqdm import tqdm

# COMMAND ----------

dbutils.fs.ls('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/')

# COMMAND ----------

# List files in dbfs (Data Bricks File System)
dbutils.fs.ls('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data/')

# COMMAND ----------

dbutils.fs.ls('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data/ValidacionZonal')
dbutils.fs.mkdir('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Workspace')
dbutils.fs.mkdir('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Workspace/Clean')

# COMMAND ----------

# List files saved in the DAP SharePoint
dbutils.fs.ls('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Documents/')

# COMMAND ----------

files = dbutils.fs.ls('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Documents/Zonal2022')
len(files)

# COMMAND ----------

# See which is the local driver
clean_dir = "C:/Users/wb592445/WBG/Sveta Milusheva - Colombia Fare Experiment/Secondary Data and Datasets/Transmilenio/clean"
import os
os.chdir(clean_dir)
os.listdir()


# COMMAND ----------

os.listdir()

# COMMAND ----------

# MAGIC %sh ls

# COMMAND ----------

# MAGIC %sh  du -h /dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Documents

# COMMAND ----------

# MAGIC %fs ls mnt/DAP/data/ColombiaProject-TransMilenioRawData/Documents/

# COMMAND ----------

import pandas as pd
df = pd.read_csv('/dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data/ValidacionZonal/validacionZonal20230529.csv')
df

# COMMAND ----------

display(df)
