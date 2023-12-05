# Databricks notebook source
import os
from pathlib import Path
from shutil import rmtree
import pandas as pd

!pip install tqdm
import tqdm

!pip install pyunpack
!pip install patool
from pyunpack import Archive

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Workspace to transfer files from ingestion point and download point
# MAGIC - Ingestion point from OneDrive: /mnt/DAP/data/ColombiaProject-TransMilenioRawData/Documents
# MAGIC - Download point from TM Google API: /mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data

# COMMAND ----------

#  Create Workspace directory if it does not exist
dbutils.fs.mkdirs('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Workspace')
dbutils.fs.mkdirs('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Workspace/Raw')
dbutils.fs.mkdirs('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Workspace/Clean')

# COMMAND ----------

# MAGIC %md
# MAGIC Two different structures
# MAGIC - Since 2020, I have data organized by Troncal, Zonal, Dual, Salidas, Recargas
# MAGIC - For 2017, I have
# MAGIC   - Monthly csv files with all validations until September
# MAGIC   - Zonal and Troncal separate folders with daily files

# COMMAND ----------

# MAGIC %md
# MAGIC # Organize 2017 data

# COMMAND ----------

ingestion2017_dir = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Documents/2017data'
raw2017_dir = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Workspace/Raw/2017'

dbutils.fs.mkdirs(raw2017_dir)
files = dbutils.fs.ls(ingestion2017_dir)
files

# COMMAND ----------

decompressed =  [f.name for f in dbutils.fs.ls(ingestion2017_dir + "/decompressed") ]
print(decompressed)

# COMMAND ----------

 [f.name for f in dbutils.fs.ls(ingestion2017_dir + "/decompressed/ValTroncal Nov2017") ]

# COMMAND ----------

files7z = [f.name for f in files if ".7z" in f.name]
print(files7z)

# just Troncal December files can be extracted with patool
f = 'ValTroncal Dic2017.7z'
newdir = raw2017_dir + "/" + f[:-3]
dbutils.fs.mkdirs(newdir) # create dir if it does not exist
Archive( "/dbfs" + ingestion2017_dir  + "/" + f).extractall("/dbfs" + newdir )

# COMMAND ----------

f = 'ValZonal Dic2017.7z'
newdir = raw2017_dir + "/" + f[:-3]
dbutils.fs.mkdirs(newdir)
Archive( "/dbfs" + ingestion2017_dir  + "/" + f).extractall("/dbfs" + newdir )

# COMMAND ----------

dbutils.fs.ls(raw2017_dir)

# COMMAND ----------

dbutils.fs.ls(raw2017_dir + "/ValTroncal Dic2017")

# COMMAND ----------

# MAGIC %md
# MAGIC # Organize data since 2020

# COMMAND ----------

folders = ["Zonal2023",
           "Zonal2022",
           "Zonal2021",
           "Zonal2020",
           "Troncal2023",
           "Troncal2022",
           "Troncal2021",
           "Troncal2020",
           "Dual2023",
           "Dual2022",
           "Dual2021",
           "Dual2020",
           "salidas2023"]

for f in folders:
    files = dbutils.fs.ls('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Documents/' + f)
    print(f, "-", len(files))

# COMMAND ----------

df = pd.read_csv('/dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data/ValidacionZonal/validacionZonal20230529.csv')
dftest = df.head(20)
# dftest.to_csv('/dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Documents/test.csv')

# COMMAND ----------

dbutils.fs.ls('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Documents/')
