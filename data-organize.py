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

# MAGIC %md
# MAGIC # Create Workspace to transfer files from ingestion point and download point
# MAGIC - Ingestion point from OneDrive: /mnt/DAP/data/ColombiaProject-TransMilenioRawData/Documents
# MAGIC - Download point from TM Google API: /mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data

# COMMAND ----------

path = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/'

# COMMAND ----------

#  Create Workspace directory if it does not exist
dbutils.fs.mkdirs(path + '/Workspace/')
dbutils.fs.mkdirs(path + '/Workspace/Raw/')
dbutils.fs.mkdirs(path + '/Workspace/Clean/')

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
[f.name for f in files]

# COMMAND ----------

# MAGIC %md
# MAGIC **Moving individual files from Oct, Nov, and Dec 2017 to Raw/2017 folder:**
# MAGIC - Troncal Dec:  extracted from 7z with patool from .7z file
# MAGIC - Troncal and Zonal Oct, Zonal Dec: moved from decompressed individual folders
# MAGIC - Troncal and Zonal Nov: extract from decompressed folder, but using patool
# MAGIC   - _Note: valzonal_27nov2017_MCKENNEDY.gz is corrupted and cannot be extracted_ 

# COMMAND ----------

# MAGIC %md
# MAGIC **[TBC: check that we have the right amount of files]**

# COMMAND ----------

# Troncal December files can be extracted with patool
f = 'ValTroncal Dic2017.7z'
Archive( "/dbfs" + ingestion2017_dir  + "/" + f).extractall("/dbfs" + raw2017_dir )

# COMMAND ----------

# Take the others from the decompressed folder
decompressed =  [f.name for f in dbutils.fs.ls(ingestion2017_dir + "/decompressed") ]
print(decompressed)

for folder in decompressed:
    print("---------------")
    print( folder, ":")
    print([f.name for f in dbutils.fs.ls(ingestion2017_dir + "/decompressed/" + folder) ])

# COMMAND ----------

# All but november's can be directly moved
for folder in ['ValTroncal Oct2017/', 'ValZonal Dic2017/', 'ValZonal Oct2017/']:
    files = [f.name for f in dbutils.fs.ls(ingestion2017_dir + "/decompressed/" + folder) ]
    for f in tqdm(files):
        dbutils.fs.cp(ingestion2017_dir + "/decompressed/" + folder + f, raw2017_dir)

# COMMAND ----------

for folder in ['ValTroncal Nov2017/', 'ValZonal Nov2017/']:
    d = "/dbfs" + ingestion2017_dir + "/decompressed/" + folder
    subfolders = [d + f for f in os.listdir(d) ]
    for fd in tqdm(subfolders):
        files = os.listdir(fd)
        for f in files:
            if f == 'valzonal_27nov2017_MCKENNEDY.gz': # the file is corrupted and cannot be extracted
                pass
            else:
                Archive( fd + "/" + f ).extractall("/dbfs" + raw2017_dir )
    

# COMMAND ----------

len(dbutils.fs.ls(raw2017_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC # Organize data since 2020

# COMMAND ----------

# MAGIC %md
# MAGIC Create a folder inside the Workspace folder that follows the same structure that the Data folder to put both the data in Data folder and in the Documents folder.

# COMMAND ----------



# COMMAND ----------

os.listdir('/dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data/Recargas/')

# COMMAND ----------

os.listdir('/dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data/Recargas/2023')

# COMMAND ----------

os.listdir('/dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data/Recargas/2023')

# COMMAND ----------

os.listdir('/dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data/Recargas')

# COMMAND ----------

raw2020_dir = path + '/Workspace/Raw/since2020/'
dbutils.fs.mkdirs(raw2020_dir)

# COMMAND ----------

for d in ['Recargas/', 'Salidas/', 'ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/']:
    dbutils.fs.mkdirs(raw2020_dir + d)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check that we have the right amount of files**

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


