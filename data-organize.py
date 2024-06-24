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

# Trying to export from DBFS to local machine
#dbutils.fs.put("/FileStore/my-stuff/my-file.txt", "This is the actual text that will be saved to disk. Like a 'Hello world!' example")

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
# MAGIC - Since 2020, I have data organized by Troncal, Zonal, Dual, Salidas, Recargas. All daily files in one of the following folders **path + '/Workspace/Raw/since2020/ + ...**
# MAGIC   - Recargas
# MAGIC   - Salidas
# MAGIC   - ValidacionDual
# MAGIC   - ValidacionTroncal
# MAGIC   - ValidacionZonal
# MAGIC
# MAGIC
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

f = "/dbfs" + ingestion2017_dir + "/decompressed/ValZonal Nov2017/16. valzonal_16nov2017/valzonal_16nov2017_ETIB.gz"
Archive( f ).extractall("/dbfs" + raw2017_dir )
df = pd.read_csv('/dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Workspace/Raw/2017/valzonal_16nov2017_ETIB')

# COMMAND ----------

# MAGIC %md
# MAGIC # Organize data since 2020

# COMMAND ----------

# MAGIC %md
# MAGIC Create a folder inside the Workspace folder that follows the same structure that the Data folder to put both the data in the Data folder and in the Documents folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion point: check that we have the right amount of files

# COMMAND ----------

dbutils.fs.ls('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Documents/')

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

names = [f[0][77:] for f in files]
# check for duplicates
rawnames = [n[:15] for n in names]
print(len(rawnames) == len(names))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Data folder structure

# COMMAND ----------

os.listdir('/dbfs' + path + '/Data/')

# COMMAND ----------

# os.listdir('/dbfs' + path + '/Data/Recargas/')
# os.listdir('/dbfs' + path + '/Data/Recargas/2023')

# COMMAND ----------

# os.listdir('/dbfs' + path + '/Data/ValidacionZonal')
# os.listdir('/dbfs' + path + '/Data/ValidacionZonal/2024')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create folders

# COMMAND ----------

raw2020_dir = path + '/Workspace/Raw/since2020/'
dbutils.fs.mkdirs(raw2020_dir)

# COMMAND ----------

for d in ['Recargas/', 'Salidas/', 'ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/']:
    dbutils.fs.mkdirs(raw2020_dir + d)

# COMMAND ----------

os.listdir('/dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Workspace/Raw/since2020/ValidacionZonal')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Move daily validaciones files
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Move from query point (Data folder)**

# COMMAND ----------


raw2020_dir = path + '/Workspace/Raw/since2020/'

# Validaciones
for d in ['ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/']:
    files = [f.name for f in dbutils.fs.ls(path + "/Data/" + d) ]
    vfiles = [f for f in files if 'validacion' in f]
    print(len(vfiles))
    
    for f in tqdm(vfiles):
        dbutils.fs.cp(path + "/Data/" + d + f, 
                      path + '/Workspace/Raw/since2020/'+ d + f)

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Move from ingestion point (Documents folder)**

# COMMAND ----------

dic_d =  {"Zonal2023/"  : 'ValidacionZonal/'    ,
          "Zonal2022/"  : 'ValidacionZonal/'    ,
          "Zonal2021/"  : 'ValidacionZonal/'    ,
          "Zonal2020/"  : 'ValidacionZonal/'    ,
          "Troncal2023/": 'ValidacionTroncal/'  ,
          "Troncal2022/": 'ValidacionTroncal/'  ,
          "Troncal2021/": 'ValidacionTroncal/'  ,
          "Troncal2020/": 'ValidacionTroncal/'  ,
          "Dual2023/"   : 'ValidacionDual/'     ,
          "Dual2022/"   : 'ValidacionDual/'     ,
          "Dual2021/"   : 'ValidacionDual/'     ,
          "Dual2020/"   : 'ValidacionDual/'     }

# COMMAND ----------

for d in [ "Zonal2023/"   ,
            "Zonal2022/"  ,
            "Zonal2021/"  ,
            "Zonal2020/"  ,
            "Troncal2023/",
            "Troncal2022/",
            "Troncal2021/",
            "Troncal2020/",
            "Dual2023/"   ,
            "Dual2022/"   ,
            "Dual2021/"   ,
            "Dual2020/"   ]:
   df = dic_d[d]
   files = [f.name for f in dbutils.fs.ls(path + "/Documents/" + d) ]
   vfiles = [f for f in files if 'validacion' in f]
   print(len(vfiles))
    
   for f in tqdm(vfiles):
              dbutils.fs.cp(path + "/Documents/" + d + f, 
                      path + '/Workspace/Raw/since2020/'+ df + f)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check number of files in final destination folder

# COMMAND ----------


