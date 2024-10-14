# Databricks notebook source
# MAGIC %md
# MAGIC # Clean data
# MAGIC

# COMMAND ----------

# Directories
pathdb  = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/'
path = '/dbfs/' + pathdb
user = os.listdir('/Workspace/Repos')[0]
git = f'/Workspace/Repos/{user}/ColombiaTransMilenio'

# COMMAND ----------

!pip install tqdm
import sys
import os
import csv
from glob import iglob
from tqdm import tqdm 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create parquet files

# COMMAND ----------


#sys.path.append(os.path.abspath('./spark_code'))
#%run ./spark_code/install_import_packages
#%run ./spark_code/start_spark

# utilities
# generate variables

# COMMAND ----------

# MAGIC %tb 

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Since 2020
# MAGIC
# MAGIC Structure:
# MAGIC - Create datasets by by type or by month? 
# MAGIC   - we may continue to update this data

# COMMAND ----------


file_path = path + '/Workspace/Raw/since2020/ValidacionTroncal/'
files_dir = os.listdir(file_path)
print(len(files_dir))

headers = []
files = []
for filename in tqdm(files_dir):
    files.append(filename)
    try:
        with open(file_path + filename) as fin:
            csvin = csv.reader(fin)
            headers.append(next(csvin, []))
    except:
        with open(file_path + filename, encoding = 'latin1') as fin:
            csvin = csv.reader(fin)
            headers.append(next(csvin, []))

# COMMAND ----------

unique_headers = set(tuple(x) for x in headers) # this will make that each time it is run
print(len(unique_headers))

# COMMAND ----------

# See files
[f.name for f in  dbutils.fs.ls(raw + '/since2020')]


# COMMAND ----------

# See files
files = [f.name for f in  dbutils.fs.ls(raw + '/since2020/ValidacionTroncal/')]
fdates = [ int(f[-12:-4]) for f in files ]
print( min(fdates), max(fdates))

# COMMAND ----------



# COMMAND ----------

files[-1][-12:-4]
