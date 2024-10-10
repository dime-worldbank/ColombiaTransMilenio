# Databricks notebook source
# MAGIC %md
# MAGIC # Clean data
# MAGIC

# COMMAND ----------

# Modules
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

# Directories
path  = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/'
raw   = path + '/Workspace/Raw/'
clean = path + '/Workspace/Clean/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Since 2020
# MAGIC
# MAGIC Structure:
# MAGIC - Datasets by month with all validaciones. The decision to do it by month is that we may continue to update this data, so we can run the process for each new month separately.

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
