# Databricks notebook source
# MAGIC %md
# MAGIC # Reorganize data - from Data folder
# MAGIC Moves to Workspace/Raw folder all files from the Download Point (Data folder).

# COMMAND ----------

V_DIR = '/Volumes/prd_csc_mega/sColom15/vColom15/'
dbutils.fs.ls(V_DIR)

# COMMAND ----------

def get_volume_size_tb(path):
    total_bytes = 0
    files = dbutils.fs.ls(path)
    for file in files:
        if file.isDir():
            total_bytes += get_volume_size_tb(file.path)  # recursivo
        else:
            total_bytes += file.size
    return total_bytes / (1024**4)  # Bytes → Terabytes

# Ejemplo de uso

path = V_DIR +"/Workspace/Raw/since2020/ValidacionZonal/"
size_tb = get_volume_size_tb(path)
print(f"El volumen ocupa aproximadamente {size_tb:.4f} TB")

# COMMAND ----------

import os
files = os.listdir( V_DIR + "Documents")
tot = 0
for f in files :
    path = V_DIR + "Documents/" + f
    size_tb = get_volume_size_tb(path)
    a = size_tb
    tot +=a
print(f"El volumen ocupa aproximadamente {tot} TB")

# COMMAND ----------

import os
#files = [ 'ValidacionZonal', 'ValidacionCable', 'ValidacionTroncal', 'ValidacionDual', 'Recargas']
files = os.listdir( V_DIR + "Data")

tot = 0
for f in files :
    path = V_DIR + "Data/" + f
    size_tb = get_volume_size_tb(path)
    a = size_tb
    tot +=a
print(f"El volumen ocupa aproximadamente {tot} TB")

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
import patoolib

!pip install rarfile
import rarfile


# Directories
V_DIR = '/Volumes/prd_csc_mega/sColom15/vColom15/'

# Functions

# Check if the given file path already exists on DBFS
def dbfs_file_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise
  

# Validaciones
for d in ['ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/', 'ValidacionCable/']:
    files = [f.name for f in dbutils.fs.ls(V_DIR + "/Data/" + d) ]
    vfiles = [f for f in files if 'validacion' in f]
    print(len(vfiles))
    
    for f in tqdm(vfiles):
        origin = f'{V_DIR}/Data/{d}{f}'
        target = f'{V_DIR}/Workspace/Raw/since2020/{d}{f}'
        # Only copy new files
        if not dbfs_file_exists(target):
            dbutils.fs.cp(origin, target)
            print(f'{target} COPIED')
