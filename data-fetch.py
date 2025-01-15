# Databricks notebook source
# MAGIC %pip install google-cloud-storage tqdm

# COMMAND ----------

import os
from pathlib import Path
from shutil import rmtree
from google.cloud import storage
from tqdm import tqdm

# COMMAND ----------

DATA_DIR = '/Volumes/prd_csc_mega/sColom15/vColom15/Data'
dbutils.fs.mkdirs(DATA_DIR) # Creates the given directory if it does not exist

# COMMAND ----------

# MAGIC %sh du -h /Volumes/prd_csc_mega/sColom15/vColom15/Data

# COMMAND ----------

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

# COMMAND ----------

bucket_name = 'validaciones_tmsa'
storage_client = storage.Client.create_anonymous_client()

blobs = storage_client.list_blobs(bucket_name)
# blobs includes files in nested folders
for blob in tqdm(blobs):
    # skip folders
    if blob.name.endswith("/"):
        continue

    target = f'{DATA_DIR}/{blob.name}'
    # Only download files that are not yet downloaded
    if not dbfs_file_exists(target):
        dbutils.fs.mkdirs(str(Path(target).parent))
        print(f'downloading to: {target}')
        blob.download_to_filename(filename=target, client=storage_client)

# COMMAND ----------

# MAGIC %sh du -h /Volumes/prd_csc_mega/sColom15/vColom15/Data
