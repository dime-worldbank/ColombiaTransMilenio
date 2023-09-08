# Databricks notebook source
# MAGIC %pip install google-cloud-storage tqdm

# COMMAND ----------

import os
from pathlib import Path
from shutil import rmtree
from google.cloud import storage
from tqdm import tqdm

# COMMAND ----------

DATA_DIR = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data'
os.makedirs(DATA_DIR, exist_ok=True)
dbutils.fs.mkdirs(DATA_DIR)

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
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(filename=target, client=storage_client)

        # upload to dbfs, aka the enterprise data lake
        dbutils.fs.cp(f"file:{target}", f"dbfs:{target}")

        # remove file after uploading to dbfs to free up local fs space
        os.remove(target)
    else:
        print(f'{target} already exists, skipping')

# COMMAND ----------

# Check that files are uploaded to dbfs
dbutils.fs.ls('/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data/')

# COMMAND ----------

# Clean up empty local temp folders
rmtree(DATA_DIR)
