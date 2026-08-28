# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Exploring catalog
# MAGIC To understand where data is stored

# COMMAND ----------

from collections import defaultdict
import os

# COMMAND ----------

V_DIR = '/Volumes/prd_csc_mega/sColom15/vColom15/' # Unity Catalog Volume Usage Directory
dbutils.fs.ls(V_DIR) # option 1: use dbutils commands


# COMMAND ----------

# MAGIC %md
# MAGIC ## Documents folder

# COMMAND ----------

os.listdir(V_DIR+ "/Documents") # option 2: use os commands

# COMMAND ----------

os.listdir(V_DIR+ "/Documents/2019data") # option 2: use os commands

# COMMAND ----------

os.listdir(V_DIR+ "/Documents/2018data") # option 2: use os commands

# COMMAND ----------

os.listdir(V_DIR+ "/Documents/2018data/rar_decompressed") # option 2: use os commands

# COMMAND ----------

os.listdir(V_DIR+ "/Documents/2016data") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data folder: where we periodically download data 

# COMMAND ----------

os.listdir(V_DIR+ "/Data") # option 2: use os commands

# COMMAND ----------

zonalfiles_data = os.listdir(V_DIR+ "/Data" +"/ValidacionZonal") # daily files with validaciones are saved in ValidacionZonal
print(zonalfiles_data[:10])
print(zonalfiles_data[-10:])

# COMMAND ----------

os.listdir(V_DIR+ "/Data" +"/ValidacionZonal/2026") # the yearly folders in ValidacionZonal have aggregate data

# COMMAND ----------

# MAGIC %md
# MAGIC ## The workspace folder: were we move data to reorganize and start the cleaning process

# COMMAND ----------

os.listdir(V_DIR+ "/Workspace")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw

# COMMAND ----------

os.listdir(V_DIR+ "/Workspace/Raw")

# COMMAND ----------

os.listdir(V_DIR+ "/Workspace/Raw/Recharges")

# COMMAND ----------

# MAGIC %md
# MAGIC #### /from2016to2019

# COMMAND ----------

files1619 = os.listdir(V_DIR+ "/Workspace/Raw/from2016to2019")

# COMMAND ----------

files1619[:20]

# COMMAND ----------

files1619[-10:]

# COMMAND ----------

[x for x in files1619 if "2016" in x]

# COMMAND ----------

files17 = [x for x in files1619 if "2017" in x]
len(files17)

# COMMAND ----------

files17[:10]

# COMMAND ----------

[f for f in files17 if "Jul2017" in f]

# COMMAND ----------

[f for f in files17 if "201707" in f]

# COMMAND ----------

entry_path = os.path.join(V_DIR, "Workspace/Raw/from2016to2019")

top_level = os.listdir(entry_path)

subfolders = [item for item in top_level if os.path.isdir(os.path.join(entry_path, item))]
loose_files = [item for item in top_level if os.path.isfile(os.path.join(entry_path, item))]

loose_file_types = sorted(set(
    os.path.splitext(f)[1].lower().strip('.')
    for f in loose_files
    if os.path.splitext(f)[1]
))

subfolder_file_types = defaultdict(set)
for subfolder in subfolders:
    subfolder_path = os.path.join(entry_path, subfolder)
    for file in os.listdir(subfolder_path):
        if os.path.isfile(os.path.join(subfolder_path, file)):
            ext = os.path.splitext(file)[1].lower().strip('.')
            if ext:
                subfolder_file_types[subfolder].add(ext)

print(f"Subfolders: {len(subfolders)}")
print(f"Loose files: {len(loose_files)}")
print(f"Loose file types: {loose_file_types}")
print(f"\nFile types per subfolder:")
for subfolder, types in subfolder_file_types.items():
    print(f"  {subfolder}: {sorted(types)}")


# COMMAND ----------

import os
from collections import Counter

entry_path = "/Volumes/prd_csc_mega/sColom15/vColom15/Workspace/Raw/since2020"

# Count ALL files (loose + subfolders) by extension
loose_exts = Counter()
subfolder_exts = Counter()
subfolder_file_count = 0

for fname in os.listdir(entry_path):
    fpath = os.path.join(entry_path, fname)
    if os.path.isfile(fpath):
        ext = os.path.splitext(fname)[1].lower()
        loose_exts[ext] += 1
    elif os.path.isdir(fpath):
        for inner_fname in os.listdir(fpath):
            inner_fpath = os.path.join(fpath, inner_fname)
            if os.path.isfile(inner_fpath):
                ext = os.path.splitext(inner_fname)[1].lower()
                subfolder_exts[ext] += 1
                subfolder_file_count += 1

print("=== Loose files by extension ===")
for ext, count in sorted(loose_exts.items(), key=lambda x: -x[1]):
    print(f"  {ext:8s}: {count}")
print(f"  {'TOTAL':8s}: {sum(loose_exts.values())}")

print(f"\n=== Subfolder files by extension ===")
for ext, count in sorted(subfolder_exts.items(), key=lambda x: -x[1]):
    print(f"  {ext:8s}: {count}")
print(f"  {'TOTAL':8s}: {subfolder_file_count}")

# Total Files and files that need further inspection
cell14_loose = sum(v for k, v in loose_exts.items() if k in ('.csv', '.txt', '.zip', '.gz'))
cell14_sub = sum(v for k, v in subfolder_exts.items() if k in ('.csv', '.txt', '.zip', '.gz'))
cell14_total = cell14_loose + cell14_sub

# What's excluded
excluded_loose = sum(v for k, v in loose_exts.items() if k not in ('.csv', '.txt', '.zip', '.gz'))
print(f"\n=== Non-conventional ===")
excluded_detail = {k: v for k, v in loose_exts.items() if k not in ('.csv', '.txt', '.zip', '.gz')}
for ext, count in sorted(excluded_detail.items(), key=lambda x: -x[1]):
    print(f"  {ext:8s}: {count}")
print(f"  {'TOTAL':8s}: {excluded_loose}")

print(f"\n=== Grand total all files: {sum(loose_exts.values()) + subfolder_file_count} ===")
print(f"  Standard:  {cell14_total}")
print(f"  To inspect:  {sum(loose_exts.values()) + subfolder_file_count - cell14_total}")

# COMMAND ----------

# DBTITLE 1,Inspect Files with no extension
import chardet

# What are these 658 files with no extension?
no_ext_files = []
for fname in os.listdir(entry_path):
    fpath = os.path.join(entry_path, fname)
    if os.path.isfile(fpath) and os.path.splitext(fname)[1] == '':
        no_ext_files.append(fpath)

print(f"Files with no extension: {len(no_ext_files)}")
print(f"\nSample file names (first 15):")
for f in sorted(no_ext_files)[:15]:
    print(f"  {os.path.basename(f)}")

# Check what's inside a few of them
print("\n=== Content inspection (first 3 files) ===")
for fpath in sorted(no_ext_files)[:3]:
    size = os.path.getsize(fpath)
    with open(fpath, 'rb') as f:
        raw = f.read(512)
    det = chardet.detect(raw)
    line = raw.decode(det.get('encoding') or 'latin-1', errors='replace').split('\n')[0]
    print(f"\n  File: {os.path.basename(fpath)} ({size:,} bytes)")
    print(f"  Encoding: {det.get('encoding')}")
    print(f"  First line: {line[:200]}")

# COMMAND ----------

os.listdir(os.path.join(V_DIR, "Workspace/Raw/from2016to2019/VALTRONCAL_30-06-2018"))

# COMMAND ----------

os.listdir(os.path.join(V_DIR, "Workspace/Raw/from2016to2019/2019ER10074"))

# COMMAND ----------

# Given heatmap in data-clean-silverfrom2016to2019, try to find data for the missing months. 
items_2017 = [name for name in os.listdir(os.path.join(V_DIR, "Workspace/Raw/from2016to2019")) if "2017" in name ]
for item in items_2017:
    print(item)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Since 2020

# COMMAND ----------

os.listdir(V_DIR+ "/Workspace/Raw/since2020")

# COMMAND ----------

print(os.listdir(V_DIR+ "/Workspace/Raw/since2020/ValidacionZonal")[:10])
print(os.listdir(V_DIR+ "/Workspace/Raw/since2020/ValidacionZonal")[-10:])

# COMMAND ----------

base_path = os.path.join(V_DIR, "Workspace/Raw/since2020")
folders = ['ValidacionCable', 'ValidacionDual', 'ValidacionTroncal', 'ValidacionZonal']

for folder in folders:
    folder_path = os.path.join(base_path, folder)
    contents = os.listdir(folder_path)
    
    subfolders = [item for item in contents if os.path.isdir(os.path.join(folder_path, item))]
    loose_files = [item for item in contents if os.path.isfile(os.path.join(folder_path, item))]
    
    loose_file_types = sorted(set(
        os.path.splitext(f)[1].lower().strip('.')
        for f in loose_files
        if os.path.splitext(f)[1]
    ))

    subfolder_file_types = defaultdict(set)
    for subfolder in subfolders:
        subfolder_path = os.path.join(folder_path, subfolder)
        for file in os.listdir(subfolder_path):
            if os.path.isfile(os.path.join(subfolder_path, file)):
                ext = os.path.splitext(file)[1].lower().strip('.')
                if ext:
                    subfolder_file_types[subfolder].add(ext)

    print(f"\n--- {folder} ---")
    print(f"  Subfolders: {len(subfolders)}")
    print(f"  Loose files: {len(loose_files)}")
    print(f"  Loose file types: {loose_file_types if loose_file_types else 'none'}")
    print(f"  File types per subfolder:")
    for subfolder, types in subfolder_file_types.items():
        print(f"    {subfolder}: {sorted(types)}")


# COMMAND ----------

import chardet

# are there files with no extension?
no_ext_files = []
for fname in os.listdir(entry_path):
    fpath = os.path.join(entry_path, fname)
    if os.path.isfile(fpath) and os.path.splitext(fname)[1] == '':
        no_ext_files.append(fpath)

print(f"Files with no extension: {len(no_ext_files)}")
print(f"\nSample file names (first 15):")
for f in sorted(no_ext_files)[:15]:
    print(f"  {os.path.basename(f)}")

# Check what's inside a few of them
print("\n=== Content inspection (first 3 files) ===")
for fpath in sorted(no_ext_files)[:3]:
    size = os.path.getsize(fpath)
    with open(fpath, 'rb') as f:
        raw = f.read(512)
    det = chardet.detect(raw)
    line = raw.decode(det.get('encoding') or 'latin-1', errors='replace').split('\n')[0]
    print(f"\n  File: {os.path.basename(fpath)} ({size:,} bytes)")
    print(f"  Encoding: {det.get('encoding')}")
    print(f"  First line: {line[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### byheader_dir

# COMMAND ----------

base_path = os.path.join(V_DIR, "Workspace/Raw/byheader_dir")
os.listdir(base_path)

# COMMAND ----------

import os
from collections import defaultdict

base_path = os.path.join(V_DIR, "Workspace/Raw/byheader_dir")
folders = os.listdir(base_path)

for folder in sorted(folders):
    folder_path = os.path.join(base_path, folder)
    contents = os.listdir(folder_path)
    
    subfolders = [item for item in contents if os.path.isdir(os.path.join(folder_path, item))]
    loose_files = [item for item in contents if os.path.isfile(os.path.join(folder_path, item))]
    
    file_types = sorted(set(
        os.path.splitext(f)[1].lower().strip('.')
        for f in loose_files
        if os.path.splitext(f)[1]
    ))
    
    # first and latest file by modification time
    if loose_files:
        files_with_mtime = sorted(
            loose_files,
            key=lambda f: os.path.getmtime(os.path.join(folder_path, f))
        )
        first_file = files_with_mtime[0]
        latest_file = files_with_mtime[-1]
    else:
        first_file = latest_file = "none"

    print(f"\n--- {folder} ---")
    print(f"  Subfolders: {len(subfolders)} {subfolders if subfolders else ''}")
    print(f"  Loose files: {len(loose_files)}")
    print(f"  File types: {file_types if file_types else 'none'}")
    print(f"  First file:  {first_file}")
    print(f"  Latest file: {latest_file}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Other Workspace folders

# COMMAND ----------



# COMMAND ----------

os.listdir(V_DIR+ "/Workspace/Clean")

# COMMAND ----------

os.listdir(V_DIR+ "/Workspace/Construct")

# COMMAND ----------

os.listdir(V_DIR+ "/Workspace/bogota-hdfs")

# COMMAND ----------

os.listdir(V_DIR+ "/Workspace/bogota-hdfs/sample-will")

# COMMAND ----------

os.listdir(V_DIR+ "/Workspace/bogota-hdfs/intermediate")

# COMMAND ----------


os.listdir(V_DIR+ "/Workspace/pct_files")

# COMMAND ----------

os.listdir(V_DIR+ "/Workspace/bogota-hdfs/intermediate")
