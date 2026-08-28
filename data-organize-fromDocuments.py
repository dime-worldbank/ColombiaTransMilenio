# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Reorganize data - from Documents 
# MAGIC
# MAGIC Creates a Workspace/Raw folder to transfer there all files from the Ingestion point: the Documents folder that has the files we used to have in OneDrive. Use the Notebook `data-organize/fromData` to move data from the Download Point (Data folder).
# MAGIC

# COMMAND ----------

# Modules
import os
import shutil
import io
from pathlib import Path
from shutil import rmtree
import numpy as np
import pandas as pd

!pip install tqdm
from tqdm import tqdm

!pip install pyunpack
!pip install patool
!pip install py7zr
!pip install rarfile
import rarfile
from pyunpack import Archive
import patoolib
import py7zr
import zipfile

from random import sample, seed
seed(510)

from collections import defaultdict
from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

# Directories
V_DIR   = '/Volumes/prd_csc_mega/sColom15/vColom15/'
source  = V_DIR + "/Documents/"

# COMMAND ----------

# MAGIC %run ./utils/handle_files

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Workspace

# COMMAND ----------

[x.name for x in dbutils.fs.ls(V_DIR )]

# COMMAND ----------

#  Create Workspace directory if it does not exist
dbutils.fs.mkdirs(V_DIR + "/Workspace/")
dbutils.fs.mkdirs(V_DIR + "/Workspace/Raw/")
dbutils.fs.mkdirs(V_DIR + "/Workspace/Clean/")
dbutils.fs.mkdirs(V_DIR + "/Workspace/variable_dicts")
dbutils.fs.mkdirs(V_DIR + "/Workspace/Raw/from2016to2019")
dbutils.fs.mkdirs(V_DIR + "/Workspace/Raw/since2020")
dbutils.fs.mkdirs(V_DIR + "/Workspace/Raw/Recharges")

# COMMAND ----------

[x.name for x in dbutils.fs.ls(V_DIR + "/Workspace/")]

# COMMAND ----------

[x.name for x in dbutils.fs.ls(V_DIR + "/Workspace/Raw")]

# COMMAND ----------

print([x.name for x in dbutils.fs.ls(V_DIR + "/Workspace/variable_dicts/")])
print([x.name for x in dbutils.fs.ls(V_DIR + "/Workspace/Clean/")])


# COMMAND ----------

# MAGIC %md
# MAGIC `/Workspace/Raw/` 
# MAGIC Is a unique folder for storing all raw data. It still has different file structures:
# MAGIC
# MAGIC - `/since2020`: data since 2020 is organized in ValidacionTroncal, ValidacionZonal, ValidacionDual, Salidas, Recargas folders. 
# MAGIC
# MAGIC   - Recargas
# MAGIC   - Salidas
# MAGIC   - ValidacionDual (validaciones)
# MAGIC   - ValidacionTroncal (validaciones)
# MAGIC   - ValidacionZonal (validaciones)
# MAGIC
# MAGIC
# MAGIC - `/from2016to2019`: this has only validaciones data (not salidas nor recargas), but not organized in subfolders as in many cases we don't have separate files for each.
# MAGIC
# MAGIC - `/byheader_dir`: raw validaciones files organized in folders by header
# MAGIC
# MAGIC `/Workspace/variable_dicts/` 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Moving 2016-2019 validaciones data 
# MAGIC Moving to V_DIR/Workspace/Raw/from2016to2019 folder.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC All of these data come from the Ingestion point, three folders in V_DIR/Documents:
# MAGIC * /2016data
# MAGIC * /2017data
# MAGIC * /2018data
# MAGIC * /2019data
# MAGIC
# MAGIC Moving to V_DIR/Workspace/Raw/from2016to2019 folder. Different strategy depending data structure (zipped or rar, in subfolders or not, etc.)
# MAGIC * /2016data: directly copy files
# MAGIC * /2017data
# MAGIC   - Monthly rar files: copy from `decompressed` folder (it is not possible to unrar in Databricks)
# MAGIC   - `ValTroncal Oct2017.7z`,`ValZonal Oct2017.7z`, `ValZonal Dic2017.7z`: copy from `decompressed` individual folders
# MAGIC   - `ValTroncal Nov2017.7z`,`ValZonal Nov2017.7z`: extract from `decompressed` folder using patool
# MAGIC   - `ValTroncal Dic2017.7z`: extracted from 7z with patool from .7z file
# MAGIC   - Corrupted files: valzonal_27nov2017_MCKENNEDY.gz 
# MAGIC * /2018data
# MAGIC   - 7z files that can be extracted
# MAGIC   - Files that end in 7z but actually are zips that can be extracted
# MAGIC   - Rar files were decompressed externally and uploaded in rar_decompressed folders, copied from there
# MAGIC   - Corrupted files: 'Valzonal_20180901.zip', 'Valzonal_20180902.zip'
# MAGIC * /2019data
# MAGIC   - 7z files that can be extracted
# MAGIC   - files to copy in folder "other"
# MAGIC
# MAGIC Even if extracting from zip/rar/7z folder, there are many zipped them. Thus, unzipping files from destination folder and removing zips from there.

# COMMAND ----------

# DBTITLE 1,Detect pending files to organize (2016-2019)
# Detect what still needs to be organized for 2016-2019 data
destination = V_DIR + "/Workspace/Raw/from2016to2019/"

# Get files already in destination (top-level)
existing_top = set(os.listdir(destination)) if os.path.isdir(destination) else set()

# Also index files inside one level of subdirectories
# (some archives extract into dated subdirs, e.g. VALTRONCAL_01-06-2018/*.gz)
existing_in_subdirs = set()
for item in list(existing_top):
    subdir_path = os.path.join(destination, item)
    if os.path.isdir(subdir_path):
        for fname in os.listdir(subdir_path):
            if os.path.isfile(os.path.join(subdir_path, fname)):
                existing_in_subdirs.add(fname)

existing_all = existing_top | existing_in_subdirs
print(f"Files in destination (top-level): {len([f for f in existing_top if os.path.isfile(os.path.join(destination, f))])}")
print(f"Files in subdirectories: {len(existing_in_subdirs)}")
print("="*60)

# Track what needs to be moved
pending = []

# Known broken/excluded files (intentionally skipped in cell 15)
broken_files = {'Valzonal_20180901.zip', 'Valzonal_20180902.zip', 'Validacion_Troncal_20191031.zip'}

def check_archive_status(names, existing_all, existing_in_subdirs):
    """
    Check organization status of archive contents.
    Returns: (status, details)
      - 'complete': all files are organized (extracted and unzipped where needed)
      - 'has_pending_zips': archive extracted but .zip files in subdirs still need unzipping
      - 'not_extracted': archive contents genuinely missing from destination
    """
    pending_zips = []
    missing = []

    for n in names:
        basename = os.path.basename(n)
        if not basename or n.endswith('/'):
            continue
        if basename in broken_files:
            continue

        # File found at top-level or in a subdirectory (as non-zip) → organized
        if basename in existing_all and not basename.endswith('.zip'):
            continue

        # It's a .zip file or not found — check if an unzipped version exists
        if basename.endswith('.zip'):
            stem = basename[:-4]
            # Check for common unzipped extensions (.csv, .xls, .xlsx, .txt)
            if any((stem + ext) in existing_all for ext in ['.csv', '.xls', '.xlsx', '.txt']):
                continue  # Unzipped version exists somewhere in destination
            # .zip exists in a subdirectory — downstream notebooks handle zips
            # directly (data-byheader walks subdirs and ingestion reads zips on-the-fly)
            if basename in existing_in_subdirs:
                continue
            # .zip exists at top-level (will be handled by cell 15's unzip loop)
            if basename in existing_top:
                continue
            # Truly missing
            missing.append(basename)
        elif basename in existing_all:
            # Non-zip file found (caught by first check above, but defensive)
            continue
        else:
            missing.append(basename)

    if missing:
        return 'not_extracted', missing
    elif pending_zips:
        return 'has_pending_zips', pending_zips
    else:
        return 'complete', []

# --- 2016 ---
subf2016 = [f for f in os.listdir(f"{source}/2016data") if os.path.isfile(f"{source}/2016data/{f}")] if os.path.isdir(f"{source}/2016data") else []
for file in subf2016:
    if file not in existing_top:
        pending.append((f"2016data/{file}", file))

print(f"\n[2016] Files to move: {len([p for p in pending if p[0].startswith('2016')])}")

# --- 2017 ---
decompressed_dir_2017 = f"{source}/2017data/decompressed/"
monthly_files = ['02_ValidacionesFeb2017.csv', '03_ValidacionesMar2017.csv', '04_ValidacionesAbr2017.csv',
                 '01_ValidacionesEnero2017.csv','05_ValidacionesMay2017.csv', '06_ValidacionesJun2017.csv', 
                 '07_ValidacionesJul2017.csv', '08_ValidacionesAgo2017.csv', '09_ValidacionesSept2017.csv']
for file in monthly_files:
    if file not in existing_top:
        pending.append((f"2017data/decompressed/{file}", file))

for folder in ['ValTroncal Oct2017/', 'ValZonal Dic2017/', 'ValZonal Oct2017/']:
    try:
        files = [f.name for f in dbutils.fs.ls(f"{decompressed_dir_2017}/{folder}")]
        for f in files:
            if f not in existing_top:
                pending.append((f"2017data/decompressed/{folder}{f}", f))
    except Exception:
        pass

# Check Nov 2017 gz extractions
for folder in ['ValTroncal Nov2017/', 'ValZonal Nov2017/']:
    directory = f"{decompressed_dir_2017}/{folder}"
    if os.path.isdir(directory):
        for subfolder in os.listdir(directory):
            fd = os.path.join(directory, subfolder)
            if os.path.isdir(fd):
                for f in os.listdir(fd):
                    if f == 'valzonal_27nov2017_MCKENNEDY.gz':
                        continue
                    output_name = f[:-3] if f.endswith('.gz') else f
                    if output_name not in existing_all:
                        pending.append((f"2017data/decompressed/{folder}{subfolder}/{f} (gz to extract)", output_name))

# Check Troncal Dic 2017 archive (it's actually a zip despite .7z extension)
try:
    with py7zr.SevenZipFile(f"{source}/2017data/ValTroncal Dic2017.7z", mode='r') as z:
        names = z.getnames()
    status, details = check_archive_status(names, existing_all, existing_in_subdirs)
    if status == 'not_extracted':
        pending.append((f"2017data/ValTroncal Dic2017.7z (archive to extract)", "ValTroncal Dic2017.7z"))
    elif status == 'has_pending_zips':
        for zf_name in details:
            pending.append((f"2017data/ValTroncal Dic2017.7z -> {zf_name} (zip in subdir to unzip)", zf_name))
except (py7zr.Bad7zFile, Exception):
    try:
        with zipfile.ZipFile(f"{source}/2017data/ValTroncal Dic2017.7z", 'r') as zf:
            names = [n for n in zf.namelist() if n and not n.endswith('/')]
        status, details = check_archive_status(names, existing_all, existing_in_subdirs)
        if status == 'not_extracted':
            pending.append((f"2017data/ValTroncal Dic2017.7z (archive to extract)", "ValTroncal Dic2017.7z"))
        elif status == 'has_pending_zips':
            for zf_name in details:
                pending.append((f"2017data/ValTroncal Dic2017.7z -> {zf_name} (zip in subdir to unzip)", zf_name))
    except Exception:
        pending.append((f"2017data/ValTroncal Dic2017.7z (unreadable)", "ValTroncal Dic2017.7z"))

print(f"[2017] Files to move: {len([p for p in pending if p[0].startswith('2017')])}")

# --- 2018 ---
subf2018 = os.listdir(f"{source}/2018data") if os.path.isdir(f"{source}/2018data") else []
rar_decompressed = os.listdir(f"{source}/2018data/rar_decompressed") if os.path.isdir(f"{source}/2018data/rar_decompressed") else []
for file in rar_decompressed:
    if file not in existing_top:
        pending.append((f"2018data/rar_decompressed/{file}", file))

# Check archive contents against existing files
files_rar = ['ValTroncal Dic2018 csvs.rar', 'ValZonal Feb2018 csvs.rar', 'ValZonal Mar2018 csvs.rar']
files_not_7z = files_rar + ['ValTroncal Nov2018.7z', 'ValTroncal Oct2018.7z',
                            'ValTroncal Abr2018.7z', 'ValZonal Abr2018.7z']
archives_2018 = [f for f in subf2018 if f not in files_not_7z and f != 'rar_decompressed' and os.path.isfile(f"{source}/2018data/{f}")]
for file in archives_2018:
    filepath = f"{source}/2018data/{file}"
    try:
        with py7zr.SevenZipFile(filepath, mode='r') as z:
            names = z.getnames()
        status, details = check_archive_status(names, existing_all, existing_in_subdirs)
        if status == 'not_extracted':
            pending.append((f"2018data/{file} (archive to extract)", file))
        elif status == 'has_pending_zips':
            for zf_name in details:
                pending.append((f"2018data/{file} -> {zf_name} (zip in subdir to unzip)", zf_name))
    except py7zr.Bad7zFile:
        try:
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                names = [n for n in zip_ref.namelist() if n and not n.endswith('/')]
            status, details = check_archive_status(names, existing_all, existing_in_subdirs)
            if status == 'not_extracted':
                pending.append((f"2018data/{file} (archive to extract)", file))
            elif status == 'has_pending_zips':
                for zf_name in details:
                    pending.append((f"2018data/{file} -> {zf_name} (zip in subdir to unzip)", zf_name))
        except zipfile.BadZipFile:
            pending.append((f"2018data/{file} (unreadable archive)", file))

# Check ValTroncal Nov/Oct 2018 (zips with .7z extension)
for file in ['ValTroncal Nov2018.7z', 'ValTroncal Oct2018.7z']:
    try:
        with zipfile.ZipFile(f"{source}/2018data/{file}", 'r') as zip_ref:
            names = [n for n in zip_ref.namelist() if n and not n.endswith('/')]
        status, details = check_archive_status(names, existing_all, existing_in_subdirs)
        if status == 'not_extracted':
            pending.append((f"2018data/{file} (zip to extract)", file))
        elif status == 'has_pending_zips':
            for zf_name in details:
                pending.append((f"2018data/{file} -> {zf_name} (zip in subdir to unzip)", zf_name))
    except Exception:
        pending.append((f"2018data/{file} (unreadable)", file))

print(f"[2018] Files/archives to process: {len([p for p in pending if p[0].startswith('2018')])}")

# --- 2019 ---
subf2019 = os.listdir(f"{source}/2019data") if os.path.isdir(f"{source}/2019data") else []
other_2019 = os.listdir(f"{source}/2019data/other") if os.path.isdir(f"{source}/2019data/other") else []
other_2019 = [f for f in other_2019 if f not in broken_files]  # exclude intentionally skipped files
for file in other_2019:
    if file not in existing_top:
        pending.append((f"2019data/other/{file}", file))

archives_2019 = [f for f in subf2019 if f != 'other' and os.path.isfile(f"{source}/2019data/{f}")]
for file in archives_2019:
    filepath = f"{source}/2019data/{file}"
    try:
        with py7zr.SevenZipFile(filepath, mode='r') as z:
            names = z.getnames()
        status, details = check_archive_status(names, existing_all, existing_in_subdirs)
        if status == 'not_extracted':
            pending.append((f"2019data/{file} (archive to extract)", file))
        elif status == 'has_pending_zips':
            for zf_name in details:
                pending.append((f"2019data/{file} -> {zf_name} (zip in subdir to unzip)", zf_name))
    except py7zr.Bad7zFile:
        try:
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                names = [n for n in zip_ref.namelist() if n and not n.endswith('/')]
            status, details = check_archive_status(names, existing_all, existing_in_subdirs)
            if status == 'not_extracted':
                pending.append((f"2019data/{file} (archive to extract)", file))
            elif status == 'has_pending_zips':
                for zf_name in details:
                    pending.append((f"2019data/{file} -> {zf_name} (zip in subdir to unzip)", zf_name))
        except zipfile.BadZipFile:
            pending.append((f"2019data/{file} (unreadable archive)", file))

print(f"[2019] Files/archives to process: {len([p for p in pending if p[0].startswith('2019')])}")

# --- Summary ---
print("\n" + "="*60)
print(f"TOTAL pending items: {len(pending)}")
if pending:
    print("\nSample of pending items:")
    for src, fname in pending[:20]:
        print(f"  -> Moving: {fname}  (from {src})")
    if len(pending) > 20:
        print(f"  ... and {len(pending) - 20} more.")
else:
    print("\n✓ All files are already organized. Nothing to move.")

# COMMAND ----------

# DBTITLE 1,Move 2016-2019 data (skip existing)
destination = V_DIR + "/Workspace/Raw/from2016to2019/"
existing_at_dest = set(os.listdir(destination)) if os.path.isdir(destination) else set()

# Also index files in subdirectories (some archives extract into dated subdirs)
existing_in_subdirs = set()
for item in list(existing_at_dest):
    subdir_path = os.path.join(destination, item)
    if os.path.isdir(subdir_path):
        for fname in os.listdir(subdir_path):
            if os.path.isfile(os.path.join(subdir_path, fname)):
                existing_in_subdirs.add(fname)
existing_all = existing_at_dest | existing_in_subdirs

def archive_already_extracted(names):
    """Check if archive contents are already in destination (top-level or subdirs).
    Accounts for .zip files that were unzipped to .csv/.xls/.xlsx/.txt."""
    for n in names:
        basename = os.path.basename(n)
        if not basename or n.endswith('/'):
            continue
        if not basename.endswith('.zip'):
            if basename in existing_all:
                continue
            return False
        # .zip: check if it exists or its unzipped version exists
        stem = basename[:-4]
        if any((stem + ext) in existing_all for ext in ['.zip', '.csv', '.xls', '.xlsx', '.txt']):
            continue
        return False
    return True

def smart_copy(src_path, dest_path, filename):
    """Copy file only if it doesn't already exist in destination. Returns True if copied."""
    if filename in existing_at_dest:
        return False
    print(f"  -> Moving: {filename}")
    dbutils.fs.cp(src_path, dest_path)
    return True

# 2016
subf2016 = [f for f in os.listdir(f"{source}/2016data") if os.path.isfile(f"{source}/2016data/{f}")]
print(subf2016) # ready to directly copy 

copied, skipped = 0, 0
for file in tqdm(subf2016):
    if smart_copy(f"{source}/2016data/{file}", f"{destination}/{file}", file):
        copied += 1
    else:
        skipped += 1
print(f"[2016] Copied: {copied}, Skipped (already exists): {skipped}")
    
# 2017
subf2017 = os.listdir(f"{source}/2017data")  
print(subf2017)

decompressed_dir = f"{source}/2017data/decompressed/"
decompressed = os.listdir(decompressed_dir)
print("Folders in decompressed:")
print(decompressed)

for folder in decompressed:
    subf = [f.name for f in dbutils.fs.ls(f"{V_DIR}/Documents/2017data/decompressed/{folder}") ]
    print("---------------")
    print( folder, ":")
    if len(subf) > 10:
        print(sample(subf, 10))
        endings = [f[-4:] for f in subf]
        print("File endings:", np.unique(endings))
    else: 
        print(subf)


monthly_files = ['02_ValidacionesFeb2017.csv', '03_ValidacionesMar2017.csv', '04_ValidacionesAbr2017.csv',
                 '01_ValidacionesEnero2017.csv','05_ValidacionesMay2017.csv', '06_ValidacionesJun2017.csv', 
                 '07_ValidacionesJul2017.csv', '08_ValidacionesAgo2017.csv', '09_ValidacionesSept2017.csv' ]
copied, skipped = 0, 0
for file in tqdm(monthly_files):
    if smart_copy(f"{decompressed_dir}/{file}", f"{destination}/{file}", file):
        copied += 1
    else:
        skipped += 1
print(f"[2017 monthly] Copied: {copied}, Skipped: {skipped}")
    
copied, skipped = 0, 0
for folder in ['ValTroncal Oct2017/', 'ValZonal Dic2017/', 'ValZonal Oct2017/']: # copy from `decompressed` individual folders
    files = [f.name for f in dbutils.fs.ls(f"{decompressed_dir}/{folder}") ]
    for f in tqdm(files):
        if smart_copy(f"{decompressed_dir}/{folder}/{f}", f"{destination}/{f}", f):
            copied += 1
        else:
            skipped += 1
print(f"[2017 folders] Copied: {copied}, Skipped: {skipped}")

for folder in ['ValTroncal Nov2017/', 'ValZonal Nov2017/']: # extract
    directory = f"{decompressed_dir}/{folder}"
    subfolders = [directory + f for f in os.listdir(directory) ]
    for fd in tqdm(subfolders):
        files = os.listdir(fd)
        files = [x for x in files if x != 'valzonal_27nov2017_MCKENNEDY.gz'] # the file is corrupted and cannot be extracted
        for f in files:
            # Skip if output already exists (gz extracts to filename without .gz)
            output_name = f[:-3] if f.endswith('.gz') else f
            if output_name in existing_all:
                continue
            Archive( fd + "/" + f ).extractall(destination)

# Extract Troncal DIC 2017 (skip if contents already extracted)
try:
    with py7zr.SevenZipFile(f"{source}/2017data/ValTroncal Dic2017.7z", mode='r') as z:
        names = z.getnames()
    if not archive_already_extracted(names):
        patoolib.extract_archive(f"{source}/2017data/ValTroncal Dic2017.7z", outdir=destination)
    else:
        print("[2017 Troncal Dic] Already extracted, skipping")
except Exception:
    patoolib.extract_archive(f"{source}/2017data/ValTroncal Dic2017.7z", outdir=destination)

# 2018 
subf2018 = [f for f in os.listdir(f"{source}/2018data") if os.path.isfile(f"{source}/2018data/{f}")]
print(subf2018)

files_rar    = ['ValTroncal Dic2018 csvs.rar',  'ValZonal Feb2018 csvs.rar',  'ValZonal Mar2018 csvs.rar']
files_not_7z = files_rar + ['ValTroncal Nov2018.7z', 'ValTroncal Oct2018.7z', 'ValTroncal Abr2018.7z', 'ValZonal Abr2018.7z']
files7z  = [f for f in subf2018 if f not in files_not_7z and os.path.isfile(f"{source}/2018data/{f}")]


for file in tqdm(files7z):
    filepath = f"{source}/2018data/{file}"
    try:
        with py7zr.SevenZipFile(filepath, mode='r') as z:
            names = z.getnames()
        # Skip if all contents already extracted
        if archive_already_extracted(names):
            continue
        with py7zr.SevenZipFile(filepath, mode='r') as z:
            z.extractall(path=destination) # zonal for Oct and Aug 2018 do not have "zonal" in filenames, but troncal for those months do
    except py7zr.Bad7zFile:
        # Some files have .7z extension but are actually zip files
        try:
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                names = [n for n in zip_ref.namelist() if n and not n.endswith('/')]
                if archive_already_extracted(names):
                    continue
                zip_ref.extractall(destination)
        except zipfile.BadZipFile:
            print(f"⚠️ Skipping {file}: neither valid 7z nor zip")

for file in tqdm(['ValTroncal Nov2018.7z', 'ValTroncal Oct2018.7z']):
    with zipfile.ZipFile(f"{source}/2018data/{file}", 'r') as zip_ref:
        names = [n for n in zip_ref.namelist() if n and not n.endswith('/')]
        if archive_already_extracted(names):
            continue
        zip_ref.extractall(destination)

rar_decompressed = os.listdir(f"{source}/2018data/rar_decompressed")
print(f"[2018 rar_decompressed] {len(rar_decompressed)} files")
np.unique([f[-3:] for f in rar_decompressed ])
copied, skipped = 0, 0
for file in tqdm(rar_decompressed):
    if smart_copy(f"{source}/2018data/rar_decompressed/{file}", f"{destination}/{file}", file):
        copied += 1
    else:
        skipped += 1
print(f"[2018 rar] Copied: {copied}, Skipped: {skipped}")

# 2019
subf2019 = [f for f in os.listdir(f"{source}/2019data") if os.path.isfile(f"{source}/2019data/{f}")]

files_not_7z = ["other"]
files7z  = [f for f in subf2019 if f not in files_not_7z ]

for file in tqdm(files7z):
    filepath = f"{source}/2019data/{file}"
    try:
        with py7zr.SevenZipFile(filepath, mode='r') as z:
            names = z.getnames()
        # Skip if all contents already extracted
        if archive_already_extracted(names):
            continue
        with py7zr.SevenZipFile(filepath, mode='r') as z:
            z.extractall(path=destination)
    except py7zr.Bad7zFile:
        try:
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                names = [n for n in zip_ref.namelist() if n and not n.endswith('/')]
                if archive_already_extracted(names):
                    continue
                zip_ref.extractall(destination)
        except zipfile.BadZipFile:
            print(f"⚠️ Skipping {file}: neither valid 7z nor zip")

files_other = os.listdir(f"{source}/2019data/other")
files_other  = [f for f in files_other if f !=  'Validacion_Troncal_20191031.zip'  ]
copied, skipped = 0, 0
for file in tqdm(files_other):
    if smart_copy(f"{source}/2019data/other/{file}", f"{destination}/{file}", file):
        copied += 1
    else:
        skipped += 1
print(f"[2019 other] Copied: {copied}, Skipped: {skipped}")
    

# unzip files in destination
files_in_dest = os.listdir(destination)

zipfiles = [f for f in files_in_dest if ".zip" in f] 
print("Total zipfiles:", len(zipfiles))
broken = ['Valzonal_20180901.zip', 'Valzonal_20180902.zip']
for file in tqdm(zipfiles):
    if file in broken:
        os.remove(f"{destination}/{file}")
        continue
    with zipfile.ZipFile(f"{destination}/{file}", 'r') as zip_ref:
        zip_ref.extractall(destination)
    os.remove(f"{destination}/{file}")

# COMMAND ----------

# Remaining archive files in destination (nested archives from 7z extractions)
files_in_dest = os.listdir(destination)
rar_files = [f for f in files_in_dest if f.endswith(".rar")]
sevenz_files = [f for f in files_in_dest if f.endswith(".7z")]
zip_files = [f for f in files_in_dest if f.endswith(".zip")]
print(f"Before cleanup - rar: {len(rar_files)}, 7z: {len(sevenz_files)}, zip: {len(zip_files)}")


# COMMAND ----------

# DBTITLE 1,Extract nested archives in destination (.7z, .rar, .zip)
# Extract .7z files
for file in tqdm(sevenz_files, desc="Extracting .7z"):
    filepath = f"{destination}/{file}"
    try:
        with py7zr.SevenZipFile(filepath, mode='r') as z:
            z.extractall(path=destination)
    except py7zr.Bad7zFile:
        try:
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(destination)
        except Exception:
            print(f"\u26a0\ufe0f Could not extract: {file}")
            continue
    os.remove(filepath)

# Extract .rar files
for file in tqdm(rar_files, desc="Extracting .rar"):
    filepath = f"{destination}/{file}"
    try:
        patoolib.extract_archive(filepath, outdir=destination)
    except Exception as e:
        print(f"\u26a0\ufe0f Could not extract {file}: {e}")
        continue
    os.remove(filepath)

# Extract any .zip files (including new ones from nested archives)
files_in_dest = os.listdir(destination)
zip_files = [f for f in files_in_dest if f.endswith(".zip")]
broken = ['Valzonal_20180901.zip', 'Valzonal_20180902.zip']
for file in tqdm(zip_files, desc="Extracting .zip"):
    filepath = f"{destination}/{file}"
    if file in broken:
        os.remove(filepath)
        continue
    try:
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall(destination)
    except zipfile.BadZipFile:
        print(f"\u26a0\ufe0f Bad zip: {file}")
        continue
    os.remove(filepath)

# Final check
files_in_dest = os.listdir(destination)
nrars = len([f for f in files_in_dest if f.endswith(".rar")])
n7z = len([f for f in files_in_dest if f.endswith(".7z")])
nzip = len([f for f in files_in_dest if f.endswith(".zip")])
print(f"\nAfter cleanup - rar: {nrars}, 7z: {n7z}, zip: {nzip}")
assert nrars == 0
assert n7z == 0
assert nzip == 0

# COMMAND ----------

files = [f for f in files_in_dest if os.path.isfile(os.path.join(destination, f))]
dirs =  [f for f in files_in_dest if os.path.isdir(os.path.join(destination, f))]
print(f"Files in destination (top-level): {len(files)}, Subdirectories: {len(dirs)}")


# COMMAND ----------

for d in dirs:
    print("-------------------------")
    print(d)
    print("-------------------------")
    print(os.listdir(destination + "/" + d))

# COMMAND ----------

dirs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Moving validaciones since 2020
# MAGIC Moves data to the `Workspace/raw/since2020` folder. 

# COMMAND ----------

# See /Data file structure
[x.name for x in dbutils.fs.ls(V_DIR + '/Data/')]

# COMMAND ----------

# Copy /Data file structure
for d in ['Recargas/', 'Salidas/', 'ValidacionCable/',
          'ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/']:
    dbutils.fs.mkdirs(V_DIR + '/Workspace/Raw/since2020/' + d)


# COMMAND ----------

print("Folder - Number of files")

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
    files = dbutils.fs.ls(V_DIR + '/Documents/' + f)
    print(f, "-", len(files))

# COMMAND ----------

# Move
destination = V_DIR + "/Workspace/Raw/since2020/"
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

notfound = []
notfound_count = 0
tot_files = 0
for d in list(dic_d.keys()):
   df = dic_d[d]
   files = [f.name for f in dbutils.fs.ls(source + d) ]
   vfiles = [f for f in files if 'validacion' in f]
   tot_files += len(vfiles)
    
   for f in tqdm(vfiles):
        output_filepath =   destination + df + f  
        if not os.path.exists(output_filepath):
                notfound_count += 1
                notfound.append(output_filepath)
                dbutils.fs.cp(source + d + f, output_filepath)
                

# COMMAND ----------

print(f"Not copied: {notfound_count} out of {tot_files} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Move recharges 2017-2019
# MAGIC
# MAGIC All of these data come from the Ingestion point: V_DIR/Documents/Recharges2017-2019
# MAGIC
# MAGIC Many are nested zipped files:
# MAGIC * Moving to V_DIR/Workspace/Raw/Recargas/compressed first
# MAGIC * Then moving to /decompressed

# COMMAND ----------

origin      = V_DIR + "/Documents/Recharges2017-2019"
destination = V_DIR + "/Workspace/Raw/Recharges"

compressed_dir = f"{destination}/compressed/"
decompressed_dir = f"{destination}/decompressed/"

os.makedirs(compressed_dir  , exist_ok=True)
os.makedirs(decompressed_dir, exist_ok=True)

for y in ["2017", "2018", "2019"]:
    os.makedirs(f"{destination}/decompressed/{y}", exist_ok=True)
    os.makedirs(f"{destination}/compressed/{y}", exist_ok=True)


# COMMAND ----------

allfiles = os.listdir(origin)
files2017= [f for f in allfiles if '2017' in f]
files2018= [f for f in allfiles if '2018' in f]
files2019= [f for f in allfiles if '2019' in f]
assert len(allfiles) == len(files2017) + len(files2018) + len(files2019) +1

# COMMAND ----------

for file in files2017:
    in_path = os.path.join(origin, file)
    fmt = detect_format(in_path)
    print(f"{file}: detected as {fmt}")

    if fmt == "zip":
        with zipfile.ZipFile(in_path, "r") as z:
            z.extractall(f"{compressed_dir}/2017")

    elif fmt == "7z":
        with py7zr.SevenZipFile(in_path, "r") as z:
            z.extractall(f"{compressed_dir}/2017")

    else:
        print(f"Skipping {file}: unknown format")

compressed2017 = os.listdir(compressed_dir + "/2017/")
for filename in tqdm(compressed2017):
    src_path = compressed_dir + "/2017/" + filename
   
    # Move zip files
    if file.lower().endswith(".zip"):
        try:
            with zipfile.ZipFile(src_path, "r") as z:
                z.extractall(decompressed_dir + "/2017/")

        except zipfile.BadZipFile as e:
            print(f"⚠️ Bad ZIP file, skipping: {file} - error {e}")
       
    # Move CSV files
    elif filename.lower().endswith(".csv"):
       shutil.move(src_path, decompressed_dir + "/2017/" + filename)

print(np.unique([f[-3:] for f in os.listdir(decompressed_dir + "/2017") ]))
print(np.unique([f[-3:] for f in os.listdir(compressed_dir + "/2017") ]))


# COMMAND ----------

allfiles

# COMMAND ----------

folders2019 = [ 'Recargas_Ene2019',
               'Recargas Abr2019',
 'Recargas Ago 2019',
 'Recargas Feb2019',
 'Recargas Jul2019',
 'Recargas Jun2019',
 'Recargas Mar2019',
 'Recargas May2019',
 'Recargas Sep2019' ]

for f in folders2019:
    print(f)
    print(len(os.listdir(f"{origin}/{f}")))

# COMMAND ----------

folders2018 = [ 'Recargas Abr2018',
                'Recargas Ene2018',
                'Recargas Feb2018',
                'Recargas Mar2018',
                'Recargas_Jun2018' ]

for f in folders2018:
    print(f)
    print(len(os.listdir(f"{origin}/{f}")))

# COMMAND ----------

print(np.unique([f[-3:] for f in os.listdir(decompressed_dir + "/2017") ]))
print(np.unique([f[-3:] for f in os.listdir(compressed_dir + "/2017") ]))

# COMMAND ----------

os.listdir(decompressed_dir + "/2017")
