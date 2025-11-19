# Databricks notebook source
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

destination = V_DIR + "/Workspace/Raw/from2016to2019/"

# 2016
subf2016 = os.listdir(f"{source}/2016data")
print(subf2016) # ready to directly copy 

for file in tqdm(subf2016):
    dbutils.fs.cp(f"{source}/2016data/{file}" ,
                  f"{destination}/{file}")
    
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
for file in tqdm(monthly_files):
    dbutils.fs.cp(f"{decompressed_dir}/{file}" ,
                  f"{destination}/{file}") # Copy monthly files
    
for folder in ['ValTroncal Oct2017/', 'ValZonal Dic2017/', 'ValZonal Oct2017/']: # copy from `decompressed` individual folders
    files = [f.name for f in dbutils.fs.ls(f"{decompressed_dir}/{folder}") ]
    for f in tqdm(files):
        dbutils.fs.cp(f"{decompressed_dir}/{folder}/{f}", 
                      f"{destination}/{f}")

for folder in ['ValTroncal Nov2017/', 'ValZonal Nov2017/']: # extract
    directory = f"{decompressed_dir}/{folder}"
    subfolders = [directory + f for f in os.listdir(directory) ]
    for fd in tqdm(subfolders):
        files = os.listdir(fd)
        files = [x for x in files if x != 'valzonal_27nov2017_MCKENNEDY.gz'] # the file is corrupted and cannot be extracted
        for f in files:
            Archive( fd + "/" + f ).extractall(destination)

patoolib.extract_archive(f"{source}/2017data/ValTroncal Dic2017.7z", outdir=destination) # Extract Troncal DIC 2017

# 2018 
subf2018 = os.listdir(f"{source}/2018data")
print(subf2018)

files_rar    = ['ValTroncal Dic2018 csvs.rar',  'ValZonal Feb2018 csvs.rar',  'ValZonal Mar2018 csvs.rar']
files_not_7z = files_rar + ['ValTroncal Nov2018.7z', 'ValTroncal Oct2018.7z']
files7z  = [f for f in subf2018 if f not in files_not_7z]


for file in tqdm(files7z):
    with py7zr.SevenZipFile(f"{source}/2018data/{file}", mode='r') as z:
        z.extractall(path=destination) # zonal for Oct and Aug 2018 do not have "zonal" in filenames, but troncal for those months do

for file in tqdm(['ValTroncal Nov2018.7z', 'ValTroncal Oct2018.7z']):
    with zipfile.ZipFile(f"{source}/2018data/{file}", 'r') as zip_ref:
        zip_ref.extractall(destination)

rar_decompressed = os.listdir(f"{source}/2018data/rar_decompressed")
assert len(rar_decompressed) == 90
np.unique([f[-3:] for f in rar_decompressed ])
for file in tqdm(rar_decompressed):
    dbutils.fs.cp(f"{source}/2018data/rar_decompressed/{file}" ,
                  f"{destination}/{file}") # Copy 

# 2019
subf2019 = os.listdir(f"{source}/2019data")

files_not_7z = ["other"]
files7z  = [f for f in subf2019 if f not in files_not_7z ]

for file in tqdm(files7z):
    with py7zr.SevenZipFile(f"{source}/2019data/{file}", mode='r') as z:
        z.extractall(path=destination)

files_other = os.listdir(f"{source}/2019data/other")
files_other  = [f for f in files_other if f !=  'Validacion_Troncal_20191031.zip'  ]
for file in tqdm(files_other):
    dbutils.fs.cp(f"{source}/2019data/other/{file}" ,
                  f"{destination}/{file}") # Copy 
    

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

assert len([f for f in files_in_dest if ".rar" in f]) == 0
assert len([f for f in files_in_dest if ".7z" in f]) == 0
assert len([f for f in files_in_dest if ".zip" in f]) == 0

files_in_dest = os.listdir(destination) #2703
print("Number of files in destination:", len(files_in_dest))


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
