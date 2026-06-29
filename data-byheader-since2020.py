# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Classify files by header (since 2020)
# MAGIC Classifies raw CSV/zip files into header groups and records the mapping in `file_to_header` Delta table.  
# MAGIC No physical file copying — files stay in their original location.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 2
# Pip install non-standard packages
%pip install tqdm chardet

import os
import csv
import zipfile
from tqdm import tqdm
import pandas as pd
from random import seed
import chardet

# COMMAND ----------

# MAGIC %run ./utils/handle_files

# COMMAND ----------

# DBTITLE 1,Cell 4
# Directories
S_DIR = '/Volumes/prd_csc_mega/sColom15/'
V_DIR = f'{S_DIR}vColom15/'
user = 'wbrau@worldbank.org'
git = f'/Workspace/Users/{user}/ColombiaTransMilenio'

# Important sub-directories for this notebook
raw_dir = V_DIR + '/Workspace/Raw/'


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Set default catalog and schema */
# MAGIC
# MAGIC USE CATALOG prd_mega;
# MAGIC USE SCHEMA scolom15;
# MAGIC
# MAGIC SELECT
# MAGIC   current_catalog() as current_catalog,
# MAGIC   current_schema()  as current_schema;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %sql
# MAGIC -- Create table to list filename and header if it does not exist
# MAGIC CREATE TABLE IF NOT EXISTS file_to_header_since2020 (
# MAGIC     raw_filepath   STRING,
# MAGIC     header         STRING,
# MAGIC     source_period  STRING,
# MAGIC     zipped         INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Cell 7
# Read existing classification table
rawfiles_to_header = spark.read.format("delta").table("file_to_header_since2020")
rawfiles_to_header = rawfiles_to_header.toPandas()
print(f"Already classified: {len(rawfiles_to_header)} files")

# COMMAND ----------

# Parameters
# Old list of headers by Sebastian for 2016-2017 data:  letters (one - seven)
# New list of headers by Wendy for data since 2020: numbers (since 8)

unique_header_dict = {'header_one': 
                            ['Fecha de Liquidación', 'Fecha de Uso', 'Day Group Type', 'Hora Pico S/N', 'Fase',
                            'Emisor', 'Operador', 'Línea', 'Estación', 'Acceso de Estación', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Número de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transacción', 'Valor', 'Saldo Después de Transacción'],
                    'header_two': 
                          ['Fecha de Clearing;Fecha de Transaccion;Hora Pico SN;Fase;Emisor;Operador;Linea;Ruta;Parada;Tipo Vehiculo;ID Vehiculo;Dispositivo;Tipo Tarjeta;Nombre de Perfil;Numero Tarjeta;Tipo de Tarifa;Saldo Previo a Transaccion;Valor;Saldo Despues de Transaccion',],
                    'header_three': 
                          ['Fecha de Liquidación', 'Fecha de Uso', 'Day Group Type', 'Hora Pico S/N', 'Fase', 'Emisor', 'Operador', 'Línea', 'Ruta', 'Parada', 'Tipo de Vehículo', 'ID de Vehículo', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Número de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transacción', 'Valor', 'Saldo Después de Transacción'],
                    'header_four': 
                          ['Fecha de Clearing;Fecha de Transaccion;Day Group Type;Hora Pico SN;Fase;Emisor;Operador;Linea;Estacion;Acceso de Estación;Dispositivo;Tipo de Tarjeta;Nombre de Perfil;Numero de Tarjeta;Tipo de Tarifa;Saldo Previo a Transaccion;Valor;Saldo Despues de Transaccion',],
                    'header_five': 
                          ['Fecha de Clearing;Fecha de Transaccion;Hora Pico SN;Fase;Emisor;Operador;Linea;Ruta;Parada;Tipo Vehiculo;ID Vehiculo;Dispositivo;Tipo Tarjeta;Nombre de Perfil;Numero Tarjeta;Tipo de Tarifa;Saldo Previo a Transaccion;Valor;Saldo Despues de Transaccion;Ruta_Modificada;Linea_Modificada;Cenefa;Parada_Modificada',],
                    'header_six':
                           ['Fecha de Clearing', 'Fecha de Transaccion', 'DAY_GROUP_CD', 'Hora Pico SN', 'Fase', 'Emisor', 'Operador', 'Linea', 'Ruta', 'Parada', 'Tipo Vehiculo', 'ID Vehiculo', 'Dispositivo', 'Tipo Tarjeta', 'Nombre de Perfil', 'Numero Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transaccion', 'Valor', 'Saldo Despues de Transaccion'],
                    'header_seven': 
                          ['Fecha de Clearing', 'Fecha de Transaccion', 'DAY_GROUP_CD', 'Hora Pico SN', 'Emisor', 'Operador', 'Linea', 'Estacion', 'Acceso de Estación', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Numero de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transaccion', 'Valor', 'Saldo Despues de Transaccion'],
                    'header_08': 
                        ['Unnamed: 0', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion',  'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
                    'header_09':
                         ['Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
                    'header_10': 
                        ['Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor', 'archivo'],
                    'header_11': 
                        ['Acceso_Estacion', 'Day_Group_Type', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor', 'archivo'],
                    'header_12': 
                        ['Acceso_Estacion', 'Day_Group_Type', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
                    'header_13': 
                        ['Unnamed: 0', 'Acceso_Estacion', 'Day_Group_Type', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
                   'header_14':  
                       ['Unnamed: 0', 'Acceso_Estacion', 'Day_Group_Type', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Valor', 'ID_Vehiculo', 'Ruta', 'Tipo_Vehiculo', 'Sistema'],
                    'header_15': 
                        ['Unnamed: 0', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor', 'Sistema'],
                    'header_16': []}

broken_files = ["validacionDual20230630.csv",
                "validacionTroncal20200725.csv",
                "validacionZonal20200601.csv",
                "validacionZonal20220628.csv"]



# COMMAND ----------

# # Uncomment this to test for broken files

#file_test_is_broken = '' # complete path of the file goes here

#with open(file_test_is_broken, "r") as text_file:
#    unknown = text_file.readlines()
#unknown

# COMMAND ----------

# List all rawfiles
all_raw_filepaths = []

for v in ['ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/' ]:
    file_dir       = f'/{raw_dir}/since2020/{v}/'
    filenames       = os.listdir(file_dir)
    raw_filepaths_v = [file_dir + filename for filename in filenames]

    all_raw_filepaths += raw_filepaths_v



# Keep the ones we should classify
not_classified = all_raw_filepaths # will reclassify
not_classified = list(set(all_raw_filepaths).difference(rawfiles_to_header.raw_filepath))
not_classified_not_broken = [f for f in not_classified if os.path.basename(f) not in broken_files]
n_to_classify = len(not_classified_not_broken)
print(f"{n_to_classify} files to classify")

# COMMAND ----------

# DBTITLE 1,Cell 11
if n_to_classify > 0:
    
    # Check no duplicates in original file paths
    old_and_new_raw_filepaths = list(rawfiles_to_header.raw_filepath) + not_classified_not_broken
    assert len(old_and_new_raw_filepaths) == len(set(old_and_new_raw_filepaths)), "Duplicate raw filepaths detected!"

    # Detect headers by reading first row of each file
    headers = []
    skipped_files = []
    for f in tqdm(not_classified_not_broken):
        try:
            enc = detect_encoding(f)
            if enc is None or enc.lower() == "unknown":
                enc = "latin-1"
            with open(f, encoding=enc) as fin:
                csvin = csv.reader(fin)
                headers.append(next(csvin, []))
        except:
            try:
                csvin = pd.read_csv(f, nrows=0)  # handles zip files
                headers.append(list(csvin.columns))
            except:
                print(f"WARNING: skipping unreadable file: {os.path.basename(f)}")
                skipped_files.append(f)

    # Remove skipped files from classification list
    if skipped_files:
        not_classified_not_broken = [f for f in not_classified_not_broken if f not in skipped_files]
        print(f"Skipped {len(skipped_files)} unreadable files")

    # See how many unique headers we found
    seed(510)
    unique_headers = list(set(tuple(x) for x in headers)) 
    print(f'Unique headers found: {len(unique_headers)}')
    for x in range(len(unique_headers)):
        head = unique_headers[x] 
        print(f'----------------')
        print(sum([h == list(head) for h in headers]), "files")
        print(head)

    # Assert all detected headers are in our known dictionary
    for val in unique_headers: 
        assert list(val) in list(unique_header_dict.values()), f"{val} not in unique_header_dict"

    assert len(not_classified_not_broken) == len(headers)

    # Map each file to its header group
    file_header_dict_inv = {}
    for file, header in zip(not_classified_not_broken, headers):
        for key, value in unique_header_dict.items():
            if header == value:  
                file_header_dict_inv[file] = key
                break  

    # Summary of files per header
    from collections import Counter
    header_counts = Counter(file_header_dict_inv.values())
    for key, cnt in sorted(header_counts.items()):
        print(f"{key}: {cnt}")

    # Build classification dataframe
    file_to_header_df = pd.DataFrame({"raw_filepath": not_classified_not_broken})
    file_to_header_df["header"] = file_to_header_df.raw_filepath.map(file_header_dict_inv)
    file_to_header_df["source_period"] = "since2020"
    file_to_header_df["zipped"] = [int(f.endswith('.zip')) for f in file_to_header_df.raw_filepath]

    # Validate: every file got a header assigned
    assert file_to_header_df.header.isin(unique_header_dict.keys()).all(), "Some files have no header match!"

    # Validate: each zip file contains exactly one file (needed for ingestion)
    zip_files = file_to_header_df[file_to_header_df.zipped == 1]
    for zip_path in tqdm(zip_files.raw_filepath, desc="Checking zips"):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            assert len(zip_ref.namelist()) == 1, f"Zip {zip_path} has multiple files!"

    # Append new classifications to existing table
    rawfiles_to_header = pd.concat([rawfiles_to_header, file_to_header_df], axis=0).drop_duplicates(subset=["raw_filepath"]).reset_index(drop=True)
    print(f"\nTotal classified files: {len(rawfiles_to_header)}")

else:
    print("No new files to classify.")


# COMMAND ----------

# DBTITLE 1,Add broken files with header=broken
# Add broken files to the classification table with header = "broken"
# This includes: (1) pre-known broken files from Cell 8, (2) newly discovered skipped files from Cell 11

# Resolve known broken_files basenames to full paths
known_broken_fullpaths = [f for f in all_raw_filepaths if os.path.basename(f) in broken_files]

# Combine with any newly skipped files from Cell 11
all_broken = known_broken_fullpaths + (skipped_files if 'skipped_files' in dir() else [])

# Remove any that are already in the table (in case of re-runs)
already_recorded = set(rawfiles_to_header.raw_filepath)
all_broken = [f for f in all_broken if f not in already_recorded]

if all_broken:
    broken_df = pd.DataFrame({
        "raw_filepath": all_broken,
        "header": "broken",
        "source_period": "since2020",
        "zipped": [int(f.endswith('.zip')) for f in all_broken]
    })
    rawfiles_to_header = pd.concat([rawfiles_to_header, broken_df], axis=0).drop_duplicates(subset=["raw_filepath"]).reset_index(drop=True)
    print(f"Added {len(all_broken)} broken files to classification table")
    for f in all_broken:
        print(f"  - {os.path.basename(f)}")
else:
    print("No broken files to add (already recorded or none found)")

print(f"\nTotal records (classified + broken): {len(rawfiles_to_header)}")

# COMMAND ----------

# DBTITLE 1,Cell 12
# Save classification to Delta table
rawfiles_to_header_spark = spark.createDataFrame(
    rawfiles_to_header[["raw_filepath", "header", "source_period", "zipped"]]
)
rawfiles_to_header_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("file_to_header_since2020")

print(f"Saved {rawfiles_to_header_spark.count()} rows to file_to_header_since2020")
