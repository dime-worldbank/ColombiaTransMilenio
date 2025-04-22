# Databricks notebook source
# MAGIC %md
# MAGIC # Reorganize data by header
# MAGIC

# COMMAND ----------

# Pip install non-standard packages
!pip install rarfile
!pip install findspark
!pip install pyspark
!pip install plotly
!pip install pyspark_dist_explore
!pip install geopandas
!pip install seaborn
!pip install folium
!pip install editdistance
!pip install scikit-mobility
!pip install chart_studio
!pip install tqdm
!pip install pyunpack
!pip install patool

import shutil
import sys
import os
import zipfile

from tqdm import tqdm
import pandas as pd
from random import seed
from deltalake import DeltaTable, write_deltalake

# COMMAND ----------

# Directories
S_DIR = '/Volumes/prd_csc_mega/sColom15/'
V_DIR = f'{S_DIR}vColom15/'
user = 'wbrau@worldbank.org'
git = f'/Workspace/Users/{user}/ColombiaTransMilenio'
#git2 = f'/Workspace/Users/{user}/Colombia-BRT_IE-temp/'

# Important sub-directories for this notebook
raw_dir      =  V_DIR + '/Workspace/Raw/'
byheader_dir =  V_DIR + '/Workspace/Raw/byheader_dir/'



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

# Create general folder to save files by header
try:
   os.mkdir(byheader_dir)
except FileExistsError:
    print("byheader directory exists, with the folders:")
    print(os.listdir(byheader_dir))

# Create table to list filename and header if it does not exist
table_name = "file_to_header"
table_exists = spark.catalog.tableExists(table_name)
if not table_exists:
    spark.sql(f"""
    CREATE TABLE {table_name} (
        raw_filepath STRING,
        header       STRING
        header_filepath STRING
    )
    USING DELTA
""")

# Read the table
rawfiles_to_header  = spark.read.format("delta").table(table_name)
rawfiles_to_header   = rawfiles_to_header .toPandas()

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

rawfiles_to_header  = rawfiles_to_header.drop(columns = ["filename"]) 
rawfiles_to_header.head()

# COMMAND ----------

# List all rawfiles
all_raw_filepaths = []

for v in ['ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/' ]:
    file_dir       = f'/{raw_dir}/since2020/{v}/'
    filenames       = os.listdir(file_path)
    raw_filepaths_v = [file_path + filename for filename in files_dir]

    all_raw_filepaths += filepaths_v

# Keep the ones we should classify
not_classified = list(set(all_raw_filepaths).difference(rawfiles_to_header.raw_filepath))
not_classified_not_broken = [f for f in not_classified if os.path.basename(f) not in broken_files]
print(len(not_classified_not_broken))

# COMMAND ----------

old_and_new_raw_filepaths = list(rawfiles_to_header.raw_filepath) + not_classified_not_broken
old_and_new_filenames     = [os.path.basename(f) for f in old_and_new_raw_filepaths]

# Check no duplicates in original file paths
assert len(old_and_new_raw_filepaths) == len(set(old_and_new_raw_filepaths))

# Check no duplicates in names alone!
# If not, we will have problems when moving to folders by header 
# as two different files might have same name
assert len(old_and_new_filenames) == len(set(old_and_new_filenames))


# COMMAND ----------

headers = []

# get the headers
for f in not_classified_not_broken:

    try:
        with open(f) as fin:
            csvin = csv.reader(fin)
            headers.append(next(csvin, []))
    except:
        try:
            with open(f, encoding = 'latin1') as fin:
                csvin = csv.reader(fin)
                headers.append(next(csvin, []))
        except:
            csvin = pd.read_csv(f, nrows = 0) # this opens zip files as well
            headers.append(list(csvin.columns))


# COMMAND ----------

# see how many and which headers we have
seed(510)
unique_headers = list(set(tuple(x) for x in headers)) 
print(f'Unique headers: {len(unique_headers)}')
for x in range(len(unique_headers)):
      head = unique_headers[x] 
      print(f'----------------')
      print(sum([h == list(head) for h in headers]), "files")
      print(head)


# check whether any type of header is missing in the unique list of headers
# if so, we need to manually add a new type of header to the list
for val in unique_headers: 
    assert list(val) in list(unique_header_dict.values()), f"{val} not in unique_header_dict"

# Check all lists have the same length
assert len(not_classified_not_broken) == len(headers)

# file_header_dict lists all files per header
# with the format {header_number : [list of files]}

file_header_dict = {key: [] for key in unique_header_dict.keys()}

for file, header in zip(files, headers):
    for key, value in unique_header_dict.items():
        if header == value:  
            file_header_dict[key].append(file)
            break  

# see number of files in each header
for key, val in file_header_dict.items():
    print(f"{key}: {len(val)}")

# file_header_dict_inv maps each file to the header
# with the format {file : header_number}
file_header_dict_inv = {}

for key, values in file_header_dict.items():
    for value in values:
        file_header_dict_inv[value] = key 

# COMMAND ----------

# Create dataset with filename, complete file path, and corresponding header
file_to_header_df = pd.DataFrame({ "raw_filepath": not_classified_not_broken})
file_to_header_df["header"] = file_to_header_df.raw_filepath.map(file_header_dict_inv)

# check there is a header assigned to each file
if file_to_header_df.shape[0] > 0 :
    assert file_to_header_df.header.isin(unique_header_dict.keys()).mean() == 1

## add to the table with preexistent files
rawfiles_to_header  = pd.concat([rawfiles_to_header , file_to_header_df], axis = 0).drop_duplicates()

## recheck there are no duplicates in filename
filenames = [os.path.basename(f) for f in rawfiles_to_header.raw_filepath]
assert len(filenames) == len(set(filenames))
           
# Include the names of zipped files in the list
rawfiles_to_header ["zipped"] = [f.endswith('.zip') * 1 for f in rawfiles_to_header.raw_filepath ]
zip_files = rawfiles_to_header[rawfiles_to_header.zipped == 1].reset_index(drop = True)

# Check that each zip file contains just one file
for zip_path in tqdm(zip_files.raw_filepath):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
       assert len(zip_ref.namelist()) == 1

# COMMAND ----------


def unzip_and_rename(zip_path, output_dir):

    '''
    Inputs:
    * zip_path: complete zip file path
    * output_dir: output directory

    Output:
    * Saving unzipped file in output_dir with the same name as the zip file, but without the .zip extension
    * Returns the name of the unzipped file
    '''

    base_name       = os.path.basename(zip_path)
    base_name_nozip = os.path.splitext(base_name)[0]  # original zip file name

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        inner_file = zip_ref.namelist()[0] # before we asserte all zips had only one zipped file inside
       
        ext = os.path.splitext(inner_file)[1]  # zipped file extension
        output_name = base_name_nozip + ext

        output_file_path = os.path.join(output_dir, output_name)

        with zip_ref.open(inner_file) as source, open(output_file_path, 'wb') as target:
            target.write(source.read())

        return output_name



# COMMAND ----------

# create header folders if they do not exist
for folder in file_header_dict.keys():
    header_dir = byheader_dir + folder
    try:
        os.mkdir(header_dir)
    except FileExistsError:
        print(f"{folder} directory exists")


# COMMAND ----------

# copy each file in each header folder, if not already copied
# in case of zip files: do not copy, extract if not already extracted

for item in tqdm(range(rawfiles_to_header .shape[0])):
    
    already_copied = rawfiles_to_header.header_filepath[item] != ""
    if already_copied:
        continue

    header_dir      = byheader_dir + rawfiles_to_header .header[item]
    zipped          = rawfiles_to_header .zipped[item]
    raw_filepath    = rawfiles_to_header .raw_filepath[item]
    filename        = rawfiles_to_header .filename[item]
    
    if zipped == 1:
        output_name     = unzip_and_rename(raw_filepath, header_dir)
        output_filepath = os.path.join(header_dir, output_name) 
        
    if zipped == 0:
        output_name     = filename
        output_filepath = os.path.join(header_dir, output_name) 
        if not os.path.exists(output_filepath):
            shutil.copy(raw_filepath, output_filepath) 

    rawfiles_to_header .header_filepath[item] = output_filepath


# COMMAND ----------

# (1) CHECK THAT ALL FILES ARE SAVED IN THE CORRESPONDING FOLDER
assert sum(rawfiles_to_header .header_filepath == "") == 0
for filepath in rawfiles_to_header.header_filepath:
    assert os.path.exists(output_filepath)

# COMMAND ----------

# (2) REMOVE FILES THAT DO NOT BELONG TO THE FOLDER
for folder in file_header_dict.keys():

     # CAREFUL; I REMOVED SOME 2017 DATA FROM THEIR FOLDERS ACCIDENTALY BECAUSE I DIDN'T HAVE THE FOLLOWING CONDITION; RUN THE CODE AGAIN FOR 2017 data
    if folder in ["header_one", "header_two", "header_three", "header_four", "header_five", "header_six", "header_seven"]:
        continue

    header_dir         = byheader_dir + folder
    files_in_folder    = os.listdir(header_dir)
    shouldbe_in_folder = rawfiles_to_header .header_filepath[rawfiles_to_header .header == folder]
    shouldbe_in_folder = [os.path.basename(f) for f in shouldbe_in_folder]

    remove = list(set(files_in_folder).difference(shouldbe_in_folder))
    remove = [header_dir + "/" + f for f in remove]
    print(f"Folder {folder}: remove {len(remove)} files")

    if len(remove) > 0:
        for file in tqdm(remove):
            os.remove(file)

# COMMAND ----------

# overwrite delta table
rawfiles_to_header_spark = spark.createDataFrame(rawfiles_to_header)
rawfiles_to_header_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)
