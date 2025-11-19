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
!pip install deltalake

import shutil
import sys
import os
import zipfile
import chardet
from tqdm import tqdm
import pandas as pd
from random import seed
from deltalake import DeltaTable, write_deltalake

# COMMAND ----------

# MAGIC %run ./utils/handle_files

# COMMAND ----------

# Directories
S_DIR = '/Volumes/prd_csc_mega/sColom15/'
V_DIR = f'{S_DIR}vColom15/'
user = 'wbrau@worldbank.org'
git = f'/Workspace/Users/{user}/ColombiaTransMilenio'

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

# MAGIC %sql
# MAGIC -- Create table to list filename and header if it does not exist
# MAGIC CREATE TABLE IF NOT EXISTS file_to_header (
# MAGIC     raw_filepath STRING,
# MAGIC     header       STRING,
# MAGIC     header_filepath STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# Create general folder to save files by header
try:
   os.mkdir(byheader_dir)
except FileExistsError:
    print("byheader directory exists, with the folders:")
    print(os.listdir(byheader_dir))


# Read the table
rawfiles_to_header  = spark.read.format("delta").table("file_to_header")
rawfiles_to_header   = rawfiles_to_header.toPandas()

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

#file_dir       = f'/{raw_dir}/from2016to2019/'
#filenames       = os.listdir(file_dir)
#raw_filepaths = [file_dir + filename for filename in filenames]
#all_raw_filepaths += raw_filepaths


# Keep the ones we should classify
not_classified = all_raw_filepaths # will reclassify
not_classified = list(set(all_raw_filepaths).difference(rawfiles_to_header.raw_filepath))
not_classified_not_broken = [f for f in not_classified if os.path.basename(f) not in broken_files]
n_to_classify = len(not_classified_not_broken)
print(f"{n_to_classify} files to classify")

# COMMAND ----------

if n_to_classify > 0:
    
    # Check no duplicates in original file paths
    # and check no duplicates in names alone! 
    # If not, we will have problems when moving to folders by header 
    # as two different files might have same name
    old_and_new_raw_filepaths = list(rawfiles_to_header.raw_filepath) + not_classified_not_broken
    old_and_new_filenames     = [os.path.basename(f) for f in old_and_new_raw_filepaths]
    assert len(old_and_new_raw_filepaths) == len(set(old_and_new_raw_filepaths))
    assert len(old_and_new_filenames) == len(set(old_and_new_filenames))

    # get the headers and encodings
    headers = []
    for f in tqdm(not_classified_not_broken):
        try:
            enc = detect_encoding(f)
            assert enc == 'ISO-8859-1'
            with open(f, encoding = enc) as fin:
                csvin = csv.reader(fin)
                headers.append(next(csvin, []))
        except:
            csvin = pd.read_csv(f, nrows = 0) # this opens zip files as well
            headers.append(list(csvin.columns))
   

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
    for file, header in zip(not_classified_not_broken, headers):
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

    # Create dataset with file path, corresponding header, and indicator of whether it is a zipped file
    file_to_header_df = pd.DataFrame({ "raw_filepath": not_classified_not_broken,})
    file_to_header_df["header"] = file_to_header_df.raw_filepath.map(file_header_dict_inv)
    file_to_header_df["zipped"] = [f.endswith('.zip') * 1 for f in file_to_header_df.raw_filepath ]

    # check there is a header assigned to each file
    assert file_to_header_df.header.isin(unique_header_dict.keys()).mean() == 1

    # check that each zip file contains just one file
    zip_files = file_to_header_df[rawfiles_to_header.zipped == 1].reset_index(drop = True)
    for zip_path in tqdm(zip_files.raw_filepath):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            assert len(zip_ref.namelist()) == 1

    ## add to the table with preexistent files
    rawfiles_to_header  = pd.concat([rawfiles_to_header , file_to_header_df], axis = 0).drop_duplicates().reset_index(drop = True)

    ## recheck there are no duplicates in filename
    filenames = [os.path.basename(f) for f in rawfiles_to_header.raw_filepath]
    assert len(filenames) == len(set(filenames))

    # create header folders if they do not exist
    for folder in file_header_dict.keys():
        header_dir = byheader_dir + folder
        try:
            os.mkdir(header_dir)
        except FileExistsError:
            print(f"{folder} directory exists")


# COMMAND ----------

# Copy each file in each header folder, if not already copied
# in case of zip files: do not copy, extract if not already extracted
not_copied = (rawfiles_to_header.header_filepath == "") | (rawfiles_to_header.header_filepath.isnull()) 
n_to_copy = sum( not_copied )  
print(f"{n_to_copy} files to copy")

print("1) Copy files")
if n_to_copy > 0:
    for item in tqdm(range(rawfiles_to_header.shape[0])):
        
        not_copied_f = not_copied[item]
        if not_copied_f:
           
            header_dir      = byheader_dir + rawfiles_to_header .header[item]
            zipped          = rawfiles_to_header .zipped[item]
            raw_filepath    = rawfiles_to_header .raw_filepath[item]
            
            if zipped == 1:
                output_name     = unzip_and_rename(raw_filepath, header_dir)
                output_filepath = os.path.join(header_dir, output_name) 
                
            if zipped == 0:
                output_name     = os.path.basename(raw_filepath)
                output_filepath = os.path.join(header_dir, output_name) 
                if not os.path.exists(output_filepath):
                    shutil.copy(raw_filepath, output_filepath) 

            rawfiles_to_header .header_filepath[item] = output_filepath
    
print("2) Checking that files are saved in the corresponding folder")
assert sum(rawfiles_to_header.header_filepath == "") == 0

errors = []
errors_count = 0
notfound = []
notfound_count = 0

for filepath in tqdm(rawfiles_to_header.header_filepath):
    if not os.path.exists(filepath):
        errors.append(filepath)
        errors_count +=1
        error = rawfiles_to_header[rawfiles_to_header.header_filepath == filepath].reset_index()
        assert error.zipped[0] == 0
        try:
            shutil.copy(error.raw_filepath[0], error.header_filepath[0]) 
            assert os.path.exists(filepath)
        except FileNotFoundError:
            notfound_count += 1
            notfound.append(filepath)

print("3) Remove files that do not belong to the folder")
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

print(errors_count)
print(notfound_count)
print(errors == notfound)
assert errors_count   == 0
assert notfound_count ==  0

# COMMAND ----------

# overwrite delta table
rawfiles_to_header_spark = spark.createDataFrame(rawfiles_to_header)
rawfiles_to_header_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("file_to_header")
