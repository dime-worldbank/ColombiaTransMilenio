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

!pip install tqdm
from tqdm import tqdm

import pandas as pd

from random import seed


# COMMAND ----------

from random import seed

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

headers = []
files = []
filenames = []

# get the headers
for v in ['ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/' ]:

    file_path = f'/{raw_dir}/since2020/{v}/'
    files_dir = os.listdir(file_path)
    print(v, len(files_dir))


    for filename in tqdm(files_dir):
        
        file_path_name = file_path + filename

        if filename in broken_files:
            continue

        filenames.append(filename)
        files.append(file_path_name)

        try:
            with open(file_path_name) as fin:
                csvin = csv.reader(fin)
                headers.append(next(csvin, []))
        except:
            try:
                with open(file_path_name, encoding = 'latin1') as fin:
                    csvin = csv.reader(fin)
                    headers.append(next(csvin, []))
            except:
                csvin = pd.read_csv(file_path_name, nrows = 0) # this opens zip files as well
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

# COMMAND ----------

# check whether any type of header is missing in the unique list of headers
# if not, we need to manually add a new type of header to the list
for val in unique_headers: 
    print( list(val) in list(unique_header_dict.values()) )

for val in unique_headers: 
    assert list(val) in list(unique_header_dict.values())

# COMMAND ----------

# Check all lists have the same length
assert len(files) == len(filenames)
assert len(files) == len(headers)

# Check no duplicates in original file paths
assert len(files) == len(set(files))

# Check no duplicates in names alone!
# If not, we will have problems when moving to folders by header 
# as two different files might have same name
assert len(filenames) == len(set(filenames))




# COMMAND ----------

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
# This is to keep track of the already processed 

file_to_header_df = pd.DataFrame({  "filename": filenames,
                                    "raw_filepath": files})
file_to_header_df["header"] = file_to_header_df.raw_filepath.map(file_header_dict_inv)

# check there is a header assigned to each file
assert file_to_header_df.header.isin(unique_header_dict.keys()).mean() == 1

# COMMAND ----------



# COMMAND ----------

# Create the folder to save files by header
try:
   os.mkdir(byheader_dir)
except FileExistsError:
    print("byheader directory exists")

print(os.listdir(byheader_dir))



# COMMAND ----------

# copy each file in each header folder, if not already copied

for folder, files in file_header_dict.items():
    print('FOLDER: ' + folder ) # folder 
    header_dir = byheader_dir + folder
    try:
        os.mkdir(header_dir)
    except FileExistsError:
        print(f"{folder} directory exists")

    copy = files # initial files to copy
    if len(copy) > 0:
        for file in tqdm(copy):
           
            destination_file = os.path.join(header_dir, os.path.basename(file))
          
            # Check if the file already exists in the destination directory
            if not os.path.exists(destination_file):
                shutil.copy(file, header_dir) # copy if it does not exist
                print(f"Copied {file} to {header_dir}")
            
    # This is commented until we work with the full data
    # ALSO, BEWARE THAT NOW WE ARE REMOVING ZIP FILES: WE SHOULD CHECK FOR FILENAMES NO MATTER IF THEY END WITH .ZIP OR NOT
    # remove = list(set(copied).difference(copy0))   # if one file actually did not belonged to that folder 
    # print(Files to remove:", len(remove))
    # CAREFUL; I REMOVED SOME 2017 DATA FROM THEIR FOLDERS ACCIDENTALY BECAUSE OF THIS; RUN THE CODE AGAIN FOR 2017 data
    #if len(remove) > 0:
    #    for file in remove:
    #        os.remove(header_dir + "/" + file)
    #        print('Deleted ' + header_dir + "/" + file)
   
# see what we just saved
for folder, files in file_header_dict.items():
    print('FOLDER: ' + folder ) # folder 
    header_dir = byheader_dir + folder
    f = os.listdir(header_dir)
    print(len(f))
    print(files[:1])

# COMMAND ----------

# UNZIP
for folder in os.listdir(byheader_dir):
    all_files = os.listdir(byheader_dir + folder)
    zip_files = [f for f in all_files if f.endswith('.zip')]
    print(f"Found {len(zip_files)} zip files out of {len(all_files)} in {folder}")

    if len(zip_files) > 0:
        for zip_file in tqdm(zip_files):

            zip_file_path = byheader_dir + folder + "/" + zip_file

             # Extract the ZIP file
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(byheader_dir + folder)

            # Delete the ZIP file after extraction
            os.remove(zip_file_path)

