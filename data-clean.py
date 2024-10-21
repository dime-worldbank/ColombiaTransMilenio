# Databricks notebook source
# MAGIC %md
# MAGIC # Clean data
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

# COMMAND ----------

# Directories
pathdb  = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/'
path = '/dbfs/' + pathdb
user = os.listdir('/Workspace/Repos')[0]
git = '/Workspace/Repos/' +user+ '/ColombiaTransMilenio'

# COMMAND ----------

# MAGIC 
%run ./spark_code/hola.py
%run ./spark_code/packages.py
%run ./spark_code/setup2.py

hola("Running hola.py works fine :)")
working("Running packages.py works fine :)")
setup_run("Running setup.py works fine :)")


# COMMAND ----------

# MAGIC %md
# MAGIC ## (1) Unify structures based on file headers
# MAGIC
# MAGIC <mark>With data since 2020 so far</mark>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Since 2020
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Headers: different types of datasets

# COMMAND ----------

headers = []
files = []

for v in ['ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/' ]:

    file_path = path + f'/Workspace/Raw/since2020/{v}/'
    files_dir = os.listdir(file_path)
    print(v, len(files_dir))


    for filename in tqdm(files_dir):
        files.append(file_path + filename)
        try:
            with open(file_path + filename) as fin:
                csvin = csv.reader(fin)
                headers.append(next(csvin, []))
        except:
            try:
                with open(file_path + filename, encoding = 'latin1') as fin:
                    csvin = csv.reader(fin)
                    headers.append(next(csvin, []))
            except:
                csvin = pd.read_csv(file_path + filename, nrows = 0)
                headers.append(list(csvin.columns))

# COMMAND ----------

# see how many and which headers we have
seed(510)
unique_headers = list(set(tuple(x) for x in headers)) # this will make that each time it is run
print(f'Unique headers: {len(unique_headers)}')
for x in range(len(unique_headers)):
      head = unique_headers[x] 
      print(f'----------------')
      print(sum([h == list(head) for h in headers]), "files")
      print(head)

# COMMAND ----------

# Old list of headers by Sebastian for 2016-2017 data:  letters (one - seven)
# New list of headers by Wendy for data since 2020: numbers (8-16)

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
                        ['', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion',  'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
                    'header_09':
                         ['Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
                    'header_10': 
                        ['Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor', 'archivo'],
                    'header_11': 
                        ['Acceso_Estacion', 'Day_Group_Type', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor', 'archivo'],
                    'header_12': 
                        ['Acceso_Estacion', 'Day_Group_Type', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
                    'header_13': 
                        ['', 'Acceso_Estacion', 'Day_Group_Type', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Sistema', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor'],
                   'header_14':  
                       ['', 'Acceso_Estacion', 'Day_Group_Type', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Tipo_Tarifa', 'Tipo_Tarjeta', 'Valor', 'ID_Vehiculo', 'Ruta', 'Tipo_Vehiculo', 'Sistema'],
                    'header_15': 
                        ['', 'Dispositivo', 'Emisor', 'Estacion_Parada', 'Fase', 'Fecha_Clearing', 'Fecha_Transaccion', 'Hora_Pico_SN', 'ID_Vehiculo', 'Linea', 'Nombre_Perfil', 'Numero_Tarjeta', 'Operador', 'Ruta', 'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion', 'Tipo_Tarjeta', 'Tipo_Vehiculo', 'Valor', 'Sistema'],
                    'header_16': []}

# check whether any type of header is missing in the unique list of headers
for val in unique_headers: 
    print(list(val) in list(unique_header_dict.values()))  


# COMMAND ----------

# file_header_dict maps each file to the corresponding header type

file_header_dict = {key: [] for key in unique_header_dict.keys()}

for file, header in zip(files, headers):
    for key, value in unique_header_dict.items():
        if header == value:  # Check if the header matches the value in unique_header_dict
            file_header_dict[key].append(file)
            break  # Exit the inner loop once the match is found

# see number of files in each header
for key, val in file_header_dict.items():
    print(f"{key}: {len(files)}")

del(headers, files)

# COMMAND ----------

# Move the files to folders based on their header
byheader_dir = path + '/Workspace/Raw/byheader_dir/'
try:
   os.mkdir(byheader_dir)
except FileExistsError:
    print("byheader directory exists")

os.listdir(byheader_dir)

# COMMAND ----------

# check we have no duplicate filenames!

dupfiles = [item for sublist in list(file_header_dict.values()) for item in sublist]

for v in ['ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/' ]:
    file_path = path + f'/Workspace/Raw/since2020/{v}/'
    dupfiles = [s.replace(file_path, "") for s in dupfiles]

print(len(dupfiles), len(set(dupfiles)))


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
            # Construct the destination file path
            destination_file = os.path.join(header_dir, os.path.basename(file))
            # Check if the file already exists in the destination directory
            if not os.path.exists(destination_file):
                shutil.copy(file, header_dir) # copy if it does not exist
                print(f"Copied {file} to {header_dir}")
            
    # This is commented until we work with the full data
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

# MAGIC %md
# MAGIC ### Importing based on different schemata

# COMMAND ----------

# import depending on header
spark_handlers = [spark_df_handler() for _ in range(len(file_header_dict))]

# COMMAND ----------

#until now we have 5 headers

spark_handler_one, spark_handler_two, spark_handler_three, spark_handler_four, spark_handler_five = spark_df_handler(), spark_df_handler(), spark_df_handler(), spark_df_handler(), spark_df_handler()
spark_handlers = [spark_handler_one, spark_handler_two, spark_handler_three, spark_handler_four, spark_handler_five]


delimiters = [','    , ';'   , ','    , ';'     ,';'     ]
encodings = ["utf-8" ,"utf-8", "utf-8", "utf-8" ,"latin1"]
dfs = []

from IPython.display import display

for idx, handler in enumerate(spark_handlers):
    print('\n Schema ' + str(idx + 1))
    handler.load(type = 'new_data',
                 path = os.path.join(pathdb ,"Workspace/Raw/byheader_dir" , header_folder_names[idx]),
                 delimiter = delimiters[idx], 
                 encoding = encodings[idx])
    display(handler.dfraw.limit(2).toPandas())

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# See files
[f.name for f in  dbutils.fs.ls(raw + '/since2020')]


# COMMAND ----------

# See files
files = [f.name for f in  dbutils.fs.ls(raw + '/since2020/ValidacionTroncal/')]
fdates = [ int(f[-12:-4]) for f in files ]
print( min(fdates), max(fdates))

# COMMAND ----------



# COMMAND ----------

files[-1][-12:-4]
