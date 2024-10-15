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

hola("Running hola.py works fine :)")
working("Running packages.py works fine :)")


# COMMAND ----------

# MAGIC %md
# MAGIC ## (1) Unify structures based on file headers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Since 2020
# MAGIC
# MAGIC Structure:
# MAGIC - Create datasets by by type or by month? 
# MAGIC   - we may continue to update this data

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
      print(f'---------------- Header {x}:')
      print(sum([h == list(head) for h in headers]), "files")
      print(head)

# COMMAND ----------

# unique file with no headers: empty data
print(unique_headers[8])
filter = [h == list(unique_headers[8]) for h in headers]
noheaders = list(compress(files, filter)) 
print(noheaders)

# COMMAND ----------

# Old list of headers by Sebastian for 2016-2017 data

unique_header_list = [['Fecha de Liquidación', 'Fecha de Uso', 'Day Group Type', 'Hora Pico S/N', 'Fase', 'Emisor', 'Operador', 'Línea', 'Estación', 'Acceso de Estación', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Número de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transacción', 'Valor', 'Saldo Después de Transacción'],
                      ['Fecha de Clearing;Fecha de Transaccion;Hora Pico SN;Fase;Emisor;Operador;Linea;Ruta;Parada;Tipo Vehiculo;ID Vehiculo;Dispositivo;Tipo Tarjeta;Nombre de Perfil;Numero Tarjeta;Tipo de Tarifa;Saldo Previo a Transaccion;Valor;Saldo Despues de Transaccion',],
                      ['Fecha de Liquidación', 'Fecha de Uso', 'Day Group Type', 'Hora Pico S/N', 'Fase', 'Emisor', 'Operador', 'Línea', 'Ruta', 'Parada', 'Tipo de Vehículo', 'ID de Vehículo', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Número de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transacción', 'Valor', 'Saldo Después de Transacción'],
                      ['Fecha de Clearing;Fecha de Transaccion;Day Group Type;Hora Pico SN;Fase;Emisor;Operador;Linea;Estacion;Acceso de Estación;Dispositivo;Tipo de Tarjeta;Nombre de Perfil;Numero de Tarjeta;Tipo de Tarifa;Saldo Previo a Transaccion;Valor;Saldo Despues de Transaccion',],
                      ['Fecha de Clearing;Fecha de Transaccion;Hora Pico SN;Fase;Emisor;Operador;Linea;Ruta;Parada;Tipo Vehiculo;ID Vehiculo;Dispositivo;Tipo Tarjeta;Nombre de Perfil;Numero Tarjeta;Tipo de Tarifa;Saldo Previo a Transaccion;Valor;Saldo Despues de Transaccion;Ruta_Modificada;Linea_Modificada;Cenefa;Parada_Modificada',],
                      ['Fecha de Clearing', 'Fecha de Transaccion', 'DAY_GROUP_CD', 'Hora Pico SN', 'Fase', 'Emisor', 'Operador', 'Linea', 'Ruta', 'Parada', 'Tipo Vehiculo', 'ID Vehiculo', 'Dispositivo', 'Tipo Tarjeta', 'Nombre de Perfil', 'Numero Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transaccion', 'Valor', 'Saldo Despues de Transaccion'],
                      ['Fecha de Clearing', 'Fecha de Transaccion', 'DAY_GROUP_CD', 'Hora Pico SN', 'Emisor', 'Operador', 'Linea', 'Estacion', 'Acceso de Estación', 'Dispositivo', 'Tipo de Tarjeta', 'Nombre de Perfil', 'Numero de Tarjeta', 'Tipo de Tarifa', 'Saldo Previo a Transaccion', 'Valor', 'Saldo Despues de Transaccion']]

for val in unique_headers: 
    print(list(val) in unique_header_list)  

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
