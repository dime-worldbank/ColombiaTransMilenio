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

## Important sub-directories for this notebook
byheader_dir = path + '/Workspace/Raw/byheader_dir/'

# COMMAND ----------

# MAGIC 
%run ./spark_code/hola.py
%run ./spark_code/packages.py
%run ./spark_code/setup.py

## Note that these won't work if there are errors in the code. Make sure first that everything is OK!

hola("Running hola.py works fine :)")
working("Running packages.py works fine :)")
working2("Running setup.py works fine :)")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Despite setup is supposed to have worked, then I get the error NameError: name 'spark_df_handler' is not defined... So for now I will run setup.py from here
# MAGIC

# COMMAND ----------

#del(spark_df_handler)


## Set up spark
# Check which computer this is running on
if multiprocessing.cpu_count() == 6:
    on_server = False
else:
    on_server = True

# start spark session according to computer
if on_server:
    spark = SparkSession \
        .builder \
        .master("local[75]") \
        .config("spark.driver.memory", "200g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config('spark.local.dir', '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Workspace/') \
        .config("spark.sql.execution.arrow.enabled", "true")\
        .getOrCreate()
else:
    spark = SparkSession.builder.master("local[*]") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.execution.arrow.enabled", "true")\
    .getOrCreate()

# Set paths
if on_server:
    print("OK: ON SERVER")
    path = '/dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData'
    user = os.listdir('/Workspace/Repos')[0]
    git = f'/Workspace/Repos/{user}/Colombia-BRT-IE-temp'
else:
    print("Not on server - no path defined")

## Class to handle spark and df in session

class spark_df_handler:

    """Class to collect spark connection and catch the df in memory.

    Attributes
    ----------
    spark : an initialised spark connection
    df : a spark dataframe that holds the raw data
    on_server : whether

    Methods
    -------
    load(path, pickle = True)
        Loads a pickle or csv

    generate_variables()
        generates additional variables after raw import

    transform()

    memorize(df)
        Catch the df in memory
    """

    def __init__(self,
                spark = spark,
                on_server = on_server):
        self.spark = spark
        self.on_server = on_server






    def load(self, path =  path, type = 'parquet', file = 'parquet_df', delimiter = ';', encoding = "utf-8"):
        if type =='parquet':
            self.df = spark.read.format("parquet").load(os.path.join(path, file)) # changed
            

        elif type == 'pickle':
            name = os.path.join(path, file)
            pickleRdd = self.spark.sparkContext.pickleFile(name = name).collect()
            self.df = self.spark.createDataFrame(pickleRdd)


                
        elif type =='new_data':
            self.dfraw = self.spark.read.format("csv").option("header", "true")\
                                        .option("delimiter", delimiter)\
                                        .option("charset", encoding)\
                                            .load(os.path.join(path,"*")) # reads all files in the path

        else:
            self.dfraw = self.spark.read.format("csv").option("header", "true")\
                                        .option("delimiter", ";")\
                                        .option("charset", "utf-8")\
                                        .load(path) # changed -- reads all files in the path

        

            #self.clean()
            #self.gen_vars()
            
    def transform(self, header_format):
        if header_format == 'format_one':
            self.df = self.dfraw.select(F.to_timestamp(self.dfraw['Fecha de Transaccion'],'yyyyMMddHHmmss')\
                            .alias('transaction_timestamp'),
                            self.dfraw['Emisor'].alias('emisor'),
                            self.dfraw['Operador'].alias('operator'),
                            self.dfraw['Linea'].alias('line'),
                            self.dfraw['Estacion'].alias('station'),
                            self.dfraw['Acceso de Estación'].alias('station_access'),
                            self.dfraw['Dispositivo'].cast('int').alias('machine'),
                            self.dfraw['Tipo de Tarjeta'].alias('card_type'),
                            self.dfraw['Nombre de Perfil'].alias('account_name'),
                            self.dfraw['Numero de Tarjeta'].cast('long').alias('cardnumber'),
                            F.trim(self.dfraw['Saldo Previo a Transaccion']).cast('int')\
                            .alias('balance_before'),
                            F.trim(self.dfraw['Valor']).cast('int').alias('value'),
                            F.trim(self.dfraw['Saldo Despues de Transaccion']).cast('int')\
                            .alias('balance_after'))

        elif header_format == 'format_two':
            self.df = self.dfraw.select(F.to_timestamp(self.dfraw['Fecha de Uso'],'dd-MM-yyyy HH:mm:ss')\
                            .alias('transaction_timestamp'),
                            self.dfraw['Emisor'].alias('emisor'),
                            self.dfraw['Operador'].alias('operator'),
                            self.dfraw['Línea'].alias('line'),
                            self.dfraw['Estación'].alias('station'),
                            self.dfraw['Acceso de Estación'].alias('station_access'),
                            self.dfraw['Dispositivo'].cast('int').alias('machine'),
                            self.dfraw['Tipo de Tarjeta'].alias('card_type'),
                            self.dfraw['Nombre de Perfil'].alias('account_name'),
                            self.dfraw['Número de Tarjeta'].cast('long').alias('cardnumber'),
                            F.trim(self.dfraw['Saldo Previo a Transacción']).cast('int')\
                            .alias('balance_before'),
                            F.trim(self.dfraw['Valor']).cast('int').alias('value'),
                            F.trim(self.dfraw['Saldo Después de Transacción']).cast('int')\
                            .alias('balance_after'))
            
        elif header_format == 'format_three':
            self.df = self.dfraw.select(F.to_timestamp(self.dfraw['Fecha de Transaccion'],'yyyy/MM/dd HH:mm:ss')\
                            .alias('transaction_timestamp'),
                            self.dfraw['Emisor'].alias('emisor'),
                            self.dfraw['Operador'].alias('operator'),
                            self.dfraw['Linea'].alias('line'),
                            self.dfraw['Parada'].alias('station'),
                            self.dfraw['Parada'].alias('station_access'),
                            self.dfraw['Dispositivo'].cast('int').alias('machine'),
                            self.dfraw['Tipo Tarjeta'].alias('card_type'),
                            self.dfraw['Nombre de Perfil'].alias('account_name'),
                            F.trim(self.dfraw['Numero Tarjeta']).cast('long').alias('cardnumber'),
                            F.trim(self.dfraw['Saldo Previo a Transaccion']).cast('int')\
                            .alias('balance_before'),
                            F.trim(self.dfraw['Valor']).cast('int').alias('value'),
                            F.trim(self.dfraw['Saldo Despues de Transaccion']).cast('int')\
                            .alias('balance_after'))
            
        elif header_format == 'format_four':
            self.df = self.dfraw.select(F.to_timestamp(self.dfraw['Fecha de Transaccion'],'yyyyMMddHHmmss')\
                            .alias('transaction_timestamp'),
                            self.dfraw['Emisor'].alias('emisor'),
                            self.dfraw['Operador'].alias('operator'),
                            self.dfraw['Linea'].alias('line'),
                            self.dfraw['Parada'].alias('station'),
                            self.dfraw['Parada'].alias('station_access'),
                            self.dfraw['Dispositivo'].cast('int').alias('machine'),
                            self.dfraw['Tipo Tarjeta'].alias('card_type'),
                            self.dfraw['Nombre de Perfil'].alias('account_name'),
                            F.trim(self.dfraw['Numero Tarjeta']).cast('long').alias('cardnumber'),
                            F.trim(self.dfraw['Saldo Previo a Transaccion']).cast('int')\
                            .alias('balance_before'),
                            F.trim(self.dfraw['Valor']).cast('int').alias('value'),
                            F.trim(self.dfraw['Saldo Despues de Transaccion']).cast('int')\
                            .alias('balance_after'))
            
        elif header_format == 'format_five':
            self.df = self.dfraw.select(F.to_timestamp(self.dfraw['Fecha de Uso'],'dd-MM-yyyy HH:mm:ss')\
                            .alias('transaction_timestamp'),
                            self.dfraw['Emisor'].alias('emisor'),
                            self.dfraw['Operador'].alias('operator'),
                            self.dfraw['Línea'].alias('line'),
                            self.dfraw['Parada'].alias('station'),
                            self.dfraw['Parada'].alias('station_access'),
                            self.dfraw['Dispositivo'].cast('int').alias('machine'),
                            self.dfraw['Tipo de Tarjeta'].alias('card_type'),
                            self.dfraw['Nombre de Perfil'].alias('account_name'),
                            self.dfraw['Número de Tarjeta'].cast('long').alias('cardnumber'),
                            F.trim(self.dfraw['Saldo Previo a Transacción']).cast('int')\
                            .alias('balance_before'),
                            F.trim(self.dfraw['Valor']).cast('int').alias('value'),
                            F.trim(self.dfraw['Saldo Después de Transacción']).cast('int')\
                            .alias('balance_after'))
            
            # new formats Wendy  


            ## this goes with header_08, header_09, header_10, header_15
        elif header_format == 'format_6':
            self.df = self.dfraw.select( 
                F.to_timestamp( F.regexp_replace(self.dfraw['Fecha_Transaccion'], ' UTC', ''),
                                'yyyy-MM-dd HH:mm:ss').alias('transaction_timestamp'),
            self.dfraw['Emisor'].alias('emisor'),
            self.dfraw['Operador'].alias('operator'),
            self.dfraw['Linea'].alias('line'),
            self.dfraw['Estacion_Parada'].alias('station'),
            self.dfraw['Estacion_Parada'].alias('station_access'),
            self.dfraw['Dispositivo'].cast('int').alias('machine'),
            self.dfraw['Tipo_Tarjeta'].alias('card_type'),
            self.dfraw['Nombre_Perfil'].alias('account_name'),
            self.dfraw['Numero_Tarjeta'].alias('cardnumber'),
            F.trim(self.dfraw['Saldo_Previo_a_Transaccion']).cast('int')\
                .alias('balance_before'),
            F.trim(self.dfraw['Valor']).cast('int').alias('value'),
            F.trim(self.dfraw['Saldo_Despues_Transaccion']).cast('int')\
                .alias('balance_after'),
            self.dfraw['Sistema'].alias('system'))
            
            ## this goes with header_11, header_12, header_13, header_14
        elif header_format == 'format_7':
                self.df = self.dfraw.select( 
                    F.to_timestamp( F.regexp_replace(self.dfraw['Fecha_Transaccion'], ' UTC', ''),
                    'yyyy-MM-dd HH:mm:ss').alias('transaction_timestamp'),
                self.dfraw['Emisor'].alias('emisor'),
                self.dfraw['Operador'].alias('operator'),
                self.dfraw['Linea'].alias('line'),
                self.dfraw['Estacion_Parada'].alias('station'),
                self.dfraw['Acceso_Estacion'].alias('station_access'),
                self.dfraw['Dispositivo'].cast('int').alias('machine'),
                self.dfraw['Tipo_Tarjeta'].alias('card_type'),
                self.dfraw['Nombre_Perfil'].alias('account_name'),
                self.dfraw['Numero_Tarjeta'].alias('cardnumber'),
                F.trim(self.dfraw['Saldo_Previo_a_Transaccion']).cast('int')\
                    .alias('balance_before'),
                F.trim(self.dfraw['Valor']).cast('int').alias('value'),
                F.trim(self.dfraw['Saldo_Despues_Transaccion']).cast('int')\
                    .alias('balance_after'),
                self.dfraw['Sistema'].alias('system'))

    def memorize(self):
        # Register as table to run SQL queries
        self.df.createOrReplaceTempView("df_table")
        self.spark.sql('CACHE TABLE df_table').collect()

        return self.df

# COMMAND ----------

# MAGIC %md
# MAGIC ## (1) Unify structure 
# MAGIC <mark> With data since 2020 so far </mark>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1. Reorganize files by header

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
# MAGIC ### 1.2 Importing based on different schemata, check NAs and save
# MAGIC
# MAGIC The section below is written so it can be run independently fgrom the section above

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2.a. Importing based on different schemata

# COMMAND ----------

# import depending on header
header_folders = os.listdir(byheader_dir)
print(f"We have {len(header_folders)} folders: {header_folders}")
spark_handlers = [spark_df_handler() for _ in range(len(header_folders))]

# COMMAND ----------

# Link each header to its formats
headers    =  ['header_one', 'header_two', 'header_three', 'header_four', 'header_five', 'header_six', 'header_seven',
               'header_08', 'header_09', 'header_10', 'header_11', 'header_12', 'header_13', 'header_14', 'header_15', 'header_16']
print(f"The following folder should be added to the headers list: {set(header_folders).difference(headers)}. The set SHOULD be empty. ")

delimiters = [','   , ';'   , ','   , ';'  ,
              ';'   , ','   ,  ','  , ','  ,
              ','   , ','   ,  ','  , ','  ,
              ','   , ','   ,  ','  , ','  ]
encodings = ["utf-8" ,"utf-8", "utf-8", "utf-8" ,"latin1", "utf-8" ,"utf-8",
             "utf-8" ,"utf-8", "utf-8" ,"utf-8","utf-8" ,"utf-8","utf-8" ,"utf-8", "utf-8"]

# different headers might have the same or different formats (find the format for headers six and seven, but as for now those folders are missing)
formats =   ['format_two', 'format_four', 'format_five', 'format_one', 'format_three', '', '',
             'format_6' , 'format_6' , 'format_6',
             'format_7' , 'format_7' , 'format_7', 'format_7',
             'format_6']

# COMMAND ----------

for idx, handler in enumerate(spark_handlers):
          
    if idx < 7: # skip the first 7 headers
        continue
    
    h = headers[idx]
    print('\n Schema ' + h)
    handler.load(type = 'new_data',
                 path = os.path.join(pathdb ,"Workspace/Raw/byheader_dir/" , h),
                 delimiter = delimiters[idx], 
                 encoding = encodings[idx])
    display(handler.dfraw.limit(1).toPandas())

# COMMAND ----------

for idx, handler in enumerate(spark_handlers):
    
    # skip the first 7 headers, use it later to work with full data
    # skip the 16th header, empty data
    if (idx < 7) | (idx == 15): 
        continue
    
    h = headers[idx]
    print('\n Schema ' + h)

    handler.transform(header_format = formats[idx])
    display(handler.df.limit(1).toPandas())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2.b. Handling NAs

# COMMAND ----------

#Accelerate queries with Delta: This query contains a highly selective filter. To improve the performance of queries, convert the table to Delta and run the OPTIMIZE ZORDER BY command on the table dbfs:/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Workspace/Raw/byheader_dir/header_08/validacionDual20200229.csv, dbfs:/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Workspace/Raw/byheader_dir/header_09/validacionDual20220619.csv.

# COMMAND ----------

for idx, handler in enumerate(spark_handlers):
    if (idx < 7) | (idx == 15): 
        continue

    h = headers[idx]
    print('\n Schema ' + h)
    handler.df.where(handler.df['value'].isNull()).show()


# COMMAND ----------

# MAGIC %md
# MAGIC - There are some nulls in values, but while other columns are OK. These obs. are OK.
# MAGIC - We should check what seem to be corrupted files:  Schema header_10

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2.c. Join and save all data

# COMMAND ----------

# Initialize an empty DataFrame
df = spark_handlers[7].df  # Start with the first DataFrame

for idx, handler in enumerate(spark_handlers):
    
    # skip the first 7 headers, use it later to work with full data
    # skip the 8th which we used to initialize the df
    # skip the 16th header, empty data
    if (idx < 8) | (idx == 15): 
        continue

    # Loop through the rest of the Spark handlers and union their DataFrames
    df = df.union(spark_handler.df)  


# COMMAND ----------

os.mkdir(os.path.join(path, 'Workspace/bogota-hdfs'))

# COMMAND ----------

# Create parquet file of raw data
df.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/parquet_df_raw_new_data'))

# COMMAND ----------

os.listdir(os.path.join(path, 'Workspace/bogota-hdfs/parquet_df_raw_new_data'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling duplicates

# COMMAND ----------


