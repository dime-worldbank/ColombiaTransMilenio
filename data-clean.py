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
git = '/Workspace/Repos/' +user+ '/ColombiaTransMilenio/'
git2 = '/Workspace/Repos/' +user+ '/Colombia-BRT_IE-temp/'
## Important sub-directories for this notebook
byheader_dir = path + '/Workspace/Raw/byheader_dir/'

# COMMAND ----------

# MAGIC 
%run ./utils/import_test.py
%run ./utils/packages.py
%run ./utils/setup.py
%run ./utils/utilities.py


## Note that these won't work if there are errors in the code. Make sure first that everything is OK!

# Functions
import_test_function("Running hola.py works fine :)")
import_test_packages("Running packages.py works fine :)")
import_test_setup("Running setup.py works fine :)")
import_test_utilities("Running utilities.py works fine :)")

# Classes and others
objects_in_dir = dir()
print('ImportTestClass'    in objects_in_dir, 
      'spark_df_handler'   in objects_in_dir,
      'user_window'        in objects_in_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Things that did not imported OK

# COMMAND ----------

# generate variables
def generate_variables(df):
    
    ## Generate additional variables
    # Create real balance variable
    df = df.withColumn('real_balance_after', 
                                    df['balance_before'] - df['value'])
    
    # Create transfer dummy variable
    df = df.withColumn('transfer',
                                    F.when(df['value'] < 500, 1).otherwise(0).cast('byte'))

    # Create transfer time variable
    df = df.withColumn('transfer_time',
                                    F.when(df['transfer'] == True,
                                        ( F.unix_timestamp(df['transaction_timestamp'])
                                        - F.unix_timestamp(F.lag(df['transaction_timestamp'])\
                                            .over(user_window)))/60).otherwise(0))
    df = df.withColumn('transfer_time',
                                    F.when(df['transfer_time'] > 95, 0).otherwise(df['transfer_time']))

    
    # Time variables
    df = df.withColumn('year'     , F.year(df['transaction_timestamp']))
    df = df.withColumn('month'   , F.date_trunc('month', df['transaction_timestamp']))
    df = df.withColumn('week'   , F.date_trunc('week', df['transaction_timestamp']))
    df = df.withColumn('dayofweek', F.dayofweek('transaction_timestamp').cast('byte'))
    df = df.withColumn('day'      , F.date_trunc('day', df['transaction_timestamp']))
    df = df.withColumn('hour'     , F.hour('transaction_timestamp').cast('byte'))
    df = df.withColumn('minute'   , F.minute('transaction_timestamp').cast('byte'))

    return df
    
def update_dictionaries(df, variable, new_variable):
    # add new factor levels to dictionary
    if variable == 'emisor':
        dictionary = emisor_dict
    elif variable == 'operator':
        dictionary = operator_dict
    elif variable == 'station':
        dictionary = station_dict
        dictionary.columns  = ['station', 'count']
    elif variable == 'line':
        dictionary = line_dict
    elif variable == 'account_name':
        dictionary = account_name_dict  

    new_distinct    = df.select(variable).distinct().cache()
    old_distinct   = dictionary.reset_index()
    id_max         = old_distinct[new_variable].max()
    old_distinct   = spark.createDataFrame(old_distinct)
    old_distinct   = old_distinct.withColumnRenamed(variable, variable + '_dict')
    new_dictionary = old_distinct.join(new_distinct, 
                                        old_distinct[variable + '_dict'] == new_distinct[variable], 
                                        how = 'outer')
    new_dictionary = new_dictionary.withColumn(new_variable, 
                                                F.when(F.isnull(F.col(new_variable)), 
                                                        F.lit(id_max) + 1).otherwise(F.col(new_variable)))
    window = Window.orderBy(new_variable)
    new_dictionary = new_dictionary.withColumn(variable + '_dict', 
                                                F.when(F.isnull(F.col(variable + '_dict')), 
                                                        F.col(variable)).otherwise(F.col(variable + '_dict')))
    new_dictionary = new_dictionary.withColumn(new_variable, 
                                                F.when(F.isnull(F.col('count')),
                                                        F.row_number().over(window)-1).otherwise(F.col(new_variable)))
    #print(new_dictionary.toPandas())
    new_dictionary = new_dictionary.select(old_distinct.columns[0:])
    return new_dictionary

def enumerate_factors(df, variable, old_dict = False, return_dict = False):
    # name for new variable
    new_variable = variable + '_id'
    
    # either re-use old dict and amend new factor levels
    if old_dict == True:
        dict_df = update_dictionaries(df, variable, new_variable)
        #import pdb; pdb.set_trace()
        df = df.join(dict_df,
                df[variable] == dict_df[variable + '_dict'],
                how ='left')
    
    # or create new dict
    elif old_dict == False:
        indexer = feature.StringIndexer(inputCol=variable, outputCol=new_variable)
        fitted_indexer = indexer.fit(df)
        df =fitted_indexer.transform(df)

    # save dictionary
    if return_dict == True:
        output_dict = df.groupby(variable, new_variable).count().sort('count', ascending = False)
  
      
    
    # cast as byte
    df = df.withColumn(new_variable, df[new_variable].cast('smallint'))
    
    # drop factor labels
    df = df.drop(variable)

    if return_dict == True:
        return df, output_dict
    elif return_dict == False:
        return df

# COMMAND ----------

# setup
## Just a test to see if importing from the main notebook works
def import_test_setup(x):
    print(x)

## Initiate Spark
## Class to handle spark and df in session


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


    def load(self, 
             path =  path, 
             type = 'parquet', 
             file = 'parquet_df', 
             delimiter = ';', 
             encoding = "utf-8"):
        
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


## utilities

# First, we create a few windows
# window by cardnumber
user_window = Window\
    .partitionBy('cardnumber').orderBy('transaction_timestamp')
    
# window  by cardnumber, explicity unbounded (should be same as above, but to be sure)
user_window_unbounded = Window\
    .partitionBy('cardnumber')\
    .orderBy('transaction_timestamp') \
    .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# user day window
user_day_window = Window\
    .partitionBy('cardnumber', 'day').orderBy('transaction_timestamp')

# user day window starting from last day
user_day_window_rev = Window\
    .partitionBy('cardnumber', 'day').orderBy(F.desc('transaction_timestamp'))

# user week window
user_week_window = Window\
    .partitionBy('cardnumber', 'week').orderBy('transaction_timestamp')

# user week window starting from last day
user_week_window_rev = Window\
    .partitionBy('cardnumber', 'week').orderBy(F.desc('transaction_timestamp'))

# user month window
user_month_window = Window\
    .partitionBy('cardnumber', 'month').orderBy('transaction_timestamp')

# user month window starting with last month
user_month_window_rev = Window\
    .partitionBy('cardnumber', 'month').orderBy(F.desc('transaction_timestamp'))

# function to convert days to secs
days = lambda i: i * 86400

# rolling month window
rolling_month_window = Window.orderBy(F.col('timestamp'))\
    .rangeBetween(-days(28), Window.currentRow)

# rolling month window, rev
rolling_month_window = Window.orderBy(F.desc('timestamp'))\
    .rangeBetween(Window.currentRow, days(28))

# rolling user month window
rolling_user_month_window = Window.partitionBy('cardnumber')\
    .orderBy(F.col('timestamp'))\
    .rangeBetween(-days(28), Window.currentRow)

# rolling user month window, starting with last month
rolling_user_month_window_rev = Window.partitionBy('cardnumber')\
    .orderBy(F.desc('timestamp'))\
    .rangeBetween(Window.currentRow, days(28))

# COMMAND ----------

# MAGIC %md
# MAGIC # (0) Reorganize files by header & unzip

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
                csvin = pd.read_csv(file_path + filename, nrows = 0) # this opens zip files as well
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

print(os.listdir(byheader_dir))


# check we have no duplicate filenames!
# if we had, we need to differentiate files by their Dual versus Troncal versus Zonalorigin
dupfiles = [item for sublist in list(file_header_dict.values()) for item in sublist]

for v in ['ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/' ]:
    file_path = path + f'/Workspace/Raw/since2020/{v}/'
    dupfiles = [s.replace(file_path, "") for s in dupfiles]

print(len(dupfiles), len(set(dupfiles)))


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


# COMMAND ----------

# MAGIC %md
# MAGIC # (1) Create raw parquet files
# MAGIC <mark> With data since 2020 so far </mark>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Unify structure
# MAGIC
# MAGIC This section can be run independently from (1.1)

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

# import data & visualize if necessary

for idx, handler in enumerate(spark_handlers):
          
    if idx < 7: # skip the first 7 headers
        continue
    
    h = headers[idx]
    print('\n Schema ' + h)
    handler.load(type = 'new_data',
                 path = os.path.join(pathdb ,"Workspace/Raw/byheader_dir/" , h),
                 delimiter = delimiters[idx], 
                 encoding = encodings[idx])
    # display(handler.dfraw.limit(1).toPandas())    # display is commented to hide dataset content

# COMMAND ----------

# transform data

for idx, handler in enumerate(spark_handlers):
    
    # skip the first 7 headers, use it later to work with full data
    # skip the 16th header, empty data
    if (idx < 7) | (idx == 15): 
        continue
    
    h = headers[idx]
    print('\n Schema ' + h)

    handler.transform(header_format = formats[idx])
    # display(handler.df.limit(1).toPandas())    # display is commented to hide dataset content

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2.b. Handling NAs

# COMMAND ----------

for idx, handler in enumerate(spark_handlers):
    if (idx < 7) | (idx == 15): 
        continue

    h = headers[idx]
    print('\n Schema ' + h)
    handler.df.where(handler.df['value'].isNull()).show()


# COMMAND ----------

# MAGIC %md
# MAGIC - There are some nulls in values, but while other columns are OK. These obs. are OK. The maximum number of rows by schema is 4. So no big issues here.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2.c. Join, count, and save

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
    df = df.union(handler.df)  


# COMMAND ----------

# Create parquet file of raw data
df.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/parquet_df_raw_2020-2024_withdups'))
del spark_handlers, handler

# COMMAND ----------

df.count()

# COMMAND ----------

4367228459/1_000_000

# COMMAND ----------

# MAGIC %md
# MAGIC # (2) Create clean parquet files
# MAGIC
# MAGIC This section is written so it can be run independently from (1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Remove duplicates
# MAGIC

# COMMAND ----------

sh = spark_df_handler()
sh.load(type = 'parquet', path = pathdb + '/Workspace/bogota-hdfs/', file = 'parquet_df_raw_2020-2024_withdups')

# COMMAND ----------

sh.df.cache()

# COMMAND ----------

# Create var that flags consecutive observations that have the same transaction timestamp, line and cardnumber
sh.df = sh.df.withColumn('duplicate',
                   F.when( (F.lag(sh.df['transaction_timestamp']).over(user_window) == sh.df['transaction_timestamp']) \
                         & (F.lag(sh.df['line']).over(user_window) == sh.df['line']), 1).otherwise(0))

# COMMAND ----------

duplicate_mean = sh.df.agg(F.mean('duplicate')).first()[0]
print(f"Percentage of duplicates: {duplicate_mean * 100}")

# COMMAND ----------

4367228459 * (1 - 0.00183) / 1_000_000

# COMMAND ----------

# Tabulate duplicates
df_count_dupes = df_count_dupes.withColumn('year', F.year(df['transaction_timestamp']))
df_count_dupes = df_count_dupes.filter(df_count_dupes['duplicate'] == 1).groupby(df_count_dupes['year'], df_count_dupes['month']).count().toPandas()
df_count_dupes

# COMMAND ----------

# Drop dupes
sh.df  = sh.df.filter(sh.df['duplicate'] == 0)
sh.df  = sh.df.drop('duplicate')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2. Add variables

# COMMAND ----------

# generate new variables
sh.df = generate_variables(sh.df )
sh.df 

# COMMAND ----------

print(os.listdir(path + '/Workspace/variable_dicts'))

# COMMAND ----------

account_name_dict = pd.read_csv(path + '/Workspace/variable_dicts/account_name_dict_dict.csv')
account_name_dict["perc"] = account_name_dict["count"] / account_name_dict["count"].sum() * 100
account_name_dict

# COMMAND ----------

# Commented because running this took 4 hs
# Encode strings
# as for now, lets just do it for account_name
# sh.df, account_name_dict = enumerate_factors(sh.df, 'account_name', old_dict = False, return_dict = True)
# account_name_dict_pd = account_name_dict.toPandas()
# account_name_dict_pd.to_csv(os.path.join(git, "csv_outputs/" + variable + "_dict.csv"))

# COMMAND ----------

# Create parquet file of raw data
sh.df.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/parquet_df_clean_2020-2024_temp'))

# COMMAND ----------

# MAGIC %md
# MAGIC # (3) Prepare sample

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1. Remove super swipers and infrequent users

# COMMAND ----------

# load data
sh = spark_df_handler()
sh.load(type = 'parquet', path = pathdb + '/Workspace/bogota-hdfs/', file = 'parquet_df_clean_2020-2024_temp')

# Correct variables (and save again the dataset with the corrections made)
sh.df = sh.df.withColumn('year'   , F.year(sh.df['transaction_timestamp']))

# COMMAND ----------

sh.df.select("transaction_timestamp", "year", "month", "week", "day").show(5)

# COMMAND ----------

# filter relevant periods and columns
df_filtered = sh.df \
                .where( (F.col('transaction_timestamp') > dt.datetime(2021, 12, 31, tzinfo = timezone)) & \
                        (F.col('transaction_timestamp') < dt.datetime(2024, 7, 1,   tzinfo = timezone)) ) \
                .select('cardnumber', 'day', 'month', 'year', 'account_name_id', 'value', 'transfer')
df_filtered.cache()

# COMMAND ----------

# count transactions by day
usage_count_day = df_filtered.groupby("cardnumber", 'day', 'year').count()
usage_count_day = usage_count_day.withColumn("more100swipes",   
                           F.when( usage_count_day["count"] > 100, 1).otherwise(0))
usage_count_day = usage_count_day.withColumn("more20swipes",   
                           F.when( usage_count_day["count"] > 100, 1).otherwise(0))

# super swipers
superswipers = usage_count_day.groupBy("cardnumber").agg(
    F.sum("more100swipes").alias("days_more100swipes"),
    F.sum("more20swipes").alias("days_more20swipes"))

superswipers = superswipers.withColumn("1day_more100swipes",
                           F.when(superswipers["days_more100swipes"] > 0, 1).otherwise(0))

superswipers = superswipers.withColumn("2days_more20swipes",
                           F.when(superswipers["days_more20swipes"] > 1, 1).otherwise(0))

# frequent users (exclude 2024 for now)
freq = usage_count_day.groupBy("cardnumber", "year").agg(
            F.countDistinct("day").alias("n_days"),
            F.sum("count").alias("n_valid"))


freq = freq.withColumn("year_more12days",
                           F.when( ((F.col("n_days")  >= 12) & (F.col("year") != 2024)) | \
                                   ((F.col("n_days")  >= 6 ) & (F.col("year") == 2024)) , 1).otherwise(0))
freq = freq.withColumn("year_more12valid",
                           F.when( ((F.col("n_valid")  >= 12) & (F.col("year") != 2024)) | \
                                   ((F.col("n_valid")  >= 6 ) & (F.col("year") == 2024)) , 1).otherwise(0))

freq =  freq.groupBy("cardnumber").agg(
    F.max("year_more12days").alias("more12days_any_year"),
    F.max("year_more12valid").alias("more12valid_any_year"))


# COMMAND ----------

# Total cards
superswipers.count()

# COMMAND ----------

# Total cards
freq.count()

# COMMAND ----------

dbutils.fs.mkdirs(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate'))

# Save intermediate
#usage_count_day.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/usage_count_day'))
#superswipers.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/superswipers'))
#freq.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/freq'))

# COMMAND ----------

# maybe save to Pandas (afterwards)
#superswipers.toPandas().to_csv(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/superswipers.csv'), index = False)
#freq.toPandas().to_csv(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/freq.csv'), index = False)


# COMMAND ----------

# Proportion of superswipers
superswipers_perc = superswipers.agg(*[
    F.round(F.mean(F.col(col)) * 100, 6).alias(col + "_percent")
    for col in ["1day_more100swipes", "2days_more20swipes"]
])

superswipers_perc.show()

# COMMAND ----------

# Proportion of unfrequent users
freq_perc = freq.agg(*[
    F.round(F.mean(F.col(col)) * 100, 5).alias(col + "_percent")
    for col in ["more12valid_any_year", "more12days_any_year"]
])

freq_perc.show()

# COMMAND ----------

# Join transactions dataset with superswipers and freq datasets
df_filtered = df_filtered.join(superswipers, on="cardnumber", how="left")
df_filtered = df_filtered.join(freq, on="cardnumber", how="left")
df_filtered.show(2)

# COMMAND ----------

# Proportion of transactions of super swipers and of frequent users
dummy_columns = ["more12valid_any_year", "more12days_any_year", "1day_more100swipes", "2days_more20swipes"] 
percentages = df_filtered.agg(*[
    (F.mean(F.col(col)) * 100).alias(col + "_perc")
    for col in dummy_columns
])
percentages.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Out of 14,802,496 cards present from January 2022 to **July 2024**, I am removing:
# MAGIC - Swiper swipers
# MAGIC   - more than 100 swipes in a day: 5.34E-4 cards and less than 0.01% transactions.
# MAGIC   - more than 20 swipes in two days: 2.3E-4 cards and less than 0.01% transactions.
# MAGIC - Infrequent users
# MAGIC   - Those at are present for less than 12 _different days_ in 2022 and 2023, and less than 6 days in 2024: 31% of cards, but less than 2% of transactions.
# MAGIC   - If I were to use the 12 _transactions_ criteria instead: 22% of cards and less than 1% of transactions. But we prefer the days criteria.

# COMMAND ----------

# Remove super swipers and infrequent users form dataset and save
df_regular_users = df_filtered.filter(
    (F.col('more12days_any_year') == 1) &
    (F.col('1day_more100swipes') == 0) &
    (F.col('2days_more20swipes') == 0)
) \
    .select('cardnumber', 'day', 'month', 'year', 'account_name_id', 'value', 'transfer')

# COMMAND ----------

df_filtered.count()

# COMMAND ----------

df_regular_users.cache()
df_regular_users.count() 

# COMMAND ----------

df_regular_users.write.mode('overwrite').parquet(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/df_regular-users-2022-2024'))

# COMMAND ----------

# original  Sebastian's relevant code
# usage_count_day = df.groupby("cardnumber", 'day').count()
#    daily_usage_outlier_accounts = usage_count_day.where(usage_count_day['count'] > 100).select('cardnumber').distinct()
#    daily_usage_outlier_accounts_repeated = usage_count_day\
#        .where(usage_count_day['count'] > 20)\
#        .groupby('cardnumber').count().filter(F.col('count') > 2).select('cardnumber').distinct()
#    usage_count_year = df.groupby("cardnumber").count()
#    infrequent_user_accounts = usage_count_year.where(usage_count_year['count'] < 12).select('cardnumber').distinct()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify apoyo or subsidy users

# COMMAND ----------

account_name_dict = pd.read_csv(path + '/Workspace/variable_dicts/account_name_dict_dict.csv')
account_name_dict[["account_name", "account_name_id"]]

# COMMAND ----------

price_subsidy_18 = [1575, 1725]
price_subsidy_22 = [1650, 1800] # same since Feb 2019
price_subsidy_23 = [2250, 2500] # same for 2024, though since Feb tariff unified to 2500
timestamp_threshold = dt.datetime(2022, 1, 31, tzinfo= timezone)  # Adjust timezone as needed

# COMMAND ----------

# Analize user's profiles
df_regular_users = df_regular_users.withColumn("profile-apoyo",   
                           F.when( F.col('account_name_id').isin([3, 4]), 1).otherwise(0)) \
                .withColumn("profile-adulto",   
                           F.when( F.col('account_name_id').isin([0, 5, 7, 8]), 1).otherwise(0)) \
                .withColumn("profile-anonymous",   
                           F.when( F.col('account_name_id') == 1, 1).otherwise(0)) \
                .withColumn("sisben-subsidy-value",
                            F.when(    ( (F.col('day') <= timestamp_threshold) & (F.col('value').isin(price_subsidy_18 + price_subsidy_22) ) ) |
                                       ( (F.col('day') > timestamp_threshold)  & (F.col('value').isin(price_subsidy_22 + price_subsidy_23) ) ) \
                                   , 1).otherwise(0))         


# COMMAND ----------

card_types = df_regular_users.groupby("cardnumber").agg(
    F.max("profile-apoyo").alias("profile-apoyo-any"),
    F.max("profile-adulto").alias("profile-adulto-any"),
    F.max("profile-anonymous").alias("profile-anonymous-any"),
    F.max("sisben-subsidy-value").alias("sisben-subsidy-value-any"),
    F.mean("profile-apoyo").alias("profile-apoyo-mean"),
    F.mean("profile-adulto").alias("profile-adulto-mean"),
    F.mean("profile-anonymous").alias("profile-anonymous-mean"),
    F.mean("sisben-subsidy-value").alias("sisben-subsidy-value-mean")
)

# COMMAND ----------

# Calculate the percentage of each type of users
percentages = card_types.agg(*[
    F.round(F.mean(F.col(col)) * 100, 2).alias(col + "_percent_1s")
    for col in ["profile-apoyo-any", "profile-adulto-any", "profile-anonymous-any", "sisben-subsidy-value-any"]
])

# Show both results
percentages.show()


# Calculate the intersections 


# COMMAND ----------

card_types.toPandas().to_csv(os.path.join(pathdb, 'Workspace/bogota-hdfs/intermediate/card_types.csv'), index = False)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2. Plot

# COMMAND ----------

daily_cards = sh_raw.df\
                .where( (F.col('transaction_timestamp') > dt.datetime(2021, 12, 31, tzinfo = timezone)) &\
                        (F.col('transaction_timestamp') < dt.datetime(2024, 4, 1,   tzinfo = timezone)) )\
                .select('cardnumber', 'day')\
                .distinct()\
                .groupby('day')\
                .count()\
                .toPandas()\
                .set_index('day')

# COMMAND ----------

daily_cards.sort_values("count").head(20)

# COMMAND ----------

# Plot 
# Set up frame
fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (20, 5))
fig.subplots_adjust(hspace = 0.4)


sns.lineplot(x = daily_cards[daily_cards["count"] > 60_000].index , 
             y = daily_cards[daily_cards["count"] > 60_000].values[:,0]/1_000,
             color = 'tab:blue')
#result.to_csv(os.path.join(git,'csv_outputs/report/count_all/active_accounts_per_day_raw.csv'))

ymin, ymax = axes.get_ylim()
ydiff = (ymax-ymin)
ylim = ymax - ydiff * 0.9
axes.set_ylim(0, ymax)

#axes.text(dt.datetime(2018, 7, 15, tzinfo = timezone), ylim, 'No data for this period', fontsize = 15)
#axes.arrow(x = dt.datetime(2018, 7, 1, tzinfo = timezone), y = ylim * 0.9, dx = dt.datetime(1, 8, 1, tzinfo = timezone), dy =  0)

axes.axvline(x = dt.datetime(2023, 2, 1, tzinfo = timezone), color ='r')
axes.text(dt.datetime(2023, 2, 1, tzinfo = timezone), ylim, 'Policy change')

#add_labels(axes)
axes.set(ylabel = 'Number of active accounts (thousands)', xlabel = 'Day')
#save_plot('plots/report/count_all/active_accounts_per_day_raw')

# COMMAND ----------

# MAGIC %md
# MAGIC # Later

# COMMAND ----------

#os.listdir(os.path.join(path, 'Workspace/bogota-hdfs'))
#dbutils.fs.mv(pathdb + 'Workspace/bogota-hdfs/parquet_df_clean_joined',pathdb + 'Workspace/bogota-hdfs/sample-will/parquet_df_clean_joined', recurse=True)
