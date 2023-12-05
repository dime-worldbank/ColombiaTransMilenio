# Databricks notebook source
## install_packages.py
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

# COMMAND ----------

## import_packages.py
### spark etc
# import rarfile
import findspark, os, pyspark, time, sys
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark_dist_explore import Histogram, hist, distplot, pandas_histogram
from pyspark import *
from pyspark.sql import *
from pyspark.rdd import *
from pyspark.ml import *
from pyspark.sql.types import ArrayType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import FloatType
import multiprocessing

### data wrangling
import pandas as pd
pd.options.display.float_format = '{:,.0f}'.format
# pd.set_option("display.max_rows", 100)
pd.options.display.max_columns = None
import datetime as dt
import numpy as np
from random import sample, seed
seed(510)
# timezone = dt.timezone(offset = -dt.timedelta(hours=5), name = "America/Bogota")
timezone = dt.timezone(offset = -dt.timedelta(hours=0), name = "America/Bogota")
import re
#import fiona
import geopandas as gpd
import copy
from collections import Counter
import skmob
from skmob.preprocessing import clustering

### plotting
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import chart_studio.plotly as py
import plotly.graph_objs as go
import seaborn as sns
import folium
from folium.plugins import HeatMap, DualMap, Fullscreen
from folium.features import DivIcon
from branca.element import Template, MacroElement
import locale
from matplotlib.ticker import FuncFormatter
import matplotlib.lines as mlines
font = {'family' : 'Calibri',
        'weight' : 'normal',
        'size'   : 18}

import matplotlib

### jupyter
from IPython.display import HTML
import warnings
warnings.filterwarnings('ignore')
import os
from IPython.display import display, HTML

display(HTML(data="""
<style>
    div#notebook-container    { width: 95%; }
    div#menubar-container     { width: 65%; }
    div#maintoolbar-container { width: 99%; }
</style>
"""))

# COMMAND ----------

## start_spark.py


## Set up spark
# Check which computer this is running on
if multiprocessing.cpu_count() == 6:
    on_server = False
else:
    on_server = True

print(on_server)

# start spark session according to computer
if on_server:
    spark = SparkSession \
        .builder \
        .master("local[75]") \
        .config("spark.driver.memory", "200g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config('spark.local.dir', '/data/wb550947/data') \
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
    path = '/dbfs/mnt/DAP/data/ColombiaProject-TransMilenioRawData'
    git = os.getcwd() # because I am running in Databricks directly from repo!
else:
    print("Not on server - no path defined")

# COMMAND ----------

## utilities.py

policy = dt.datetime(2017, 4, 1, tzinfo = timezone)

# First, we create a few windows
# window by cardnumber
user_window = Window\
    .partitionBy('cardnumber').orderBy('transaction_timestamp')

# window by cardnumber starting with last transaction
user_window_rev = Window\
    .partitionBy('cardnumber').orderBy(F.desc('transaction_timestamp'))

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

# Function to style the cells of a crosstab table
def crosstab_plot(df, title = "Crosstab", color = "#2ecc71"):
    cm = sns.light_palette(color, as_cmap=True)
    styles = [dict(selector="th",
                props=[("font-size", "90%"),
                    ("text-align", "right"),
                    ("background-color", "#edf9f2")]),
                dict(selector="caption",
                     props=[("font-size", "150%"),
                           ("color", "black")])]
    temp = df.style.background_gradient(cmap=cm) \
                    .set_caption(title) \
                    .set_table_styles(styles)
    return temp.format('{:,.0f}')

# Function that takes a df, main var and pivot vars and returns a list of crosstab dfs
def crosstab_list_count(df, main_var, list_of_pivot_vars, title):
    dfs = {}
    for i in list_of_pivot_vars:
        temp = df.groupBy(main_var) \
              .pivot(i) \
              .count() \
              .toPandas() \
              .fillna(0) \
              .sort_values(main_var) \
              .set_index(main_var)
        dfs[i] = crosstab_plot(temp,
              title + i)
        temp.to_csv('../csv_outputs/' + main_var + i + 'crosstab.csv')
    return dfs

# Add vertical lines for policy changes
def add_vlines(axes, all_lines = True):
    ymin, ymax = axes.get_ylim()
    ydiff = (ymax-ymin)
    ylim = ymax - ydiff * 0.07
    
    if ymax >  1000:
        axes.yaxis.set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ','))) 
    elif ymax < 1:
        axes.yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y))) 
    else:
        pass
    
    axes.axvline(x = dt.datetime(2017, 4, 1, tzinfo = timezone), color ='r')
    axes.text(dt.datetime(2017, 4, 2, 12, tzinfo = timezone), ylim, '1')
    axes.axvline(x = dt.datetime(2017, 12, 1, tzinfo = timezone), color ='r', lw =0.7)
    axes.text(dt.datetime(2017, 12, 2, 12, tzinfo = timezone), ylim, '2')
    axes.axvline(x = dt.datetime(2018, 2, 2, tzinfo = timezone), color ='r', lw = 0.7)
    axes.text(dt.datetime(2018, 2, 3, 12, tzinfo = timezone), ylim, '3')
    if all_lines:
        axes.axvline(x = dt.datetime(2019, 2, 1, tzinfo = timezone), color ='r', lw = 0.7)
        axes.text(dt.datetime(2019, 2, 2, 12, tzinfo = timezone), ylim, '4')
    else:
        pass
    return axes

# Add labels
def add_labels(axes):
    
    matplotlib.font_manager._rebuild()
    plt.rc('font', **font)
    
    handles, labels = axes.get_legend_handles_labels()
    number_labels = len(set(labels))
    count_labels = len(labels)

    if ((number_labels == 1) | (count_labels == 0)):
        return axes
    
    elif (number_labels == count_labels):
        axes.legend(handles, labels, loc = 'best')

    elif (number_labels == (count_labels/2)):
        axes.legend(handles[::2], labels[::2], loc = 'best')
        
    elif (number_labels == (count_labels/3)):
        axes.legend(handles[::3], labels[::3], loc = 'upper right')
        
    elif (number_labels == (count_labels/4)):
        axes.legend(handles[::2], labels[::2], loc = 'best')
        
    elif (count_labels == 7):
        axes.legend(handles[::2], labels[::2], loc = 'best')
        
    else:
        pass
    return axes

# Save plot
def save_plot(name, plt = plt):
    plt.savefig(os.path.join(git, name), dpi = 400, bbox_inches='tight')
    plt.savefig(os.path.join(git, name + '.eps'), format = 'eps', bbox_inches='tight')
    plt.show()

linestyles = ['-', '--', '-.', ':']
colors = ['tab:blue', 'tab:purple', 'tab:orange', 'tab:red']

# COMMAND ----------

## setup.py

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

    memorize(df)
        Catch the df in memory
    """

    def __init__(self,
                spark = spark,
                on_server = on_server):
        """
        Parameters
        ----------
        """
        self.spark = spark
        self.on_server = on_server

    def load(self, path =  path, type = 'parquet', file = 'parquet_df', delimiter = ';', encoding = "utf-8"):
        if type == 'pickle':
            name = os.path.join(path, file)
            pickleRdd = self.spark.sparkContext.pickleFile(name = name).collect()
            self.df = self.spark.createDataFrame(pickleRdd)

        elif type =='parquet':
            self.df = spark.read.format("parquet").load(os.path.join(path, 'bogota-hdfs/' + file))

        elif type =='sample':
            self.df = self.spark.read.format("csv").option("header", "true")\
                                        .option("delimiter", ",")\
                                        .option("encoding", "UTF-8")\
                                        .load(os.path.join(path,'Sample/1pct_cleaned.csv'))

            self.df = self.df.select(F.to_date(self.df.transaction_date).alias('transaction_date'),
                F.to_timestamp(self.df.transaction_timestamp).alias('transaction_timestamp'),
                self.df.line_id.cast('short').alias('line_id'),
                self.df.station_id.cast('short').alias('station_id'),
                self.df.cardnumber.cast('long').alias('cardnumber'),
                self.df.account_name_id.cast('byte').alias('account_name_id'),
                self.df.emisor_id.cast('byte').alias('emisor_id'),
                self.df.operator_id.cast('byte').alias('operator_id'),
                self.df.balance_before.cast('int').alias('balance_before'),
                self.df.balance_after.cast('int').alias('balance_after'),
                self.df.value.cast('int').alias('value'),
                self.df.real_balance_after.cast('int').alias('real_balance_after'),
                self.df.lost_subsidy_year.cast('byte').alias('lost_subsidy_year'),
                self.df.left_in_april.cast('byte').alias('left_in_april'),
                self.df.october_user.cast('byte').alias('october_user'),
                self.df.transfer.cast('byte').alias('transfer'),
                self.df.negative_trip_number.cast('byte').alias('negative_trip_number'),
                self.df.negative_trip.cast('byte').alias('negative_trip'),
                self.df.transfer_time.cast('float').alias('transfer_time'),
                F.to_timestamp(self.df.month).alias('month'),
                F.to_timestamp(self.df.week).alias('week'),
                F.to_timestamp(self.df.day).alias('day'),
                self.df.dayofweek.cast('byte').alias('dayofweek'),
                self.df.hour.cast('byte').alias('hour'),
                self.df.minute.cast('byte').alias('minute'),
                self.df.second.cast('byte').alias('second'))
            
        elif type =='new_data':
            self.dfraw = self.spark.read.format("csv").option("header", "true")\
                                        .option("delimiter", delimiter)\
                                        .option("charset", encoding)\
                                        .load(os.path.join(path,"*"))

        else:
            self.dfraw = self.spark.read.format("csv").option("header", "true")\
                                        .option("delimiter", ";")\
                                        .option("charset", "utf-8")\
                                        .load(os.path.join(path,"*.csv"))

            self.df = self.dfraw.select(F.to_timestamp(self.dfraw.fechatransaccion,'dd/MM/yyyy HH:mm:ss')\
                            .alias('transaction_timestamp'),
                          self.dfraw.daygrouptype.alias('day'),
                          self.dfraw.fase.alias('phase'),
                          self.dfraw.emisor.alias('emisor'),
                          self.dfraw.operador.alias('operator'),
                          self.dfraw.linea.alias('line'),
                          self.dfraw.estacion.alias('station'),
                          self.dfraw.accesoestacion.alias('station_access'),
                          self.dfraw.dispositivo.cast('int').alias('machine'),
                          self.dfraw.tipotarjeta.alias('card_type'),
                          self.dfraw.nombreperfil.alias('account_name'),
                          self.dfraw.nrotarjeta.cast('long').alias('cardnumber'),
                          self.dfraw.saldoprevioatransaccion.cast('int')\
                            .alias('balance_before'),
                          self.dfraw.valor.cast('int').alias('value'),
                          self.dfraw.saldodespuesdetransaccion.cast('int')\
                            .alias('balance_after'))

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
          
        elif header_format == 'TESTING':
            self.df = self.dfraw



    def clean(self):
        self.df = clean_data(self.df)
        
    def clean_new_data(self):
        self.df = clean_new_data(self.df)

    def gen_vars(self):
        self.var_gen = var_generator(self.df)
        self.df = self.var_gen.generate_variables()
        self.df = self.var_gen.enumerate_factors()

    def gen_home_location(self):
        self.home_gen = home_location_generator(self.df, self.spark)
        self.df = self.home_gen.generate_home_location()
        self.df = self.home_gen.find_upz_for_hl()

    def gen_mobility_measures(self):
        # Distance to previous station
        self.df = self.df.withColumn('distance',
                           distance_to_point(F.array(self.df['station_id'], F.lag('station_id').over(user_window_rev))))

        # Distance in the whole day
        self.df = self.df.withColumn('day_distance', F.max(F.sum(self.df['distance']).over(user_day_window)).over(user_day_window_rev))
        
        # Define filters
        weekday_filter = (self.df['dayofweek'] < 7) & (self.df['dayofweek'] > 1) & (self.df['day'].isin(holidays) == False)
        saturday_filter = (self.df['dayofweek'] == 7)
        sunday_filter = (self.df['dayofweek'] == 1) & (self.df['day'].isin(holidays) == True)
        no_filter = (self.df['dayofweek'] == self.df['dayofweek'])
        # create a list of them
        self.filters = [weekday_filter, saturday_filter, sunday_filter, no_filter]
        # another list to hold the filtered dfs
        self.filtered_dfs = ['weekdays', 'saturdays', 'sundays_holidays', 'no_filter']
        # loop over the lists and apply the filters, generate mobility measures
        for idx, val in enumerate(self.filters):
            self.filtered_dfs[idx] = self.df.filter(val)
            self.mob_gen = mobility_measure_generator(self.filtered_dfs[idx])
            self.filtered_dfs[idx] = self.mob_gen.generate_mobility_measures()

    def memorize(self):
        # Register as table to run SQL queries
        self.df.createOrReplaceTempView("df_table")
        self.spark.sql('CACHE TABLE df_table').collect()

        return self.df

    def sample(self, data = 'none', divisor = 100):
        if str(data) == 'none':
            df = self.df
        else:
            df = data

        # Create list of unique ids
        unique_ids = [i.cardnumber for i in df.select('cardnumber').distinct().collect()]
        unique_ids_apoyo = [i.cardnumber for i in df.where(df['account_name_id'] == 2) \
                    .select('cardnumber').distinct().collect()]

        # how many unique ids do we have?
        print("Number of unique IDs: {:,}".format(len(unique_ids))) # --> 13304122
        # how many unique apoyo ids do we have?
        print("Number of unique Apoyo IDs: {:,}".format(len(unique_ids_apoyo))) # --> 703410
        # take a random sample of 1%

        sample_size_full = int(len(unique_ids) / int(divisor))
        sample_size_apoyo = int(len(unique_ids_apoyo * 10) / int(divisor))

        print("Number of sampled IDs: {:,}".format(sample_size_full))
        print("Number of sampled Apoyo IDs: {:,}".format(sample_size_apoyo))

        sample_ids = sample(unique_ids, sample_size_full)
        sample_apoyo_ids = sample(unique_ids_apoyo, sample_size_apoyo)

        # Create a filtered df with the transactions of these cards only
        sampled_df = df.filter(df.cardnumber.isin(sample_ids))
        print("Number of observations in sample: {:,}".format(sampled_df.count()))
        sampled_apoyo_df = df.filter(df.cardnumber.isin(sample_apoyo_ids))
        print("Number of observations in Apoyo sample: {:,}".format(sampled_apoyo_df.count()))

        # Write to csv
        sampled_df.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv') \
                              .save(os.path.join(path + '/1pctsample/full'), header = 'true')
        sampled_apoyo_df.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv') \
                              .save(os.path.join(path + '/10pctsample/apoyo'), header = 'true')

# COMMAND ----------

spark

# COMMAND ----------

files = os.listdir(f'{path}/Data/ValidacionZonal')
csvfiles =  [f for f in files if "csv" in f]

# COMMAND ----------

csvfiles[0:10]

# COMMAND ----------

for idx, file in enumerate(csvfiles[:5]):
    handler = spark_df_handler()
    handler.dfraw = handler.spark.read.format("csv").option("header", "true")\
                                        .option("delimiter", ",")\
                                        .option("charset",  "utf-8")\
                                        .load(f'/mnt/DAP/data/ColombiaProject-TransMilenioRawData/Data/ValidacionZonal/{file}')
    handler.transform(header_format = 'TESTING')
    if handler.df.where(handler.df['Valor'].isNull()).count() != 0:
        print('\n File: ' + str(idx) + ' file: ' + str(file) + ' with ' + str(handler.dfraw.count()) + ' rows: \n')
        display(handler.df.where(handler.df['Valor'].isNull()).toPandas())
        display(handler.dfraw.limit(3).toPandas())
    display(handler.dfraw.limit(1).toPandas())
