# Databricks notebook source
# Pip install non-standard packages
!pip install tqdm

# Import modules
import os 
import pandas as pd 
import numpy as np
from tqdm import tqdm


# COMMAND ----------

pathdb  = '/mnt/DAP/data/ColombiaProject-TransMilenioRawData/'
path = '/dbfs/' + pathdb
user = os.listdir('/Workspace/Repos')[0]
git = '/Workspace/Repos/' +user+ '/ColombiaTransMilenio'

# COMMAND ----------

dbutils.fs.mkdirs(pathdb + '/Workspace/Clean/timing-old-cleaning')

# COMMAND ----------

## SET COLUMN NAMES

# ZONAL AND DUAL COLUMNS

zonalcols  = ['Emisor', 'Estacion_Parada',
                 'Fecha_Clearing', 'Fecha_Transaccion', 'Linea', 
                 'Nombre_Perfil', 'Numero_Tarjeta', 'Ruta',
                'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion',
                 'Tipo_Tarjeta', 'Valor'] # current names
new_zonalcols = [ 'emisor', 'estacionparada',
                  'fechaclearing', 'fechatransaccion', 'linea',
                  'nombreperfil', 'nrotarjeta', 'ruta',
                  'saldodespuesdetransaccion', 'saldoprevioatransaccion',
                  'tipotarjeta','valor'] # homogeneous names
zonaltypes = ['category', 'category',
                'str', 'str', 'category', 
                'category', 'str', 'category',
               'float64', 'float64',
                'category', 'float64'] # coltypes
zonalcoltypes = dict(zip(zonalcols, zonaltypes))


# TRONCAL COLUMNS
troncalcols  = ['Acceso_Estacion', 'Emisor', 'Estacion_Parada',
                      'Fecha_Clearing', 'Fecha_Transaccion', 'Linea', 
                      'Nombre_Perfil', 'Numero_Tarjeta', 'Ruta',
                     'Saldo_Despues_Transaccion', 'Saldo_Previo_a_Transaccion',
                      'Tipo_Tarjeta', 'Valor'] # current names
new_troncalcols = ['accesoestacion', 'emisor', 'estacionparada',
                  'fechaclearing', 'fechatransaccion', 'linea', 
                  'nombreperfil', 'nrotarjeta', 'ruta',
                  'saldodespuesdetransaccion', 'saldoprevioatransaccion',
                  'tipotarjeta','valor'] #homogeneous names
troncaltypes = ['category', 'category', 'category',
                      'str', 'str', 'category', 
                      'category', 'str', 'category',
                     'float64', 'float64',
                      'category', 'float64'] # coltypes
troncalcoltypes = dict(zip(troncalcols, troncaltypes))

# COMMAND ----------

for v in ['ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/' ]:

    print(f"{v} -------------------")

    rawdir    = path + f'/Workspace/Raw/since2020/{v}/'
    cleandir  = path + '/Workspace/Clean/timing-old-cleaning'
  
    dbutils.fs.mkdirs(pathdb + '/Workspace/Clean/timing-old-cleaning/{v}')
  
    # List of the the raw files to be cleaned
    os.chdir(rawdir)
    files = os.listdir()
    print(f"Files in directory: {len(files)}")

    # Get the unique days with some data for that year
    days  = list(set([l[-12:-4] for l in files]))
    first = [x[0] for x in days]
    print(set(first))

    notdayfiles = [l for l in files if l[-12] != '2']
    print(notdayfiles)
    print([l for l in files if 'validacionZonal20220628' in l])
    print([l for l in files if 'validacionDual20230301' in l])


    # remove 
    files = [l for l in files if (l != 'validacionZonal20220628-WBGXDP0436.csv') & (l != 'validacionDual20230301 (1).csv')]
    print(f"Files without duplicates: {len(files)}")
    days  = list(set([l[-12:-4] for l in files]))
    first = [x[0] for x in days]
    print(set(first))


    # days to int
    days_int = [int(x) for x in days]

    # since Sept 2023
    days_int_new = [x for x in days_int if x >= 20230901]
    days_str_new = [str(x) for x in days_int_new]

    files = [l for l in files if l[-12:-4] in days_str_new]
    print(f"Files since Aug 2023: {len(files)}")

    if v == 'ValidacionZonal/' :
        for f in tqdm(files):
            zonal   = pd.read_csv(f, usecols = zonalcols, dtype  = zonalcoltypes)

            # remove duplicates
            zonal['fecha']   = zonal.Fecha_Transaccion.str[:19]
            zonal.drop_duplicates(subset = ['Numero_Tarjeta','fecha'], inplace = True)

            # add typevalid and rename the other columns
            zonal['typevalid'] = 'zonal'
            zonal.rename(columns=dict(zip(zonalcols, new_zonalcols)), inplace=True)

                # save clean data to folder
            os.chdir(cleandir) ########
            day  = f[-12:-4]
            zonal.to_csv("Zonal"+day+"_clean.csv", index = False)



# COMMAND ----------

for v in ['ValidacionDual/', 'ValidacionTroncal/', 'ValidacionZonal/' ]:

   rawdir    = path + f'/Workspace/Raw/since2020/{v}/'
   cleandir  = path + '/Workspace/Clean/timing-old-cleaning'
  
    dbutils.fs.mkdirs(pathdb + '/Workspace/Clean/timing-old-cleaning/{v}')
  
    # List of the the raw files to be cleaned
    os.chdir(rawdir)
    files = os.listdir()
        
    
    # Remove the files that were already cleaned
   # print("Number of raw files:", len(files))
   # os.chdir(cleandir+"/"+v) 
   # already_cleaned = os.listdir()
   # files = [file for file in files if file.replace('validacion','')[:-4]+"_clean.csv" not in already_cleaned]
  #  print("Number of raw files to clean:", len(files))
        
    
    # Get the unique days with some data for that year
    days  = list(set([l[-12:-4] for l in files]))
    
    for day in tqdm(range(len(days))):
        
        # Get each transaction type file for each day 
        # Sometimes there is no data for som day - transaction type combination

        t = [f for f in troncalf if "Troncal"+days[day] in f ]
        z = [f for f in zonalf if "Zonal"+days[day]   in f ]
        d = [f for f in dualf if "Dual"+days[day]    in f ]


        # whenever we have a file, import it, clean it and save it to the clean folder

