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

    file_path = path + f'/Workspace/Raw/since2020/{v}/'

# COMMAND ----------


# Data directories
rawdir           = transmileniodir + "/raw"
cleandir         = transmileniodir + "/clean"




# COMMAND ----------

years = ["2023"]

for y in years:
    
    
    # List of the the raw files to be cleaned
    os.chdir(rawdir+"/"+y+"data/Troncal"+y)
    troncalf = os.listdir()
        
    os.chdir(rawdir+"/"+y+"data/Zonal"+y)
    zonalf = os.listdir()
        
    os.chdir(rawdir+"/"+y+"data/Dual"+y)
    dualf = os.listdir()
    
    files = troncalf + zonalf + dualf
    
    # Remove stata files
    files = [file for file in files if ".dta" not in file] 
    
    # Remove the files that were already cleaned
    print("Number of raw files:", len(files))
    os.chdir(cleandir+"/"+y+"data_clean") 
    already_cleaned = os.listdir()
    files = [file for file in files if file.replace('validacion','')[:-4]+"_clean.csv" not in already_cleaned]
    print("Number of raw files to clean:", len(files))
        
    
    # Get the unique days with some data for that year
    days  = list(set([l[-12:-4] for l in files]))
    
    for day in tqdm(range(len(days))):
        
        # Get each transaction type file for each day 
        # Sometimes there is no data for som day - transaction type combination

        t = [f for f in troncalf if "Troncal"+days[day] in f ]
        z = [f for f in zonalf if "Zonal"+days[day]   in f ]
        d = [f for f in dualf if "Dual"+days[day]    in f ]


        # whenever we have a file, import it, clean it and save it to the clean folder

        if t != []:
            t = t[0]

            os.chdir(rawdir+"/"+y+"data/Troncal"+y)
            troncal = pd.read_csv(t, usecols = troncalcols, dtype  = troncalcoltypes)

             # remove duplicates
            troncal['fecha'] = troncal.Fecha_Transaccion.str[:19]
            troncal.drop_duplicates(subset = ['Numero_Tarjeta','fecha'], inplace = True)

             # add typevalid and rename the other columns
            troncal['typevalid'] = 'troncal'
            troncal.rename(columns=dict(zip(troncalcols, new_troncalcols)), inplace=True)

            # save clean data to folder
            os.chdir(cleandir+"/"+y+"data_clean") 
            troncal.to_csv("Troncal"+days[day]+"_clean.csv", index = False)



        if z != []:
            z = z[0]

            os.chdir(rawdir+"/"+y+"data/Zonal"+y)
            zonal   = pd.read_csv(z, usecols = zonalcols, dtype  = zonalcoltypes)

            # remove duplicates
            zonal['fecha']   = zonal.Fecha_Transaccion.str[:19]
            zonal.drop_duplicates(subset = ['Numero_Tarjeta','fecha'], inplace = True)

            # add typevalid and rename the other columns
            zonal['typevalid'] = 'zonal'
            zonal.rename(columns=dict(zip(zonalcols, new_zonalcols)), inplace=True)

             # save clean data to folder
            os.chdir(cleandir+"/"+y+"data_clean") ########
            zonal.to_csv("Zonal"+days[day]+"_clean.csv", index = False)

        

        if d != []:
            d = d[0]

            os.chdir(rawdir+"/"+y+"data/Dual"+y)
            dual    = pd.read_csv(d, usecols = zonalcols, dtype  = zonalcoltypes) # same cols as zonal

            # remove duplicates
            dual['fecha']    = dual.Fecha_Transaccion.str[:19]
            dual.drop_duplicates(subset = ['Numero_Tarjeta','fecha'], inplace = True)  

            # add typevalid and rename the other columns
            dual['typevalid'] = 'dual'
            dual.rename(columns=dict(zip(zonalcols, new_zonalcols)), inplace=True)

            # save clean data to folder
            os.chdir(cleandir+"/"+y+"data_clean") 
            dual.to_csv("Dual"+days[day]+"_clean.csv", index = False)
