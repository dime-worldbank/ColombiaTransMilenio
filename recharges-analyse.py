# Databricks notebook source
import os
import pandas as pd

!pip install tqdm
from tqdm import tqdm

# COMMAND ----------

# Directories
V_DIR = '/Volumes/prd_csc_mega/sColom15/vColom15/'
recargas = os.listdir(V_DIR + "/Data/Recargas")
recargas2025 = os.listdir(V_DIR + "/Data/Recargas/2025")
recargasjun25 = [x for x in recargas if x[-10:] == "062025.zip"]


# COMMAND ----------

alldf = pd.DataFrame()
for f in tqdm(recargasjun25):
    df = pd.read_csv(V_DIR + "/Data/Recargas/" + f)
    alldf = pd.concat([alldf, df], axis = 0)

# COMMAND ----------

alldf.shape

# COMMAND ----------

alldf.Nombre_Perfil.value_counts()

# COMMAND ----------

alldf.Tipo_Tarjeta.value_counts()

# COMMAND ----------

alldf.Tipo_Pago.value_counts()

# COMMAND ----------

alldf.Tipo_Transaccion.value_counts()

# COMMAND ----------

alldf.Tipo_Recarga.value_counts()

# COMMAND ----------

recarga160 = alldf[(alldf.Valor_Recarga == 160000)]
recarga160.Tipo_Pago.value_counts()
recarga160maas = recarga160[recarga160.Tipo_Pago.isin(['MAAS-NEQUI(40)', 'MaaSapp(38)',  'MAAS-COMPENSAR(41)'])]

# COMMAND ----------

recarga160maas.Numero_Tarjeta.nunique()

# COMMAND ----------

alldf.Numero_Tarjeta.nunique()

# COMMAND ----------

recarga168 = alldf[(alldf.Valor_Recarga == 168000) & (alldf.Tipo_Transaccion == "Carga de Tarjeta")]
recarga168.Tipo_Pago.value_counts()
