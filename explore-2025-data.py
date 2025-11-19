# Databricks notebook source
# MAGIC %run ./utils/packages

# COMMAND ----------

# MAGIC %run ./utils/setup

# COMMAND ----------

# MAGIC %run ./utils/spark_df_handler

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

paths = []
headers = os.listdir(byheader_dir)
for h in headers:
    files = os.listdir(f"{byheader_dir}/{h}")
    paths += [f"{byheader_dir}/{h}/{file}" for file in files]
paths2025 = [p for p in paths if p[-12:-8] == "2025"]

# COMMAND ----------

len(paths2025) / 2

# COMMAND ----------

treatment_sample = pd.read_csv(V_DIR + 'Workspace/Construct/treatment_groups_sample10.csv')
personalized_cards_sample = treatment_sample.cardnumber[treatment_sample.profile_final.isin(['apoyo_subsidyvalue', 'adulto'])]

# COMMAND ----------

# Keep cards in sample in 2025 data
alldf1 = pd.DataFrame()

for file in tqdm(paths2025[0:170]):
    df = pd.read_csv(file)
    df = df[df.Numero_Tarjeta.isin(personalized_cards_sample)]
    alldf1 = pd.concat([alldf1, df], axis = 0)

# COMMAND ----------

alldf2 = pd.DataFrame()

for file in tqdm(paths2025[170:len(paths2025)]):
    df = pd.read_csv(file)
    df = df[df.Numero_Tarjeta.isin(personalized_cards_sample)]
    alldf2 = pd.concat([alldf2, df], axis = 0)

# COMMAND ----------

relevant_cols = ["Numero_Tarjeta", "Fecha_Transaccion", "Nombre_Perfil", "Valor"]
alldf = pd.concat([alldf1[relevant_cols], alldf2[relevant_cols]], axis = 0)

# add date values to alldf dataframe
alldf["datetime"]   = pd.to_datetime(alldf.Fecha_Transaccion)
alldf["ymonth"] = alldf.datetime.dt.to_period("M")
alldf["year"]   = alldf.datetime.dt.year

# tag transfers
# Calculate time difference in minutes
alldf = alldf.sort_values(["Numero_Tarjeta", "datetime"]).reset_index(drop = True)
alldf['time_diff_min'] = alldf.groupby('Numero_Tarjeta')['datetime'].diff().dt.total_seconds() / 60

# Tag if time difference is less than 125 minutes
alldf['transfer'] = alldf['time_diff_min'] < 125

# add treatment until Jan 25
alldf.rename(columns = {"Numero_Tarjeta": "cardnumber"}, inplace = True)
alldf = alldf.merge(treatment_sample[["cardnumber", "profile_final", "treatment"]],
                   on = "cardnumber",
                   how = "left")

# COMMAND ----------

max_date = alldf.datetime.dt.date.max()
print(f"Max date in alldf: {max_date}")
alldf.to_csv(f'{V_DIR}Workspace/Construct/transactions_2025_until{max_date}.csv', index = False)

# COMMAND ----------

mar25 = alldf[alldf.ymonth == "2025-03"].reset_index(drop = True)
mar25["free_trip"] = (mar25.Valor == 0) & (mar25.transfer == False)
print(f"Number of non-transfer free trips: {np.sum(mar25.free_trip)}")
cards_with_free_trips = mar25.cardnumber[mar25.free_trip].unique()
print(f"Number of cards with non-transfer free trips: {len(cards_with_free_trips)}")
print(f"Card type for cards with non-transfer free trips: {mar25.profile_final[mar25.free_trip].unique()}")
byc = mar25[mar25.transfer == False].groupby(["cardnumber", "profile_final", "Nombre_Perfil"], as_index = False).agg({"free_trip": ["sum", "mean"]})
byc.columns = ["cardnumber", "profile_final", "Nombre_Perfil", "tot_free_trips", "avg_free_trips"]
byc["new_subsidy"] = (byc.tot_free_trips == 5) | (byc.tot_free_trips == 7) | (byc.avg_free_trips == 1)
byc["new_subsidy_lenient"] = ((byc.tot_free_trips > 2) & (byc.tot_free_trips < 10)) | (byc.avg_free_trips > 0.8)

# COMMAND ----------

treatment_sample.profile_final.value_counts()

# COMMAND ----------

byc.profile_final.value_counts()

# COMMAND ----------

byc.shape

# COMMAND ----------

pd.crosstab(byc.profile_final[byc.new_subsidy], byc.Nombre_Perfil[byc.new_subsidy])

# COMMAND ----------

pd.crosstab(byc.profile_final[byc.new_subsidy_lenient], byc.Nombre_Perfil[byc.new_subsidy_lenient])

# COMMAND ----------

abr25 = alldf[alldf.ymonth == "2025-04"].reset_index(drop = True)
abr25["free_trip"] = (abr25.Valor == 0) & (abr25.transfer == False)
print(f"Number of non-transfer free trips: {np.sum(abr25.free_trip)}")
cards_with_free_trips = abr25.cardnumber[abr25.free_trip].unique()
print(f"Number of cards with non-transfer free trips: {len(cards_with_free_trips)}")
print(f"Card type for cards with non-transfer free trips: {abr25.profile_final[abr25.free_trip].unique()}")
byc = abr25[abr25.transfer == False].groupby(["cardnumber", "profile_final", "Nombre_Perfil"], as_index = False).agg({"free_trip": ["sum", "mean"]})
byc.columns = ["cardnumber", "profile_final", "Nombre_Perfil", "tot_free_trips", "avg_free_trips"]
byc["new_subsidy"] = (byc.tot_free_trips == 5) | (byc.tot_free_trips == 7) | (byc.avg_free_trips == 1)
byc["new_subsidy_lenient"] = ((byc.tot_free_trips > 2) & (byc.tot_free_trips < 10)) | (byc.avg_free_trips > 0.8)

# COMMAND ----------

pd.crosstab(byc.profile_final[byc.new_subsidy], byc.Nombre_Perfil[byc.new_subsidy])
