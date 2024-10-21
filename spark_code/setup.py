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
        """
        Parameters
        ----------
        """
        self.spark = spark
        self.on_server = on_server

    def load(self, path =  path, type = 'parquet', file = 'parquet_df', delimiter = ';', encoding = "utf-8"):

      '''
      Function to load different types of files and parse data
      '''
        
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

      '''
      Function to homogenize types of files
      '''
    
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
            self.df = self.dfraw.select(F.to_timestamp(self.dfraw['Fecha_Transaccion'],'dd-MM-yyyy HH:mm:ss')\
                            .alias('transaction_timestamp'),
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
          elif header_format == 'format_6':
            self.df = self.dfraw.select(F.to_timestamp(self.dfraw['Fecha_Transaccion'],'dd-MM-yyyy HH:mm:ss')\
                            .alias('transaction_timestamp'),
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
     
    # 2023 edition: changing the sample function. Adding subgroup and not apoyo IDs option.
   # def sample(self, data = 'none', perc = 100, subgroup = 'apoyo'): # by default, no sample. Added subgroup parameters

        '''
        Parameters
        - perc: sample percentage. Then divisor will be 100 / perc. Example: perc = 20 ---> divisor = 100/20 = 5 
          By default perc = 100, that is, divisor = 1. No sample.
        - subgroup: can be one of "full", "apoyo", "not apoyo"
        '''

'''
        print(f"Sample of {perc}% for {subgroup} obs.")

        divisor = 100 / perc

        if str(data) == 'none':
            df = self.df
        else:
            df = data

        # Create list of unique ids
        unique_ids = [i.cardnumber for i in df.select('cardnumber').distinct().collect()]
        unique_ids_apoyo = [i.cardnumber for i in df.where(df['account_name_id'] == 2) \
                    .select('cardnumber').distinct().collect()]
        unique_ids_not_apoyo = list[set(unique_ids).difference(unique_ids_apoyo)]

        # how many unique ids do we have?
        print("Number of unique IDs: {:,}".format(len(unique_ids))) 
        print("Number of unique Apoyo IDs: {:,}".format(len(unique_ids_apoyo)))
        print("Number of unique NOT Apoyo IDs: {:,}".format(len(unique_ids_not_apoyo)))
       
        if subgroup == "full":
            ids = unique_ids
        if subgroup == "apoyo":
            ids = unique_ids_apoyo
        if subgroup == "not apoyo":
            ids = unique_ids_not_apoyo
        
        sample_size = int(len(ids) / int(divisor))

        print(f"Number of sampled {subgroup} IDs: {sample_size}")
        
        seed(510)
        sample_ids = sample(ids, sample_size)
       

        # Create a filtered df with the transactions of these cards only
        sampled_df = df.filter(df.cardnumber.isin(sample_ids))
        print("Number of observations in sample: {:,}".format(sampled_df.count()))
        
        return sampled_df # modified to return df, not write
        # Write to csv
        #sampled_df.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv') \
        #                      .save(os.path.join(path + '/1pctsample/full'), header = 'true')
        #sampled_apoyo_df.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv') \
        #                      .save(os.path.join(path + '/10pctsample/apoyo'), header = 'true')
'''