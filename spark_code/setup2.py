def setup_run(x):
  print(z)

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
     
