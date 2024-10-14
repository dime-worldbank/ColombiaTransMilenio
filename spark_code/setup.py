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
        if type == 'pickle':
            name = os.path.join(path, file)
            pickleRdd = self.spark.sparkContext.pickleFile(name = name).collect()
            self.df = self.spark.createDataFrame(pickleRdd)

        elif type =='parquet':
            self.df = spark.read.format("parquet").load(os.path.join(path, file)) # changed
          
        elif type =='sample':
            self.df = self.spark.read.format("csv").option("header", "true")\
                                        .option("delimiter", ",")\
                                        .option("encoding", "UTF-8")\
                                        .load(os.path.join(path, file, "*")) # changed

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
                                         .load(os.path.join(path,"*")) # reads all files in the path

        else:
            self.dfraw = self.spark.read.format("csv").option("header", "true")\
                                        .option("delimiter", ";")\
                                        .option("charset", "utf-8")\
                                        .load(path) # changed -- reads all files in the path

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

   '''

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
'''
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