# Databricks notebook source
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
