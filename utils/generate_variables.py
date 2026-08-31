# Databricks notebook source
from import_packages import *
from setup import git
from windows import *

################################################################################
## This script is used to create additional variables

# import dictionaries
station_dict = pd.read_csv(os.path.join(git + '/csv_outputs/station_fix_dict.csv')).iloc[:,[1,2,3]].set_index('station_id')
station_geo_dict = pd.read_csv(os.path.join(git + '/csv_outputs/station_geo_dict.csv')).iloc[:,[2,4,5]].set_index('station_id')
emisor_dict = pd.read_csv(os.path.join(git + '/csv_outputs/emisor_dict.csv')).iloc[:,[1,2,3]].set_index('emisor_id')
operator_dict = pd.read_csv(os.path.join(git + '/csv_outputs/operator_dict.csv')).iloc[:,[1,2,3]].set_index('operator_id')
account_name_dict = pd.read_csv(os.path.join(git + '/csv_outputs/account_name_dict.csv'))\
            .iloc[:,[1,2,3]].set_index('account_name_id')
line_dict = pd.read_csv(os.path.join(git + '/csv_outputs/line_dict.csv')).iloc[:,[1,2,3]].set_index('line_id')

# Create class that generates variables, given a list of vars to keep.
# Since it has only one method and we are not using its state this could be turned into a function to simplify things.
class var_generator:
    """

    Attributes
    ----------
    spark : an initialised spark connection

    Methods
    -------
    [add description]

    """

    def __init__(self,
                df):
        """
        Parameters
        ----------
        df : the df to transform
        vars_to_keep : the variables to keep after transformation
        """
        self.df = df
        self.vars_to_keep = [   'transaction_timestamp',
                                'line',
                                'station',
                                'operator',
                                'cardnumber',
                                'emisor',
                                'account_name',
                                'lost_subsidy_year',
                                #'lost_subsidy_year_alt',
                                'left_in_april',
                                'october_user',
                                'balance_before',
                                'balance_after',
                                'value',
                                'real_balance_after',
                                'transfer',
                                'negative_trip_number',
                                'negative_trip',
                                'transfer_time',
                                'month',
                                'week',
                                'day',
                                'dayofweek',
                                'hour',
                                'minute',
                                'second']

    def generate_variables(self):
        policy = dt.datetime(2017, 4, 1, 3, tzinfo = timezone)

        ## Generate additional variables
        # Create real balance variable
        self.df = self.df.withColumn('real_balance_after', self.df['balance_before'] - self.df['value'])

        # Create negative trip variable
        self.df = self.df.withColumn('negative_trip',
            F.when(self.df['real_balance_after'] < 0, 1)\
            .otherwise(0).cast('byte'))

        # Create variable for number of negative trips using window function
        self.df = self.df.withColumn('negative_trip_number', F.when(
            ((F.lag(self.df['real_balance_after'], 4).over(user_window) < 0)
                     & (F.lag(self.df['real_balance_after'], 3).over(user_window) < 0)
                     & (F.lag(self.df['real_balance_after'], 2).over(user_window) < 0)
                     & (F.lag(self.df['real_balance_after'], 1).over(user_window) < 0)
                     & (self.df['real_balance_after'] < 0)), 5).when(
            ((F.lag(self.df['real_balance_after'], 3).over(user_window) < 0)
                     & (F.lag(self.df['real_balance_after'], 2).over(user_window) < 0)
                     & (F.lag(self.df['real_balance_after'], 1).over(user_window) < 0)
                     & (self.df['real_balance_after'] < 0)), 4).when(
            ((F.lag(self.df['real_balance_after'], 2).over(user_window) < 0)
                     & (F.lag(self.df['real_balance_after'], 1).over(user_window) < 0)
                     & (self.df['real_balance_after'] < 0)), 3).when(
            ((F.lag(self.df['real_balance_after']).over(user_window) < 0)
                     & (self.df['real_balance_after'] < 0)), 2).when(
            self.df['real_balance_after'] < 0, 1).otherwise(0))
        # overwrite NAs from window function
        self.df = self.df.fillna(0, subset=['negative_trip_number'])

        # Compute day of week, hour, day of year and create variables
        self.df = self.df.withColumn('month', F.date_trunc('month', self.df['transaction_timestamp']))
        self.df = self.df.withColumn('week', F.date_trunc('week', self.df['transaction_timestamp']))
        self.df = self.df.withColumn('dayofweek', F.dayofweek('transaction_timestamp').cast('byte'))
        self.df = self.df.withColumn('day', F.date_trunc('day', self.df['transaction_timestamp']))
        self.df = self.df.withColumn('hour', F.hour('transaction_timestamp').cast('byte'))
        self.df = self.df.withColumn('minute', F.minute('transaction_timestamp').cast('byte'))
        self.df = self.df.withColumn('second', F.second('transaction_timestamp').cast('byte'))

        # Create transfer dummy variable
        self.df = self.df.withColumn('transfer',
            F.when(self.df['value'] < 500, 1)\
            .otherwise(0).cast('byte'))

        # Create transfer time variable
        self.df = self.df.withColumn('transfer_time',
            F.when(self.df['transfer'] == True,
            (F.unix_timestamp(self.df['transaction_timestamp'])
            - F.unix_timestamp(F.lag(self.df['transaction_timestamp'])\
            .over(user_window)))/60) \
            .otherwise(0))

        self.df = self.df.withColumn('transfer_time',
            F.when(self.df['transfer_time'] > 95, 0).otherwise(self.df['transfer_time']))

        ## Consider using a broadcast variable for these joins
        # Create lost_subsidy variable
        # lost_subsidy = self.df.where((self.df['account_name'] == '(006) Apoyo Ciudadano') & \
        #                              (1700 <= self.df['value']) & \
        #                             (self.df['transaction_timestamp'] > policy)) \
        #    .select('cardnumber') \
        #    .distinct() \
        #.withColumn('lost_subsidy', F.lit(1))
        #lost_subsidy = lost_subsidy.withColumnRenamed('cardnumber','id')
        #lost_subsidy = lost_subsidy.withColumnRenamed('lost_subsidy','sub')

        #self.df = self.df.withColumn('lost_subsidy', F.lit(0))
        #self.df = self.df.join(lost_subsidy,self.df['cardnumber'] == lost_subsidy['id'], how ='left')
        #self.df = self.df.withColumn('lost_subsidy_year_alt', F.coalesce(self.df['sub'], self.df['lost_subsidy']))

        cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
        self.df =  self.df.withColumn('lost_subsidy',
            F.when(cnt_cond((self.df['account_name'] == '(006) Apoyo Ciudadano') \
            & (1450 <= self.df['value']) & (self.df['value'] <= 1650) \
            & (self.df['day'] > dt.datetime(2017, 4, 1, tzinfo = timezone))) \
            .over(user_window_rev) > 0, 0).otherwise(1))

        self.df = self.df.withColumn('lost_subsidy_year',
            F.min(self.df['lost_subsidy']).over(user_window_unbounded).cast('byte'))

        # Create October user variable
        self.df =  self.df.withColumn('october_user',
            F.when(cnt_cond((self.df['account_name'] == '(006) Apoyo Ciudadano') \
            & (self.df['day'] < dt.datetime(2016, 11, 1, tzinfo = timezone))) \
            .over(user_window) > 0, 1).otherwise(0).cast('byte'))

        # Create left_in_april variable
        self.df =  self.df.withColumn('left_in_april',
            F.when(cnt_cond((self.df['account_name'] == '(006) Apoyo Ciudadano') \
            & (self.df['day'] >= dt.datetime(2017, 4, 1, tzinfo = timezone))) \
            .over(user_window_rev) > 0, 0).otherwise(1).cast('byte'))

        # Create fraud_flag variable
        self.df = self.df.withColumn('fraud_flag', F.when(cnt_cond(
            (F.when(cnt_cond(self.df['transfer_time'] < 5.75)\
            .over(user_window) > 1, True).otherwise(False)) &
            (F.when(F.count(self.df['value'])\
            .over(user_day_window) > 9, True).otherwise(False)))\
            .over(user_window) > 2, 1).otherwise(0).cast('byte'))

        # Fix station variable for SITP stations
        self.df = self.df.withColumn('station_raw', self.df['station'])
        self.df = self.df.withColumn('station', F.when(self.df['operator'] != '(201) Trunk agency',
                                                 self.df['station_access']).otherwise(self.df['station_raw']))

        return  self.df.select(self.vars_to_keep)
    
    def recreate_dictionaries(self, variable, new_variable, spark):
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
        new_distinct = self.df.select(variable).distinct().cache()
        old_distinct = dictionary.reset_index()
        id_max = old_distinct[new_variable].max()
        old_distinct = spark.createDataFrame(old_distinct)
        old_distinct = old_distinct.withColumnRenamed(variable, variable + '_dict')
        new_dictionary = old_distinct.join(new_distinct, old_distinct[variable + '_dict'] == new_distinct[variable], how = 'outer')
        new_dictionary = new_dictionary.withColumn(new_variable, F.when(F.isnull(F.col(new_variable)), F.lit(id_max) + 1)\
            .otherwise(F.col(new_variable)))
        window = Window.orderBy(new_variable)
        new_dictionary = new_dictionary.withColumn(variable + '_dict', F.when(F.isnull(F.col(variable + '_dict')), F.col(variable))\
                                                  .otherwise(F.col(variable + '_dict')))
        new_dictionary = new_dictionary.withColumn(new_variable, F.when(F.isnull(F.col('count')), 
                                                                             F.row_number().over(window)-1)\
            .otherwise(F.col(new_variable)))
        #print(new_dictionary.toPandas())
        new_dictionary = new_dictionary.select(old_distinct.columns[0:])
        return new_dictionary

    def enumerate_factors(self, variable, create_dict, spark = None, old_dict = False):
        # name for new variable
        new_variable = variable + '_id'
        # either create new dict
        if old_dict == False:
            self.indexer = feature.StringIndexer(inputCol=variable, outputCol=new_variable)
            self.fitted_indexer = self.indexer.fit(self.df)
            self.df = self.fitted_indexer.transform(self.df)
        # or re-use old dict and amend new factor levels
        elif old_dict == True:
            dict_df = self.recreate_dictionaries(variable, new_variable, spark)
            #import pdb; pdb.set_trace()
            self.df = self.df.join(dict_df,
                    self.df[variable] == dict_df[variable + '_dict'],
                    how ='left')
        # cast as byte
        self.df = self.df.withColumn(new_variable, self.df[new_variable].cast('smallint'))
        # drop factor labels
        self.vars_to_keep.append(new_variable)
        self.vars_to_keep.remove(variable)
        if create_dict == True:
            self.variable_count = self.df.groupby(variable, new_variable).count().sort(
            'count', ascending = False)
            if old_dict == False:
                self.variable_count.toPandas().to_csv(os.path.join(git, "csv_outputs/" + variable + "_dict.csv"))
            else:
                self.variable_count.toPandas().to_csv(os.path.join(git, "csv_outputs/" + variable + "_new_dict.csv"))

        return self.df.select(self.vars_to_keep)
