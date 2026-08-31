from import_packages import *
from windows import *

################################################################################

# Define function to find first non-trunk user account id
@pandas_udf("integer", PandasUDFType.GROUPED_AGG)
def first_non_trunk(account, operator):
    if len(account) == 1:
        first_account = account.iloc[0]
    else:
        non_trunk_rides = account[operator != 0]
        if len(non_trunk_rides) > 0:
            first_account = non_trunk_rides.iloc[0]
        else:
            first_account = account.iloc[0]
    return first_account

policy = dt.datetime(2017, 4, 1, 3, tzinfo = timezone)
oct_2017 = dt.datetime(2017, 10, 1, tzinfo = timezone)

user_window_unbounded = Window\
    .partitionBy('cardnumber')\
    .orderBy('transaction_timestamp') \
    .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# Create function that cleans data
def clean_data(df):
    # prep calcs for usage outliers
    usage_count_day = df.groupby("cardnumber", 'day').count()
    daily_usage_outlier_accounts = usage_count_day.where(usage_count_day['count'] > 100).select('cardnumber').distinct()
    daily_usage_outlier_accounts_repeated = usage_count_day\
        .where(usage_count_day['count'] > 20)\
        .groupby('cardnumber').count().filter(F.col('count') > 2).select('cardnumber').distinct()
    usage_count_year = df.groupby("cardnumber").count()
    infrequent_user_accounts = usage_count_year.where(usage_count_year['count'] < 12).select('cardnumber').distinct() 

    # filter
    df = df.withColumn('implausible_switch', F.when(
                ((F.lag(clean_df['account_name_id'], -1).over(user_window_rev) != 3) & \
                (clean_df['account_name_id'] == 3)), 1).otherwise(0)) \
        .withColumn('plausible_switch', F.when(
            ((F.lag(df['account_name_id'], -1).over(user_window_rev) == 3) & \
             (df['account_name_id'] != 3)), 1).otherwise(0)) \
        .withColumn('account_name_id_imputed', 
                F.when(F.sum(F.col('implausible_switch')).over(user_window_unbounded) == 0, 
                       F.col('account_name_id')) \
                .otherwise(first_non_trunk(F.col('account_name_id'), F.col('operator_id')).over(user_window_unbounded))) \
        .withColumn('early_zero', F.when(((df['value'] == 0) & (df['transaction_timestamp'] < policy)),1).otherwise(0)) \
        .where(df['transaction_timestamp'] >= dt.datetime(2016, 10, 1, tzinfo = timezone)) \
        .where(df['transaction_timestamp'] < dt.datetime(2019, 10, 1, tzinfo = timezone)) \
        .where(df['balance_before'] < 1000000) \
        .where(((df['value'] == 200) & (df['transaction_timestamp'] < policy)) == False) \
        .where(((df['value'] == 700) & (df['transaction_timestamp'] >= policy) & (df['transaction_timestamp'] < oct_2017)) == False) \
        .where(((df['value'] == 900) & (df['transaction_timestamp'] < oct_2017)) == False) \
        .where(((df['value'] == 1000) & (df['transaction_timestamp'] >= policy) & (df['transaction_timestamp'] < oct_2017)) == False) \
        .where(((df['value'] == 1450) & (df['transaction_timestamp'] < policy)) == False) \
        .where(((df['value'] == 1550) & (df['transaction_timestamp'] >= policy) & (df['transaction_timestamp'] < oct_2017)) == False) \
        .where(((df['value'] == 1600) & (df['transaction_timestamp'] < oct_2017)) == False) \
        .where(((df['value'] == 1650) & (df['transaction_timestamp'] < policy)) == False) \
        .where(((df['value'] == 1700) & (df['transaction_timestamp'] >= policy) & (df['transaction_timestamp'] < oct_2017)) == False) \
        .where(((df['value'] == 2200) & (df['transaction_timestamp'] < policy)) == False) \
        .join(daily_usage_outlier_accounts, 
                    df['cardnumber'] == daily_usage_outlier_accounts['cardnumber'],
                    how ='leftanti').select(df.columns[0:]) \
        .join(daily_usage_outlier_accounts_repeated, 
                    df['cardnumber'] == daily_usage_outlier_accounts_repeated['cardnumber'],
                    how ='leftanti').select(clean_df.columns[0:])\
        .join(infrequent_user_accounts, 
                    df['cardnumber'] == infrequent_user_accounts['cardnumber'],
                    how ='leftanti').select(df.columns[0:])\
        .drop('implausible_switch')
        
    return df