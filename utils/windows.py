# Databricks notebook source
# WINDOWS

# window by cardnumber
user_window = Window\
    .partitionBy('cardnumber').orderBy('transaction_timestamp')
    
# window  by cardnumber, explicity unbounded (should be same as above, but to be sure)
user_window_unbounded = Window\
    .partitionBy('cardnumber')\
    .orderBy('transaction_timestamp') \
    .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# user day window
user_day_window = Window\
    .partitionBy('cardnumber', 'day').orderBy('transaction_timestamp')

# user day window starting from last day
user_day_window_rev = Window\
    .partitionBy('cardnumber', 'day').orderBy(F.desc('transaction_timestamp'))

# user week window
user_week_window = Window\
    .partitionBy('cardnumber', 'week').orderBy('transaction_timestamp')

# user week window starting from last day
user_week_window_rev = Window\
    .partitionBy('cardnumber', 'week').orderBy(F.desc('transaction_timestamp'))

# user month window
user_month_window = Window\
    .partitionBy('cardnumber', 'month').orderBy('transaction_timestamp')

# user month window starting with last month
user_month_window_rev = Window\
    .partitionBy('cardnumber', 'month').orderBy(F.desc('transaction_timestamp'))

# function to convert days to secs
days = lambda i: i * 86400

# rolling month window
rolling_month_window = Window.orderBy(F.col('timestamp'))\
    .rangeBetween(-days(28), Window.currentRow)

# rolling month window, rev
rolling_month_window = Window.orderBy(F.desc('timestamp'))\
    .rangeBetween(Window.currentRow, days(28))

# rolling user month window
rolling_user_month_window = Window.partitionBy('cardnumber')\
    .orderBy(F.col('timestamp'))\
    .rangeBetween(-days(28), Window.currentRow)

# rolling user month window, starting with last month
rolling_user_month_window_rev = Window.partitionBy('cardnumber')\
    .orderBy(F.desc('timestamp'))\
    .rangeBetween(Window.currentRow, days(28))

