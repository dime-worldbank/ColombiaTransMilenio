# WINDOWS

## Just a test to see if importing from the main notebook works
def import_test_utilities(x):
    print(x)

# window by cardnumber
user_window = Window\
    .partitionBy('cardnumber').orderBy('transaction_timestamp')