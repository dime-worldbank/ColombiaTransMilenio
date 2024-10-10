## start_spark.py


## Set up spark
# Check which computer this is running on
if multiprocessing.cpu_count() == 6:
    on_server = False
else:
    on_server = True

print(on_server)

# Set paths
if on_server:
    print("OK: ON SERVER")
    path = '/dbfs/' + pathdb
    user = os.listdir('/Workspace/Repos')[0]
    git = f'/Workspace/Repos/{user}/ColombiaTransMilenio'
else:
    print("Not on server - no path defined")

# start spark session according to computer
if on_server:
    spark = SparkSession \
        .builder \
        .master("local[75]") \
        .config("spark.driver.memory", "200g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config('spark.local.dir', pathdb) \
        .config("spark.sql.execution.arrow.enabled", "true")\
        .config("spark.sql.legacy.timeParserPolicy","LEGACY")\
        .getOrCreate()
else:
    spark = SparkSession.builder.master("local[*]") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.execution.arrow.enabled", "true")\
    .getOrCreate()