import pyspark
import config

# creating spark session
spark = pyspark.sql.SparkSession.builder.getOrCreate()

# reading data from the source database
df = spark.read.jdbc(
    url        = f'jdbc:mysql://source-db:{config.MySQL.port}/{config.MySQL.database}',
    table      = config.MySQL.table,
    properties = {
        'user'    : config.MySQL.user,
        'password': config.MySQL.password,
        'driver'  : 'com.mysql.cj.jdbc.Driver'
    }
)
# df = df.limit(1_000_000)


# observing schema of the data
df.printSchema()

# describing the dataset
df.describe().show()

# removing missing values
df = df.dropna()

# removing the id column
df = df.drop(df.id)

# preprocessing the text of tweets
df = df.withColumn(
    # replacing every username with @user
    colName= 'text',
    col    = pyspark.sql.functions.regexp_replace(
        pyspark.sql.functions.col('text'), 
        pattern    = r'@\S*',
        replacement= '@user'
    )
).withColumn(
    # replacing http/https urls with http
    colName = 'text',
    col     = pyspark.sql.functions.regexp_replace(
        pyspark.sql.functions.col('text'), 
        pattern    = r'http\S*',
        replacement= 'http'
    )
)

# writing the processed data to sink database
( df.write
    .format("mongodb")
    .option("spark.mongodb.write.database", 'default')
    .option("spark.mongodb.write.collection", 'Tweet')
    .option("spark.mongodb.write.connection.uri", "mongodb://root:password@sink-db")
    # .mode('append')
    .mode('overwrite')
    .save()
)