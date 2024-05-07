import pyspark
import config
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder.getOrCreate()

tweets = spark.read.jdbc(
    url        = f'jdbc:mysql://source-db:{config.MySQL.port}/{config.MySQL.database}',
    table      = config.MySQL.table,
    properties = {
        'user'    : config.MySQL.user,
        'password': config.MySQL.password,
        'driver'  : 'com.mysql.cj.jdbc.Driver'
    }
)

tweets = tweets.limit(10)

( tweets.write
    .format("mongodb")
    .option("spark.mongodb.write.database", 'default')
    .option("spark.mongodb.write.collection", 'Tweet')
    .option("spark.mongodb.write.connection.uri", "mongodb://root:password@sink-db")
    .mode('append')
    .save()
)