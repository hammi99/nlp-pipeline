import pathlib

if pathlib.Path('Sentiment').exists():
	exit()


import config
import pyspark

spark = pyspark.sql.SparkSession.builder.getOrCreate()

( spark.read
    .format('mongodb')
    .option('database'      , f'{config.MongoDb.database}')
    .option('collection'    , f'{config.MongoDb.collection}')
    .option('connection.uri', f'mongodb://{config.MongoDb.user}:{config.MongoDb.password}@{config.MongoDb.host}:{config.MongoDb.port}')
	.load()
	.select('createdAt', 'negative', 'positive', 'neutral', 'compound')
	.orderBy('createdAt', ascending= True)
	.write.csv('Sentiment', header= True)
)