import pathlib
import config
import pyspark

def copyData(to= 'Sentiment', limit= None):
    if pathlib.Path('Sentiment').exists(): return
        
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    
    df = ( spark.read
		.format('mongodb')
		.option('database'      , f'{config.MongoDb.database}')
		.option('collection'    , f'{config.MongoDb.collection}')
		.option('connection.uri', f'mongodb://{config.MongoDb.user}:{config.MongoDb.password}@{config.MongoDb.host}:{config.MongoDb.port}')
		.load()
		.select('createdAt', 'negative', 'positive', 'neutral', 'compound')
		.orderBy('createdAt', ascending= True)
    )

    if limit is not None:
        df = df.limit(limit)

    df.write.csv(to, header= True)