class MySQL:
    host     = 'source-db'
    port     = 3306
    user     = 'root'
    password = 'password'
    database = 'default'
    table    = 'Tweet'

class MongoDb:
    host       = 'sink-db'
    port       = 27017
    user       = 'root'
    password   = 'password'
    database   = 'default'
    readCollection  = 'Tweet'
    writeCollection = 'Sentiment'
    
class Spark:
    home = 'sparkFiles/spark-3.5.1-bin-hadoop3'