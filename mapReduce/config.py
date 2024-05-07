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
    database   = 'default'
    collection = 'Tweet'