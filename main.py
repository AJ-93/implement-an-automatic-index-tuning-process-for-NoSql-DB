from pymongo import MongoClient


CONNECTION_STRING = "mongodb://localhost:27017"
DATABASE = 'movies'
COLLECTION = 'metadata'

filter = {"op": "query", "millis": {"$gt": 200}}

conn = MongoClient(CONNECTION_STRING)

database = conn.get_database(DATABASE)
COLLECTION_SYS = database.system.profile

database.command('profile', 1, filter)

list_slow_queries = COLLECTION_SYS.find(filter)
for query in list_slow_queries:
    print(query['command']['filter'])
    print(query['planSummary'])
    print(query['docsExamined'])
    print(query['nreturned'])
    print(query['keysExamined'])




