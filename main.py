from pymongo import MongoClient


CONNECTION_STRING = "mongodb://localhost:27017"
DATABASE = 'movies'
COLLECTION = 'metadata'

filter = {"op": "query", "millis": {"$gt": 0}}

conn = MongoClient(CONNECTION_STRING)

database = conn.get_database(DATABASE)
COLLECTION_SYS = database.system.profile

database.command('profile', 1, filter)

list_slow_queries = COLLECTION_SYS.find(filter)
potential_index_list = []
for query in list_slow_queries:
    potential_index = query['command']['filter']
    print (potential_index.keys())
    query_plan = query['planSummary']
    docs_scanned = query['docsExamined']
    docs_returned = query['nreturned']
    index_keys_used = query['keysExamined']
    if potential_index :
        if query_plan == 'COLLSCAN' :
            if index_keys_used != 0:
                if docs_scanned == docs_returned:
                    potential_index_list[i] = potential_index
                    i=i+1

print (potential_index_list)





