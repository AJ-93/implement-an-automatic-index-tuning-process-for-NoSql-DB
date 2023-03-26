from pymongo import MongoClient


CONNECTION_STRING = "mongodb://localhost:27017"
DATABASE = 'movies'
COLLECTION = 'metadata'

filter = {"op": "query", "millis": {"$gt": 0}}

conn = MongoClient(CONNECTION_STRING)

database = conn.get_database(DATABASE)
collection = database[COLLECTION]
COLLECTION_SYS = database.system.profile

database.command('profile', 1, filter)
document = collection.find_one()
fields_of_collection = list(document.keys())
print (fields_of_collection)

list_slow_queries = COLLECTION_SYS.find(filter)

potential_index_list = []
i=0
for query in list_slow_queries:
    #print(query)
    index = query['command']['filter']
    potential_index = index.keys()
    potential_index = ' '.join(potential_index)
    query_plan = query['planSummary']
    docs_scanned = query['docsExamined']
    docs_returned = query['nreturned']
    index_keys_used = query['keysExamined']
    if index is not None:
        if (query_plan == 'COLLSCAN') and (potential_index in fields_of_collection):
                    potential_index_list.append(potential_index)


