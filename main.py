from pymongo import MongoClient
from collections import Counter
import psutil

CONNECTION_STRING = "mongodb://localhost:27017"
DATABASE = 'movies'
COLLECTION = 'metadata'
NUMBER_OF_INDEX_CANDIDATES = 3
MONGO_RAM_PROCESS = [mem for mem in psutil.process_iter(attrs=['pid', 'name']) if mem.info['name'] == 'mongod.exe']
MONGO_MEMORY = 0.0
if len(MONGO_RAM_PROCESS) > 0:
    MONGO_MEMORY = MONGO_RAM_PROCESS[0].memory_info().rss / (1024 * 1024)
INDEX_POOL = MONGO_MEMORY * 0.3
print(INDEX_POOL)

conn = MongoClient(CONNECTION_STRING)
filter = {"op": "query", "millis": {"$gt": 0}}

database = conn.get_database(DATABASE)
collection = database[COLLECTION]
COLLECTION_SYS = database.system.profile

database.command('profile', 1, filter)
document = collection.find_one()
fields_of_collection = list(document.keys())

list_slow_queries = COLLECTION_SYS.find(filter)

potential_index_list = []
i=0
for query in list_slow_queries:
    print(query)
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

#Get the top 3 popular index candidate in the list
count_index = Counter(potential_index_list)
top_popular_index = [index[0] for index in count_index.most_common(NUMBER_OF_INDEX_CANDIDATES)]
collection_stats = collection.stats()
total_index_size_collection_mb = (collection_stats['totalIndexSize'])/ (1024 * 1024)

if INDEX_POOL < total_index_size_collection_mb:
    for i in range(len(top_popular_index)):
        collection.create_index(top_popular_index[i])
        print("index created: {top_popular_index[i]}")