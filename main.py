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
potential_ind_dict_list = []
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
                    #getting the potential index list along with the time taken by the correcsponding query
                    potential_ind_dict = {'index':query['command']['filter'], 'millis':query['millis'] }
                    potential_ind_dict_list.append(potential_ind_dict)

#sorting the index list by the time taken to execute the query in descending order
sorted_potential_ind_dict_list = sorted(potential_ind_dict_list, key=lambda k:k['millis'], reverse=True)

#Get the top 3 popular index candidate in the list
top_popular_index_millis_list = sorted_potential_ind_dict_list[:NUMBER_OF_INDEX_CANDIDATES]

#get the filter values or index values from the dict
index_list = [index['index'] for index in top_popular_index_millis_list]
final_index_list = list([','.join(index.keys()) for index in index_list])

collection_list = database.list_collections()

total_index_size = 0
#calcuating the total index size for the whole database
for coll in collection_list:
    if(coll['name']!='system.profile'):
        collection_index_stats = database.command('collstats',coll['name'])
        total_index_size += collection_index_stats['totalIndexSize']

# taking the SI unit of conversion where 1000 bytes are equal to 1KB
total_index_size_mb = (total_index_size)/ (1000 * 1000)

#create index
if INDEX_POOL < total_index_size_mb:
    for i in range(len(final_index_list)):
        collection.create_index([(final_index_list[i],1)])
        print(f"index created: {final_index_list[i]}")