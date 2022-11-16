from pymongo import MongoClient
import requests
from requests.auth import HTTPDigestAuth

conn = MongoClient("mongodb+srv://ajaykumarchadayan:Intelcoreiat3@cluster0.px5g5ok.mongodb.net/?retryWrites=true&w=majority")

database = conn.get_database('sample_mflix')


result = database.comments.find({'name': 'Teresa Thomas'})
#for comm in result:
   # print(comm)

# Drop index
database.restaurants.drop_index('name_1')

query_result = database.comments.find({'name': 'Teresa Thomas'}).explain()
print(query_result['executionStats'])
# 'executionTimeMillis': 25, 'totalKeysExamined': 0, 'totalDocsExamined': 41079, 'executionStages': {'stage': 'COLLSCAN'

database.comments.create_index('name')
index_info = database.comments.index_information()
print(index_info)

query_result_afterIndex = database.comments.find({'name': 'Teresa Thomas'}).explain()
print(query_result_afterIndex['executionStats'])


index_info = database.comments.index_information()
print(index_info)

index_stats = database.comments.aggregate( [ {"$indexStats": { } } ] )
for index in index_stats:
    print(index)