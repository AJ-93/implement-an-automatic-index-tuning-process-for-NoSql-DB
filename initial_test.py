from pymongo import MongoClient

conn = MongoClient("mongodb+srv://ajaykumarchadayan:Intelcoreiat3@cluster0.px5g5ok.mongodb.net/?retryWrites=true&w=majority")

database = conn.get_database('sample_mflix')


result = database.comments.find({})

query_result = database.comments.find({}).explain()

print(query_result['executionStats'])