from pymongo import MongoClient
import time
import random
# Create a MongoDB client
client = MongoClient("mongodb://localhost:27017")

# Connect to the database
db = client["movielens_dataset"]

# Get the collection
collection = db["movies"]

# # Query for documents with genre = "Comedy" and rating > 3
# query = {
#     "$match": {
#         "genres": "Comedy",
#         "ratings": {"$elemMatch": {"rating": {"$gt": 3}}}
#     }
# }
#
# # Projection to include specific fields
# projection = {
#     "$project": {
#         "movieId": 1,
#         "title": 1,
#         "tags.tag": 1,
#         "ratings.rating": 1
#     }
# }
#
# # Sorting by rating in descending order
# sort = {
#     "$sort": {
#         "ratings.rating": -1
#     }
# }
#
# # Execute the aggregation pipeline
# result = collection.aggregate([query, projection, sort])
#
# # Iterate over the result

# start_time = time.time()
# result = collection.find({"tags": {"$elemMatch": {"tag": {"$eq": "bizarre"}}}}).hint([("tags", 1)])
# #execution_time_without_index = time.time() - start_time
#
# for doc in result:
#      pass
# execution_time_without_index = time.time() - start_time
# # print(execution_time_without_index)
# print(execution_time_without_index)
# unique_genres = db.movies.distinct("genres")
# random.shuffle(unique_genres)
# random_genres = random.choice(unique_genres)
# print(type(random_genres))
# query_without_index = {"genres": random_genres}
# print(type(query_without_index))
# result_without_index = collection.find(query_without_index).hint([("$natural", 1)])
# for doc in result_without_index:
#     print(doc)
pipeline = [
    {"$indexStats": {}}
]
result = list(collection.aggregate(pipeline))
opcount_dict = {}
for index in result:
    index_name = index['name'].rsplit('_', 1)[0]
    print(index_name)