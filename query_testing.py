from pymongo import MongoClient
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
# for doc in result:
#     print(doc)

# result = collection.find({"tags": {"$elemMatch": {"tag": {"$eq": "Sci Fi"}}}})
#
# for doc in result:
#     print(doc)

# unique_genres = db.movies.distinct("genres")
# random.shuffle(unique_genres)
# random_genres = random.choice(unique_genres)
# print(type(random_genres))
# query_without_index = {"genres": random_genres}
# print(type(query_without_index))
# result_without_index = collection.find(query_without_index).hint([("$natural", 1)])
# for doc in result_without_index:
#     print(doc)

a = [1,2]
b = [2,3,4,5]

if a not in b:
    print("SSSS")