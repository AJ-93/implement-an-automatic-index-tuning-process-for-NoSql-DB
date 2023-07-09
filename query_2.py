from pymongo import MongoClient
import random
# Create a MongoDB client
client = MongoClient("mongodb://localhost:27017")

# Connect to the database
db = client["movielens_dataset"]

# Get the collection
collection = db["movies"]
#collection = db["system.indexes"]

# # Aggregate to find the minimum and maximum ratings.rating with movie name
# pipeline = [
#     {
#         "$unwind": "$ratings"
#     },
#     {
#         "$group": {
#             "_id": None,
#             "minRating": { "$min": "$ratings.rating" },
#             "maxRating": { "$max": "$ratings.rating" },
#             "minMovie": { "$min": "$title" },
#             "maxMovie": { "$max": "$title" }
#         }
#     }
# ]
#
# # Execute the aggregation pipeline
# result = list(collection.aggregate(pipeline))
#
# # Extract the results
# min_rating = result[0]["minRating"]
# max_rating = result[0]["maxRating"]
# min_movie = result[0]["minMovie"]
# max_movie = result[0]["maxMovie"]
#
# # Print the results
# print("Movie with Minimum Rating: ", min_movie, " - Rating: ", min_rating)
# print("Movie with Maximum Rating: ", max_movie, " - Rating: ", max_rating)
# print(result)

# indexes = collection.list_indexes()
# print(indexes)
#
# total_indexes = sum(1 for _ in indexes)
# print(total_indexes)

# unique_tags = db.movies.distinct("title")
# movieid_list = [i for i in range(1, 10681)]
# random.shuffle(movieid_list)
# random_movieid = random.choice(movieid_list)
#
# print(type(unique_tags[random_movieid]))
#
# document = collection.find_one()
# fields_of_collection = list(document.keys())
# print(fields_of_collection)
pipeline = [
    {"$indexStats": {}}
]
result = list(collection.aggregate(pipeline))

print(result)
