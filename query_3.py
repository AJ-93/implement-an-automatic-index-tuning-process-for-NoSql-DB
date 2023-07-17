from pymongo import MongoClient
import time
import random
# Create a MongoDB client
client = MongoClient("mongodb://localhost:27017")

# Connect to the database
db = client["movielens_dataset"]

# Get the collection
collection = db["movies"]
#collection = db["system.indexes"]
movie_id = 1  #1.7248797416687012
# find max tag count for movieid
pipeline = [
    {
        "$match": {
            "movieId": { "$gt": 600 }
        }
    },
    {
        "$project": {
            "_id":None,
            "movieId": 1,
            "title": 1,
            "ratingCount": { "$size": "$ratings" }
        }
    }
]

start_time = time.time()
result = collection.aggregate(pipeline)
for i in result:
     print(i)
execution_time = time.time() - start_time

print(execution_time)