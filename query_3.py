from pymongo import MongoClient
import time

# Create a MongoDB client
client = MongoClient("mongodb://localhost:27017")

# Connect to the database
db = client["movielens_dataset"]

# Get the collection
collection = db["movies"]

pipeline = [
    {
        "$match": {
            "movieId": { "$gt": 800 }
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

print(f"Total execution time {execution_time}")