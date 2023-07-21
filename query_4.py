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
movie_id = 1
# find max tag count for movieid
pipeline = [
{
    "$match": {
      "ratings": {"$elemMatch": {"rating": {"$gt":5}}}
    }
  },
  {
  "$group": {
  "_id": "$movieId",
  "tagCount": {"$sum": {"$size": "$tags"}},
  "movieId": {"$first": "$movieId"},
  "title": {"$first": "$title"}
}
},
  {
    "$project": {
      "movieId": 1,
      "title": 1,
      "tagCount": 1
    }
  },
  {
    "$sort": {
      "tagCount": -1
    }
  },
  {
    "$limit": 1
  }
  ]

start_time = time.time()
result = collection.aggregate(pipeline)
for i in result:
     print(i)
execution_time = time.time() - start_time

print(execution_time)