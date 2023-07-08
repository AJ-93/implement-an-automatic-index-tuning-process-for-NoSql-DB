from pymongo import MongoClient

# Create a MongoDB client
client = MongoClient("mongodb://localhost:27017")

# Connect to the database
db = client["movielens_dataset"]

# Get the collection
collection = db["movies"]

# Query for documents with genre = "Comedy" and rating > 3
query = {
    "$match": {
        "genres": "Comedy",
        "ratings": {"$elemMatch": {"rating": {"$gt": 3}}}
    }
}

# Projection to include specific fields
projection = {
    "$project": {
        "movieId": 1,
        "title": 1,
        "tags.tag": 1,
        "ratings.rating": 1
    }
}

# Sorting by rating in descending order
sort = {
    "$sort": {
        "ratings.rating": -1
    }
}

# Execute the aggregation pipeline
result = collection.aggregate([query, projection, sort])

# Iterate over the result
for doc in result:
    print(doc)