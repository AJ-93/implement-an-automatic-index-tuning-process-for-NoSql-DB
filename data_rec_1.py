from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["movielens_dataset"]
collection = db["movies"]

# Read movies data from file
with open("movies.dat", "r") as file:
    movies_data = file.readlines()

# Read users data from file
with open("tags.dat", "r") as file:
    tag_data = file.readlines()

# Read ratings data from file
with open("ratings.dat", "r") as file:
    ratings_data = file.readlines()

# Process and insert data into MongoDB collection
for movie_line in movies_data:
    movie_id, title, genres = movie_line.strip().split("::")
    movie_id = int(movie_id)
    genres = genres.split("|")
    document = {
        "movieId": movie_id,
        "title": title,
        "genres": genres,
        "tags": [],
        "ratings": []
    }
    collection.insert_one(document)

#UserID::MovieID::Tag::Timestamp
for user_line in tag_data:
    user_id, movie_id, tag,timestamp = user_line.strip().split("::")
    user_id = int(user_id)
    movie_id = int(movie_id)
    tag = str(tag)
    timestamp = int(timestamp)
    document = {
        "userId": user_id,
        "movieId": movie_id,
        "tag": tag,
        "timestamp": timestamp
    }
    collection.update_one({"movieId": movie_id}, {"$push": {"tags": document}})

for rating_line in ratings_data:
    user_id, movie_id, rating, timestamp = rating_line.strip().split("::")
    user_id = int(user_id)
    movie_id = int(movie_id)
    rating = float(rating)
    timestamp = int(timestamp)
    document = {
        "userId": user_id,
        "rating": rating,
        "timestamp": timestamp
    }
    collection.update_one({"movieId": movie_id}, {"$push": {"ratings": document}})

# Close the MongoDB connection
client.close()