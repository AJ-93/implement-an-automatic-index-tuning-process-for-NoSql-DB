import csv
import json
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['movielens_dataset']
collection = db['movies']


# Function to read a CSV file and return its data as a list of dictionaries
def read_csv(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return list(reader)


# Open the JSON file
with open('metadata.json', 'r') as file:
    # Read each line (JSON object) from the file
    for line in file:
        # Parse the JSON object
        movie = json.loads(line)

        # Insert the movie into the collection
        collection.insert_one(movie)

print("movies database done!!")
# Merge movies data with other CSV files
print("reading ratings.csv file")
ratings_data = read_csv('ratings.csv')
print("reading ratings.csv file done!!")
print("reading tags.csv file")
tags_data = read_csv('tags.csv')
print("reading tags.csv file done!!")

# # Open the JSON file
# with open('reviews.json', 'r') as file:
#     # Read each line (JSON object) from the file
#     for line in file:
#         # Parse the JSON object
#         review = json.loads(line)
#         item_id = review['item_id']
#
#         # Find the matching movie document in the collection
#         movie = collection.find_one({'item_id': item_id})
#
#         # Add the review to the movie document
#         if movie:
#             if 'reviews' not in movie:
#                 movie['reviews'] = []
#             movie['reviews'].append(review)
#
#             # Update the movie document in the collection
#             collection.update_one({'_id': movie['_id']}, {'$set': movie})
# Update each movie document in the collection with corresponding ratings and tags data
print("STart updating the file!!")
for movie in collection.find():
    movie_id = movie['item_id']

    # Find matching ratings data
    ratings = [rating for rating in ratings_data if rating['movieId'] == str(movie_id)]
    movie['ratings'] = ratings

    # Find matching tags data
    tags = [tag for tag in tags_data if tag['movieId'] == str(movie_id)]
    movie['tags'] = tags

    # Update the movie document in the collection
    collection.update_one({'_id': movie['_id']}, {'$set': movie})

print("Data import and merging completed successfully.")