from pymongo import MongoClient
import random
import time

CONNECTION_STRING = "mongodb://localhost:27017"
conn = MongoClient(CONNECTION_STRING)
db = conn["movielens_dataset"]
collection = db["movies"]
def query_movieId():
    movieid_list = [i for i in range(1, 10681)]
    random.shuffle(movieid_list)
    random_movieid = random.choice(movieid_list)
    #get time of running query without the index
    query_without_index = {"movieId": random_movieid}
    # Use $natural to disable the use of indexes
    start_time = time.time()
    result_without_index = collection.find(query_without_index).hint([("$natural", 1)])
    execution_time_without_index = time.time() - start_time
    random_movieid = random_movieid + 2
    query_with_index = {"movieId": random_movieid}
    start_time_index = time.time()
    result_with_index = collection.find(query_with_index).hint("movieId")
    execution_time_with_index = time.time() - start_time_index
    profit = abs(execution_time_without_index - execution_time_with_index)
    return profit

def query_genres():
    unique_genres = db.movies.distinct("genres")
    random.shuffle(unique_genres)
    random_genres = random.choice(unique_genres)
    #get time of running query without the index
    query_without_index = {"genres": random_genres}
    start_time = time.time()
    result_without_index = collection.find(query_without_index).hint([("$natural", 1)])
    execution_time_without_index = time.time() - start_time
    random_genres = random.choice(unique_genres)
    query_with_index = {"genres": random_genres}
    start_time_index = time.time()
    result_with_index = collection.find(query_with_index).hint("genres")
    execution_time_with_index = time.time() - start_time_index
    profit = abs(execution_time_without_index - execution_time_with_index)
    return profit

def query_ratings():
    unique_ratings = db.movies.distinct("ratings.rating")
    random.shuffle(unique_ratings)
    random_rating = random.choice(unique_ratings)
    #get time of running query without the index
    query_without_index = {"ratings": {"$elemMatch": {"rating": {"$gt": random_rating}}}}
    # Use $natural to disable the use of indexes
    start_time = time.time()
    result_without_index = collection.find(query_without_index).hint([("$natural", 1)])
    execution_time_without_index = time.time() - start_time
    random_genres = random.choice(unique_ratings)
    query_with_index = {"ratings": {"$elemMatch": {"rating": {"$gt": random_rating}}}}
    start_time_index = time.time()
    result_with_index = collection.find(query_with_index).hint("ratings.rating")
    execution_time_with_index = time.time() - start_time_index
    profit = abs(execution_time_without_index - execution_time_with_index)
    return profit

def query_tags():
    unique_ratings = db.movies.distinct("tags.tag")
    unique_ratings_count = len(unique_ratings)
    tag_numbers = [i for i in range(1, unique_ratings_count)]
    random.shuffle(tag_numbers)
    random_tag_number = random.choice(tag_numbers)
    #get time of running query without the index
    query_without_index = {"tags": {"$elemMatch": {"tag": {"$eq": unique_ratings[random_tag_number]}}}}
    # Use $natural to disable the use of indexes
    start_time = time.time()
    result_without_index = collection.find(query_without_index).hint([("$natural", 1)])
    execution_time_without_index = time.time() - start_time
    random.shuffle(tag_numbers)
    random_tag_number = random.choice(tag_numbers)
    #get time of running query without the index
    query_with_index = {"tags.tag": unique_ratings[random_tag_number]}
    start_time_index = time.time()
    result_with_index = collection.find(query_with_index).hint("tags.tag")
    execution_time_with_index = time.time() - start_time_index
    profit = abs(execution_time_without_index - execution_time_with_index)
    return profit

def query_title():
    movieid_list = [i for i in range(1, 10681)]
    random.shuffle(movieid_list)
    random_movieid = random.choice(movieid_list)
    unique_titles = db.movies.distinct("title")
    find_title = unique_titles[random_movieid]
    #get time of running query without the index
    query_without_index = {"title":find_title }
    # Use $natural to disable the use of indexes
    start_time = time.time()
    result_without_index = collection.find(query_without_index).hint([("$natural", 1)])
    execution_time_without_index = time.time() - start_time
    random_movieid = random.choice(movieid_list)
    query_with_index = {"title": unique_titles[random_movieid]}
    start_time_index = time.time()
    result_with_index = collection.find(query_with_index).hint("title")
    execution_time_with_index = time.time() - start_time_index
    profit = abs(execution_time_without_index - execution_time_with_index)
    return profit



