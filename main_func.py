from pymongo import MongoClient
import queries
from queries import *

CONNECTION_STRING = "mongodb://localhost:27017"
DATABASE = 'movielens_dataset'
COLLECTION = 'movies'
NUMBER_OF_INDEX_CANDIDATES = 2
INDEX_CHANGE_THRESHOLD = 0
filter = {
    "$or": [
        {"op": "command"},
        {"op": "query"}
        ],
    "millis": {"$gt": 0}

}
conn = MongoClient(CONNECTION_STRING)

database = conn.get_database(DATABASE)
collection = getattr(database, COLLECTION)
COLLECTION_SYS = database.system.profile
database.command('profile', 1, filter)
document = collection.find_one()
fields_of_collection = list(document.keys())
INDEX_POOL = (len(fields_of_collection)) - 2

collection_list = database.list_collections()
indexes_list = collection.list_indexes()
total_number_indexes = sum(1 for _ in indexes_list)

def read_logs(COLLECTION_SYS,filter):
    list_slow_queries = COLLECTION_SYS.find(filter)
    potential_ind_dict_list = []
    indexes_list = collection.list_indexes()
    index_list_object = [index['name'].rsplit('_', 1)[0] for index in indexes_list]
    for query in list_slow_queries:
        if query['op'] == 'query':
            index = query['command']['filter']
            potential_index = list(index)
        elif (query['op'] == "command") and 'pipeline' in query['command'] and '$match' in query['command']['pipeline'][0]:
            index = query['command']['pipeline'][0]['$match']
            potential_index = list(index)
        else:
            continue
        if 'errMsg' not in query:
            query_plan = query['planSummary']
            potential_index = list(set(potential_index))
            if potential_index is not None:
                if (query_plan == 'COLLSCAN'):
                        for i in potential_index:
                            if (i in fields_of_collection) and (i not in index_list_object):
                                potential_ind_dict = {'index': i, 'millis': query['millis']}
                                potential_ind_dict_list.append(potential_ind_dict)
    return potential_ind_dict_list

def sort_potentialIndex(potential_ind_dict_list,NUMBER_OF_INDEX_CANDIDATES):
    # sorting the index list by the time taken to execute the query in descending order
    sorted_potential_ind_dict_list = sorted(potential_ind_dict_list, key=lambda k: k['millis'], reverse=True)
    top_popular_index_millis_list = sorted_potential_ind_dict_list[:NUMBER_OF_INDEX_CANDIDATES]
    # get the filter values or index values from the dict
    index_list = [index['index'] for index in top_popular_index_millis_list]
    final_index_list = list(index_list)
    return final_index_list, top_popular_index_millis_list

def create_indexes(final_index_list,total_number_indexes):
    if total_number_indexes < INDEX_POOL:
        f_index_list = final_index_list.copy()
        for element in f_index_list:
            collection.create_index([(element, 1)])
            final_index_list.remove(element)
            indexes_list = collection.list_indexes()
            total_number_indexes = sum(1 for _ in indexes_list)
            if total_number_indexes == INDEX_POOL:
                break

    indexes_list_new = collection.list_indexes()
    total_number_indexes_new = sum(1 for _ in indexes_list_new)

    if total_number_indexes_new >= INDEX_POOL and len(final_index_list) != 0:
        # calculate profit for all the existing indexes
        pipeline = [
            {"$indexStats": {}}
        ]
        result = list(collection.aggregate(pipeline))
        opcount_dict = {}
        for index in result:
            index_name = index['name'].rsplit('_', 1)[0]
            num_operations = index['accesses']['ops']
            opcount_dict[index_name] = num_operations

        indexes_list = collection.list_indexes()
        indexes = [index['name'].rsplit('_', 1)[0] for index in indexes_list]
        final_profit_list = {}

        for index in indexes:
            if (index == 'movieId'):
                profit_movieId = queries.query_movieId()
                total_profit = float(profit_movieId) + float(opcount_dict['movieId'])
                final_profit_list['movieId'] = total_profit
            elif (index == 'title'):
                profit_title = queries.query_title()
                total_profit = float(profit_title) + float(opcount_dict['title'])
                final_profit_list['title'] = total_profit
            elif (index == 'genres'):
                profit_genres = queries.query_genres()
                total_profit = float(profit_genres) + float(opcount_dict['genres'])
                final_profit_list['genres'] = total_profit
            elif (index == 'ratings') or (index == 'ratings.rating'):
                profit_ratings = queries.query_ratings()
                total_profit = float(profit_ratings) + float(opcount_dict['ratings'])
                final_profit_list['ratings'] = total_profit
            elif (index == 'tags') or (index == 'tags.tag'):
                profit_tags = queries.query_tags()
                total_profit = float(profit_tags) + float(opcount_dict['tags'])
                final_profit_list['tags'] = total_profit
        print(f"Profit of the existing Indexes:{final_profit_list}")
        for i in range(len(final_index_list)):
            collection.create_index([(final_index_list[i], 1)])
            print(f"potential index created for profit check: {final_index_list[i]}")

        potential_index_profit_list = {}

        for potential_index in final_index_list:
            if (potential_index == 'movieId'):
                profit_movieId = queries.query_movieId()
                potential_index_profit_list['movieId'] = profit_movieId
            elif (potential_index == 'title'):
                profit_title = queries.query_title()
                potential_index_profit_list['title'] = profit_title
            elif (potential_index == 'genres'):
                profit_genres = queries.query_genres()
                potential_index_profit_list['genres'] = profit_genres
            elif (potential_index == 'ratings') or (potential_index == 'ratings.rating'):
                profit_ratings = queries.query_ratings()
                potential_index_profit_list['ratings'] = profit_ratings
            elif (potential_index == 'tags') or (potential_index == 'tags.tag'):
                profit_tags = queries.query_tags()
                potential_index_profit_list['tags'] = profit_tags

        print(f"Potential indexes with profit:{potential_index_profit_list}")
        rest_potential_index_profit_list = list(potential_index_profit_list.keys())
        # take the potential index with the highest profit
        max_profit_potential_index = max(potential_index_profit_list, key=potential_index_profit_list.get)
        max_profit = potential_index_profit_list[max_profit_potential_index]

        sum_profit_old = sum(final_profit_list.values())
        print(f"Old profit: {sum_profit_old}")
        sum_profit_new = sum(final_profit_list.values()) + max_profit
        print(f"New profit: {sum_profit_new}")
        # compare with threshold to check if index change is required
        if ((sum_profit_new - sum_profit_old) > INDEX_CHANGE_THRESHOLD):
            min_profit_index = min(final_profit_list, key=final_profit_list.get)
            collection.drop_index([(min_profit_index, 1)])
            collection.create_index([(max_profit_potential_index, 1)])
        else:
            for i in range(len(final_index_list)):
                collection.drop_index([(final_index_list[i], 1)])
                print(f"potential index removed: {final_index_list[i]}")

        # removing the potential index not selected

        for element in rest_potential_index_profit_list:
            if element != max_profit_potential_index:
                collection.drop_index([(element, 1)])




potential_ind_dict_list = read_logs(COLLECTION_SYS,filter)
print(f"potential index list with query timings {potential_ind_dict_list}")

#Get the top 2 popular index candidate in the list
final_index_list, top_popular_index_millis_list = sort_potentialIndex(potential_ind_dict_list,NUMBER_OF_INDEX_CANDIDATES)
print(f"final_index_list {final_index_list}")
print(f"Top 2 index candidates based on based on urgency {top_popular_index_millis_list}")

create_indexes(final_index_list,total_number_indexes)

