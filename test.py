from pymongo import MongoClient
import random
# Create a MongoDB client
client = MongoClient("mongodb://localhost:27017")

# Connect to the database
db = client["movielens_dataset"]
COLLECTION = 'movies'
# Get the collection
collection = db["movies"]

filter = {
    "$or": [
        {"op": "command"},
        {"op": "query"}
    ]
}
COLLECTION_SYS = db.system.profile

db.command('profile', 1, filter)
list_slow_queries = COLLECTION_SYS.find(filter)
potential_index =[]
potential_index_list = []
potential_ind_dict_list = []
indexes_list = collection.list_indexes()
index_list_object = [index['name'].rsplit('_', 1)[0] for index in indexes_list]
document = collection.find_one()
fields_of_collection = list(document.keys())
NUMBER_OF_INDEX_CANDIDATES =2
for query in list_slow_queries:
    for query in list_slow_queries:
        print(query)
        if query['op'] == 'query':
            index = query['command']['filter']
            potential_index = list(index)
        elif (query['op'] == "command") and 'pipeline' in query['command'] and '$match' in query['command']['pipeline'][0]:  # and ('aggregate' in query['command']) and ('$match' in query['command']['pipeline']):
            index = query['command']['pipeline'][0]['$match']
            print(index)
#             potential_index = list(index)
#         else:
#             continue
#         if 'errMsg' not in query:
#             query_plan = query['planSummary']
#             if potential_index is not None:
#                 if (query_plan == 'COLLSCAN') and (potential_index not in index_list_object):
#                     print(potential_index)
#                     # getting the potential index list along with the time taken by the correcsponding query
#                     if len(potential_index) > 1:
#                         for i in potential_index:
#                             if i in fields_of_collection:
#                                 potential_ind_dict = {'index': i, 'millis': query['millis']}
#                                 potential_ind_dict_list.append(potential_ind_dict)
#                     elif len(potential_index) == 1:
#                             if potential_index in fields_of_collection:
#                                 potential_ind_dict = {'index': index, 'millis': query['millis']}
#                                 potential_ind_dict_list.append(potential_ind_dict)
#
# print(potential_ind_dict_list)
#
# print(potential_ind_dict_list)
# #sorting the index list by the time taken to execute the query in descending order
# sorted_potential_ind_dict_list = sorted(potential_ind_dict_list, key=lambda k:k['millis'], reverse=True)
# #Get the top 3 popular index candidate in the list
# top_popular_index_millis_list = sorted_potential_ind_dict_list[:NUMBER_OF_INDEX_CANDIDATES]
# print(top_popular_index_millis_list)
# #get the filter values or index values from the dict
# index_list = [index['index'] for index in top_popular_index_millis_list]
# final_index_list = list(index_list)
# print(len(final_index_list))

columns = ['a', 'b', 'c', 'd']
original_columns = columns.copy()

for column in original_columns:
    # Create the index
    print(column)
    columns.remove(column)
    if (len(columns)<2):
        break

print(columns)