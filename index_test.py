import pymongo
import time

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["index_test_db"]
collection = db["index_test_collection"]

filter = {"op": "insert", "millis": {"$gt": 0}}
db.command('profile', 1, filter)

# Insert documents without index
operation_start_time = time.time()
for i in range(1000000):
    collection.insert_one({'column1': f'field{i + 3000001}', 'column2': i+15, 'column3': False, 'column4': i * 0.08, 'column5': f'2022-09-0{i % 30 + 1}', 'column6': ['A', 'B', 'C', 'D','E','D'] })
    print(f'records inserted {i}')
operation_end_time = time.time()
print("Time taken to insert 1 million documents with 4 index: ", operation_end_time - operation_start_time, "seconds")

# # Create index
# collection.create_index("field1")
#
# # Insert documents with index
# start_time = time.time()
# for i in range(1000000, 2000000):
#     collection.insert_one({"field1": i, "field2": "test"})
# end_time = time.time()
# print("Time taken to insert 1 million documents with index: ", end_time - start_time, "seconds")