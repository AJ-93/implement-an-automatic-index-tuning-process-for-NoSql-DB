import pymongo
import time

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["index_impact_db"]
collection = db["index_impact_collection"]

filter = {"op": "insert", "millis": {"$gt": 0}}
db.command('profile', 1, filter)

# Insert documents without index
operation_start_time = time.time()
for i in range(1000000):
    collection.insert_one({'column1': f'field{i+20000000}', 'column2': i+70, 'column3': True, 'column4': i * 3.860, 'column5': f'2023-01-0{i % 30 + 1}', 'column6': ['A', 'D','E','B','G','H','C','F'] })
    print(f'records inserted {i}')
operation_end_time = time.time()
total_time = operation_end_time - operation_start_time
f =open("time_rec.txt","a")
f.write("\nTime taken to insert 1 million documents with 12 indexes:" + str(total_time)+ "seconds\n")
f.close()

for i in range(1000000):
    collection.delete_one({'column2': i+70})
    print(f'records deleted {i}')


# # Create index
# collection.create_index("field1")
#
# # Insert documents with index
# start_time = time.time()
# for i in range(1000000, 2000000):
#     collection.insert_one({"field1": i, "field2": "test"})
# end_time = time.time()
# print("Time taken to insert 1 million documents with index: ", end_time - start_time, "seconds")