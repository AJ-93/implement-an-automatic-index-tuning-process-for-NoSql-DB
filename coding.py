# import json
#
# s =  '{"first_name":"Ajay","city_name":"Magdeburg","id_number":"123"}'
#
# data = json.loads(s)
#
# list_keys = data.keys()
# list_values = data.values()
# final_key_list = []
#
# for key in list_keys:
#     key_split = key.split('_')
#     final_key = key_split[0] + ''.join(x.capitalize() for x in key_split[1:])
#     final_key_list.append(final_key)
#
# convert_to_dict = dict(zip(final_key_list, list_values))
#
# final_json = json.dumps(convert_to_dict)
# print(final_json)
#
my_list = [1, 2, 4]
num = 10

# # Create a dictionary to store the values
# variables = {}
#
# # Assign values to the dictionary keys
# for element in my_list:
#     variables[f"num_{element}"] = num
#
# # Print the values
# for key, value in variables.items():
#     print(key, "=", value)

my_dict = {"a": 20, "b": 5, "c": 15, "d": 7}
my_dict_1 = {"a": 20}
print(max(my_dict,key=my_dict.get))

