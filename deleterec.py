from pymongo import MongoClient
import requests
from requests.auth import HTTPDigestAuth

conn = MongoClient("mongodb://localhost:27017")

database = conn.get_database('index_test_db')
collection = database["index_test_collection"]
for i in range(1000000):
    collection.delete_one({'column1': f'field{i + 6000001}'})
    print(f'records deleted {i}')
#
# database.metadata.delete_one(
#   { "title": "Seven Samurai" }
# )
#
# database.metadata.delete_one(
#   { "title": "Se7en" }
# )
# database.metadata.delete_one(
#   { "title": "One Flew Over the Cuckoo's Nest" }
# )
# database.metadata.delete_one(
#   { "title": "Goodfellas" }
# )
# database.metadata.delete_one(
#   { "title": "The Matrix" }
# )
# database.metadata.delete_one(
#   { "title": "Star Wars: Episode V - The Empire Strikes Back" }
# )
# database.metadata.delete_one(
#   { "title": "Seven Samurai" }
# )
# database.metadata.delete_one(
#   { "title": "The Lord of the Rings: The Two Towers" }
# )
# database.metadata.delete_one(
#   { "title": "Inception" }
# )
# database.metadata.delete_one(
#   { "title": "Fight Club" }
# )
# database.metadata.delete_one(
#   { "title": "Forrest Gump" }
# )
# database.metadata.delete_one(
#   { "title": "The Good the Bad and the Ugly" }
# )
# database.metadata.delete_one(
#   { "title": "The Lord of the Rings: The Fellowship of the Ring"}
# )
# database.metadata.delete_one(
#   { "title": "Pulp Fiction" }
# )
# database.metadata.delete_one(
#   { "title": "The Lord of the Rings: The Return of the King" }
# )
# database.metadata.delete_one(
#   { "title": "Schindler's List" }
# )
# database.metadata.delete_one(
#   { "title": "12 Angry Men" }
# )
# database.metadata.delete_one(
#   { "title": "The Godfather: Part II" }
# )
# database.metadata.delete_one(
#   { "title": "The Dark Knight" }
# )
# database.metadata.delete_one(
#   { "title": "The Godfather" }
# )
# database.metadata.delete_one(
#   { "title": "The Shawshank Redemption" }
# )
# database.metadata.delete_one(
# {"title":"It's a Wonderful Life"})
# database.metadata.delete_one(
# {"title":"The Silence of the Lambs"})
# database.metadata.delete_one(
# {"title":"Saving Private Ryan"})
# database.metadata.delete_one(
# {"title":"City of God"})
# database.metadata.delete_one(
# {"title":"Life Is Beautiful"})
# database.metadata.delete_one(
# {"title":"The Green Mile"})
# database.metadata.delete_one(
# {"title":"Star Wars"})
# database.metadata.delete_one(
# {"title":"Interstellar"})
# database.metadata.delete_one(
# {"title":"Terminator 2: Judgment Day"})
# database.metadata.delete_one(
# {"title":"Back to the Future"})
# database.metadata.delete_one(
# {"title":"Spirited Away"})
# database.metadata.delete_one(
# {"title":"Psycho"})
# database.metadata.delete_one(
# {"title":"The Pianist"})
# database.metadata.delete_one(
# {"title":"Leon: The Professional"})
# database.metadata.delete_one(
# {"title":"Parasite"})
# database.metadata.delete_one(
# {"title":"The Lion King"})
# database.metadata.delete_one(
# {"title":"Gladiator"})
# database.metadata.delete_one(
# {"title":"American History X"})
# database.metadata.delete_one(
# {"title":"The Usual Suspects"})
# database.metadata.delete_one(
# {"title":"The Departed"})
# database.metadata.delete_one(
# {"title":"The Prestige"})
# database.metadata.delete_one(
# {"title":"Casablanca"})
# database.metadata.delete_one(
# {"title":"Whiplash"})
# database.metadata.delete_one(
# {"title":"The Intouchables"})
# database.metadata.delete_one(
# {"title":"Modern Times"})
# database.metadata.delete_one(
# {"title":"Once Upon a Time in the West"})
# database.metadata.delete_one(
# {"title":"Hara-Kiri"})
# database.metadata.delete_one(
# {"title":"Grave of the Fireflies"})
# database.metadata.delete_one(
# {"title":"Alien"})
# database.metadata.delete_one(
# {"title":"Rear Window"})
# database.metadata.delete_one(
# {"title":"City Lights"})
# database.metadata.delete_one(
# {"title":"Memento"})
# database.metadata.delete_one(
# {"title":"Cinema Paradiso"})
# database.metadata.delete_one(
# {"title":"Apocalypse Now"})
# database.metadata.delete_one(
# {"title":"Indiana Jones and the Raiders of the Lost Ark"})
# database.metadata.delete_one(
# {"title":"Django Unchained"})
# database.metadata.delete_one(
# {"title":"WALL-E"})
# database.metadata.delete_one(
# {"title":"The Lives of Others"})
# database.metadata.delete_one(
# {"title":"Sunset Blvd."})
# database.metadata.delete_one(
# {"title":"The Shining"})
# database.metadata.delete_one(
# {"title":"Paths of Glory"})
# database.metadata.delete_one(
# {"title":"The Great Dictator"})
# database.metadata.delete_one(
# {"title":"Avengers: Infinity War"})
# database.metadata.delete_one(
# {"title":"Witness for the Prosecution"})
# database.metadata.delete_one(
# {"title":"Aliens"})
# database.metadata.delete_one(
# {"title":"American Beauty"})
# database.metadata.delete_one(
# {"title":"The Dark Knight Rises"})
# database.metadata.delete_one(
# {"title":"Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb"})
# database.metadata.delete_one(
# {"title":"Spider-Man: Into the Spider-Verse"})
# database.metadata.delete_one(
# {"title":"Joker"})
# database.metadata.delete_one(
# {"title":"Old Boy"})
# database.metadata.delete_one(
# {"title":"Braveheart"})
# database.metadata.delete_one(
# {"title":"Toy Story"})
# database.metadata.delete_one(
# {"title":"Amadeus"})
# database.metadata.delete_one(
# {"title":"Coco"})
# database.metadata.delete_one(
# {"title":"Spider-Man: No Way Home"})
# database.metadata.delete_one(
# {"title":"Inglourious Basterds"})
# database.metadata.delete_one(
# {"title":"The Boat"})
# database.metadata.delete_one(
# {"title":"Avengers: Endgame"})
# database.metadata.delete_one(
# {"title":"Princess Mononoke"})
# database.metadata.delete_one(
# {"title":"Once Upon a Time in America"})
# database.metadata.delete_one(
# {"title":"Good Will Hunting"})
# database.metadata.delete_one(
# {"title":"Toy Story 3"})
# database.metadata.delete_one(
# {"title":"Requiem for a Dream"})
# database.metadata.delete_one(
# {"title":"3 Idiots"})
# database.metadata.delete_one(
# {"title":"Your Name."})
# database.metadata.delete_one(
# {"title":"Singin' in the Rain"})
# database.metadata.delete_one(
# {"title":"Star Wars: Episode VI - Return of the Jedi"})
# database.metadata.delete_one(
# {"title":"Reservoir Dogs"})
# database.metadata.delete_one(
# {"title":"Eternal Sunshine of the Spotless Mind"})
# database.metadata.delete_one(
# {"title":"2001: A Space Odyssey"})
# database.metadata.delete_one(
# {"title":"High and Low"})
# database.metadata.delete_one(
# {"title":"Citizen Kane"})
# database.metadata.delete_one(
# {"title":"Lawrence of Arabia"})
# database.metadata.delete_one(
# {"title":"Capernaum"})
# database.metadata.delete_one(
# {"title":"M"})
# database.metadata.delete_one(
# {"title":"North by Northwest"})
# database.metadata.delete_one(
# {"title":"The Hunt"})
# database.metadata.delete_one(
# {"title":"Vertigo"})
# database.metadata.delete_one(
# {"title":"Amlie"})
#
#
# print("records deleted")