#  --- TASK INSTRUCTIONS ---
# The data in this database has been pulled from https://swapi.dev/. As well as 'people', the API has data on starships.
# In Python, pull data on all available starships from the API.
# The 'pilots' key contains URLs pointing to the characters who pilot the starship.
# Use these to replace 'pilots' with a list of ObjectIDs from our characters collection, then insert the starships into their own collection. Use functions at the very least!

# --- IMPORTING LIBRARIES ---
import requests  # Requests library for HTML requests to APIs
import pymongo  # Pymongo library for accessing NoSQL/MongoDB database(s)

def collect_from_api(url: str):  # Takes an API URL and returns a collection of the results as a list of dictionaries/JSON files
    collection = requests.request('GET', url).json()['results']
    return collection

def create_starships_table_from_collection(ship_collection: list, database_name: str): # Takes a collection of JSON files and a database name to create a new 'starships' table in the specified database
    client = pymongo.MongoClient()  # Initialise Pymongo client
    db = client[database_name]  # Connect to database with the supplied name
    for ship in ship_collection:  # Loop through each ship in the ship collection
        if ship['pilots']:  # Check if there are any pilots listed on this ship. If there are then find their details.
            for pilot_url in ship['pilots']:    # Loop through each pilot registered to the ship
                pilot_details = requests.request('GET', pilot_url).json()   # Find the details of the current pilot
                pilot_id = db.characters.find_one({'name': pilot_details['name']}, {'_id': 1}) # Find the ObjectID of the character in the 'characters' table with a matching name to the current pilot
                ship['pilots'][ship['pilots'].index(pilot_url)] = pilot_id['_id']   # Replace the pilot URL with their character ObjectID
        db.starships.insert_one(ship)  # Add the ship to the new 'starships' collection of the supplied database
    return

starships = collect_from_api('https://swapi.dev/api/starships')
create_starships_table_from_collection(starships, 'starwars')
