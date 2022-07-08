# --- IMPORTING LIBRARIES ---
import pymongo  # Pymongo library for accessing NoSQL/MongoDB database(s)
import requests  # Requests library for HTTP requests to APIs


# --- DEFINING FUNCTIONS ---
def destroy_collection(collection_name: str, database_name: str):   # Deletes a collection from a MongoDB database. Useful for testing insertions and collection creation functions
    client = pymongo.MongoClient()  # Initialise Pymongo client
    db = client[database_name]  # Connect to database with the supplied name
    try:
        exec(f"db.{collection_name}.drop()")  # Attempt to delete the specified collection within the specified database
    except:
        return print("Database could not be destroyed")


def insert_pilots(ship_collection: list, database_name: str):   # Inserts a character ObjectID from the character_collection into a ships 'pilots' array where pilot name matches a character name, replacing the pilots URL
    client = pymongo.MongoClient()  # Initialise Pymongo client
    db = client[database_name]  # Connect to database with the supplied name
    for ship in ship_collection:  # Loop through each ship in the ship collection
        if ship['pilots']:  # Check if there are any pilots listed on this ship. If there are then find their details.
            for pilot_url in ship['pilots']:    # Loop through each pilot referenced in the ship document
                pilot_details = requests.request('GET', pilot_url).json()   # Find the details of the current pilot using the Endpoint URL referenced in the 'pilots' list
                pilot_id = db.characters.find_one({'name': pilot_details['name']}, {'_id': 1})  # Find the ObjectID of the character in the 'characters' table with a matching name to the current pilot
                ship['pilots'][ship['pilots'].index(pilot_url)] = pilot_id['_id']   # Replace the pilot URL in 'pilots' array with their character ObjectID from 'characters'


def insert_into_collection(doc, collection_name: str, database_name: str):
    client = pymongo.MongoClient()  # Initialise Pymongo client
    db = client[database_name]  # Connect to database with the supplied name
    try:
        exec(f"db.{collection_name}.insert_one(doc)")  # Attempt to add the doc to the specified collection in the specified database
        print(f"Document inserted into '{collection_name}' successfully")
    except:
        print("Document could not be inserted")


def create_collection_from_docs(doc_collection: list, collection_name: str, database_name: str): # Takes a collection of JSON files, table name, and a database name to create a new table in the specified database
    client = pymongo.MongoClient()  # Initialise Pymongo client
    db = client[database_name]  # Connect to database with the supplied name
    for doc in doc_collection:  # Loop through each document in the collection
        insert_into_collection(doc, collection_name, database_name) # Add the doc to the new collection in the supplied database. If collection doesn't exist it is created here
    return