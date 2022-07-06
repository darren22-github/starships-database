#  --- TASK INSTRUCTIONS ---
# The data in this database has been pulled from https://swapi.dev/. As well as 'people', the API has data on starships.
# In Python, pull data on all available starships from the API.
# The 'pilots' key contains URLs pointing to the characters who pilot the starship.
# Use these to replace 'pilots' with a list of ObjectIDs from our characters collection, then insert the starships into their own collection.
# Use functions at the very least!

# ---- NOTES ---
# This program pulls starship data from 'Star Wars API' (https://swapi.dev/) and inserts references to characters that pilot each starship as ObjectIDs
# The functions used do not currently allow developers to specify their own tables, due to the nature of the exec() function which does not return values (i.e. will not return records/documents).
#   Because of this, the table containing Star Wars characters must be called 'characters'

# --- IMPORTING LIBRARIES ---
import requests  # Requests library for HTTP requests to APIs
import pymongo  # Pymongo library for accessing NoSQL/MongoDB database(s)

# --- DEFINING FUNCTIONS  ---
def destroy_collection(collection_name: str, database_name: str):   # Deletes a collection from a MongoDB database. Useful for testing insertions and collection creation functions
    client = pymongo.MongoClient()  # Initialise Pymongo client
    db = client[database_name]  # Connect to database with the supplied name
    try:
        exec(f"db.{collection_name}.drop()")  # Attempt to delete the specified collection within the specified database
    except:
        return print(f"Collection: '{collection_name}' could not be destroyed")

def gather_from_api(url: str):  # Takes an API Endpoint URL and returns a collection of the results as a list of dictionaries/JSON files
    results = requests.request('GET', url).json()['results']    # HTTP GET Request to supplied URL. HTTP Response parsed to JSON and stored as 'results'
    return results  # Return list of results

def collect_ships_from_swapi(): # Uses collect_from_api() to iterate through starships pages on swapi
    url = 'https://swapi.dev/api/starships/?page=1' # Starting page of SWAPI starships pages
    results = []    # Empty list to store the results of each GET request to SWAPI starships pages. This will store all
    i = 1   # Dummy variable to count the number of iterations / the page number
    while requests.request('GET', url).status_code == 200:  # While the HTTP response returns 200 ('OK') execute the for-loop body
        for ship in gather_from_api(url):   # Add the results of the GET request to the list of results to be returned by this function
            results.append(ship)    # This was completed using a for loop to avoid storing lists within lists. This method stores the ship document itself
        i += 1  # Increment the iteration counter / page number
        url = f'https://swapi.dev/api/starships/?page={i}'  # Update the URL to point to the next page ('page= {page number} +1')
    return results

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
        print(f"Document inserted into {collection_name} successfully")
    except:
        print("Document could not be inserted")

def create_collection_from_docs(doc_collection: list, collection_name: str, database_name: str): # Takes a collection of JSON files, table name, and a database name to create a new table in the specified database
    client = pymongo.MongoClient()  # Initialise Pymongo client
    db = client[database_name]  # Connect to database with the supplied name
    for doc in doc_collection:  # Loop through each document in the collection
        insert_into_collection(doc, collection_name, database_name) # Add the doc to the new collection in the supplied database. If collection doesn't exist it is created here
    return


# --- Main ---
if __name__ == '__main__':  # If in __main__ then call functions
    collection = 'starships'
    database = 'starwars'
    print("Gathering data from API...")
    starships = collect_ships_from_swapi()

    destroy_collection(collection, database) # Destroy any previous attempts
    insert_pilots(starships, database)

    print(f"Inserting documents to collection: '{collection}' in database: '{database}' ...")
    create_collection_from_docs(starships, collection, database)
