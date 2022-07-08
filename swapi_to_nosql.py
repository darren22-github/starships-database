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


from pymongo_functions import *
from sw_api_functions import *

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
