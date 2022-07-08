# --- IMPORTING LIBRARIES ---
import requests  # Requests library for HTTP requests to APIs

# --- DEFINING FUNCTIONS ---
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