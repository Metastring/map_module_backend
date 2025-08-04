import requests

# GeoServer credentials and URL
GEOSERVER_URL = "http://localhost:8080/geoserver/rest"
USERNAME = "admin"
PASSWORD = "geoserver"

# Function to fetch all workspaces
def get_all_workspaces():
    """
    Fetch the list of all workspaces from GeoServer.
    """
    url = f"{GEOSERVER_URL}/workspaces"
    response = requests.get(url, auth=(USERNAME, PASSWORD), headers={"Accept": "application/json"})
    
    if response.status_code == 200:
        print("Workspaces fetched successfully!")
        print(response.json())  # Print the JSON response
    else:
        print(f"Failed to fetch workspaces. Status code: {response.status_code}")
        print(response.text)  # Print the error message

if __name__ == "__main__":
    # Fetch all workspaces
    get_all_workspaces()