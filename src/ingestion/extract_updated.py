import requests
import os
import json
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

APP_NAME = 'Junamies/FoobarApp 1.0'
vessel_location_url = 'https://meri.digitraffic.fi/api/ais/v1/locations'
sea_state_estimation_url = 'https://meri.digitraffic.fi/api/sse/v1/measurements'
port_url = 'https://meri.digitraffic.fi/api/port-call/v1/port-calls'

# Define bounding box (Oulu, Mariehamn, Kotka, Tallinn)
bounding_box = {
    'min_lat': 59.4,  
    'max_lat': 65.0,  
    'min_lon': 19.9,  
    'max_lon': 27.0   
}

# Set up retry strategy for requests
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"]  # Use method_whitelist as it's widely supported
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)

# Set up data directory
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

def is_within_bounding_box(lat, lon):
    """Check if a vessel's coordinates are within the defined bounding box."""
    return (bounding_box['min_lat'] <= lat <= bounding_box['max_lat']) and \
           (bounding_box['min_lon'] <= lon <= bounding_box['max_lon'])

def append_json_data(file_path, new_data):
    """Append new data to an existing JSON file, ensuring it stays in list format and prints row count."""
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as json_file:
                existing_data = json.load(json_file)
        except json.JSONDecodeError:
            existing_data = []
    else:
        existing_data = []

    if isinstance(existing_data, list):
        existing_data.append(new_data)
    else:
        existing_data = [existing_data, new_data]

    with open(file_path, 'w') as json_file:
        json.dump(existing_data, json_file, indent=4)

    # Print the number of rows (entries in the JSON file)
    row_count = len(existing_data)
    logging.info(f"Updated {file_path}: {row_count} rows")

def get_vessel_location():
    """Fetch vessel location data within the bounding box and append it to a file if new MMSI."""
    try:
        response = requests.get(vessel_location_url, timeout=10)
        response.raise_for_status()  # Raises an error for bad responses (4xx, 5xx)
        
        data = response.json()

        # Log response structure for debugging
        logging.debug(f"API response: {json.dumps(data, indent=4)}")

        # Ensure response format is correct
        if isinstance(data, dict):
            vessels_list = data.get('features', [])
        elif isinstance(data, list):
            vessels_list = data
        else:
            logging.error("Unexpected API response format")
            return

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching vessel location data: {e}")
        return
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON response: {e}")
        return

    # Debug: Check if vessels_list is structured correctly
    logging.debug(f"Vessels list: {vessels_list}")

    # Filter vessels based on bounding box
    filtered_vessels = [
        vessel for vessel in vessels_list
        if isinstance(vessel, dict) and 'geometry' in vessel and 
        'coordinates' in vessel['geometry'] and 
        is_within_bounding_box(vessel['geometry']['coordinates'][1], vessel['geometry']['coordinates'][0])
    ]

    if not filtered_vessels:
        logging.warning("No vessels found within the bounding box.")
        return

    # Define file path for storing vessel data
    file_path = os.path.join(DATA_DIR, 'vessel_location_data.json')

    # Read existing data to check for existing MMSIs
    vessel_mmsis = set()
    if os.path.exists(file_path):
        with open(file_path, 'r') as json_file:
            try:
                existing_data = json.load(json_file)
                if not isinstance(existing_data, list):
                    logging.error("Unexpected format in vessel_location_data.json, resetting file.")
                    existing_data = []
                vessel_mmsis = {vessel.get('properties', {}).get('mmsi') for vessel in existing_data if isinstance(vessel, dict)}
            except json.JSONDecodeError:
                logging.error("Failed to decode vessel_location_data.json, resetting file.")
                existing_data = []
    else:
        existing_data = []

    # Filter out vessels that already exist based on MMSI
    new_vessels = [
        vessel for vessel in filtered_vessels 
        if isinstance(vessel, dict) and 'properties' in vessel and 
        vessel['properties'].get('mmsi') not in vessel_mmsis
    ]

    if new_vessels:
        # Append new vessels to the existing data
        existing_data.extend(new_vessels)
        with open(file_path, 'w') as json_file:
            json.dump(existing_data, json_file, indent=4)
        logging.info(f"{len(new_vessels)} new vessel(s) added.")
    else:
        logging.info("No new vessels to add.")

def get_sea_state_estimation():
    """Fetch sea state estimation data and append it to a file."""
    try:
        response = http.get(sea_state_estimation_url, timeout=10)
        if response.status_code == 200 and response.text:
            try:
                data = response.json()
            except json.JSONDecodeError:
                logging.error(f"Error decoding JSON for sea state estimation data: {response.text}")
                return

            append_json_data(os.path.join(DATA_DIR, 'sea_state_estimation_data.json'), data)
            logging.info("Sea state estimation data updated.")
        else:
            logging.error(f"Failed to fetch sea state estimation data. Status code: {response.status_code}")
    except Exception as e:
        logging.error(f"Error fetching sea state estimation data: {e}")

def get_port_call():
    """Fetch port call data and append it to a file."""
    try:
        response = http.get(port_url, timeout=10)
        if response.status_code == 200 and response.text:
            try:
                data = response.json()
                append_json_data(os.path.join(DATA_DIR, 'port_call_data.json'), data)
                logging.info("Port call data updated.")
            except json.JSONDecodeError:
                logging.error(f"Error decoding JSON: {response.text}")
        else:
            logging.error(f"Failed to fetch port call data. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching port data: {e}")
