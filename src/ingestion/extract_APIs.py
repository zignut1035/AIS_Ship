import requests
import uuid
import os
import json

APP_NAME = 'Junamies/FoobarApp 1.0'
vessel_location_url = 'https://meri.digitraffic.fi/api/ais/v1/locations'
sea_state_estimation_url = 'https://meri.digitraffic.fi/api/sse/v1/measurements'
port_url = 'https://meri.digitraffic.fi/api/port-call/v1/port-calls'

os.makedirs('data', exist_ok=True)

# Define your bounding box (for Oulu to the North, Mariehamn to the West, Kotka to the East, Tallinn to the South)
bounding_box = {
    'min_lat': 59.4,  # Southernmost point (Tallinn)
    'max_lat': 65.0,  # Northernmost point (Oulu)
    'min_lon': 19.9,  # Westernmost point (Mariehamn)
    'max_lon': 27.0   # Easternmost point (Kotka)
}

# Function to check if the vessel's coordinates are within the bounding box
def is_within_bounding_box(lat, lon):
    return (bounding_box['min_lat'] <= lat <= bounding_box['max_lat']) and (bounding_box['min_lon'] <= lon <= bounding_box['max_lon'])



# Function to fetch vessel location data within the bounding box
def get_vessel_location():
    try:
        response = requests.get(vessel_location_url, timeout=10)
        if response.status_code == 200 and response.text:
            data = response.json()  # Parse JSON data

            # Filter vessels based on the bounding box
            filtered_vessels = []
            for vessel in data.get('features', []):
                coordinates = vessel['geometry']['coordinates']
                lat = coordinates[1]
                lon = coordinates[0]

                # Check if the vessel is within the bounding box
                if is_within_bounding_box(lat, lon):
                    filtered_vessels.append(vessel)

            # Save the filtered vessel location data to a file
            with open('data/vessel_location_data.json', 'w') as json_file:
                json.dump(filtered_vessels, json_file, indent=4)
            print('Vessel data saved to data/vessel_location_data.json')

        else:
            print(f"Failed to fetch vessel location data. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error fetching vessel location data: {e}")

import requests
import json

def get_sea_state_estimation():
    try:
        response = requests.get(sea_state_estimation_url, timeout=10)
        if response.status_code == 200 and response.text:
            try:
                data = response.json()  # Try parsing the JSON data
            except json.JSONDecodeError:
                print(f"Error decoding JSON for sea state estimation data: {response.text}")
                return

            # Directly store the data without filtering
            with open('data/sea_state_estimation_data.json', 'w') as json_file:
                json.dump(data, json_file, indent=4)
            print('State Estimation data saved to data/sea_state_estimation_data.json')

        else:
            print(f"Failed to fetch sea state estimation data. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error fetching sea state estimation data: {e}")


def get_port_call():
    try:
        # Make a GET request to fetch the port data
        response = requests.get(port_url, timeout=10)

        # Check if the request was successful
        if response.status_code == 200 and response.text:
            try:
                # Try parsing the response as JSON
                data = response.json()

                # Save the fetched data to a file
                with open('data/port_call_data.json', 'w') as json_file:
                    json.dump(data, json_file, indent=4)

                print('Port call data saved to data/port_call_data.json')

            except Exception as e:
                print(f"Error decoding JSON: {e}")
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching port data: {e}")

            
def main():
    # Fetch all required data
    print(f"Fetching data for {APP_NAME} ({str(uuid.uuid4())})")

    get_vessel_location()
    get_sea_state_estimation()
    get_port_call()
# Run the main function
if __name__ == "__main__":
    main()