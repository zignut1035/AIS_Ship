import requests
from datetime import datetime
import uuid

APP_NAME = 'Junamies/FoobarApp 1.0'
vessel_location_url = 'https://meri.digitraffic.fi/api/ais/v1/locations'
sea_state_estimation_url = 'https://meri.digitraffic.fi/api/sse/v1/measurements'
dir_way_url = 'https://meri.digitraffic.fi/api/winter-navigation/v1/dirways'

# Convert timestamp to readable datetime
def convert_timestamp(timestamp_ms):
    return datetime.utcfromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

# Function to process each vessel
def process_vessel_data(vessel_data):
    try:
        mmsi = vessel_data['mmsi']
        coordinates = vessel_data['geometry']['coordinates']
        sog = vessel_data['properties']['sog']
        cog = vessel_data['properties']['cog']
        heading = vessel_data['properties']['heading']
        timestamp_external = vessel_data['properties']['timestampExternal']

        # Convert timestamp to readable format
        timestamp = convert_timestamp(timestamp_external)

        # Print extracted data
        print(f"Vessel MMSI: {mmsi}")
        print(f"Coordinates: {coordinates[0]}, {coordinates[1]}")
        print(f"Speed Over Ground: {sog} knots")
        print(f"Course Over Ground: {cog}°")
        print(f"Heading: {heading}°")
        print(f"Timestamp: {timestamp}")
        print("-" * 50)
    except KeyError as e:
        print(f"Error processing vessel data: Missing key {e}")

# Function to fetch vessel location data
def get_vessel_location():
    try:
        response = requests.get(vessel_location_url, timeout=10)
        if response.status_code == 200 and response.text:
            data = response.json()  # Parse JSON data
            for vessel in data:
                process_vessel_data(vessel)
        else:
            print(f"Failed to fetch vessel location data. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error fetching vessel location data: {e}")

# Function to fetch sea state estimation data
def get_sea_state_estimation():
    try:
        response = requests.get(sea_state_estimation_url, timeout=10)
        if response.status_code == 200 and response.text:
            data = response.json()  # Parse JSON data
            print('Sea State Estimation Data:', data)
        else:
            print(f"Failed to fetch sea state estimation data. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error fetching sea state estimation data: {e}")

# Function to fetch direction way data
def get_direction_way():
    try:
        response = requests.get(dir_way_url, timeout=10)
        if response.status_code == 200 and response.text:
            data = response.json()  # Parse JSON data
            print('Direction Way Data:', data)
        else:
            print(f"Failed to fetch direction way data. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error fetching direction way data: {e}")

def main():
    # Fetch all required data
    print(f"Fetching data for {APP_NAME} ({str(uuid.uuid4())})")

    get_vessel_location()
    get_sea_state_estimation()
    get_direction_way()

# Run the main function
if __name__ == "__main__":
    main()
