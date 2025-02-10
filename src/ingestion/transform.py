import json
import os
import pandas as pd
import logging
from extract_APIs import get_vessel_location, get_sea_state_estimation, get_port_call

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define paths
EXTRACTED_DATA_PATH = "data/"
TRANSFORMED_DATA_PATH = "transformed_data/"

# Ensure output directory exists
os.makedirs(TRANSFORMED_DATA_PATH, exist_ok=True)

def load_json(file_name):
    """ Load JSON file and handle errors. """
    try:
        with open(os.path.join(EXTRACTED_DATA_PATH, file_name), "r") as file:
            return json.load(file)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logging.error(f"Error loading {file_name}: {e}")
        return None

def transform_vessel_data():
    """ Transform vessel location data. """
    get_vessel_location()  # Fetch latest data
    data = load_json("vessel_location_data.json")
    if not data:
        return

    vessels = []
    for vessel in data:
        properties = vessel.get("properties", {})
        geometry = vessel.get("geometry", {})

        vessels.append({
            "mmsi": properties.get("mmsi"),
            "speed": properties.get("sog"),
            "course": properties.get("cog"),
            "nav_status": properties.get("navStat"),
            "rot": properties.get("rot"),
            "pos_accuracy": properties.get("posAcc"),
            "raim": properties.get("raim"),
            "heading": properties.get("heading"),
            "timestamp": properties.get("timestamp"),
            "timestamp_external": properties.get("timestampExternal"),
            "latitude": geometry.get("coordinates", [None, None])[1],
            "longitude": geometry.get("coordinates", [None, None])[0]
        })

    df = pd.DataFrame(vessels)
    df.to_csv(os.path.join(TRANSFORMED_DATA_PATH, "vessel_data.csv"), index=False)
    logging.info("Vessel data transformed and saved.")

def transform_sea_state_data():
    """ Transform sea state estimation data. """
    get_sea_state_estimation()  # Fetch latest data
    data = load_json("sea_state_estimation_data.json")
    if not data or "features" not in data:
        logging.error("No valid sea state data found.")
        return

    sea_states = []
    for feature in data["features"]:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})

        sea_states.append({
            "site_number": properties.get("siteNumber"),
            "site_name": properties.get("siteName"),
            "site_type": properties.get("siteType"),
            "last_update": properties.get("lastUpdate"),
            "sea_state": properties.get("seaState"),
            "trend": properties.get("trend"),
            "wind_wave_dir": properties.get("windWaveDir"),
            "confidence": properties.get("confidence"),
            "heel_angle": properties.get("heelAngle"),
            "light_status": properties.get("lightStatus"),
            "temperature": properties.get("temperature"),
            "latitude": geometry.get("coordinates", [None, None])[1],
            "longitude": geometry.get("coordinates", [None, None])[0]
        })

    df = pd.DataFrame(sea_states)
    df.to_csv(os.path.join(TRANSFORMED_DATA_PATH, "sea_state_data.csv"), index=False)
    logging.info("Sea state data transformed and saved.")


def transform_port_call_data():
    """Transform port call data."""
    get_port_call()  # Fetch latest data
    data = load_json("port_call_data.json")
    if not data or "portCalls" not in data:
        logging.error("No valid port call data found.")
        return

    port_calls = []
    for call in data["portCalls"]:
        port_area = call.get("portAreaDetails", [{}])[0]  # Get first port area detail if available
        imo_info = call.get("imoInformation", [{}])[0]  # Get first IMO info if available

        port_calls.append({
            "port_call_id": call.get("portCallId"),
            "port_call_timestamp": call.get("portCallTimestamp"),
            "vessel_name": call.get("vesselName"),
            "vessel_type_code": call.get("vesselTypeCode"),
            "imo": call.get("imoLloyds"),
            "mmsi": call.get("mmsi"),
            "nationality": call.get("nationality"),
            "port_to_visit": call.get("portToVisit"),
            "prev_port": call.get("prevPort"),
            "next_port": call.get("nextPort"),
            "eta": port_area.get("eta"),
            "etd": port_area.get("etd"),
            "ata": port_area.get("ata"),
            "atd": port_area.get("atd"),
            "berth_name": port_area.get("berthName"),
            "port_area_name": port_area.get("portAreaName"),
            "arrival_draught": port_area.get("arrivalDraught"),
            "departure_draught": port_area.get("departureDraught"),
            "crew_count": imo_info.get("numberOfCrew"),
            "passenger_count": imo_info.get("numberOfPassangers")
        })

    df = pd.DataFrame(port_calls)
    df.to_csv(os.path.join(TRANSFORMED_DATA_PATH, "port_call_data.csv"), index=False)
    logging.info("Port call data transformed and saved.")


def merge_vessel_port_sea_state_data():
    """ Merge vessel_data, port_call_data, and sea_state_data based on mmsi. """
    # Load the CSV files
    vessel_df = pd.read_csv(os.path.join(TRANSFORMED_DATA_PATH, "vessel_data.csv"))
    port_call_df = pd.read_csv(os.path.join(TRANSFORMED_DATA_PATH, "port_call_data.csv"))
    sea_state_df = pd.read_csv(os.path.join(TRANSFORMED_DATA_PATH, "sea_state_data.csv"))
    
    # Merge vessel data with port call data using mmsi as the key
    merged_df = vessel_df.merge(port_call_df, on="mmsi", how="left")
    
    # Now, merge the result with sea state data based on latitude and longitude
    merged_df = merged_df.merge(sea_state_df, on=["latitude", "longitude"], how="left")

    # Save the merged data to the data directory as ais_data.csv
    merged_df.to_csv(os.path.join(EXTRACTED_DATA_PATH, "ais_data.csv"), index=False)
    logging.info("Vessel, port call, and sea state data merged successfully and saved to data/ais_data.csv.")


# Running the data transformation functions
def main():
    print("Transforming data...")
    transform_vessel_data()
    transform_sea_state_data()
    transform_port_call_data()
    merge_vessel_port_sea_state_data()
    print("Data transformation and merging completed.")

if __name__ == "__main__":
    main()
