import pandas as pd
import os
import logging
from scipy.spatial import cKDTree
import numpy as np
import json
from sqlalchemy import create_engine

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define paths
EXTRACTED_DATA_PATH = "data/"
TRANSFORMED_DATA_PATH = "transformed_data/"

# Ensure output directory exists
os.makedirs(TRANSFORMED_DATA_PATH, exist_ok=True)

# PostgreSQL connection string using SQLAlchemy
DATABASE_URL = "postgresql+psycopg2://natuser:postgres@localhost:5432/ais_db"  # Replace 'yourpassword' with the actual password
engine = create_engine(DATABASE_URL)

def load_json(file_name):
    """ Load JSON file and handle errors. """
    try:
        with open(os.path.join(EXTRACTED_DATA_PATH, file_name), "r") as file:
            return json.load(file)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logging.error(f"Error loading {file_name}: {e}")
        return None

def transform_vessel_data():
    """Transform vessel data into a structured format."""
    data = load_json("vessel_location_data.json")
    
    if not data:
        return
    
    vessel_data = []
    
    for entry in data:
        if isinstance(entry, list) and len(entry) > 0:  
            for feature in entry:
                if isinstance(feature, dict) and "properties" in feature and "geometry" in feature:
                    properties = feature["properties"]
                    geometry = feature["geometry"]
                    
                    mmsi = properties.get("mmsi")
                    sog = properties.get("sog")
                    cog = properties.get("cog")
                    nav_stat = properties.get("navStat")
                    rot = properties.get("rot")
                    pos_acc = properties.get("posAcc")
                    raim = properties.get("raim")
                    heading = properties.get("heading")
                    timestamp = properties.get("timestamp")
                    timestamp_external = properties.get("timestampExternal")
                    
                    coordinates = geometry.get("coordinates", [None, None])
                    latitude = coordinates[1]
                    longitude = coordinates[0]
                    
                    vessel_data.append([mmsi, sog, cog, nav_stat, rot, pos_acc, raim, heading,
                                        timestamp, timestamp_external, latitude, longitude])
        
    df = pd.DataFrame(vessel_data, columns=["mmsi", "sog", "cog", "nav_stat", "rot", "pos_acc", "raim", "heading", "timestamp", "timestamp_external", "latitude", "longitude"])
    
    df.to_csv(os.path.join(TRANSFORMED_DATA_PATH, "vessel_data.csv"), index=False)
    logging.info("Vessel data transformed and saved.")

    # Store the data in PostgreSQL
    df.to_sql('vessel_data', engine, if_exists='replace', index=False)
    logging.info("Vessel data stored in PostgreSQL.")

def transform_sea_state_data():
    """Transform sea state estimation data into a structured format."""
    data = load_json("sea_state_estimation_data.json")
    
    if not data or not isinstance(data, list) or len(data) == 0 or "features" not in data[0]:
        return

    sea_state_data = []
    
    features = data[0]["features"]
    
    for feature in features:
        if isinstance(feature, dict) and "properties" in feature and "geometry" in feature:
            properties = feature["properties"]
            geometry = feature["geometry"]
            
            site_number = properties.get("siteNumber")
            site_name = properties.get("siteName")
            site_type = properties.get("siteType")
            last_update = properties.get("lastUpdate")
            sea_state = properties.get("seaState")
            trend = properties.get("trend")
            wind_wave_dir = properties.get("windWaveDir")
            confidence = properties.get("confidence")
            heel_angle = properties.get("heelAngle")
            light_status = properties.get("lightStatus")
            temperature = properties.get("temperature")
            
            coordinates = geometry.get("coordinates", [None, None])
            latitude = coordinates[1]
            longitude = coordinates[0]

            sea_state_data.append([site_number, site_name, site_type, last_update, sea_state, trend, 
                                   wind_wave_dir, confidence, heel_angle, light_status, temperature,
                                   latitude, longitude])
    
    df = pd.DataFrame(sea_state_data, columns=["site_number", "site_name", "site_type", "last_update", "sea_state", "trend", 
                                               "wind_wave_dir", "confidence", "heel_angle", "light_status", "temperature", 
                                               "latitude", "longitude"])
    
    df.to_csv(os.path.join(TRANSFORMED_DATA_PATH, "sea_state_data.csv"), index=False)
    logging.info("Sea state estimation data transformed and saved.")

    # Store the data in PostgreSQL
    df.to_sql('sea_state_data', engine, if_exists='replace', index=False)
    logging.info("Sea state data stored in PostgreSQL.")

def transform_port_call_data():
    """Transform port call data into a structured format."""
    data = load_json("port_call_data.json")
    
    if not data or not isinstance(data, list) or len(data) == 0 or "portCalls" not in data[0]:
        return

    port_call_data = []
    
    port_calls = data[0]["portCalls"]
    
    for port_call in port_calls:
        if isinstance(port_call, dict):
            port_call_id = port_call.get("portCallId")
            port_call_timestamp = port_call.get("portCallTimestamp")
            customs_reference = port_call.get("customsReference")
            port_to_visit = port_call.get("portToVisit")
            prev_port = port_call.get("prevPort")
            next_port = port_call.get("nextPort")
            domestic_traffic_arrival = port_call.get("domesticTrafficArrival")
            domestic_traffic_departure = port_call.get("domesticTrafficDeparture")
            arrival_with_cargo = port_call.get("arrivalWithCargo")
            discharge = port_call.get("discharge")
            vessel_name = port_call.get("vesselName")
            imo_lloyds = port_call.get("imoLloyds")
            nationality = port_call.get("nationality")
            vessel_type_code = port_call.get("vesselTypeCode")
            mmsi = port_call.get("mmsi") 
            
            agents = port_call.get("agentInfo", [])
            agent_names = [agent["name"] for agent in agents if isinstance(agent, dict)]
            
            port_area_details = port_call.get("portAreaDetails", [])
            for port_area in port_area_details:
                berth_name = port_area.get("berthName")
                port_area_name = port_area.get("portAreaName")
                eta = port_area.get("eta")
                etd = port_area.get("etd")
                ata = port_area.get("ata")
                atd = port_area.get("atd")
                
                port_call_data.append([port_call_id, port_call_timestamp, customs_reference, port_to_visit, prev_port, next_port, 
                                       domestic_traffic_arrival, domestic_traffic_departure, arrival_with_cargo, discharge,
                                       vessel_name, imo_lloyds, nationality, vessel_type_code, mmsi, ", ".join(agent_names),
                                       berth_name, port_area_name, eta, etd, ata, atd])
    
    df = pd.DataFrame(port_call_data, columns=["port_call_id", "port_call_timestamp", "customs_reference", "port_to_visit", "prev_port", "next_port",
                                               "domestic_traffic_arrival", "domestic_traffic_departure", "arrival_with_cargo", "discharge",
                                               "vessel_name", "imo_lloyds", "nationality", "vessel_type_code", "mmsi", "agents",
                                               "berth_name", "port_area_name", "eta", "etd", "ata", "atd"])
        
    df.to_csv(os.path.join(TRANSFORMED_DATA_PATH, "port_call_data.csv"), index=False)
    logging.info("Port call data transformed and saved.")

    # Store the data in PostgreSQL
    df.to_sql('port_call_data', engine, if_exists='replace', index=False)
    logging.info("Port call data stored in PostgreSQL.")

