import time
import logging
import os
import argparse
from extract_updated import get_vessel_location, get_sea_state_estimation, get_port_call
from transform_updated import transform_vessel_data, transform_sea_state_data, transform_port_call_data, merge_vessel_port_sea_state_data

# Set up logging
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("maritime_data_pipeline.log"),
            logging.StreamHandler()
        ]
    )

def parse_arguments():
    parser = argparse.ArgumentParser(description='Maritime Data Pipeline')
    parser.add_argument('--extract-only', action='store_true', help='Run only the extraction step')
    parser.add_argument('--transform-only', action='store_true', help='Run only the transformation step')
    parser.add_argument('--one-time', action='store_true', help='Run the pipeline once and exit')
    return parser.parse_args()

def extract_data():
    """Fetch all data types"""
    logging.info("Extracting data...")
    get_vessel_location()
    get_sea_state_estimation()
    get_port_call()
    logging.info("Data extraction complete.")

def transform_data():
    """Transform all data types"""
    logging.info("Transforming data...")
    transform_vessel_data()
    transform_sea_state_data()
    transform_port_call_data()
    merge_vessel_port_sea_state_data()
    logging.info("Data transformation complete.")

def main():
    # Setup
    args = parse_arguments()
    setup_logging()
    os.makedirs("data", exist_ok=True)
    os.makedirs("transformed_data", exist_ok=True)
    
    if args.one_time:
        if not args.transform_only:
            extract_data()
        if not args.extract_only:
            transform_data()
        return
    
    # Initialize timing variables
    last_vessel_update = time.time()
    last_sea_state_update = time.time()
    last_port_call_update = time.time()
    last_transform = time.time()

    # Define update intervals
    VESSEL_UPDATE_INTERVAL = 10  # seconds
    SEA_STATE_UPDATE_INTERVAL = 600  # seconds (10 minutes)
    PORT_CALL_UPDATE_INTERVAL = 900  # seconds (15 minutes)
    TRANSFORM_INTERVAL = 60  # seconds (transform data every minute)

    # Initial data collection
    if not args.transform_only:
        extract_data()
    
    # Initial transformation
    if not args.extract_only:
        transform_data()

    # Main loop
    logging.info("Starting main processing loop...")
    while True:
        current_time = time.time()

        # Extract data based on intervals
        if not args.transform_only:
            # Update vessel location every 10 seconds
            if current_time - last_vessel_update >= VESSEL_UPDATE_INTERVAL:
                logging.info("Fetching vessel location data...")
                get_vessel_location()
                last_vessel_update = current_time

            # Update sea state estimation data every 10 minutes
            if current_time - last_sea_state_update >= SEA_STATE_UPDATE_INTERVAL:
                logging.info("Fetching sea state estimation data...")
                get_sea_state_estimation()
                last_sea_state_update = current_time

            # Update port call data every 15 minutes
            if current_time - last_port_call_update >= PORT_CALL_UPDATE_INTERVAL:
                logging.info("Fetching port call data...")
                get_port_call()
                last_port_call_update = current_time

        # Transform data every minute
        if not args.extract_only and current_time - last_transform >= TRANSFORM_INTERVAL:
            transform_data()
            last_transform = current_time

        time.sleep(1)  # Sleep for 1 second before checking the time again

if __name__ == "__main__":
    main()