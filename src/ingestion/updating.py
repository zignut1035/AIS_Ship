import time
import logging
from extract_updated import get_vessel_location, get_sea_state_estimation, get_port_call

# Set up logging
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # Set up logging
    setup_logging()

    # Update the data immediately before starting the loop
    logging.info("Fetching initial vessel data...")
    get_vessel_location()
    
    logging.info("Fetching initial sea state estimation data...")
    get_sea_state_estimation()

    logging.info("Fetching initial port call data...")
    get_port_call()

    last_vessel_update = time.time()
    last_sea_state_update = time.time()
    last_port_call_update = time.time()

    VESSEL_UPDATE_INTERVAL = 10  # seconds
    SEA_STATE_UPDATE_INTERVAL = 600  # seconds
    PORT_CALL_UPDATE_INTERVAL = 900  # seconds

    while True:
        current_time = time.time()

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

        time.sleep(1)  # Sleep for 1 second before checking the time again

if __name__ == "__main__":
    main()