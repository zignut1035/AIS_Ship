import psycopg2
import pandas as pd

# Database connection details
DB_CONFIG = {
    "dbname": "ais_db",
    "user": "natuser",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
}

# SQL Query to merge the three tables
MERGE_QUERY = """
SELECT 
    vd.mmsi, vd.sog, vd.cog, vd.nav_stat, vd.rot, vd.pos_acc, vd.raim, 
    vd.heading, to_timestamp(vd.timestamp) AS vessel_timestamp, 
    vd.latitude AS vessel_latitude, vd.longitude AS vessel_longitude,
    pc.port_call_id, pc.port_call_timestamp, pc.port_to_visit, pc.prev_port, pc.next_port,
    ss.site_number, ss.site_name, ss.sea_state, ss.wind_wave_dir, ss.heel_angle, 
    ss.temperature, ss.latitude AS sea_latitude, ss.longitude AS sea_longitude
FROM vessel_data vd
LEFT JOIN port_call_data pc 
    ON vd.mmsi = pc.mmsi 
LEFT JOIN sea_state_data ss 
    ON ST_DWithin(
        ST_SetSRID(ST_MakePoint(vd.longitude, vd.latitude), 4326),
        ST_SetSRID(ST_MakePoint(ss.longitude, ss.latitude), 4326),
        0.5  -- Adjust this distance threshold as needed
    )
    """

def export_merged_data():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Execute query
        cursor.execute(MERGE_QUERY)

        # Fetch data
        columns = [desc[0] for desc in cursor.description]  # Get column names
        data = cursor.fetchall()  # Fetch all rows

        # Convert to DataFrame
        df = pd.DataFrame(data, columns=columns)

        # Handle NaN values
        df.fillna("", inplace=True)  # Replace NaN with empty strings

        # Save to CSV
        df.to_csv("data/merged_with_sea_ais_data.csv", index=False)

        print("Exported merged_with_sea_ais_data.csv successfully!")

        # Close the connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error exporting to CSV: {e}")

if __name__ == "__main__":
    export_merged_data()
