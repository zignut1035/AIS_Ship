import psycopg2
from psycopg2 import sql

def create_database():
    connection = psycopg2.connect(dbname="postgres", user="natuser", password="postgres", host="localhost")
    connection.autocommit = True
    cursor = connection.cursor()
    
    cursor.execute("CREATE DATABASE ais_db;")
    cursor.close()
    connection.close()

def create_schema():
    connection = psycopg2.connect(dbname="ais_db", user="natuser", password="postgres", host="localhost")
    cursor = connection.cursor()
    
    commands = [
        "CREATE SCHEMA IF NOT EXISTS topology;",
        "COMMENT ON SCHEMA topology IS 'PostGIS Topology schema';",
        "CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;",
        "COMMENT ON EXTENSION postgis IS 'PostGIS geometry and geography spatial types and functions';",
        "CREATE EXTENSION IF NOT EXISTS postgis_topology WITH SCHEMA topology;",
        "COMMENT ON EXTENSION postgis_topology IS 'PostGIS topology spatial types and functions';",
        "SET default_tablespace = '';",
        "SET default_table_access_method = heap;",
        """
        CREATE TABLE IF NOT EXISTS public.ais_data (
            id SERIAL PRIMARY KEY,
            "timestamp" TIMESTAMP WITH TIME ZONE DEFAULT now(),
            vessel_id TEXT NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            velocity NUMERIC(5,2) CHECK (velocity >= 0),
            cog NUMERIC(5,1) CHECK (cog >= 0 AND cog <= 360),
            geom public.geometry(Point,4326)
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_ais_geom ON public.ais_data USING gist (geom);",
        "CREATE INDEX IF NOT EXISTS idx_ais_vessel_time ON public.ais_data (vessel_id, timestamp DESC);",
        "CREATE INDEX IF NOT EXISTS idx_ais_vessel_id ON public.ais_data (vessel_id);",
        """
        CREATE TABLE IF NOT EXISTS public.ml_predictions (
            id SERIAL PRIMARY KEY,
            vessel_id TEXT,
            predicted_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            predicted_latitude DOUBLE PRECISION NOT NULL,
            predicted_longitude DOUBLE PRECISION NOT NULL,
            predicted_velocity NUMERIC(5,2) CHECK (predicted_velocity >= 0),
            predicted_cog NUMERIC(5,1) CHECK (predicted_cog >= 0 AND predicted_cog <= 360),
            geom public.geometry(Point,4326),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_ml_geom ON public.ml_predictions USING gist (geom);",
        "CREATE INDEX IF NOT EXISTS idx_ml_vessel_time ON public.ml_predictions (vessel_id, predicted_timestamp DESC);",
        "CREATE INDEX IF NOT EXISTS idx_ml_vessel_id ON public.ml_predictions (vessel_id);",
        """
        CREATE TABLE IF NOT EXISTS public.ships (
            vessel_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            ship_type TEXT,
            length NUMERIC(6,2) CHECK (length >= 0),
            width NUMERIC(6,2) CHECK (width >= 0),
            flag TEXT,
            mmsi TEXT UNIQUE,
            imo TEXT UNIQUE
        );
        """,
        "ALTER TABLE public.ais_data ADD CONSTRAINT ais_data_vessel_id_fkey FOREIGN KEY (vessel_id) REFERENCES public.ships(vessel_id) ON DELETE CASCADE;",
        "ALTER TABLE public.ml_predictions ADD CONSTRAINT ml_predictions_vessel_id_fkey FOREIGN KEY (vessel_id) REFERENCES public.ships(vessel_id) ON DELETE CASCADE;"
    ]
    
    try:
        for command in commands:
            cursor.execute(command)
        connection.commit()
    except Exception as e:
        print(f"Error executing SQL commands: {e}")
        connection.rollback()
    
    cursor.close()
    connection.close()
    
if __name__ == "__main__":
    try:
        create_database()
    except Exception as e:
        print(f"Database creation skipped (maybe already exists): {e}")
    
    create_schema()
    print("Database schema created successfully.")