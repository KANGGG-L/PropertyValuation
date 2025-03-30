import psycopg
from datetime import datetime

from utils.geolocation import get_coordinates
import os

class DatabaseManager:
    def __init__(self, DB_CONFIG):
        self.db_config = DB_CONFIG
        self.conn = None
        self.cursor = None
        self.connect_db()


    def connect_db(self):
        """
        Tries to connect to the specified database. If the connection fails, it creates the database.
        """

        try:
            # Step 1: Connect to PostgreSQL server (initially using the default 'postgres' database)
            with psycopg.connect(
                dbname="postgres",
                user=self.db_config["user"],
                password=self.db_config["password"],
                host=self.db_config["host"],
                port=self.db_config["port"],
                autocommit=True
            ) as conn:
                
                with conn.cursor() as cursor:
            
                    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (self.db_config['dbname'],))
                    exists = cursor.fetchone()
                    
                    if not exists:
                        # Create the database if it doesn't exist
                        cursor.execute(f"CREATE DATABASE {self.db_config['dbname']};")


                self.conn = psycopg.connect(
                    dbname=self.db_config["dbname"],
                    user=self.db_config["user"],
                    password=self.db_config["password"],
                    host=self.db_config["host"],
                    port=self.db_config["port"],
                    autocommit=True
                )

                self.cursor = self.conn.cursor()

                # Enable PostGIS and create tables if the database was newly created
                if not exists:
                    # Enable the PostGIS extension for spatial data support
                    self.cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                    self.create_tables()

                    sql_file_path = os.path.abspath("/database/pgsql_functions.sql")
                    self.execute_sql_file(sql_file_path)
    
        except psycopg.errors.InsufficientPrivilege as e:
            print(f"Error: Insufficient privileges to create database. {e}")
        except psycopg.errors.DatabaseError as e:
            print(f"Error: A database error occurred. {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def create_tables(self):
        
        """
        Creates the tables for rental and sold properties if they don't exist.
        """
        try:
            # Create the rental_properties table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS rental_properties (
                    id SERIAL PRIMARY KEY,
                    unit TEXT,
                    street_address TEXT NOT NULL,
                    bedroom_num INT NOT NULL,
                    bathroom_num INT NOT NULL,
                    parking_num INT NOT NULL,
                    price INT NOT NULL,
                    property_type INT NOT NULL,
                    record_date DATE NOT NULL,
                    latitude FLOAT,
                    longitude FLOAT,
                    geom GEOMETRY(Point, 4326),
                    description TEXT
                );
            """)

            # Create the sold_properties table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS sold_properties (
                    id SERIAL PRIMARY KEY,
                    unit TEXT,
                    street_address TEXT NOT NULL,
                    bedroom_num INT NOT NULL,
                    bathroom_num INT NOT NULL,
                    parking_num INT NOT NULL,
                    price INT NOT NULL,
                    property_type INT NOT NULL,
                    record_date DATE NOT NULL,
                    latitude FLOAT,
                    longitude FLOAT,
                    geom GEOMETRY(Point, 4326),
                    description TEXT
                );
            """)

        except Exception as e:
            print(f"Error occurred while creating tables: {e}")

   
    def store_data_in_postgis(self, data, mode):
        """
        Store extracted property data in a PostGIS database.
        """
        try:
            # Insert data into PostGIS
            table_name = "sold_properties" if mode else "rental_properties"

            record_date = datetime.today().date()

            for row in data:
                if row["longitude"] and row["latitude"]:

                    geom_value = f"ST_SetSRID(ST_MakePoint({row['longitude']}, {row['latitude']}), 4326)"

                    self.cursor.execute(f"""
                        INSERT INTO {table_name} (unit, street_address, bedroom_num, bathroom_num, parking_num, 
                                                price, property_type, record_date, latitude, longitude, geom, description)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, {geom_value}, %s);
                    """, (
                        row["unit"], row["street_address"], row["bedroom_num"], row["bathroom_num"],
                        row["parking_num"], row["price"], row["property_type"], record_date,
                        row["latitude"], row["longitude"], ''
                    ))
          

        except Exception as e:
            print(f"Database error: {e}")


    def query_k_nearest_properties(self, address, area_name, k, mode, 
                                   property_type, bedroom_num, bathroom_num, parking_num, 
                                   range_percentage):
        """
        Query the k nearest properties to the given address.
        :param address: The address of the property (e.g., "123 Main St").
        :param k: The number of nearest properties to return.
        :return: List of k nearest properties sorted by distance.
        """
        latitude, longitude = get_coordinates(address, area_name)

        if longitude and latitude:
            try:
                # Call the PostgreSQL function to get the k nearest properties
                self.cursor.execute("""
                    SELECT * FROM get_k_nearest_properties(%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (longitude, latitude, k, mode, property_type, bedroom_num, bathroom_num, parking_num, range_percentage))

                # Fetch the results
                nearest_properties = self.cursor.fetchall()

                # Return the nearest properties
                return nearest_properties

            except Exception as e:
                print(f"Error while querying for nearest properties: {e}")
                return None
        
        else:
            print("Invalid address or unable to retrieve coordinates.")
            return None



    def execute_sql_file(self, file_path):
        try:
            with open(file_path, 'r') as file:
                sql = file.read()

            self.cursor.execute(sql)
                
        except Exception as e:
            print(f"Error executing SQL file: {e}")

