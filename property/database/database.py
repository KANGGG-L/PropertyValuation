import psycopg
from datetime import datetime
from utils.geolocation import get_coordinates
import os

class DatabaseManager:
    """
    Manages PostgreSQL/PostGIS database for property data, supporting connection, table creation,
    spatial data insertion, nearest-neighbor spatial search, and SQL script execution.

    :param DB_CONFIG: Database connection parameters as a dictionary
    """

    def __init__(self, DB_CONFIG):
        """
        Initializes the DatabaseManager and connects to the PostgreSQL/PostGIS database.

        :param DB_CONFIG: Database connection parameters including 'host', 'port', 'dbname', 'user', and 'password'.
        """
        self.db_config = DB_CONFIG
        self.conn = None
        self.cursor = None
        self.connect_db()

    def connect_db(self):
        """
        Connects to the PostgreSQL server and ensures the target database and PostGIS extension exist.
        Creates necessary property tables and indexes if this is a new database,
        and executes initialization SQL scripts if required.
        """
        try:
            # Connect to PostgreSQL server using default database
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
                        cursor.execute(f"CREATE DATABASE {self.db_config['dbname']};")

            # Connect to the actual target database
            self.conn = psycopg.connect(
                dbname=self.db_config["dbname"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                host=self.db_config["host"],
                port=self.db_config["port"],
                autocommit=True
            )
            self.cursor = self.conn.cursor()

            if not exists:
                self.cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                self.create_tables()
                
                current_dir = os.path.dirname(os.path.abspath(__file__))
                sql_file_path = os.path.join(current_dir, "pgsql_functions.sql")
                self.execute_sql_file(sql_file_path)

        except psycopg.errors.InsufficientPrivilege as e:
            print(f"Error: Insufficient privileges to create database. {e}")
        except psycopg.errors.DatabaseError as e:
            print(f"Error: A database error occurred. {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def create_tables(self):
        """
        Creates 'rental_properties' and 'sold_properties' tables with spatial and postcode indexes if they do not exist.
        """
        try:
            tables = {
                "rental_properties": "rental_geom_idx",
                "sold_properties": "sold_geom_idx"
            }
            table_schema = """
                id SERIAL PRIMARY KEY,
                postcode INT NOT NULL,
                unit TEXT,
                street_address TEXT NOT NULL,
                bedroom_num INT NOT NULL,
                bathroom_num INT NOT NULL,
                parking_num INT NOT NULL,
                price INT NOT NULL,
                property_type INT NOT NULL,
                record_date DATE NOT NULL,
                inactive BOOLEAN NOT NULL DEFAULT FALSE,
                last_recorded_date DATE NOT NULL,
                latitude FLOAT,
                longitude FLOAT,
                geom GEOMETRY(Point, 4326),
                description TEXT
            """
            for table_name, index_name in tables.items():
                self.cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {table_schema}
                    );
                """)
                self.cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS {index_name}
                    ON {table_name} USING GIST (geom);
                """)
                self.cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS {table_name}_postcode_idx
                    ON {table_name} (postcode);
                """)
            print("Tables and indexes created successfully.")
        except Exception as e:
            print(f"Error occurred while creating tables or indexes: {e}")

    def store_data_in_postgis(self, data, mode):
        """
        Stores a list of property data records into the PostGIS database.
        Updates last_recorded_date if the record exists (matching all identifying fields), otherwise inserts a new record.

        :param data: List of property dictionaries, each containing all required fields.
        :param mode: 0 for 'rental_properties', 1 for 'sold_properties'.
        """
        try:
            table_name = "sold_properties" if mode else "rental_properties"
            today_date = datetime.today().date()

            for row in data:
                if row.get("longitude") is not None and row.get("latitude") is not None:
                    # Check if record already exists
                    self.cursor.execute(f"""
                        SELECT id FROM {table_name}
                        WHERE postcode = %s AND COALESCE(unit, '') = COALESCE(%s, '') AND
                            street_address = %s AND
                            bedroom_num = %s AND
                            bathroom_num = %s AND
                            parking_num = %s AND
                            property_type = %s AND
                            latitude = %s AND
                            longitude = %s
                        """, (
                            row["postcode"],
                            row.get("unit"),
                            row["street_address"],
                            row["bedroom_num"],
                            row["bathroom_num"],
                            row["parking_num"],
                            row["property_type"],
                            row["latitude"],
                            row["longitude"]
                        ))
                    existing = self.cursor.fetchone()
                    if existing:
                        # Update last_recorded_date
                        self.cursor.execute(f"""
                            UPDATE {table_name}
                            SET last_recorded_date = %s
                            WHERE id = %s
                        """, (today_date, existing[0]))
                    else:
                        # Insert new record
                        geom_value = f"ST_SetSRID(ST_MakePoint({row['longitude']}, {row['latitude']}), 4326)"
                        self.cursor.execute(f"""
                            INSERT INTO {table_name} (
                                postcode, unit, street_address, bedroom_num, bathroom_num, parking_num, 
                                price, property_type, record_date, last_recorded_date, inactive, 
                                latitude, longitude, geom, description
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, {geom_value}, %s);
                        """, (
                            row["postcode"],
                            row.get("unit"),
                            row["street_address"],
                            row["bedroom_num"],
                            row["bathroom_num"],
                            row["parking_num"],
                            row["price"],
                            row["property_type"],
                            today_date,
                            today_date,
                            False,
                            row["latitude"],
                            row["longitude"],
                            row.get("description", "")
                        ))
        except Exception as e:
            print(f"Database error: {e}")


    def query_k_nearest_properties(self, state, postcode, address, area_name, k, mode,
                                  property_type, bedroom_num, bathroom_num, parking_num,
                                  range_percentage):
        """
        Queries the database for the k nearest properties to a specified address,
        using property type and feature filters, and a price range if specified.

        :param state: The address to find nearby properties for.
        :param address: The address to find nearby properties for.
        :param area_name: The suburb or area name, used for geocoding.
        :param k: Number of nearest properties to retrieve.
        :param mode: 0 for 'rental_properties', 1 for 'sold_properties'.
        :param property_type: Property type code (e.g., apartment, house).
        :param bedroom_num: Required number of bedrooms.
        :param bathroom_num: Required number of bathrooms.
        :param parking_num: Required number of parking spaces.
        :param range_percentage: Percent price range around the median (or -1 to ignore price filter).

        :return: List of property records as tuples, or None if an error or invalid location.
        """
        latitude, longitude = get_coordinates(address, area_name, state, postcode)
        if longitude and latitude:
            try:
                self.cursor.execute("""
                    SELECT * FROM get_k_nearest_properties(%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (longitude, latitude, k, mode, property_type, bedroom_num, bathroom_num, parking_num, range_percentage))
                nearest_properties = self.cursor.fetchall()
                return nearest_properties
            except Exception as e:
                print(f"Error while querying for nearest properties: {e}")
                return None
        else:
            print("Invalid address or unable to retrieve coordinates.")
            return None

    def execute_sql_file(self, file_path):
        """
        Executes all SQL statements contained in a file as a single block.
        :param file_path: Path to the SQL file.
        """
        try:
            with open(file_path, 'r') as file:
                sql = file.read()
            self.cursor.execute(sql)
        except Exception as e:
            print(f"Error executing SQL file: {e}")
