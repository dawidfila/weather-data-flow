from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.url import URL
from dotenv import load_dotenv
import os
import sys

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def ensure_table_exists(conn, table_name, create_query):
    """Checks if the table exists; if not, creates it"""
    try:
        # Check if the table exists
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = :table_name
            );
        """), {"table_name": table_name})
        table_exists = result.scalar()

        if not table_exists:
            print(f"Table '{table_name}' does not exist. Creating table...")
            conn.execute(text(create_query))
            conn.commit()
            print(f"Table '{table_name}' has been created.")
        else:
            print(f"Table '{table_name}' already exists.")
    except SQLAlchemyError as e:
        print(f"Error checking or creating table '{table_name}': {e}")
        sys.exit(1)

try:
    # Create a database connection
    db_url = URL.create(
        drivername="postgresql",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        print("Successfully connected to the database")

        # Define CREATE TABLE queries
        tables = {
            "locations": """
                CREATE TABLE locations (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(100) UNIQUE,
                    country VARCHAR(50),
                    latitude DECIMAL(9,6),
                    longitude DECIMAL(9,6),
                    timezone VARCHAR(50)
                );
            """,
            "weather_current": """
                CREATE TABLE weather_current (
                    id SERIAL PRIMARY KEY,
                    location_id INT REFERENCES locations(id) ON DELETE CASCADE,
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    temp_c DECIMAL(4,1),
                    humidity INT,
                    wind_kph DECIMAL(4,1),
                    pressure_mb DECIMAL(6,2),
                    cloud INT,
                    feelslike_c DECIMAL(4,1),
                    condition_text VARCHAR(100)
                );
            """
        }

        # Check and create tables
        for table_name, create_query in tables.items():
            ensure_table_exists(conn, table_name, create_query)

except SQLAlchemyError as e:
    print(f"Database error: {str(e)}")
    sys.exit(1)
finally:
    engine.dispose()