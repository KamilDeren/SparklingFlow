import os
import psycopg2
from pyspark.sql import SparkSession

# Setup Spark session
spark = SparkSession.builder \
    .appName("PostgresTourist") \
    .config("spark.jars", "/path/to/postgresql-<version>.jar") \
    .getOrCreate()

# PostgreSQL connection parameters
jdbc_url = "jdbc:postgresql://localhost:5432/airflow"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}


# Function to create table if not exists
def create_table_if_not_exists():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()

        create_sql = """
        CREATE TABLE IF NOT EXISTS tourist (
            GridID_countrycode VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR
        );
        """
        cursor.execute(create_sql)
        conn.commit()
        cursor.close()
        conn.close()
        print("Table 'tourist' created or already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")


# Read tourist data from all CSV files in the directory
tourist_dir_path = "/data/bronze/fake_names_output/"
df_tourist = spark.read.csv(tourist_dir_path + "*.csv", header=True, inferSchema=True)

# Ensure the tourist table exists
create_table_if_not_exists()

# Write tourist data to PostgreSQL
df_tourist.write.jdbc(
    url=jdbc_url, table="tourist", mode="overwrite", properties=properties
)

print("Tourist data successfully written to PostgreSQL table!")
