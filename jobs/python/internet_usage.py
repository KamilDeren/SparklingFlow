import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp

# Setup Spark session
spark = SparkSession.builder \
    .appName("PostgresInternetUsage") \
    .config("spark.jars", "/path/to/postgresql-<version>.jar") \
    .getOrCreate()

# PostgreSQL connection parameters
jdbc_url = "jdbc:postgresql://localhost:5432/airflow"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}


# Function to create internet_usage table if not exists
def create_internet_usage_table():
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
        CREATE TABLE IF NOT EXISTS internet_usage (
            GridID_countrycode VARCHAR PRIMARY KEY,
            TimeInterval TIMESTAMP,
            InternetCDR FLOAT,
            InternetTransfer FLOAT
        );
        """
        cursor.execute(create_sql)
        conn.commit()
        cursor.close()
        conn.close()
        print("Table 'internet_usage' created or already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")


# Load internet usage data from the same CSV file
mobile_usage_file_path = "/data/bronze/partitions/milan_mobile_part1.csv"
df_main = spark.read.csv(mobile_usage_file_path, header=True, inferSchema=True)

# Transform main data for internet_usage
df_internet_usage = df_main.select(
    (col("GridID").cast("string") + "_" + col("countrycode").cast("string")).alias("GridID_countrycode"),
    unix_timestamp(col("TimeInterval") / 1000).cast("timestamp").alias("TimeInterval"),
    "internet"
)

# Ensure internet_usage table exists
create_internet_usage_table()

# Write internet_usage data to PostgreSQL
df_internet_usage.write.jdbc(
    url=jdbc_url, table="internet_usage", mode="overwrite", properties=properties
)

print("Internet usage data successfully written to PostgreSQL table!")
