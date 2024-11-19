import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp

# Setup Spark session
spark = SparkSession.builder \
    .appName("PostgresMobileUsage") \
    .config("spark.jars", "/path/to/postgresql-<version>.jar") \
    .getOrCreate()

# PostgreSQL connection parameters
jdbc_url = "jdbc:postgresql://localhost:5432/airflow"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}


# Function to create mobile_usage table if not exists
def create_mobile_usage_table():
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
        CREATE TABLE IF NOT EXISTS mobile_usage (
            GridID_countrycode VARCHAR PRIMARY KEY,
            TimeInterval TIMESTAMP,
            SmsinCDR FLOAT,
            SmsoutCDR FLOAT,
            CallinCDR FLOAT,
            CalloutCDR FLOAT
        );
        """
        cursor.execute(create_sql)
        conn.commit()
        cursor.close()
        conn.close()
        print("Table 'mobile_usage' created or already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")


# Load mobile usage data from CSV file
mobile_usage_file_path = "/data/bronze/partitions/milan_mobile_part1.csv"
df_main = spark.read.csv(mobile_usage_file_path, header=True, inferSchema=True)

# Transform main data for mobile_usage
df_mobile_usage = df_main.select(
    (col("GridID").cast("string") + "_" + col("countrycode").cast("string")).alias("GridID_countrycode"),
    unix_timestamp(col("TimeInterval") / 1000).cast("timestamp").alias("TimeInterval"),
    "smsin", "smsout", "callin", "callout"
)

# Ensure mobile_usage table exists
create_mobile_usage_table()

# Write mobile_usage data to PostgreSQL
df_mobile_usage.write.jdbc(
    url=jdbc_url, table="mobile_usage", mode="overwrite", properties=properties
)

print("Mobile usage data successfully written to PostgreSQL table!")
