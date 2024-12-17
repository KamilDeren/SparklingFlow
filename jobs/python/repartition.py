from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand
import sys

spark = SparkSession.builder.appName("PythonRepartition").getOrCreate()

# Parametryzacja

input_path = sys.argv[sys.argv.index('--input') + 1]
output_path = sys.argv[sys.argv.index('--output') + 1]
partition_num = 10

df = spark.read.csv(input_path, header=True, inferSchema=True)

# Preprocessing

# Define a list of valid numeric country codes
valid_country_codes = ['48', '49', '30', '33', '32']

# Replace zero values with random country codes
df_with_random_codes = df.withColumn(
    "countrycode",
    when(
        col("countrycode") == 0,
        # Generate a random number and map it to a country code
        when(rand() < 0.2, valid_country_codes[0])  # 20% chance for the first country code
        .when(rand() < 0.4, valid_country_codes[1])  # 20% chance for the second country code
        .when(rand() < 0.6, valid_country_codes[2])  # 20% chance for the third country code
        .when(rand() < 0.8, valid_country_codes[3])  # 20% chance for the fourth country code
        .otherwise(valid_country_codes[4])  # Default to the fifth country code
    ).otherwise(col("countrycode"))  # Keep original countrycode if it's not 0
)

# Filling null values with 0's

columns_to_fill = ["smsin", "smsout", "callin", "callout", "internet"]

df_partitioned = df_with_random_codes.repartition(partition_num)

df_filled = df_partitioned.fillna(0, subset=columns_to_fill)

df_filled.write.csv(output_path, header=True, mode='overwrite')

spark.stop()
