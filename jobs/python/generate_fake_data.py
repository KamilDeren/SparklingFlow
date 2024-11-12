from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, coalesce, udf, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from faker import Faker

fake = Faker()
spark = SparkSession.builder.appName("GenerateFakeData").getOrCreate()

df = spark.read.csv('/data/bronze/partitions/milan_mobile_part1.csv', header=True, inferSchema=True)

# Dictionary to store generated names based on GridID and countrycode
name_dict = {}


# Function to generate a fake name based on GridID and countrycode
def generate_fake_name(grid_id, country_code):
    key = f"{grid_id}_{country_code}"
    if key not in name_dict:
        first_name = fake.first_name()
        last_name = fake.last_name()
        name_dict[key] = (first_name, last_name)
    else:
        first_name, last_name = name_dict[key]
    return first_name, last_name


# Define the UDF to return a struct with first_name and last_name
def generate_fake_name_udf(grid_id, country_code):
    first_name, last_name = generate_fake_name(grid_id, country_code)
    return (first_name, last_name)


# Register the UDF with a StructType return type
generate_fake_name_udf = udf(generate_fake_name_udf, StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True)
]))

# Create a new column 'GridID_countrycode' to combine GridID and countrycode
df_with_names = df.withColumn("GridID_countrycode", concat(col('GridID').cast('string'), lit("_"), col('countrycode').cast('string')))

# Apply the UDF to generate fake names and create two new columns
df_with_names = df_with_names.withColumn("fake_name", generate_fake_name_udf(df['GridID'], df['countrycode']))

# Split the struct column into separate first_name and last_name columns
df_with_names = df_with_names.withColumn("first_name", df_with_names["fake_name.first_name"])
df_with_names = df_with_names.withColumn("last_name", df_with_names["fake_name.last_name"])

df_with_names = df_with_names.drop("fake_name")

df_with_names = df_with_names.select("GridID_countrycode", "first_name", "last_name")

df_with_names.coalesce(1).write.csv('/data/bronze/fake_names_output', header=True, mode='overwrite')

