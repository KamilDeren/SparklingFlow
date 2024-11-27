from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, coalesce, udf, col
from pyspark.sql.types import StructType, StructField, StringType
import phonenumbers
from phonenumbers import geocoder

spark = SparkSession.builder.appName("MobileUsage").getOrCreate()

df = spark.read.csv('/data/bronze/fake_names_output/part-00000-5ede5c9f-b212-4eec-abf7-19a9cfa93fd9-c000.csv', header=True, inferSchema=True)

def getCountryName(GridID_countrycode):
    code = string(GridID_countrycode).split('_')[1]
    number = phonenumbers.parse(f"+{code}123456789")
    country = geocoder.country_name_for_number(number, "en")
    return (country,)


# Rejestracja funkcji UDF
get_country_name_udf = udf(getCountryName, StructType([StructField("country", StringType(), True)]))

# Zastosowanie UDF w DataFrame
df_with_country = df.withColumn("country_struct", get_country_name_udf(col("GridID_countrycode")))

# Rozdzielenie struktury na pojedynczą kolumnę 'country'
df_with_country = df_with_country.withColumn("country", df_with_country["country_struct.country"])

# Usunięcie zbędnej kolumny 'country_struct'
df_with_country = df_with_country.drop("country_struct")

# Zapisanie wynikowego DataFrame do pliku
df_with_country.coalesce(1).write.csv('/data/silver/tourist', header=True, mode='overwrite')
