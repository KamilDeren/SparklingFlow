from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, coalesce, udf, col
from pyspark.sql.types import StructType, StructField, StringType
import phonenumbers
from phonenumbers import geocoder
import sys

spark = SparkSession.builder.appName("MobileUsage").getOrCreate()

input_path = sys.argv[sys.argv.index('--input') + 1]
output_path = sys.argv[sys.argv.index('--output') + 1]

df = spark.read.csv(input_path, header=True, inferSchema=True)


def country_name_for_number(p: phonenumbers.PhoneNumber, lang="en") -> str:
    rc = phonenumbers.geocoder.region_code_for_country_code(p.country_code)
    return phonenumbers.geocoder._region_display_name(rc, lang)


def getCountryName(GridID_countrycode):
    code = GridID_countrycode.split('_')[1]
    number = phonenumbers.parse(f"+{code}123456789")
    country = country_name_for_number(number, "en")
    return (country,)


get_country_name_udf = udf(getCountryName, StructType([StructField("country", StringType(), True)]))

df_with_country = df.withColumn("country_struct", get_country_name_udf(col("GridID_countrycode")))

df_with_country = df_with_country.withColumn("country", df_with_country["country_struct.country"])

df_with_country = df_with_country.drop("country_struct")

df_with_country.coalesce(1).write.csv(output_path, header=True, mode='overwrite')
