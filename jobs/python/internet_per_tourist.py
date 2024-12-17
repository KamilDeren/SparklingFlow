from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("InternetPerTourist").getOrCreate()

input_path_tourist = sys.argv[sys.argv.index('--input1') + 1]
input_path_internet = sys.argv[sys.argv.index('--input2') + 1]
output_path = sys.argv[sys.argv.index('--output') + 1]

tourist_df = spark.read.csv(input_path_tourist, header=True, inferSchema=True)

dane_df = spark.read.csv(input_path_internet, header=True, inferSchema=True)

tourist_df_alias = tourist_df.alias("tourist")
dane_df_alias = dane_df.alias("dane")

# Połączenie tabel na podstawie kolumny 'GridID_countrycode'
joined_df = tourist_df_alias.join(
    dane_df_alias,
    tourist_df_alias.GridID_countrycode == dane_df_alias.GridID_countrycode
)

# Grupowanie po 'GridID_countrycode' i obliczenie sumy zużycia internetu (InternetCDR)
result_df = joined_df.groupBy("tourist.GridID_countrycode", "tourist.first_name", "tourist.last_name", "tourist.country").agg(
    {"dane.InternetCDR": "sum"}
)

result_df = result_df.withColumnRenamed("sum(InternetCDR)", "total_internet_usage")

result_df.write.csv(output_path, header=True, mode='overwrite')
