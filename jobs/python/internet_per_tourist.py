from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InternetPerTourist").getOrCreate()

tourist_df = spark.read.csv('/data/silver/tourist/part-00000-71033a63-51bb-4085-92a2-06728168a3d0-c000.csv', header=True, inferSchema=True)

dane_df = spark.read.csv('/data/silver/internet_usage_output/part-00000-0f34ce6a-4f1c-4037-8ba0-717ece976cfc-c000.csv', header=True, inferSchema=True)

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

result_df.write.csv('/data/gold/total_internet_usage', header=True, mode='overwrite')