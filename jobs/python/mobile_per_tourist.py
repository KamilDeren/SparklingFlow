from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MobilePerTourist").getOrCreate()

tourist_df = spark.read.csv('/data/silver/tourist/part-00000-71033a63-51bb-4085-92a2-06728168a3d0-c000.csv', header=True, inferSchema=True)

dane_df = spark.read.csv('/data/silver/mobile_usage_output/part-00000-3b8fecf6-7850-40ad-89e8-a2c5853db7f0-c000.csv', header=True, inferSchema=True)

tourist_df_alias = tourist_df.alias("tourist")
dane_df_alias = dane_df.alias("dane")

# Połączenie tabel na podstawie kolumny 'GridID_countrycode'
joined_df = tourist_df_alias.join(
    dane_df_alias,
    tourist_df_alias.GridID_countrycode == dane_df_alias.GridID_countrycode
)

# Grupowanie po 'GridID_countrycode' i obliczenie sumy zużycia danych mobilnych (SmsInCDR, SmsOutCDR, CallInCDR, CallOutCDR)
result_df = joined_df.groupBy("tourist.GridID_countrycode", "tourist.first_name", "tourist.last_name", "tourist.country").agg(
    {
        "dane.SmsInCDR": "sum",
        "dane.SmsOutCDR": "sum",
        "dane.CallInCDR": "sum",
        "dane.CallOutCDR": "sum"
    }
)

result_df = result_df.withColumnRenamed("sum(SmsInCDR)", "total_sms_in")
result_df = result_df.withColumnRenamed("sum(SmsOutCDR)", "total_sms_out")
result_df = result_df.withColumnRenamed("sum(CallInCDR)", "total_call_in")
result_df = result_df.withColumnRenamed("sum(CallOutCDR)", "total_call_out")

result_df.write.csv('/data/gold/total_mobile_usage', header=True, mode='overwrite')