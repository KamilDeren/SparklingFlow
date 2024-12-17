from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("MobilePerTourist").getOrCreate()

input_path_tourist = sys.argv[sys.argv.index('--input1') + 1]
input_path_mobile = sys.argv[sys.argv.index('--input2') + 1]
output_path = sys.argv[sys.argv.index('--output') + 1]

tourist_df = spark.read.csv(input_path_tourist, header=True, inferSchema=True)

dane_df = spark.read.csv(input_path_mobile, header=True, inferSchema=True)

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

result_df.write.csv(output_path, header=True, mode='overwrite')