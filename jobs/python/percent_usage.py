from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

spark = SparkSession.builder.appName("MobileAndInternetUsagePercentage").getOrCreate()

input_path_mobile = sys.argv[sys.argv.index('--input1') + 1]
input_path_internet = sys.argv[sys.argv.index('--input2') + 1]

df_mobile = spark.read.csv(input_path_mobile, header=True, inferSchema=True)
df_internet = spark.read.csv(input_path_internet, header=True, inferSchema=True)

total_users_mobile = df_mobile.count()
total_users_internet = df_internet.count()

mobile_users = df_mobile.filter(
    (col("total_sms_in") > 0) |
    (col("total_sms_out") > 0) |
    (col("total_call_in") > 0) |
    (col("total_call_out") > 0)
).count()

internet_users = df_internet.filter(col("total_internet_usage") > 0).count()

percentage_mobile_data = (mobile_users / total_users_mobile) * 100
percentage_internet_users = (internet_users / total_users_internet) * 100

print(f"Procent użytkowników korzystających z danych mobilnych: {percentage_mobile_data:.2f}%")
print(f"Procent użytkowników korzystających z internetu: {percentage_internet_users:.2f}%")
