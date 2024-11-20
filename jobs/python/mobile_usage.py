from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, coalesce

spark = SparkSession.builder.appName("MobileUsage").getOrCreate()

df = spark.read.csv('/data/bronze/partitions/milan_mobile_part1.csv', header=True, inferSchema=True)

df = spark.read.csv("/data/bronze/partitions/milan_mobile_part1.csv", header=True, inferSchema=True)

df_mobile_usage = df.select(
    concat(col('GridID').cast('string'), lit("_"), col('countrycode').cast('string')).alias("GridID_countrycode"),
    (col("TimeInterval") / 1000).cast("timestamp").alias("TimeInterval"),
    col("smsin").alias("SmsInCDR"),
    col("smsout").alias("SmsOutCDR"),
    col("callin").alias("CallInCDR"),
    col("callout").alias("CallOutCDR")
)

df_mobile_usage.coalesce(1).write.csv('/data/silver/mobile_usage_output', header=True, mode="overwrite")

print("Mobile usage data successfully written to file")
