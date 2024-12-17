from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, coalesce
import sys

spark = SparkSession.builder.appName("MobileUsage").getOrCreate()

input_path = sys.argv[sys.argv.index('--input') + 1]
output_path = sys.argv[sys.argv.index('--output') + 1]

df = spark.read.csv(input_path, header=True, inferSchema=True)

df_mobile_usage = df.select(
    concat(col('GridID').cast('string'), lit("_"), col('countrycode').cast('string')).alias("GridID_countrycode"),
    col("TimeInterval"),
    col("smsin").alias("SmsInCDR"),
    col("smsout").alias("SmsOutCDR"),
    col("callin").alias("CallInCDR"),
    col("callout").alias("CallOutCDR")
)

df_mobile_usage.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
