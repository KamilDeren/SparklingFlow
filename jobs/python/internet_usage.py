from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, coalesce

spark = SparkSession.builder.appName("InternetUsage").getOrCreate()

df = spark.read.csv('/data/bronze/partitions/milan_mobile_part1.csv', header=True, inferSchema=True)

df_internet_usage = df.select(
    concat(col('GridID').cast('string'), lit("_"), col('countrycode').cast('string')).alias("GridID_countrycode"),
    col("TimeInterval"),
    col("internet").alias("InternetCDR"),
    (col("internet") * 5).alias("InternetTransfer")
)

df_internet_usage.coalesce(1).write.csv('/data/silver/internet_usage_output', header=True, mode='overwrite')

print("Internet usage data successfully written to file!")
