from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, coalesce
import sys

spark = SparkSession.builder.appName("InternetUsage").getOrCreate()

input_path = sys.argv[sys.argv.index('--input') + 1]
output_path = sys.argv[sys.argv.index('--output') + 1]

df = spark.read.csv(input_path, header=True, inferSchema=True)

df_internet_usage = df.select(
    concat(col('GridID').cast('string'), lit("_"), col('countrycode').cast('string')).alias("GridID_countrycode"),
    col("TimeInterval"),
    col("internet").alias("InternetCDR"),
    (col("internet") * 5).alias("InternetTransfer")
)

df_internet_usage.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

print("Internet usage data successfully written to file!")
