from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("CountryCount").getOrCreate()

input_path = sys.argv[sys.argv.index('--input') + 1]
output_path = sys.argv[sys.argv.index('--output') + 1]

df = spark.read.csv(input_path, header=True, inferSchema=True)

result = df.groupBy("country").count()

result.write.csv(output_path, header=True, mode='overwrite')
