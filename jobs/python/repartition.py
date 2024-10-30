from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PythonRepartition").getOrCreate()

readpath = "/data/source/data1.csv"
savepath = "/data/partitions/"

print(f"Attempting to read file from: {readpath}")
df = spark.read.csv(readpath, header=True, inferSchema=True)
print(f"Success")

df.show(5)

df_partitioned = df.repartition(10)
df_partitioned.write.mode("overwrite").csv(savepath, header=True)

spark.stop()

