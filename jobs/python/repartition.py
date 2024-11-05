from pyspark.sql import SparkSession
import os
import shutil

spark = SparkSession.builder.appName("PythonRepartition").getOrCreate()

readpath = "/data/source/data1.csv"
temp_savepath = "/data/temp_partitions/"
final_savepath = "/data/partitions/"

df = spark.read.csv(readpath, header=True, inferSchema=True)

df_partitioned = df.repartition(10)

df_partitioned.write.mode("overwrite").csv(temp_savepath, header=True)

# Rename files in the temporary directory
for i, filename in enumerate(os.listdir(temp_savepath)):
    if filename.endswith('.csv'):
        new_name = f"milan_mobile_part{i-10}.csv"
        os.rename(os.path.join(temp_savepath, filename), os.path.join(final_savepath, new_name))

# Clean up the temporary directory
shutil.rmtree(temp_savepath)

spark.stop()
