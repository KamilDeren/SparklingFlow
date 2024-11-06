from pyspark.sql import SparkSession
import os
import shutil
import sys

spark = SparkSession.builder.appName("PythonRepartition").getOrCreate()

#Parametryzacja

input_path = sys.argv[sys.argv.index('--input') + 1]
temp_savepath = sys.argv[sys.argv.index('--temp') + 1]
output_path = sys.argv[sys.argv.index('--output') + 1]
partition_num = sys.argv[sys.argv.index('--partition') + 1]

df = spark.read.csv(input_path, header=True, inferSchema=True)

df_partitioned = df.repartition(partition_num)

df_partitioned.write.mode("overwrite").csv(temp_savepath, header=True)

# Rename files in the temporary directory
for i, filename in enumerate(os.listdir(temp_savepath)):
    if filename.endswith('.csv'):
        new_name = f"milan_mobile_part{i-10}.csv"
        os.rename(os.path.join(temp_savepath, filename), os.path.join(output_path, new_name))

# Clean up the temporary directory
shutil.rmtree(temp_savepath)

spark.stop()
