# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ParquetToDelta").getOrCreate()

parquet_path = "abfss://26dd097d-f293-420e-b5f2-5c949c38b071@onelake.dfs.fabric.microsoft.com/6c97b5de-da3d-4c45-81d7-5250148c70e4/Files/sample_parquet_testing"
delta_path = "abfss://ws-edp-dev@onelake.dfs.fabric.microsoft.com/edp_dev_raw.Lakehouse/Tables/dbo/parquet_to_delta_test"

df = spark.read.parquet(parquet_path)

# Step 2: Drop the column if it exists
if "loaded_at" in df.columns:
    df = df.drop("loaded_at")

# Step 3: Write into Delta
df.write.format("delta").mode("overwrite").save(delta_path)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
