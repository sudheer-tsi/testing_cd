# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6c97b5de-da3d-4c45-81d7-5250148c70e4",
# META       "default_lakehouse_name": "edp_dev_raw",
# META       "default_lakehouse_workspace_id": "26dd097d-f293-420e-b5f2-5c949c38b071",
# META       "known_lakehouses": [
# META         {
# META           "id": "6c97b5de-da3d-4c45-81d7-5250148c70e4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd
from pyspark.sql import functions as F

# Step 1: Read Excel mapping file into Pandas
mapping_excel = "/lakehouse/default/Files/table_mapping.xlsx"   # <-- update path
mapping_df = pd.read_excel(mapping_excel)

# Convert to Spark DataFrame if needed
mapping_sdf = spark.createDataFrame(mapping_df)

# Step 2: Group by schema and table
grouped = mapping_df.groupby(["schema", "table_name"])

# Workspace paths
source_workspace = "abfss://ws-tsi-data-platform-dev-001@onelake.dfs.fabric.microsoft.com/lh_raw.lakehouse/Tables"
target_workspace = "abfss://ws-edp-dev@onelake.dfs.fabric.microsoft.com/edp_dev_raw.lakehouse/Tables"

# Step 3: Loop over each schema+table group
for (schema, table_name), group in grouped:
    print(f"Processing {schema}.{table_name} ...")

    # Build source and target paths
    source_path = f"{source_workspace}/{schema}/{table_name}"
    target_path = f"{target_workspace}/{schema}/{table_name}"

    # Read source table
    df = spark.read.format("delta").load(source_path)

    # Build column rename mapping from excel
    rename_map = dict(zip(group["org_column_name"], group["new_column_name"]))

    # Apply renaming (only keep the required columns)
    for old_col, new_col in rename_map.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    # Select only the new column order
    df = df.select(list(rename_map.values()))

    # Write to target (overwrite mode with schema update)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)

    print(f"✅ Copied {schema}.{table_name} to target")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS as400")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
