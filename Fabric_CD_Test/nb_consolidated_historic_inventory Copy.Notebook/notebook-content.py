# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3b946dbe-6e6e-4ca6-a2e8-3683257049cf",
# META       "default_lakehouse_name": "lh_core",
# META       "default_lakehouse_workspace_id": "d42020a3-ede3-4fd1-b1df-c81696e15c66"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
import time
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


"""Initialize Spark with optimized configurations for large-scale data processing."""
try:
    spark = SparkSession.builder \
        .appName("Optimized Query Processing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .config("spark.executor.memory", "30g") \
        .config("spark.executor.memoryOverhead", "10g") \
        .config("spark.driver.memory", "30g") \
        .config("spark.memory.storageFraction", "0.4") \
        .config("spark.sql.files.maxPartitionBytes", "128m") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "1") \
        .config("spark.dynamicAllocation.maxExecutors", "8") \
        .config("spark.sql.broadcastTimeout", "1800") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .getOrCreate()
    logger.info("SparkSession initialized successfully with optimized configurations.")
except Exception as e:
    logger.error(f"Error initializing SparkSession: {e}")
    raise



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Load the tables
df_crs_inventory = spark.read.format("delta").load("abfss://d42020a3-ede3-4fd1-b1df-c81696e15c66@onelake.dfs.fabric.microsoft.com/3b946dbe-6e6e-4ca6-a2e8-3683257049cf/Tables/inventory/crs_historic_inventory")
df_as400_inventory = spark.read.format("delta").load("abfss://d42020a3-ede3-4fd1-b1df-c81696e15c66@onelake.dfs.fabric.microsoft.com/3b946dbe-6e6e-4ca6-a2e8-3683257049cf/Tables/inventory/as400_historic_inventory")
# df_artiva_inventory = spark.read.format("delta").load("abfss://d42020a3-ede3-4fd1-b1df-c81696e15c66@onelake.dfs.fabric.microsoft.com/3b946dbe-6e6e-4ca6-a2e8-3683257049cf/Tables/inventory/artiva_inventory")
df_cubs_inventory = spark.read.format("delta").load("abfss://d42020a3-ede3-4fd1-b1df-c81696e15c66@onelake.dfs.fabric.microsoft.com/3b946dbe-6e6e-4ca6-a2e8-3683257049cf/Tables/inventory/cubs_historic_inventory")
df_facs_inventory = spark.read.format("delta").load("abfss://d42020a3-ede3-4fd1-b1df-c81696e15c66@onelake.dfs.fabric.microsoft.com/3b946dbe-6e6e-4ca6-a2e8-3683257049cf/Tables/inventory/facs_historic_inventory")


# Create a temporary view
df_crs_inventory.createOrReplaceTempView("crs_inventory")
df_as400_inventory.createOrReplaceTempView("as400_inventory")
# df_artiva_inventory.createOrReplaceTempView("artiva_inventory")
df_cubs_inventory.createOrReplaceTempView("cubs_inventory")
df_facs_inventory.createOrReplaceTempView("facs_inventory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = """
    SELECT 
        inventory_ck,
        debt_account_id,
        placement_id,
        client_ck,
        parent_client_id,
        debtor_id,
        agent_id,
        business_unit,
        sub_business_unit,
        mid_business_unit,
        service_type,
        placement_decile,
        list_date,
        service_date,
        placement_start_date,
        placement_end_date,
        asset_type,
        debt_stage,
        debt_status,
        debt_collected,
        total_collected,
        commission_amount,
        adjusted_debt_amount,
        listed_debt_amount,
        total_debt_amount,
        debt_balance,
        recalled_flag,
        recalled_date,
        closed_flag,
        closed_date,
        statute_flag,
        statute_date,
        CAST (NULL AS INT) workable_dialable_flag,
        seed_account_id,
        source_system,
        loaded_at,
        reporting_month,
        worked_unworked,
        historic_closed_flag,
        historic_recalled_flag,
        historic_statute_flag
    FROM crs_inventory

    UNION ALL
    SELECT 
        inventory_ck,
        debt_account_id,
        placement_id,
        client_ck,
        parent_client_id,
        debtor_id,
        agent_id,
        business_unit,
        sub_business_unit,
        mid_business_unit,
        service_type,
        placement_decile,
        list_date,
        service_date,
        placement_start_date,
        placement_end_date,
        asset_type,
        debt_stage,
        debt_status,
        debt_collected,
        total_collected,
        commission_amount,
        adjusted_debt_amount,
        listed_debt_amount,
        total_debt_amount,
        debt_balance,
        recalled_flag,
        recalled_date,
        closed_flag,
        closed_date,
        statute_flag,
        statute_date,
        workable_dialable_flag,
        seed_account_id,
        source_system,
        loaded_at,
        reporting_month,
        worked_unworked,
        historic_closed_flag,
        historic_recalled_flag,
        historic_statute_flag
    FROM facs_inventory
    UNION ALL
    SELECT 
        inventory_ck,
        debt_account_id,
        placement_id,
        client_ck,
        parent_client_id,
        debtor_id,
        agent_id,
        business_unit,
        sub_business_unit,
        mid_business_unit,
        service_type,
        placement_decile,
        list_date,
        service_date,
        placement_start_date,
        placement_end_date,
        asset_type,
        debt_stage,
        debt_status,
        debt_collected,
        total_collected,
        commission_amount,
        adjusted_debt_amount,
        listed_debt_amount,
        total_debt_amount,
        debt_balance,
        recalled_flag,
        recalled_date,
        closed_flag,
        closed_date,
        statute_flag,
        statute_date,
        CAST(NULL AS INT) workable_dialable_flag,
        seed_account_id,
        source_system,
        loaded_at,
        reporting_month,
        worked_unworked,
        historic_closed_flag,
        historic_recalled_flag,
        historic_statute_flag
    FROM as400_inventory
    UNION ALL
    SELECT 
        inventory_ck,
        debt_account_id,
        placement_id,
        client_ck,
        parent_client_id,
        debtor_id,
        agent_id,
        business_unit,
        sub_business_unit,
        mid_business_unit,
        service_type,
        placement_decile,
        list_date,
        service_date,
        placement_start_date,
        placement_end_date,
        asset_type,
        debt_stage,
        debt_status,
        debt_collected,
        total_collected,
        commission_amount,
        adjusted_debt_amount,
        listed_debt_amount,
        total_debt_amount,
        debt_balance,
        recalled_flag,
        recalled_date,
        closed_flag,
        closed_date,
        statute_flag,
        statute_date,
        workable_dialable_flag,
        seed_account_id,
        source_system,
        loaded_at,
        reporting_month,
        worked_unworked,
        historic_closed_flag,
        historic_recalled_flag,
        historic_statute_flag
    FROM cubs_inventory
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute the SQL query
df_transformed = spark.sql(query)

# Register DataFrames as temporary views
df_transformed.createOrReplaceTempView("core_inventory")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Show the result (for verification)
display(df_transformed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

output_path = "abfss://d42020a3-ede3-4fd1-b1df-c81696e15c66@onelake.dfs.fabric.microsoft.com/3b946dbe-6e6e-4ca6-a2e8-3683257049cf/Tables/inventory/historic_inventory"
# Optionally, write to a target location in Delta format

df_transformed.write.format("delta").mode("overwrite").save(output_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.session.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
