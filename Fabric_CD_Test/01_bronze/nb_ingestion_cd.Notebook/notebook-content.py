# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

df_crs_inventory = spark.read.format("delta").load("abfss://6e6de13a-f2dc-4623-b3ac-431ec6d345f1@onelake.dfs.fabric.microsoft.com/3b946dbe-6e6e-4ca6-a2e8-3683257049cf/Tables/inventory/crs_historic_inventory")
df_as400_inventory = spark.read.format("delta").load("abfss://6e6de13a-f2dc-4623-b3ac-431ec6d345f1@onelake.dfs.fabric.microsoft.com/3b946dbe-6e6e-4ca6-a2e8-3683257049cf/Tables/inventory/as400_historic_inventory")
# df_artiva_inventory = spark.read.format("delta").load("abfss://d42020a3-ede3-4fd1-b1df-c81696e15c66@onelake.dfs.fabric.microsoft.com/3b946dbe-6e6e-4ca6-a2e8-3683257049cf/Tables/inventory/artiva_inventory")
df_cubs_inventory = spark.read.format("delta").load("abfss://6e6de13a-f2dc-4623-b3ac-431ec6d345f1@onelake.dfs.fabric.microsoft.com/3b946dbe-6e6e-4ca6-a2e8-3683257049cf/Tables/inventory/cubs_historic_inventory")
df_facs_inventory = spark.read.format("delta").load("abfss://d42020a3-ede3-4fd1-b1df-c81696e15c66@onelake.dfs.fabric.microsoft.com/3b946dbe-6e6e-4ca6-a2e8-3683257049cf/Tables/inventory/facs_historic_inventory")


# Create a temporary view
df_crs_inventory.createOrReplaceTempView("crs_inventory")
df_as400_inventory.createOrReplaceTempView("as400_inventory")
# df_artiva_inventory.createOrReplaceTempView("artiva_inventory")
df_cubs_inventory.createOrReplaceTempView("cubs_inventory")
df_facs_inventory.createOrReplaceTempView("facs_inventory")

var1='6e6de13a-f2dc-4623-b3ac-431ec6d345f1'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
