# Databricks notebook source
# Set the current catalog.
spark.sql("USE CATALOG hive_metastore")

# COMMAND ----------

# Show all catalogs in the metastore.
display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

# Create a schema in the catalog that was set earlier.
spark.sql("""
  CREATE SCHEMA IF NOT EXISTS timtest_schema
  COMMENT 'A new Unity Catalog schema called timtest_schema'""")

# COMMAND ----------

# Set the current schema.
spark.sql("USE timtest_schema")


# COMMAND ----------

# Define the columns when creating a table with PySpark.
from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([StructField("id", StringType(), True)])

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP EXTERNAL LOCATION IF EXISTS silver_on_performance;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-external-locations-and-credentials#create-external-location
# MAGIC 
# MAGIC -- Run Once
# MAGIC 
# MAGIC CREATE EXTERNAL LOCATION performance_remote URL 'abfss://performance@stenterprise001.dfs.core.windows.net/'
# MAGIC     WITH (STORAGE CREDENTIAL `dbw-connector-northcentral`)
# MAGIC     COMMENT 'Performance container on stenterprise001';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTERNAL LOCATION performance_remote;

# COMMAND ----------

# Show grants on the quickstart catalog.
display(spark.sql("SHOW GRANT ON CATALOG hive_metastore"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --DROP CATALOG IF EXISTS tpcds_silver cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a catalog with a different storage root location than the metastore's.
# MAGIC 
# MAGIC CREATE CATALOG performance MANAGED LOCATION 'abfss://performance@stenterprise001.dfs.core.windows.net/';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE SCHEMA performance.raw MANAGED LOCATION 'abfss://performance@stenterprise001.dfs.core.windows.net/raw';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE performance.raw.store_sales USING PARQUET LOCATION 'abfss://performance@stenterprise001.dfs.core.windows.net/raw/tpc-ds/source_files_001GB_parquet/store_sales';

# COMMAND ----------


