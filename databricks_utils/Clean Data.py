# Databricks notebook source
# Obtain parameters 
'''
table_name = dbutils.widgets.get("table_name")
schema_name = dbutils.widgets.get("schema_name")
catalog_name = dbutils.widgets.get("catalog_name")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
schema_path = dbutils.widgets.get("schema_path")
'''
file_path = '/mnt/dev-landing/stocks/'
table_name = 'stocks'
schema_name = 'dev_bronze'
catalog_name = 'dev'
checkpoint_path = '/mnt/AutoLoader/_checkpoint/stocks'
schema_path = 'src.topics.stocks.version_100'


full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

# Drop the bronze table
spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

# Delete content in the silver table
spark.sql("delete from dev.dev_silver.stocks")

# Delete content in the transfer_log table
spark.sql("delete from dev.dev_activity_log.transfer_log")

# Remove everything in the checkpoint path
dbutils.fs.rm(checkpoint_path, True)

