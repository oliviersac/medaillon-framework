# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- setup a way of deleting the dupes
# MAGIC WITH list_of_dupes as 
# MAGIC (
# MAGIC   SELECT  
# MAGIC         IdStock, 
# MAGIC         Symbol, 
# MAGIC         count(*)
# MAGIC     FROM  
# MAGIC         dev.dev_silver.stocks 
# MAGIC     group by 
# MAGIC         IdStock, 
# MAGIC         Symbol
# MAGIC     having 
# MAGIC         count(*) > 1
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC list_of_dupes.* from list_of_dupes 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH list_of_dupes AS (
# MAGIC     SELECT  
# MAGIC         IdStock, 
# MAGIC         Symbol, 
# MAGIC         MAX(processing_time) AS max_processing_time
# MAGIC     FROM  
# MAGIC         dev.dev_silver.stocks 
# MAGIC     GROUP BY 
# MAGIC         IdStock, 
# MAGIC         Symbol 
# MAGIC     HAVING 
# MAGIC         COUNT(*) > 1
# MAGIC )
# MAGIC
# MAGIC DELETE FROM dev.dev_silver.stocks 
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM list_of_dupes 
# MAGIC     WHERE 
# MAGIC         dev.dev_silver.stocks.IdStock = list_of_dupes.IdStock 
# MAGIC         AND dev.dev_silver.stocks.Symbol = list_of_dupes.Symbol 
# MAGIC         AND dev.dev_silver.stocks.processing_time = list_of_dupes.max_processing_time
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Activity Log

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*), sum(rows_received), sum(rows_processed) from dev.dev_activity_log.transfer_log;
# MAGIC -- select * from dev.dev_activity_log.transfer_log

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup
