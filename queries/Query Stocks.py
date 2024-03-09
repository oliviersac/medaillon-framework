# Databricks notebook source
# MAGIC %md
# MAGIC Get a list of stocks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *   FROM  dev.dev_bronze.stocks 
# MAGIC ORDER BY processing_time desc;
# MAGIC -- SELECT  *   FROM  dev.dev_silver.stocks;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *   FROM  dev.dev_silver.stocks;
# MAGIC
# MAGIC --DESCRIBE dev.dev_silver.stocks;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Activity Log

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dev.dev_activity_log.transfer_log order by processing_time desc limit 10;
# MAGIC
# MAGIC -- delete from dev.dev_activity_log.transfer_log;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from dev.dev_activity_log.transfer_log;
# MAGIC delete from dev.dev_silver.stocks;
