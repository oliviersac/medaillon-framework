# Databricks notebook source
# MAGIC %md
# MAGIC Get a list of stocks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- SELECT  *   FROM  dev.dev_bronze.stocks ORDER BY processing_time desc;
# MAGIC SELECT  count(*)   FROM  dev.dev_bronze.stocks 
# MAGIC
# MAGIC
# MAGIC
# MAGIC --select processing_time, count(*) from dev.dev_bronze.stocks group by processing_time order by processing_time
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  count(*)   FROM  dev.dev_silver.stocks;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Activity Log

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select count(*), sum(rows_received), sum(rows_processed) from dev.dev_activity_log.transfer_log;
# MAGIC  select * from dev.dev_activity_log.transfer_log order by processing_time;
# MAGIC
# MAGIC -- delete from dev.dev_activity_log.transfer_log where processing_time >= '2024-03-12T02:06:49.508+00:00'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete from dev.dev_activity_log.transfer_log;
# MAGIC -- delete from dev.dev_silver.stocks;
