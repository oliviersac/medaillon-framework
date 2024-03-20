# Databricks notebook source
# Drop the bronze table
spark.sql("drop table if exists dev.dev_bronze.stocks")

# Delete content in the silver table
spark.sql("delete from dev.dev_silver.stocks")
spark.sql("delete from dev.dev_gold.daily_bid_stats")
spark.sql("delete from dev.dev_gold.stocks_top50_highest_price")
spark.sql("delete from dev.dev_gold.stocks_top50_lowest_price")

# Delete content in the transfer_log table
spark.sql("delete from dev.dev_activity_log.transfer_log")

# Remove everything in the checkpoint path
dbutils.fs.rm('/mnt/AutoLoader/_checkpoint/stocks', True)

