# Databricks notebook source
# MAGIC %md
# MAGIC ## Create activity log 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS dev.dev_activity_log.transfer_log;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.dev_activity_log.transfer_log(
# MAGIC     origin_type varchar(255) COMMENT 'The type of the origin (dev_bronze, dev_silver, dev-landing)',
# MAGIC     origin_name varchar(255) COMMENT 'The name of the origin (S3, delta_table, source)',
# MAGIC     origin_table varchar(255) COMMENT 'The table of the origin (dev.dev_bronze.stocks)', 
# MAGIC     destination_type varchar(255) COMMENT 'The type of the destination (dev_bronze, dev_silver, dev-landing)',
# MAGIC     destination_name varchar(255) COMMENT 'The name of the destination (S3, delta_table, source)',
# MAGIC     destination_table varchar(255) COMMENT 'The table of the destination (stocks)', 
# MAGIC     schema_used varchar(255) COMMENT 'The schema that was used to store the data',
# MAGIC     rows_received INT COMMENT 'The number of rows that were received for processing',
# MAGIC     rows_processed INT COMMENT 'The number of rows that were processed',
# MAGIC     processing_time TIMESTAMP COMMENT 'The date that the processing was done',
# MAGIC     transfer_status varchar(255) COMMENT 'SUCCESS or FAIL after processing',
# MAGIC     failed_reason varchar(1000) COMMENT 'The reason why the transfer failed'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create table for stocks silver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS dev.dev_silver.stocks;
# MAGIC -- create table dev.dev_silver.stocks like dev.dev_bronze.stocks;
# MAGIC
# MAGIC create table dev.dev_silver.stocks(
# MAGIC     Idstock	int,
# MAGIC     Symbol	string,
# MAGIC     Name	string,
# MAGIC     PercentChange	float,
# MAGIC     Volume	float,
# MAGIC     Currency	string,
# MAGIC     Bid	float,
# MAGIC     Ask	float,
# MAGIC     DaysLow	float,
# MAGIC     DaysHigh	float,
# MAGIC     AverageDailyVolume	float,
# MAGIC     BookValue	float,
# MAGIC     MarketCapitalization	string,
# MAGIC     ChangeValue	float,
# MAGIC     BidRealtime	float,
# MAGIC     AskRealtime	float,
# MAGIC     ChangeRealtime	float,
# MAGIC     DividendShare	float,
# MAGIC     LastTradeDate	string,
# MAGIC     EarningsShare	float,
# MAGIC     EPSEstimateNextYear	float,
# MAGIC     EPSEstimateNextQuarter	float,
# MAGIC     YearLow	float,
# MAGIC     YearHigh	float,
# MAGIC     EBITDA	string,
# MAGIC     ChangeFromYearLow	float,
# MAGIC     PercentChangeFromYearLow	float,
# MAGIC     ChangeFromYearHigh	float,
# MAGIC     LastTradePriceOnly	float,
# MAGIC     DaysRange	string,
# MAGIC     FiftydayMovingAverage	float,
# MAGIC     TwoHundreddayMovingAverage	float,
# MAGIC     ChangeFromTwoHundreddayMovingAverage	float,
# MAGIC     PercentChangeFromTwoHundreddayMovingAverage	float,
# MAGIC     ChangeFromFiftydayMovingAverage	float,
# MAGIC     PercentChangeFromFiftydayMovingAverage	float,
# MAGIC     Open	float,
# MAGIC     PreviousClose	float,
# MAGIC     ChangeinPercent	float,
# MAGIC     PriceSales	float,
# MAGIC     PriceBook	float,
# MAGIC     PERatio	float,
# MAGIC     DividendPayDate	string,
# MAGIC     ShortRatio	float,
# MAGIC     LastTradeTime	string,
# MAGIC     DividendYield	float,
# MAGIC     Insertdatetime	string,
# MAGIC     Lastupdatetime	string,
# MAGIC     EPSEstimateCurrentYear	float,
# MAGIC     IdSector	int,
# MAGIC     IdIndustry	float,
# MAGIC     Sector	string,
# MAGIC     Industry	string,
# MAGIC     Isvalid	int,
# MAGIC     industry_details	struct<IdSector:int,Sector:string,IdIndustry:float,Industry:string>,
# MAGIC     ramdom_array	array<int>,
# MAGIC     _file_path	string,
# MAGIC     _rescued_data	string,
# MAGIC     source_file	string,
# MAGIC     processing_time	timestamp
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
