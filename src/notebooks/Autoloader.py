# Databricks notebook source
import sys
sys.path.append('../../')

import importlib

from src.components.pipelines.autoloader.autoloader import Autoloader
#from src.topics.stocks.version_101 import StockData

# Obtain parameters 
file_path = dbutils.widgets.get("file_path")
table_name = dbutils.widgets.get("table_name")
schema_name = dbutils.widgets.get("schema_name")
catalog_name = dbutils.widgets.get("catalog_name")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
schema_path = dbutils.widgets.get("schema_path")

# Dynamically import the module
module = importlib.import_module(schema_path)
StockData = module.StockData

# Set the schema for the data to stream
spark_schema = StockData.getSchema()

Autoloader.autoload_to_table(spark,file_path,table_name,schema_name,catalog_name,checkpoint_path,spark_schema)
