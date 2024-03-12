# Databricks notebook source
import sys
sys.path.append('../../../')

import importlib

from src.components.pipelines.ingestion.autoloader import Autoloader

# Obtain parameters 
file_path = dbutils.widgets.get("file_path")
table_name = dbutils.widgets.get("table_name")
schema_name = dbutils.widgets.get("schema_name")
catalog_name = dbutils.widgets.get("catalog_name")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
schema_path = dbutils.widgets.get("schema_path")

# Dynamically import the schema definition
module = importlib.import_module(schema_path)
spark_schema = module.TopicSchema.getSchema()

Autoloader.autoload_to_table(spark,file_path,table_name,schema_name,catalog_name,checkpoint_path,spark_schema)
