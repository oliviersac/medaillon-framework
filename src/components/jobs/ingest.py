import sys
sys.path.append('../')

import importlib
from pyspark.sql import SparkSession
from pipelines.ingestion.autoloader import Autoloader
from handlers.parameters_handler.argument_parser import ArgumentParser
from writers.transfer_log_writer import TransferLogWriter
from common.spark_session_builder import SparkSessionBuilder

def main(parameters, spark: SparkSession) -> None:
    # Obtain parameters 
    file_path = parameters.get("-file_path")
    destination_table_name = parameters.get("-destination_table_name")
    log_table_name = parameters.get("-log_table_name")
    checkpoint_path = parameters.get("-checkpoint_path")
    schema_path = parameters.get("-schema_path")

    # Dynamically import the schema definition
    module = importlib.import_module(schema_path)
    spark_schema = module.TopicSchema.getSchema()

    # Doing autoload
    autoloader = Autoloader(spark_schema, destination_table_name)
    autoloader.autoload_to_table(spark,file_path,destination_table_name,checkpoint_path)

     # Counting rows after autoload and setting the value for the log table
    rows_received = 0
    rows_added = 0

    # Insert a new transfer log entry
    log_writer = TransferLogWriter(spark)
    log_writer.writeTransferLog('S3', 'S3-dev-landing', 'delta', destination_table_name, '', 
                                rows_received, autoloader.rows_filtered, 
                                autoloader.rows_deduped, rows_added, 'SUCCESS', '')

if __name__ == '__main__':
    parameters = ArgumentParser.parse_arguments(sys.argv)

    # Initialize SparkSession
    spark_session_builder = SparkSessionBuilder("Datalake-test")
    spark = spark_session_builder.buildSparkSession()

    main(parameters, spark)
