import sys
sys.path.append('../../../')

import importlib

from src.components.pipelines.ingestion.autoloader import Autoloader
from src.components.handlers.parameters_handler.argument_parser import ArgumentParser


def main(parameters):
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
    autoloader = Autoloader(spark_schema)
    autoloader.autoload_to_table(spark,file_path,destination_table_name,checkpoint_path)

    # Getting stats 
    rows_received = autoloader.rows_received
    rows_filtered = autoloader.rows_filtered
    rows_deduped = autoloader.rows_deduped
    rows_added = autoloader.rows_added

    # Insert a new transfer log entry
    insert_statement = f"""
    INSERT INTO dev.dev_activity_log.transfer_log(
        origin_type, origin_name, origin_table, destination_type, destination_name, destination_table, schema_used, 
        rows_received, rows_filtered, rows_deduped, rows_added,
        processing_time, transfer_status, failed_reason
    )
    VALUES(
        'S3', 'dev-landing', 'S3', 'delta', 'dev-bronze', 'dev.dev_bronze.stocks', '', 
        {rows_received}, {rows_filtered}, {rows_deduped}, {rows_added},
        current_timestamp(), 'SUCCESS', ''
    )
    """

    # Execute the insert statement
    spark.sql(insert_statement)

if __name__ == '__main__':
  parameters = ArgumentParser.parse_arguments(sys.argv)
  main(parameters)
