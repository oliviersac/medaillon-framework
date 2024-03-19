import sys
sys.path.append('../')

import importlib

from pipelines.ingestion.autoloader import Autoloader
from handlers.parameters_handler.argument_parser import ArgumentParser
from writers.transfer_log_writer import TransferLogWriter
from readers.delta_source_reader import DeltaReader


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

    # Counting rows before autoload
    table_count_origin = DeltaReader.loadTableCount(spark, destination_table_name)

    # Doing autoload
    autoloader = Autoloader(spark_schema)
    autoloader.autoload_to_table(spark,file_path,destination_table_name,checkpoint_path)

     # Counting rows after autoload and setting the value for the log table
    table_count_end = DeltaReader.loadTableCount(spark, destination_table_name)
    rows_received = table_count_end - table_count_origin
    rows_added = rows_received


    # Insert a new transfer log entry
    log_writer = TransferLogWriter(spark)
    log_writer.writeTransferLog('S3', 'S3-dev-landing', 'delta', destination_table_name, '', 
                                rows_received, autoloader.rows_filtered, 
                                autoloader.rows_deduped, rows_added, 'SUCCESS', '')

if __name__ == '__main__':
  parameters = ArgumentParser.parse_arguments(sys.argv)
  main(parameters)
