import sys
sys.path.append('../')

import importlib

from pipelines.ingestion.autoloader import Autoloader
from handlers.parameters_handler.argument_parser import ArgumentParser
from writers.transfer_log_writer import TransferLogWriter


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

    # Insert a new transfer log entry
    log_writer = TransferLogWriter(spark)
    log_writer.writeTransferLog('S3', 'dev-landing', 'S3', 'delta', 'dev-bronze', destination_table_name, '', 
                                autoloader.rows_received, autoloader.rows_filtered, 
                                autoloader.rows_deduped, autoloader.rows_added, 'SUCCESS', '')

if __name__ == '__main__':
  parameters = ArgumentParser.parse_arguments(sys.argv)
  main(parameters)
