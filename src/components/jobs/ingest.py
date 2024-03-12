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

    Autoloader.autoload_to_table(spark,file_path,destination_table_name,checkpoint_path,spark_schema)

    print("Successfully autoloaded")

if __name__ == '__main__':
  parameters = ArgumentParser.parse_arguments(sys.argv)
  main(parameters)
