import sys
sys.path.append('../')

import importlib
from pyspark.sql import SparkSession
from handlers.dataframe_handler.delta_dataframe_handler import DataFrameHandler
from readers.delta_source_reader import DeltaReader
from handlers.parameters_handler.argument_parser import ArgumentParser
from writers.delta_table_writer import DeltaTableWriter
from writers.transfer_log_writer import TransferLogWriter
from common.spark_session_builder import SparkSessionBuilder

def main(parameters: list, spark: SparkSession) -> None:
    # Obtain parameters that defines the task
    origin_table_name = parameters.get("-origin_table_name")
    destination_table_name = parameters.get("-destination_table_name")
    log_table_name = parameters.get("-log_table_name")
    transform_definiton_path = parameters.get("-transform_definiton_path")
    source_mode = parameters.get("-source_mode")
    write_mode = parameters.get("-write_mode")
    job_type = "Delta Transfer"
    schema_used = ""

    # Dynamically import the transform definition and assign it to the transformer
    module = importlib.import_module(transform_definiton_path)
    transformDefinition = module.TransformDefinition
    transformer = DataFrameHandler(transformDefinition)

    # Define readers and writers
    delta_reader_origin = DeltaReader(spark, source_mode, origin_table_name, log_table_name)
    delta_reader_destination = DeltaReader(spark, "all", origin_table_name, log_table_name)
    delta_table_writer = DeltaTableWriter(destination_table_name, write_mode)
    delta_log_writer = TransferLogWriter(spark, 'Delta', 'Delta')

    #transformer.transformDefinition


    # Obtain data and transform it
    source_df = delta_reader_origin.readFromDelta()
    if write_mode != 'all':
        destination_df = delta_reader_destination.readFromDelta()
    else:
        destination_df = None
    final_df = transformer.transformData(spark, source_df, destination_df)
 
    # Insert data into destination table    
    delta_table_writer.saveDfIntoTable(final_df)

    # Log Transfer in the log table
    delta_log_writer.writeTransferLogFromComponents(delta_reader_origin, transformer, delta_table_writer)


if __name__ == '__main__':
    parameters = ArgumentParser.parse_arguments(sys.argv)

    # Initialize SparkSession
    spark_session_builder = SparkSessionBuilder("Datalake-test")
    spark = spark_session_builder.buildSparkSession()

    main(parameters, spark)
