from pyspark.sql.functions import col, when, max as spark_max

import sys
sys.path.append('../')

import importlib
from handlers.dataframe_handler.delta_dataframe_handler import DataFrameHandler
from readers.delta_source_reader import DeltaReader
from handlers.parameters_handler.argument_parser import ArgumentParser
#from pipelines.silver.stocks_transformation_rules import TransformDefinition
from writers.transfer_log_writer import TransferLogWriter

def main(parameters):
    # Obtain parameters 
    origin_table_name = parameters.get("-origin_table_name")
    destination_table_name = parameters.get("-destination_table_name")
    log_table_name = parameters.get("-log_table_name")
    transform_definiton_path = parameters.get("-transform_definiton_path")    

    # Dynamically import the transform definition and assign it to the transformer
    module = importlib.import_module(transform_definiton_path)
    transfer_rules = module.TransformDefinition.getTransformationRules()
    transformer = DataFrameHandler(transfer_rules)

    # Pull data from source table and transform it
    source_df = DeltaReader.loadSourceByLog(spark, origin_table_name, log_table_name)
    destination_df = DeltaReader.loadTable(spark, destination_table_name)
    final_df = transformer.transformData(source_df, destination_df)
 
    # Obtain stats after processing
    rows_received = transformer.rows_received
    rows_filtered = transformer.rows_filtered
    rows_deduped = transformer.rows_deduped
    rows_added = transformer.rows_added

    # Insert data into destination table
    # Writer object here
    # and dont forget to return metrics from it too
    try:
        transfer_status = 'SUCCESS'
        failed_reason = ''
        final_df.write.mode("append").saveAsTable(destination_table_name)
    except Exception as e:
        rows_added = 0
        rows_deduped = 0
        rows_filtered = 0
        transfer_status = 'FAILED'
        failed_reason = str(e)      

    # Insert a new transfer log entry
    log_writer = TransferLogWriter(spark)
    log_writer.writeTransferLog('delta', 'dev-bronze', 'dev.dev_bronze.stocks', 'delta-bronze', 'dev-silver', destination_table_name, '', 
                                rows_received, rows_filtered, 
                                rows_deduped, rows_added, transfer_status, failed_reason)
    

if __name__ == '__main__':
  parameters = ArgumentParser.parse_arguments(sys.argv)
  main(parameters)
