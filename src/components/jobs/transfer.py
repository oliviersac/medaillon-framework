from pyspark.sql.functions import col, when, max as spark_max

import sys
sys.path.append('../')
from handlers.dataframe_handler.delta_dataframe_handler import DataFrameHandler
from readers.delta_source_reader import DeltaReader
from pipelines.silver.stocks_transformation_rules import TransformDefinition

# Obtain stats from the df
"""
schema = '' # Define schema
file_path = '/mnt/dev-landing/stocks/'
table_name = 'stocks'
schema_name = 'dev_bronze'
catalog_name = 'dev'
checkpoint_path = ''
"""

def main(parameters):
    # Obtain parameters 
    origin_table_name = parameters.get("-origin_table_name")
    destination_table_name = parameters.get("-destination_table_name")
    log_table_name = parameters.get("-log_table_name")

    # ################################################################################################
    # Pull data from bronze to silver and apply filtering logic
    # ################################################################################################

    # Pull data from source table
    source_df = DeltaReader.loadSourceByLog(spark, origin_table_name, log_table_name)

    # Obtain count of items in the df
    rows_received = source_df.count()

    # Transform data before sending it to the next layer
    transformer = DataFrameHandler(spark, TransformDefinition)
    final_df = transformer.transformData(source_df)
 
    # Obtain stats after processing
    rows_received = transformer.rows_received
    rows_filtered = transformer.rows_filtered
    rows_deduped = transformer.rows_deduped
    rows_added = transformer.rows_added

    # Insert data into destination table
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
    insert_statement = f"""
    INSERT INTO dev.dev_activity_log.transfer_log(
        origin_type, origin_name, origin_table, destination_type, destination_name, destination_table, schema_used, 
        rows_received, rows_filtered, rows_deduped, rows_added,
        processing_time, transfer_status, failed_reason
    )
    VALUES(
        'delta', 'dev-bronze', 'dev.dev_bronze.stocks', 'delta', 'dev-silver', 'dev.dev_silver.stocks', '', 
        {rows_received}, {rows_filtered}, {rows_deduped}, {rows_added},
        current_timestamp(), '{transfer_status}', \"{failed_reason}\"
    )
    """

    # Execute the insert statement
    spark.sql(insert_statement)

def parse_arguments():
    # Remove the first argument (script name)
    args = sys.argv[1:]

    # Parse arguments
    parameters = {}
    i = 0
    while i < len(args):
        # Check if parameter starts with '-' and has a corresponding value
        if args[i].startswith('-') and i + 1 < len(args):
            parameters[args[i]] = args[i + 1]
            i += 1
        else:
            print("Invalid parameter:", args[i])
        i += 1

    return parameters


if __name__ == '__main__':
  parameters = parse_arguments()
  main(parameters)