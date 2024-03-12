from pyspark.sql.functions import col, when, max as spark_max

import sys
sys.path.append('../../')
from handlers.dataframe_handler.delta_dataframe_handler import DataFrameHandler
from pipelines.silver.stocks_transformation_rules import TransformDefinition

# Obtain stats from the df
schema = '' # Define schema
file_path = '/mnt/dev-landing/stocks/'
table_name = 'stocks'
schema_name = 'dev_bronze'
catalog_name = 'dev'
checkpoint_path = ''

# ################################################################################################
# Pull data from bronze to silver and apply filtering logic
# ################################################################################################

# Pull data from source table
select_statement = f"""
SELECT 
    *
FROM 
    dev.dev_bronze.stocks
WHERE 
    processing_time > (
    SELECT CASE 
               WHEN max(processing_time) IS NULL THEN '1901-03-06T20:21:31.400+00:00'            
               ELSE max(processing_time)
           END AS processing_time
    FROM dev.dev_activity_log.transfer_log
    WHERE origin_table = 'dev.dev_bronze.stocks'
      AND rows_received > 0
      AND transfer_status = 'SUCCESS'
    );
"""
#select_statement = f"SELECT * FROM dev.dev_bronze.stocks"
source_df = spark.sql(select_statement)

# Obtain count of items in the df
rows_received = source_df.count()

# Transform data before sending it to the next layer
transformer = DataFrameHandler(spark, TransformDefinition)
final_df = transformer.transformData(source_df)
rows_processed = final_df.count()

# Obtain stats after processing
rows_received = transformer.rows_received
rows_filtered = transformer.rows_filtered
rows_deduped = transformer.rows_deduped
rows_added = transformer.rows_added

# Insert data into destination table
try:
    transfer_status = 'SUCCESS'
    failed_reason = ''
    final_df.write.mode("append").saveAsTable("dev.dev_silver.stocks")    
except Exception as e:
    rows_added = 0
    transfer_status = 'FAILED'
    failed_reason = str(e)
    


# ################################################################################################
# Add transfer log entry
# ################################################################################################

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
