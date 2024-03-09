# Databricks notebook source
from pyspark.sql.functions import col, when

# Obtain stats from the df
schema = '' # Define schema
file_path = '/mnt/dev-landing/stocks/'
table_name = 'stocks'
schema_name = 'dev_bronze'
catalog_name = 'dev'
checkpoint_path = ''

# ################################################################################################
# Get the max processing_datetime that was registered in the transfer_log table
# ################################################################################################
select_statement = """
    SELECT        
        CASE 
            WHEN max(processing_time) IS NULL THEN '1901-03-06T20:21:31.400+00:00'            
            ELSE max(processing_time)
        END AS processing_time
    FROM 
        dev.dev_activity_log.transfer_log
    WHERE        
        origin_table = 'dev.dev_bronze.stocks'
        and rows_received > 0
        and transfer_status = 'SUCCESS'
"""
transfer_log_df = spark.sql(select_statement)

# Obtain last processed_datetime
last_process_datetime = transfer_log_df.collect()[0]["processing_time"]


# ################################################################################################
# Pull data from bronze to silver and apply filtering logic
# ################################################################################################

# Pull data from source table
select_statement = f"SELECT * FROM dev.dev_bronze.stocks where processing_time > '{last_process_datetime}'"
#select_statement = f"SELECT * FROM dev.dev_bronze.stocks"
source_df = spark.sql(select_statement)

# Obtain count of items in the df
rows_received = source_df.count()






# ################################################################################################
# Transformations
# ################################################################################################

# OK Do filtering before sending in silver layer
# OK Converting formats: Like the dates like lastupdatetime its int instead of timestamp
# OK Deduplicate: We can reproduce if we delete everything except for silver
# Validation rules should be inside an object that would be defined elsewhere


## Filtering Data
## #############################
validation_rules = [
    {"column": "IdStock", "operator": ">", "value": 0},
    {"column": "Name", "operator": "<>", "value": ''}
]

filters = []
for rule in validation_rules:
    filter_expr = f"{rule['column']} {rule['operator']} '{rule['value']}'"
    filters.append(filter_expr)

condition = " AND ".join(filters)

filtered_df = source_df.filter(condition)

## Converting Data
## #############################
conversion_rules = {
    "LastTradeTime": lambda x: when(x != "", x.cast("string")).otherwise(None),
    "LastTradeDate": lambda x: when(x != "", x.cast("string")).otherwise(None),
    "Insertdatetime": lambda x: when(x != "", x.cast("string")).otherwise(None),
    "Lastupdatetime": lambda x: when(x != "", x.cast("string")).otherwise(None)
}

# Apply conversion rules
for column, conversion_func in conversion_rules.items():
    filtered_df = filtered_df.withColumn(column, conversion_func(col(column)))

# Obtain count after filtering
rows_processed = filtered_df.count()


## Deduplicating Data
## #############################

# Load the target table into a DataFrame
target_table_df = spark.table("dev.dev_silver.stocks")

dedupe_columns = ["IdStock", "Symbol", "LastTradeDate"]

# Deduplicate the DataFrame based on two specific columns
deduplicated_df = filtered_df.dropDuplicates(dedupe_columns)

# Initialize the join condition
join_condition = None

# Construct the join condition dynamically
for column in dedupe_columns:
    condition = (deduplicated_df[column] == target_table_df[column])
    if join_condition is None:
        join_condition = condition
    else:
        join_condition = join_condition & condition

# Perform left anti join to identify rows that already exist in the target table
final_df = deduplicated_df.join(target_table_df, join_condition, "left_anti")



# ################################################################################################
# Add data in silver
# ################################################################################################

# Insert data into destination table
try:
    transfer_status = 'SUCCESS'
    failed_reason = ''
    final_df.write.mode("append").saveAsTable("dev.dev_silver.stocks")    
except Exception as e:
    rows_processed = 0
    transfer_status = 'FAILED'
    failed_reason = str(e)
    


# ################################################################################################
# Add transfer log entry
# ################################################################################################

# Insert a new transfer log entry
insert_statement = f"""
INSERT INTO dev.dev_activity_log.transfer_log(
    origin_type, origin_name, origin_table, destination_type, destination_name, destination_table, schema_used, rows_received, rows_processed,
    processing_time, transfer_status, failed_reason
)
VALUES(
    'delta', 'dev-bronze', 'dev.dev_bronze.stocks', 'delta', 'dev-silver', 'dev.dev_silver.stocks', '', {rows_received}, {rows_processed},
    current_timestamp(), '{transfer_status}', \"{failed_reason}\"
)
"""

# Execute the insert statement
spark.sql(insert_statement)


