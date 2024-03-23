from pyspark.sql import SparkSession

class TransferLogWriter:
    def __init__(self, spark: SparkSession):
        self.name = ""
        self.spark = spark

    def writeTransferLog(self,origin_type, origin_table, destination_type, destination_table, schema_used,
                         rows_received, rows_filtered, rows_deduped, rows_added, transfer_status, failed_reason):
        # Insert a new transfer log entry
        insert_statement = f"""
        INSERT INTO dev.dev_activity_log.transfer_log(
            origin_type, origin_table, destination_type, destination_table, schema_used, 
            rows_received, rows_filtered, rows_deduped, rows_added,
            processing_time, transfer_status, failed_reason
        )
        VALUES(
            '{origin_type}', '{origin_table}', '{destination_type}', '{destination_table}', '{schema_used}', 
            {rows_received}, {rows_filtered}, {rows_deduped}, {rows_added},
            current_timestamp(), '{transfer_status}', '{failed_reason}'
        )
        """

        # Execute the insert statement
        self.spark.sql(insert_statement)
