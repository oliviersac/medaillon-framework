class TransferLogWriter:
    def __init__(self, spark):
        self.name = ""
        self.spark = spark

    def writeTransferLog(self,origin_type, origin_name, destination_type, destination_name, destination_table, schema_used,
                         rows_received, rows_filtered, rows_deduped, rows_added, transfer_status, failed_reason):
        # Insert a new transfer log entry
        insert_statement = f"""
        INSERT INTO dev.dev_activity_log.transfer_log(
            origin_type, origin_name, origin_table, destination_type, destination_name, destination_table, schema_used, 
            rows_received, rows_filtered, rows_deduped, rows_added,
            processing_time, transfer_status, failed_reason
        )
        VALUES(
            '{origin_type}', '{origin_name}', '{destination_type}', '{destination_name}', '{destination_table}', '{schema_used}', 
            {rows_received}, {rows_filtered}, {rows_deduped}, {rows_added},
            current_timestamp(), '{transfer_status}', '{failed_reason}'
        )
        """

        # Execute the insert statement
        spark.sql(insert_statement)
        