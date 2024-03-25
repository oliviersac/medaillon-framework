from pyspark.sql import SparkSession
from handlers.dataframe_handler.delta_dataframe_handler import DataFrameHandler
from writers.delta_table_writer import DeltaTableWriter
from readers.delta_source_reader import DeltaReader

class TransferLogWriter:
    def __init__(self, spark: SparkSession, origin_type, destination_type):
        self.name = ""
        self.spark = spark
        self.origin_type = origin_type
        self.destination_type = destination_type

    def writeTransferLog(self, origin_table, destination_table, schema_used, 
            rows_received, rows_filtered, rows_deduped, rows_added, transfer_status, failed_reason):
        # Insert a new transfer log entry
        insert_statement = f"""
        INSERT INTO dev.dev_activity_log.transfer_log(
            origin_type, origin_table, destination_type, destination_table, schema_used, 
            rows_received, rows_filtered, rows_deduped, rows_added,
            processing_time, transfer_status, failed_reason
        )
        VALUES(
            '{self.origin_type}', '{origin_table}', '{self.destination_type}', '{destination_table}', '{schema_used}', 
            {rows_received}, {rows_filtered}, {rows_deduped}, {rows_added},
            current_timestamp(), '{transfer_status}', '{failed_reason}'
        )
        """

        # Execute the insert statement
        self.spark.sql(insert_statement)


    def writeTransferLogFromComponents(self, delta_reader_origin: DeltaReader, transformer: DataFrameHandler, delta_table_writer: DeltaTableWriter):
        
        transfer_status = delta_table_writer.transfer_status
        rows_received = transformer.rows_received
        rows_filtered = transformer.rows_filtered
        rows_deduped = transformer.rows_deduped
        rows_added = transformer.rows_added
        destination_table = delta_table_writer.destination_table_name
        origin_table = delta_reader_origin.origin_table_name
        failed_reason = delta_table_writer.failed_reason
        schema_used = ''
        
        
        # Insert a new transfer log entry
        insert_statement = f"""
        INSERT INTO dev.dev_activity_log.transfer_log(
            origin_type, origin_table, destination_type, destination_table, schema_used, 
            rows_received, rows_filtered, rows_deduped, rows_added,
            processing_time, transfer_status, failed_reason
        )
        VALUES(
            '{self.origin_type}', '{origin_table}', '{self.destination_type}', '{destination_table}', '{schema_used}', 
            {rows_received}, {rows_filtered}, {rows_deduped}, {rows_added},
            current_timestamp(), '{transfer_status}', '{failed_reason}'
        )
        """

        # Execute the insert statement
        self.spark.sql(insert_statement)
    