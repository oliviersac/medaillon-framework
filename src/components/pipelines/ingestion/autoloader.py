# Import functions
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructField, StringType

import sys
sys.path.append('../../../')


from src.components.writers.transfer_log_writer import TransferLogWriter

class Autoloader:
    
    def __init__(self, spark_schema, destination_table_name, log_writer: TransferLogWriter):
        self.rows_received = 0
        self.rows_filtered = 0
        self.rows_deduped = 0
        self.rows_added = 0
        self.spark_schema = spark_schema
        self.destination_table_name = destination_table_name
        self.write_mode = "append"
        self.query = None
        self.log_writer = log_writer
    
    def process_batch(self, batch_df, batch_id):
        # Count the number of rows received in the current batch
        rows_received_in_batch = batch_df.count()
        self.rows_received += rows_received_in_batch
        
        # You can perform any additional processing or analysis on the batch_df here
        
        # Example: Write batch_df to a destination table
        batch_df.write.mode(self.write_mode).saveAsTable(self.destination_table_name)

        # Write to log
        self.log_writer.writeTransferLog('S3', 'S3-dev-landing', 'delta', self.destination_table_name, '', 
                                rows_received_in_batch, self.rows_filtered, 
                                self.rows_deduped, rows_received_in_batch, 'SUCCESS', '')


    def autoload_to_table(self, spark, file_path: StringType,destination_table_name: StringType,checkpoint_path: StringType):
        # Set the schema for the data to stream
        spark_schema = self.spark_schema

        # Add column(s) specific to the Autoloader
        spark_schema.add(StructField("_file_path", StringType(), True))

        # Defining the cloudfiles options and the checkpoint path for registering the schema using schema inference on first load
        cloudfile_options = {
            "cloudFiles.format": "json",
            "cloudFiles.schemaLocation": checkpoint_path,
            "cloudfiles.useNotifications": "true",
            "cloudFiles.schemaEvolutionMode": "rescue",
            "cloudFiles.maxFilesPerTrigger": 5000,
            "cloudFiles.maxBytesPerTrigger": "10g"
        }

        # Defining the write stream options for saving the data
        writestream_options = {
            "checkpointLocation" : checkpoint_path,
            "mergeSchema": "true" # Merging the new schema with the existing one automatically 
        }

        # Obtaining data from spark readstream
        streamingDF = (spark.readStream
            .format("cloudFiles")
            .options(**cloudfile_options)
            .schema(spark_schema)
            .load(file_path)
            .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time")))
        
        # Start the streaming query
        query = (streamingDF.writeStream
                 .options(**writestream_options)
                 .foreachBatch(self.process_batch)
                 .outputMode("append")
                 .start())
        
        # Store the query object
        self.query = query
