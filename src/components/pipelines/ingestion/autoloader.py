# Import functions
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructField, StringType

class Autoloader:
    
    def __init__(self, spark_schema, destination_table_name):
        self.rows_received = 0
        self.rows_filtered = 0
        self.rows_deduped = 0
        self.rows_added = 0
        self.spark_schema = spark_schema
        self.destination_table_name = destination_table_name
        self.write_mode = "append"
    
    def process_batch(self, batch_df, batch_id):
        # Count the number of rows received in the current batch
        rows_received_in_batch = batch_df.count()
        self.rows_received += rows_received_in_batch
        
        # You can perform any additional processing or analysis on the batch_df here
        
        # Example: Write batch_df to a destination table
        batch_df.write.mode(self.write_mode).saveAsTable(self.destination_table_name)


    def autoload_to_table(self, spark, file_path: StringType, destination_table_name: StringType, checkpoint_path: StringType):
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
            "cloudFiles.maxFilesPerTrigger": 2000,
            "cloudFiles.maxBytesPerTrigger": "10g"
        }

        # Obtaining data from spark readstream
        streamingDF = (spark.readStream
            .format("cloudFiles")
            .options(**cloudfile_options)
            .schema(spark_schema)
            .load(file_path)
            .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time")))
        
        # Apply processing to each batch of the streaming dataframe
        query = (streamingDF
                 .writeStream
                 .foreachBatch(self.process_batch)
                 .outputMode("append")
                 .option("checkpointLocation", checkpoint_path)
                 .start())
        
        # Await termination
        query.awaitTermination()
