from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

class Autoloader:
    @staticmethod
    def autoload_to_table(spark, file_path, table_name, schema_name, catalog_name, checkpoint_path, schema):
        # Set the schema for the data to stream
        spark_schema = schema

        # Add column(s) specific to the Autoloader
        spark_schema.add(StructField("_file_path", StringType(), True))

        # Defining the cloudFiles options and the checkpoint path for registering the schema using schema inference on first load
        cloudfile_options = {
            "cloudFiles.format": "json",
            "cloudFiles.schemaLocation": checkpoint_path,
            "cloudfiles.useNotifications": "true",
            "cloudFiles.schemaEvolutionMode": "rescue",
            "cloudFiles.maxFilesPerTrigger": 1000,
            "cloudFiles.maxBytesPerTrigger": "10g"
        }

        # Obtaining data from spark readstream
        streamingDF = (spark.readStream
            .format("cloudFiles")
            .options(**cloudfile_options)
            .schema(spark_schema)
            .load(file_path)
            .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time")))

        # Start the streaming query and write to table
        query = (streamingDF.writeStream
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")  # Merge schema option  
            .trigger(availableNow=True)
            .toTable(f"{catalog_name}.{schema_name}.{table_name}", mode="append"))

        # Start the streaming query
        query = query.start()

        # Wait for the termination of the streaming query
        query.awaitTermination()

        # Collect relevant information
        num_rows_processed = streamingDF.count()
        processing_time = 0 # Calculate processing time
        
        # Create DataFrame with collected information
        log_data = [(None, None, 'S3', 'dev-landing', 'dev_bronze', 'stocks', None, num_rows_processed, processing_time, None, None)]
        log_schema = StructType([
            StructField("batch_id", LongType(), True),
            StructField("parent_batch_id", LongType(), True),
            StructField("origin_type", StringType(), True),
            StructField("origin_name", StringType(), True),
            StructField("destination_schema", StringType(), True),
            StructField("destination_table", StringType(), True),
            StructField("schema_used", StringType(), True),
            StructField("rows_processed", IntegerType(), True),
            StructField("processing_time", IntegerType(), True),
            StructField("processing_datetime", TimestampType(), True),
            StructField("last_record_date_added", TimestampType(), True)
        ])
        log_df = spark.createDataFrame(log_data, schema=log_schema)

        # Write DataFrame to transfer log table
        log_df.write.mode("append").format("delta").saveAsTable("dev.dev_activity_log.transfer_log")

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Autoloader") \
        .getOrCreate()

    # Define schema and other parameters
    schema = '' # Define schema
    file_path = '/mnt/dev-landing/stocks/'
    table_name = 'stocks'
    schema_name = 'dev_bronze'
    catalog_name = 'dev'
    checkpoint_path = '/mnt/AutoLoader/_checkpoint/stocks'

    # Run the autoload process
    Autoloader.autoload_to_table(spark, file_path, table_name, schema_name, catalog_name, checkpoint_path, schema)

    # Stop SparkSession
    spark.stop()