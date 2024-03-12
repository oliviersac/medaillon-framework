# Import functions
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructField, StringType

class Autoloader:
    
    def __init__(self, spark_schema):
        self.rows_received = 0
        self.rows_filtered = 0
        self.rows_deduped = 0
        self.rows_added = 0
        self.spark_schema = spark_schema
    
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

        # Setting stats for the import
        """
        # AnalysisException: Queries with streaming sources must be executed with writeStream.start(); 
        self.rows_received = streamingDF.count()
        self.rows_added = self.rows_received
        """

        # Can we merge the data instead of just writing
        (streamingDF.writeStream
            .options(**writestream_options)  
            .trigger(availableNow=True)
            .toTable(f"{destination_table_name}", mode="append"))
