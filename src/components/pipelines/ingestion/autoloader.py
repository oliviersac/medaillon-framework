# Import functions
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

class Autoloader:
    def autoload_to_table(spark, file_path: StringType,destination_table_name: StringType,checkpoint_path: StringType, schema):
        # Set the schema for the data to stream
        spark_schema = schema

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

        # Can we merge the data instead of just writing
        (streamingDF.writeStream
            .options(**writestream_options)  
            .trigger(availableNow=True)
            .toTable(f"{destination_table_name}", mode="append"))
