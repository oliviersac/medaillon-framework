from pyspark.sql import DataFrame, SparkSession

class DeltaReader:

    def __init__(self, spark, read_mode, origin_table_name, log_table_name):        
        self.spark = spark
        self.read_mode = read_mode
        self.log_table_name = log_table_name
        self.origin_table_name = origin_table_name

    def readFromDelta(self):       
        if self.read_mode == "all":
            df = self.loadTable()
        else:
            df = self.loadSourceByLog()
        return df


    def _readSourceByLog(self) -> DataFrame:
        select_statement = f"""
        SELECT 
            *
        FROM 
            {self.origin_table_name}
        WHERE 
            processing_time > (
            SELECT CASE 
                    WHEN max(processing_time) IS NULL THEN '1901-03-06T20:21:31.400+00:00'            
                    ELSE max(processing_time)
                END AS processing_time
            FROM {self.log_table_name}
            WHERE origin_table = '{self.origin_table_name}'
            AND rows_received > 0
            AND transfer_status = 'SUCCESS'
            );
        """

        return self.spark.sql(select_statement)
    
    def _readTable(self) -> DataFrame:
        return self.spark.table(self.origin_table_name)
    
    def readTableCount(self):

        try:
            select_statement = f"""
            SELECT 
                count(*) as count_table
            FROM 
                {self.origin_table_name};        
            """
            df = self.spark.sql(select_statement)

            # Collect the DataFrame into a Python list of Rows
            rows = df.collect()

            # Access the value of the count_table column from the first row
            count_value = rows[0]["count_table"]
        except:
            count_value = 0

        return count_value
