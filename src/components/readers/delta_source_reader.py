class DeltaReader:
    def loadSourceByLog(spark, origin_table_name, log_table_name):
        select_statement = f"""
        SELECT 
            *
        FROM 
            {origin_table_name}
        WHERE 
            processing_time > (
            SELECT CASE 
                    WHEN max(processing_time) IS NULL THEN '1901-03-06T20:21:31.400+00:00'            
                    ELSE max(processing_time)
                END AS processing_time
            FROM {log_table_name}
            WHERE origin_table = '{origin_table_name}'
            AND rows_received > 0
            AND transfer_status = 'SUCCESS'
            );
        """

        return spark.sql(select_statement)
    
    def loadTable(spark, table_name):
        return spark.table(table_name)
    
    def loadTableCount(spark, table_name):

        try:
            select_statement = f"""
            SELECT 
                count(*) as count_table
            FROM 
                {table_name};        
            """
            df = spark.sql(select_statement)

            # Collect the DataFrame into a Python list of Rows
            rows = df.collect()

            # Access the value of the count_table column from the first row
            count_value = rows[0]["count_table"]
        except:
            count_value = 0

        return count_value
