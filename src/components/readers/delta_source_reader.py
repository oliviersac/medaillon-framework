class DeltaReader:
    def loadSourceByLog(spark, origin_full_table_name, log_table_name):
        select_statement = f"""
        SELECT 
            *
        FROM 
            {origin_full_table_name}
        WHERE 
            processing_time > (
            SELECT CASE 
                    WHEN max(processing_time) IS NULL THEN '1901-03-06T20:21:31.400+00:00'            
                    ELSE max(processing_time)
                END AS processing_time
            FROM {log_table_name}
            WHERE origin_table = '{origin_full_table_name}'
            AND rows_received > 0
            AND transfer_status = 'SUCCESS'
            );
        """

        return spark.sql(select_statement)
    