from pyspark.sql import DataFrame

class DeltaTableWriter:

    def saveDfIntoTable(df: DataFrame, destination_table_name):
        try:
            transfer_status = 'SUCCESS'
            failed_reason = ''
            df.write.mode("append").saveAsTable(destination_table_name)
        except Exception as e:
            rows_added = 0
            rows_deduped = 0
            rows_filtered = 0
            transfer_status = 'FAILED'
            failed_reason = str(e)      