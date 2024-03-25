from pyspark.sql import DataFrame

class DeltaTableWriter:

    def __init__(self, destination_table_name, write_mode):
        self.destination_table_name = destination_table_name
        self.write_mode = write_mode
        self.transfer_status = None
        self.failed_reason = ''

    def saveDfIntoTable(self, df: DataFrame):
        try:
            self.transfer_status = 'SUCCESS'
            self.failed_reason = ''
            df.write.mode("append").saveAsTable(self.destination_table_name)
        except Exception as e:
            self.transfer_status = 'FAILED'
            self.failed_reason = str(e)      
