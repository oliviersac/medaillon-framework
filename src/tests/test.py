import unittest
from pyspark.sql import SparkSession

import sys
sys.path.append('../')

from components.handlers.dataframe_handler.delta_dataframe_handler import DataFrameHandler  # Update this with the correct import path

class TestDataFrameHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize SparkSession
        cls.spark = SparkSession.builder \
            .appName("TestDataFrameHandler") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop SparkSession
        cls.spark.stop()

    def test_applyFilters(self):
        # Create test DataFrame
        data = [("A", 10), ("B", 20), ("C", 15), ("A", 30)]
        columns = ["IdStock", "Bid"]
        df = self.spark.createDataFrame(data, columns)
        
        # Define filter rules
        filter_rules = [{"column": "IdStock", "operator": ">", "value": 0}]

        # Instantiate DataFrameHandler
        handler = DataFrameHandler()

        # Apply filters
        filtered_df = handler._applyFilters(df, filter_rules)

        # Assert the expected count of rows
        self.assertEqual(filtered_df.count(), 3)

    # Similarly, write tests for other methods such as _applyConversions, _applyDeduplication, etc.
    def test_transformData(self):
        # Create test DataFrames
        data_origin = [("A", 10), ("B", 20), ("C", 15), ("A", 30)]
        columns_origin = ["IdStock", "Bid"]
        df_origin = self.spark.createDataFrame(data_origin, columns_origin)

        data_destination = [("A", 10), ("B", 20)]
        columns_destination = ["IdStock", "Bid"]
        df_destination = self.spark.createDataFrame(data_destination, columns_destination)
        
        # Define transformation rules
        transform_definition = YourTransformDefinition()  # Replace this with your actual transform definition
        handler = DataFrameHandler(transform_definition)

        # Transform data
        transformed_df = handler.transformData(df_origin, df_destination)

        # Assert the expected count of rows
        self.assertEqual(transformed_df.count(), 2)

if __name__ == '__main__':
    unittest.main()