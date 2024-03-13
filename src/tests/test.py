import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from my_module import DataFrameHandler  # Replace 'my_module' with the actual module name

class TestDataFrameHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("test_app") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop Spark session after testing
        cls.spark.stop()

    def setUp(self):
        # Create an instance of DataFrameHandler for testing
        self.handler = DataFrameHandler(self.spark, transformDefinition)

    def tearDown(self):
        # Clean up any resources used in tests
        pass

    def test_apply_filters(self):
        # Define test DataFrame
        df = self.spark.createDataFrame([(1, 'A'), (2, 'B'), (3, 'C')], ["id", "value"])
        filter_rules = [{"column": "id", "operator": ">", "value": 1}]

        # Apply filters
        filtered_df = self.handler._applyFilters(df, filter_rules)

        # Assert that the filtered DataFrame has expected number of rows
        self.assertEqual(filtered_df.count(), 2)

    # Add more test methods for other methods in DataFrameHandler class


if __name__ == '__main__':
    unittest.main()