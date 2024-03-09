from pyspark.sql.functions import col, when

class DataFrameHandler:   
    """Brief description of MyClass.

    A more detailed description of the class can be provided here,
    including its purpose, functionality, usage, and any important
    details that users should know.

    Args:
        param1 (type): Description of parameter 1.
        param2 (type): Description of parameter 2.

    Attributes:
        attribute1 (type): Description of attribute 1.
        attribute2 (type): Description of attribute 2.

    Raises:
        SomeError: Description of the error that may be raised.

    Examples:
        Provide examples of how to use the class.

    """

    def __init__(self, spark, transformDefinition):
        self.spark = spark
        self.transformDefinition = transformDefinition

    ## Apply filters based on filter rules
    def _applyFilters(self, df, filter_rules):
        print("Filters")
        filters = []
        for rule in filter_rules:
            filter_expr = f"{rule['column']} {rule['operator']} '{rule['value']}'"
            filters.append(filter_expr)
        condition = " AND ".join(filters)
        return df.filter(condition)

    # Convert column format 
    def _applyConversions(self, df, conversion_rules):
        for column, conversion_func in conversion_rules.items():
            df = df.withColumn(column, conversion_func(col(column)))
        return df

    # Remove deduplication in current df and check destination_table based on keys
    def _applyDeduplication(self, df, dedupe_columns, lookup_destination_table):
        # Deduplicate the DataFrame based on two specific columns
        deduplicated_df = df.dropDuplicates(dedupe_columns)

        if lookup_destination_table:
            target_table_df = self.spark.table("dev.dev_silver.stocks")
            # Initialize the join condition
            join_condition = None

            # Construct the join condition dynamically
            for column in dedupe_columns:
                condition = (deduplicated_df[column] == target_table_df[column])
                if join_condition is None:
                    join_condition = condition
                else:
                    join_condition = join_condition & condition

            # Perform left anti join to identify rows that already exist in the target table
            return deduplicated_df.join(target_table_df, join_condition, "left_anti")
    
    # Aggregate to a new column
    def _applyAggregation(self, df, aggregation_rules):
        return df
    
    # Rename and select wanted columns
    def _applySelect(self, df, selected_columns):
        return df

    # Apply transformations
    def transformData(self, df):
        df = self._applyFilters(df, self.transformDefinition.getFilterRules())
        df = self._applyConversions(df, self.transformDefinition.getConversionRules())
        df = self._applyDeduplication(df, self.transformDefinition.getDedupeRules(), True)
        df = self._applyAggregation(df, self.transformDefinition.getAggregateRules())
        df = self._applySelect(df, self.transformDefinition.getSelectedRules())
        return df
    