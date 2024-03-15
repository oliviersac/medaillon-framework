from pyspark.sql.functions import col

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

    def __init__(self, transformDefinition):

        self.transformDefinition = transformDefinition
        self.rows_received = 0   
        self.rows_filtered = 0
        self.rows_deduped = 0
        self.rows_added = 0

    ## Apply filters based on filter rules
    def _applyFilters(self, df, filter_rules):        
        df_count = df.count()
        filters = []

        # Build condition
        for rule in filter_rules:
            filter_expr = f"{rule['column']} {rule['operator']} '{rule['value']}'"
            filters.append(filter_expr)
        condition = " AND ".join(filters)

        # Apply filter
        df_filtered = df.filter(condition)
        self.rows_filtered = df_count - df_filtered.count()

        return df_filtered

    # Convert column format 
    def _applyConversions(self, df, conversion_rules):
        for column, conversion_func in conversion_rules.items():
            df = df.withColumn(column, conversion_func(col(column)))
        return df

    # Remove deduplication in current df and check destination_table based on keys
    def _applyDeduplication(self, df_origin, df_destination, dedupe_columns):
        df_count = df_origin.count()
        # Deduplicate the DataFrame based on two specific columns
        deduplicated_df = df_origin.dropDuplicates(dedupe_columns)

        # If destination df is set, origin df will be deduped with it
        if df_destination != None:    
            # Initialize the join condition
            join_condition = []

            # Construct the join condition dynamically
            # The column must not be null
            for column in dedupe_columns:
                condition = deduplicated_df[column] == df_destination[column]
                join_condition.append(condition)

            # Perform left anti join to identify rows that already exist in the target table
            deduplicated_df = deduplicated_df.join(df_destination, join_condition, "left_anti")

        self.rows_deduped = df_count - deduplicated_df.count()

        return deduplicated_df
    
    # Aggregate to a new column
    def _applyAggregation(self, df, aggregation_rules):
        return df
    
    # Rename and select wanted columns
    def _applySelect(self, df, selected_columns):
        return df

    # Apply transformations
    def transformData(self, df_origin, df_destination):
        self.rows_received = df_origin.count()
        transformation_rules = self.transformDefinition.getTransformationRules()
        df_transformed = df_origin

        # Apply transformation rules in order
        for rule in transformation_rules["transformation_rules"]:
            for key, value in rule.items():
                if key == 'filter_rule':
                    df_transformed = self._applyFilters(df_transformed, value)
                elif key == 'conversion_rule':
                    df_transformed = self._applyConversions(df_transformed, value)
                elif key == 'dedupe_rule':
                    df_transformed = self._applyDeduplication(df_transformed, df_destination, value)
                elif key == 'aggregation_rule':
                    df_transformed = self._applyAggregation(df_transformed, value)
                elif key == 'select_rule':
                    df_transformed = self._applySelect(df_transformed, value)
                elif key == 'order_rule':
                    print("unsupported for now")
                else:
                    vari = 0

        self.rows_added = df_transformed.count()

        return df_transformed

"""
match lang:
    case "JavaScript":
        print("You can become a web developer.")

    case "Python":
        print("You can become a Data Scientist")

    case "PHP":
        print("You can become a backend developer")
    
    case "Solidity":
        print("You can become a Blockchain developer")

    case "Java":
        print("You can become a mobile app developer")
    case _:
        print("The language doesn't matter, what matters is solving problems.")

"""