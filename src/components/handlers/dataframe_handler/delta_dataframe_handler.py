from pyspark.sql.functions import col, avg, min, max, count, variance
from pyspark.sql import functions as F

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
        # Extract group by column
        group_by_column = aggregation_rules.get("group_by", None)

        # Start with an empty dictionary to store aggregation expressions
        aggregation_exprs = {}

        # Iterate over each aggregation specified in the rule
        for aggregation in aggregation_rules.get("aggregations", []):
            # Extract aggregation function, column, and alias
            agg_func, column, alias = aggregation

            # Add aggregation expression to the dictionary
            if agg_func == "avg":
                aggregation_exprs[alias] = avg(column).alias(alias)
            elif agg_func == "min":
                aggregation_exprs[alias] = min(column).alias(alias)
            elif agg_func == "max":
                aggregation_exprs[alias] = max(column).alias(alias)
            elif agg_func == "count":
                aggregation_exprs[alias] = count(column).alias(alias)
            elif agg_func == "variance":
                aggregation_exprs[alias] = variance(column).alias(alias)
            # Add more elif conditions for other aggregation functions as needed

        # Apply group by if specified
        if group_by_column:
            df_grouped = df.groupBy(group_by_column).agg(aggregation_exprs)
        else:
            # If no group by column specified, apply aggregation directly
            df_grouped = df.agg(aggregation_exprs)

        return df_grouped
    
    # Rename and select wanted columns
    def _applySelect(self, df, select_rules):
        return df.select(select_rules)
    
    def _applyOrder(self, df, sort_rules):
        # Create an empty list to store orderBy expressions
        orderBy_exprs = []
        # Iterate over each sorting rule
        for rule in sort_rules:
            # Extract column name and sorting order
            column_name, sorting_order = list(rule.items())[0]
            # Add orderBy expression to the list
            orderBy_exprs.append(F.col(column_name).desc() if sorting_order == "desc" else F.col(column_name).asc())
        # Apply orderBy expressions to DataFrame
        sorted_df = df.orderBy(*orderBy_exprs)
        return sorted_df
    
    def _applyLimit(self, df, limit_rules):
        return df.limit(limit_rules)

    # Apply transformations
    def transformData(self, spark, df_origin, df_destination):
        self.rows_received = df_origin.count()
        transformation_rules = self.transformDefinition.getTransformationRules()
        df_transformed = df_origin

        # Apply transformation rules in order
        for rule in transformation_rules["transformation_rules"]:
            for key, value in rule.items():
                match key:
                    case "filter_rule":
                        df_transformed = self._applyFilters(df_transformed, value)
                    case "conversion_rule":
                        df_transformed = self._applyConversions(df_transformed, value)
                    case "dedupe_rule":
                        df_transformed = self._applyDeduplication(df_transformed, df_destination, value)
                    case "aggregation_rule":
                        df_transformed = self._applyAggregation(df_transformed, value)
                    case "select_rule":
                        df_transformed = self._applySelect(df_transformed, value)
                    case "order_rule":
                        df_transformed = self._applyOrder(df_transformed, value)
                    case "limit_rule":
                        df_transformed = self._applyLimit(df_transformed, value)
                    case "sql_rule":
                        df_transformed.createOrReplaceTempView("stock_data")
                        df_transformed = self.spark.sql(value)
                    case _:
                        df_transformed = df_transformed

        self.rows_added = df_transformed.count()

        return df_transformed
