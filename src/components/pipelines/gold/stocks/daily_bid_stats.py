from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType

class TransformDefinition:
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

    def _getJoinRule():
        return None

    def _getFilterRule():
        return [            
            {"column": "Bid", "operator": ">", "value": 0}
        ]

    def _getConversionRule():
        return {
            "AverageBid": lambda x: when(x != "", x.cast(DoubleType())).otherwise(None),
            "MinimumBid": lambda x: when(x != "", x.cast(DoubleType())).otherwise(None),
            "MaximumBid": lambda x: when(x != "", x.cast(DoubleType())).otherwise(None),
            "VarianceBid": lambda x: when(x != "", x.cast(DoubleType())).otherwise(None),
            "CountStocks": lambda x: when(x != "", x.cast(DoubleType())).otherwise(None)
        }
    
    def _getDedupeRule():
        return None
    
    def _getAggregateRule():
        return {
            "group_by": None,
            "aggregations":[
                ["avg", "bid", "AverageBid"],
                ["min", "bid", "MinimumBid"],
                ["max", "bid", "MaximumBid"],
                ["count", "*", "CountStocks"],
                ["variance", "bid", "VarianceBid"]
            ]
        }
    
    def _sqlRule():
        return  """
            SELECT
                AVG(Bid) AS AverageBid,
                MIN(Bid) AS MinimumBid,
                MAX(Bid) AS MaximumBid,
                COUNT(*) AS CountStocks,
                VARIANCE(Bid) AS VarianceBid
            FROM
                stock_data
        """
    
    def _getSelectRule():
        return [
            "Bid", "IdStock", "Symbol"
        ]
    
    def _getOrderRule():
        return None
    
    def _getLimitRule():
        return None
    
    # Build the transformation Pipeline. Order is important
    def getTransformationRules():
        return {
            "transformation_rules" : [
                {"filter_rule": TransformDefinition._getFilterRule()},
                {"select_rule": TransformDefinition._getSelectRule()},
                {"sql_rule": TransformDefinition._sqlRule()},
                {"conversion_rule": TransformDefinition._getConversionRule()}
            ]
        } 
