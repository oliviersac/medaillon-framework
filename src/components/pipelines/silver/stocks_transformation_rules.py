from pyspark.sql.functions import col, when

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

    def _getJoinRules():
        return None

    def _getFilterRule():
        return [            
            {"column": "IdStock", "operator": ">", "value": 0},
            {"column": "Name", "operator": "<>", "value": ''}
        ]

    def _getConversionRule() :
        return {
            "LastTradeTime": lambda x: when(x != "", x.cast("string")).otherwise(None),
            "LastTradeDate": lambda x: when(x != "", x.cast("string")).otherwise(None),
            "Insertdatetime": lambda x: when(x != "", x.cast("string")).otherwise(None),
            "Lastupdatetime": lambda x: when(x != "", x.cast("string")).otherwise(None)
        }
    
    def _getDedupeRule():
        # The columns must not be null
        # The column must not have been converted
        return ["IdStock", "Symbol"]
    
    def _getAggregateRule():
        return None
    
    def _getSelectedRule():
        return None
    
    def _getOrderRule():
        return None
    
    # Build the transformation Pipeline. Order is important
    def getTransformationRules():
        return {
            "transformation_rules" : [
                {"filter_rule" : TransformDefinition._getFilterRule()},
                {"conversion_rule": TransformDefinition._getConversionRule()},
                {"dedupe_rule": TransformDefinition._getDedupeRule()}
            ]
        } 
