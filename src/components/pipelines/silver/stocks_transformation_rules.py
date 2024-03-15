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

    def getJoinRules():
        return None

    def getFilterRules():
        return [
            {"column": "IdStock", "operator": ">", "value": 0},
            {"column": "Name", "operator": "<>", "value": ''}
        ]

    def getConversionRules() :
        return {
            "LastTradeTime": lambda x: when(x != "", x.cast("string")).otherwise(None),
            "LastTradeDate": lambda x: when(x != "", x.cast("string")).otherwise(None),
            "Insertdatetime": lambda x: when(x != "", x.cast("string")).otherwise(None),
            "Lastupdatetime": lambda x: when(x != "", x.cast("string")).otherwise(None)
        }
    
    def getDedupeRules():
        # The columns must not be null
        # The column must not have been converted
        return ["IdStock", "Symbol"]
    
    def getAggregateRules():
        return None
    
    def getSelectedRules():
        return None
    
    # Build the transformation Pipeline. Order is important
    def getTransformationRules():
        return {
            "transformation_rules" : [
                {"filter_rule" : TransformDefinition.getFilterRules()},
                {"conversion_rule": TransformDefinition.getConversionRules()},
                {"dedupe_rule": TransformDefinition.getDedupeRules()}
            ]
        } 
