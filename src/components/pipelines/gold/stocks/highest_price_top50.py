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

    def _getJoinRule():
        return None

    def _getFilterRule():
        return None

    def _getConversionRule() :
        return None
    
    def _getDedupeRule():
        return None
    
    def _getAggregateRule():
        # Working here
        return None
    
    def _getSelectRule():
        return ["IdStock", "Symbol"]
    
    def _getOrderRule():
        return None
    
    def _getLimitRule():
        return 50
    
    # Build the transformation Pipeline. Order is important
    def getTransformationRules():
        return {
            "transformation_rules" : [
                {"select_rule": TransformDefinition._getSelectRule()},
                {"order_rule": TransformDefinition._getOrderRule()},
                {"limit_rule": TransformDefinition._getDedupeRule()}
            ]
        } 
