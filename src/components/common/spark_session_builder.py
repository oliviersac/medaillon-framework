from pyspark.sql import SparkSession

class SparkSessionBuilder:
    def __init__(self, project_name):
        self.project_name = project_name

    def buildSparkSession(self) -> SparkSession:
        return SparkSession.builder.appName(self.project_name).getOrCreate()