from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, LongType, ArrayType

class StockData:
    @classmethod
    def getSchema(cls) -> StructType:
        schema = StructType([
            StructField('Idstock', IntegerType(), True),
            StructField('Symbol', StringType(), True),
            StructField('Name', StringType(), True),
            StructField('PercentChange', FloatType(), True),
            StructField('Volume', FloatType(), True),
            StructField('Currency', StringType(), True),
            StructField('Bid', FloatType(), True),
            StructField('Ask', FloatType(), True),
            StructField('DaysLow', FloatType(), True),
            StructField('DaysHigh', FloatType(), True),
            StructField('AverageDailyVolume', FloatType(), True),
            StructField('BookValue', FloatType(), True),
            StructField('MarketCapitalization', StringType(), True),
            StructField('ChangeValue', FloatType(), True),
            StructField('BidRealtime', FloatType(), True),
            StructField('AskRealtime', FloatType(), True),
            StructField('ChangeRealtime', FloatType(), True),
            StructField('DividendShare', FloatType(), True),
            StructField('LastTradeDate', LongType(), True),
            StructField('EarningsShare', FloatType(), True),
            StructField('EPSEstimateNextYear', FloatType(), True),
            StructField('EPSEstimateNextQuarter', FloatType(), True),
            StructField('YearLow', FloatType(), True),
            StructField('YearHigh', FloatType(), True),
            StructField('EBITDA', StringType(), True),
            StructField('ChangeFromYearLow', FloatType(), True),
            StructField('PercentChangeFromYearLow', FloatType(), True),
            StructField('ChangeFromYearHigh', FloatType(), True),
            StructField('LastTradePriceOnly', FloatType(), True),
            StructField('DaysRange', StringType(), True),
            StructField('FiftydayMovingAverage', FloatType(), True),
            StructField('TwoHundreddayMovingAverage', FloatType(), True),
            StructField('ChangeFromTwoHundreddayMovingAverage', FloatType(), True),
            StructField('PercentChangeFromTwoHundreddayMovingAverage', FloatType(), True),
            StructField('ChangeFromFiftydayMovingAverage', FloatType(), True),
            StructField('PercentChangeFromFiftydayMovingAverage', FloatType(), True),
            StructField('Open', FloatType(), True),
            StructField('PreviousClose', FloatType(), True),
            StructField('ChangeinPercent', FloatType(), True),
            StructField('PriceSales', FloatType(), True),
            StructField('PriceBook', FloatType(), True),
            StructField('PERatio', FloatType(), True),
            StructField('DividendPayDate', StringType(), True),
            StructField('ShortRatio', FloatType(), True),
            StructField('LastTradeTime', StringType(), True),
            StructField('DividendYield', FloatType(), True),
            StructField('Insertdatetime', LongType(), True),
            StructField('Lastupdatetime', LongType(), True),
            StructField('EPSEstimateCurrentYear', FloatType(), True),
            StructField('IdSector', IntegerType(), True),
            StructField('IdIndustry', FloatType(), True),
            StructField('Sector', StringType(), True),
            StructField('Industry', StringType(), True),
            StructField('Isvalid', IntegerType(), True),
            StructField('industry_details', StructType([
                StructField('IdSector', IntegerType(), True),
                StructField('Sector', StringType(), True),
                StructField('IdIndustry', FloatType(), True),
                StructField('Industry', StringType(), True)
            ]), True),
            StructField('ramdom_array', ArrayType(IntegerType()), True),
            StructField('new_column',  StructType([
                StructField("author", StringType(), True),
                StructField("sector", StringType(), True),
                StructField("list_of_stuff", ArrayType(StructType([
                    StructField("id", IntegerType(), True),
                    StructField("description", StringType(), True),
                    StructField("id_list", ArrayType(IntegerType()), True),
                    StructField("list_of_objects", ArrayType(StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                        StructField("notes", StringType(), True)
                    ])), True)
                ])), True)
            ]))
        ])
        return schema