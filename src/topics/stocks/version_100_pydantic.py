from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, LongType, ArrayType
from typing import List, Optional, Any, Union, List, Dict
from pydantic import BaseModel
from pydantic.main import ModelMetaclass

class IndustryDetails(BaseModel):
    IdSector: Optional[int]
    Sector: str
    IdIndustry: float
    Industry: str

class StockData(BaseModel):
    Idstock: int
    Symbol: str
    Name: str
    PercentChange: float
    Volume: float
    Currency: str
    Bid: Optional[float]
    Ask: Optional[float]
    DaysLow: float
    DaysHigh: float
    AverageDailyVolume: float
    BookValue: float
    MarketCapitalization: str
    ChangeValue: Optional[float]
    BidRealtime: Optional[float]
    AskRealtime: Optional[float]
    ChangeRealtime: Optional[float]
    DividendShare: Optional[float]
    LastTradeDate: int
    EarningsShare: float
    EPSEstimateNextYear: Optional[float]
    EPSEstimateNextQuarter: float
    YearLow: float
    YearHigh: float
    EBITDA: str
    ChangeFromYearLow: float
    PercentChangeFromYearLow: float
    ChangeFromYearHigh: float
    LastTradePriceOnly: float
    DaysRange: str
    FiftydayMovingAverage: float
    TwoHundreddayMovingAverage: float
    ChangeFromTwoHundreddayMovingAverage: float
    PercentChangeFromTwoHundreddayMovingAverage: float
    ChangeFromFiftydayMovingAverage: float
    PercentChangeFromFiftydayMovingAverage: float
    Open: float
    PreviousClose: float
    ChangeinPercent: float
    PriceSales: float
    PriceBook: float
    PERatio: float
    DividendPayDate: str
    ShortRatio: float
    LastTradeTime: str
    DividendYield: Optional[float]
    Insertdatetime: int
    Lastupdatetime: int
    EPSEstimateCurrentYear: float
    IdSector: Optional[int]
    IdIndustry: float
    Sector: str
    Industry: str
    Isvalid: int
    industry_details: IndustryDetails
    ramdom_array: List[int]
