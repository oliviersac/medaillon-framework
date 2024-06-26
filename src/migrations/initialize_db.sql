DROP TABLE IF EXISTS dev.dev_activity_log.transfer_log;
DROP TABLE IF EXISTS dev.dev_silver.stocks;


-- Transfer log table
CREATE TABLE IF NOT EXISTS dev.dev_activity_log.transfer_log(
    origin_type varchar(255) COMMENT 'The type of the origin (S3, delta_table, source)',
    origin_table varchar(255) COMMENT 'The table of the origin (dev.dev_bronze.stocks)', 
    destination_type varchar(255) COMMENT 'The type of the destination (dev_bronze, dev_silver, dev-landing)',
    destination_table varchar(255) COMMENT 'The table of the destination (stocks)', 
    schema_used varchar(255) COMMENT 'The schema that was used to store the data',
    rows_received INT COMMENT 'The number of rows that were received for processing',
    rows_filtered INT COMMENT 'The number of rows that were filtered while processing',
    rows_deduped INT COMMENT 'The number of rows that were deduped while processing',
    rows_added INT COMMENT 'The number of rows that were added',
    processing_time TIMESTAMP COMMENT 'The date that the processing was done',
    transfer_status varchar(255) COMMENT 'SUCCESS or FAIL after processing',
    failed_reason varchar(1000) COMMENT 'The reason why the transfer failed'
);

-- Silver Stocks
create table dev.dev_silver.stocks(
    Idstock	int,
    Symbol	string,
    Name	string,
    PercentChange	float,
    Volume	float,
    Currency	string,
    Bid	float,
    Ask	float,
    DaysLow	float,
    DaysHigh	float,
    AverageDailyVolume	float,
    BookValue	float,
    MarketCapitalization	string,
    ChangeValue	float,
    BidRealtime	float,
    AskRealtime	float,
    ChangeRealtime	float,
    DividendShare	float,
    LastTradeDate	string,
    EarningsShare	float,
    EPSEstimateNextYear	float,
    EPSEstimateNextQuarter	float,
    YearLow	float,
    YearHigh	float,
    EBITDA	string,
    ChangeFromYearLow	float,
    PercentChangeFromYearLow	float,
    ChangeFromYearHigh	float,
    LastTradePriceOnly	float,
    DaysRange	string,
    FiftydayMovingAverage	float,
    TwoHundreddayMovingAverage	float,
    ChangeFromTwoHundreddayMovingAverage	float,
    PercentChangeFromTwoHundreddayMovingAverage	float,
    ChangeFromFiftydayMovingAverage	float,
    PercentChangeFromFiftydayMovingAverage	float,
    Open	float,
    PreviousClose	float,
    ChangeinPercent	float,
    PriceSales	float,
    PriceBook	float,
    PERatio	float,
    DividendPayDate	string,
    ShortRatio	float,
    LastTradeTime	string,
    DividendYield	float,
    Insertdatetime	string,
    Lastupdatetime	string,
    EPSEstimateCurrentYear	float,
    IdSector	int,
    IdIndustry	float,
    Sector	string,
    Industry	string,
    Isvalid	int,
    industry_details	struct<IdSector:int,Sector:string,IdIndustry:float,Industry:string>,
    ramdom_array	array<int>,
    _file_path	string,
    _rescued_data	string,
    source_file	string,
    processing_time	timestamp
);


-- Top 50 highest price stocks (for gold)
create table dev.dev_gold.stocks_top50_highest_price(
    Idstock	int,
    Symbol	string,
    Bid	float
);

-- Top 50 lowest price stocks (for gold)
create table dev.dev_gold.stocks_top50_lowest_price(
    Idstock	int,
    Symbol	string,
    Bid	float
);

-- daily_bid_stats (for gold)
create table dev.dev_gold.daily_bid_stats(
    AverageBid	float,
    MinimumBid	float,
    MaximumBid	float,
    CountStocks	float,
    VarianceBid float
);