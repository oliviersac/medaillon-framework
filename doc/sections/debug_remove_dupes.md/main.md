
### Related Documentation
- [Project Main Documentation](../../../README.md)

## Finding dupes in a table
```sql
WITH list_of_dupes as 
(
  SELECT  
        IdStock, Symbol, count(*)
    FROM  
        dev.dev_silver.stocks 
    group by 
        IdStock, Symbol
    having 
        count(*) > 1
)

select 
list_of_dupes.* from list_of_dupes 
```

## Removing dupes
```sql
WITH list_of_dupes AS (
    SELECT  
        IdStock, 
        Symbol, 
        MAX(processing_time) AS max_processing_time
    FROM  
        dev.dev_silver.stocks 
    GROUP BY 
        IdStock, 
        Symbol 
    HAVING 
        COUNT(*) > 1
)

DELETE FROM dev.dev_silver.stocks 
WHERE EXISTS (
    SELECT 1
    FROM list_of_dupes 
    WHERE 
        dev.dev_silver.stocks.IdStock = list_of_dupes.IdStock 
        AND dev.dev_silver.stocks.Symbol = list_of_dupes.Symbol 
        AND dev.dev_silver.stocks.processing_time = list_of_dupes.max_processing_time
);
```