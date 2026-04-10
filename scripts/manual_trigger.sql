-- 1. Load raw data from the Stage into Bronze tables
CALL DATAWAREHOUSE.bronze.load_bronze();
 
-- 2. Clean and transform the data from Bronze into Silver tables
CALL DATAWAREHOUSE.silver.load_silver();
 
-- 3. Query your Gold layer to see the final, modeled results!
SELECT * FROM DATAWAREHOUSE.gold.fact_sales LIMIT 20;
