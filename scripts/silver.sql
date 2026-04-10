-- Silver Layer DDL

USE SCHEMA silver;

-- CRM Tables
CREATE OR REPLACE TRANSIENT TABLE crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE,
	dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TRANSIENT TABLE crm_prd_info (
	prd_id INT,
	cat_id VARCHAR(50),
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TRANSIENT TABLE crm_sales_details (
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ERP Tables
CREATE OR REPLACE TRANSIENT TABLE erp_loc_a101 (
	cid VARCHAR(50),
	cntry VARCHAR(50),
	dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TRANSIENT TABLE erp_cust_az12 (
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50),
	dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TRANSIENT TABLE erp_px_cat_g1v2 (
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50),
	dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- Silver Loading Procedure
USE SCHEMA silver;

CREATE OR REPLACE PROCEDURE load_silver()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
	-- 1. Load crm_cust_info
	TRUNCATE TABLE silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info(
		cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
	)
	SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
	FROM (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
	) AS t 
	WHERE flag_last = 1;

	-- 2. Load crm_prd_info
	TRUNCATE TABLE silver.crm_prd_info;
	INSERT INTO silver.crm_prd_info (
		prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
	)
	SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line, 
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        -- Snowflake strictly requires DATEADD for date manipulation
        DATEADD(day, -1, LEAD(CAST(prd_start_dt AS DATE)) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
	FROM bronze.crm_prd_info;

	-- 3. Load crm_sales_details
	TRUNCATE TABLE silver.crm_sales_details;
	INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
	)
	SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        -- Using TRY_TO_DATE for safe parsing
        CASE
            WHEN sls_order_dt <= 0 OR LENGTH(TO_VARCHAR(sls_order_dt)) != 8 THEN NULL
            ELSE TRY_TO_DATE(TO_VARCHAR(sls_order_dt), 'YYYYMMDD')
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt <= 0 OR LENGTH(TO_VARCHAR(sls_ship_dt)) != 8 THEN NULL
            ELSE TRY_TO_DATE(TO_VARCHAR(sls_ship_dt), 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt <= 0 OR LENGTH(TO_VARCHAR(sls_due_dt)) != 8 THEN NULL
            ELSE TRY_TO_DATE(TO_VARCHAR(sls_due_dt), 'YYYYMMDD')
        END AS sls_due_dt,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
	FROM bronze.crm_sales_details;

	-- 4. Load erp_cust_az12
	TRUNCATE TABLE silver.erp_cust_az12;
	INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
	SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END AS cid,
        CASE 
            WHEN bdate > CURRENT_DATE() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
	FROM bronze.erp_cust_az12;

	-- 5. Load erp_loc_a101
	TRUNCATE TABLE silver.erp_loc_a101;
	INSERT INTO silver.erp_loc_a101 (cid, cntry)
	SELECT 
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
	FROM bronze.erp_loc_a101;

	-- 6. Load erp_px_cat_g1v2
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	SELECT id, cat, subcat, maintenance
	FROM bronze.erp_px_cat_g1v2;

    RETURN 'Silver layer loaded successfully!';
END;
$$;

/*
-- Example of replacing the CASE statement with Cortex for messy data
    SELECT 
        REPLACE(cid, '-', '') AS cid,
        SNOWFLAKE.CORTEX.COMPLETE(
            'llama3-8b', 
            'Standardize the following country name to its full, official English spelling. If it is empty or invalid, return "n/a". Respond ONLY with the standardized name: ' || COALESCE(cntry, '')
        ) AS cntry
    FROM bronze.erp_loc_a101;
*/
