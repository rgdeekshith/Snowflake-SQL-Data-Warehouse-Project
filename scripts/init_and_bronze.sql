-- Initialization

-- Create database and schemas
-- We use IF NOT EXISTS to make the notebook cell idempotent (safe to rerun)
CREATE DATABASE IF NOT EXISTS DataWarehouse;
USE DATABASE DataWarehouse;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze DDL

USE SCHEMA bronze;

-- Using TRANSIENT tables for the Bronze layer saves on Time Travel storage costs
-- since this data can be reloaded directly from the source files.

-- CRM Tables
CREATE OR REPLACE TRANSIENT TABLE crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE
);

CREATE OR REPLACE TRANSIENT TABLE crm_prd_info (
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt TIMESTAMP_NTZ,
	prd_end_dt TIMESTAMP_NTZ
);

CREATE OR REPLACE TRANSIENT TABLE crm_sales_details (
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

-- ERP Tables
CREATE OR REPLACE TRANSIENT TABLE erp_loc_a101 (
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

CREATE OR REPLACE TRANSIENT TABLE erp_cust_az12 (
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50)
);

CREATE OR REPLACE TRANSIENT TABLE erp_px_cat_g1v2 (
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50)
);

-- Staging Infrastructure(New for Snowflake)

USE SCHEMA bronze;

-- 1. Create a File Format to replicate the 'FIELDTERMINATOR' and 'FIRSTROW' logic
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null', '');

-- 2. Create an Internal Stage to hold the CSV files uploaded from your local machine
CREATE OR REPLACE STAGE my_csv_stage
  FILE_FORMAT = csv_format;


  
-- Loading Procedure

USE SCHEMA bronze;

-- Translating the T-SQL Stored Procedure to Snowflake Scripting (SQL)
CREATE OR REPLACE PROCEDURE load_bronze()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- For bronze.crm_cust_info
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY INTO bronze.crm_cust_info
    FROM @my_csv_stage/source_crm/cust_info.csv;

    -- For bronze.crm_prd_info
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY INTO bronze.crm_prd_info
    FROM @my_csv_stage/source_crm/prd_info.csv;

    -- For bronze.crm_sales_details
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY INTO bronze.crm_sales_details
    FROM @my_csv_stage/source_crm/sales_details.csv;

    -- For bronze.erp_cust_az12
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY INTO bronze.erp_cust_az12
    FROM @my_csv_stage/source_erp/CUST_AZ12.csv;

    -- For bronze.erp_loc_a101
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY INTO bronze.erp_loc_a101
    FROM @my_csv_stage/source_erp/LOC_A101.csv;

    -- For bronze.erp_px_cat_g1v2
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY INTO bronze.erp_px_cat_g1v2
    FROM @my_csv_stage/source_erp/PX_CAT_G1V2.csv;

    RETURN 'Bronze layer loaded successfully!';
END;
$$;

-- Call the procedure to execute the load
-- CALL load_bronze();
