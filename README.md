# Snowflake Medallion Data Pipeline

This repository contains my hands-on project building a Medallion Architecture (Bronze, Silver, Gold layers) data warehouse entirely within Snowflake. 

The goal of this project was to practice building ETL pipelines using raw SQL, managing Snowflake internal cloud storage, and transforming raw CSV data into a reporting-ready star schema.

## Project Overview
This pipeline processes sample data from two simulated business systems (a CRM and an ERP) and moves it through three stages of refinement:

* **🥉 Bronze Layer:** Raw data ingestion. Data is loaded directly from CSV files sitting in a Snowflake Internal Stage into transient tables.
* **🥈 Silver Layer:** Cleansed and standardized data. This layer uses stored procedures to handle date formatting, null replacements, and text standardization (e.g., converting 'M'/'F' to 'Male'/'Female').
* **🥇 Gold Layer:** The business reporting layer. This consists of a Fact table and Dimension tables (Star Schema) built using Snowflake Views on top of the Silver tables.

## Tech Stack
* **Data Warehouse:** Snowflake
* **Language:** Snowflake SQL
* **Key Snowflake Features Used:** Internal Stages, File Formats, Stored Procedures, Views, Transient Tables.

## How to Run the Pipeline
The SQL scripts in this repository are designed to be run in standard Snowflake SQL Worksheets. Currently, the pipeline is executed manually.

1.  **Environment Setup:** Run the initialization script to create the `DATAWAREHOUSE` database and the `BRONZE`, `SILVER`, and `GOLD` schemas.
2.  **Stage Data:** Upload the source CSV files into the Snowflake Internal Stage (`@MY_CSV_STAGE`).
3.  **Load Bronze:** Execute the procedure to load raw data:
    ```sql
    CALL DATAWAREHOUSE.bronze.load_bronze();
    ```
4.  **Load Silver:** Execute the procedure to clean and transform the data:
    ```sql
    CALL DATAWAREHOUSE.silver.load_silver();
    ```
5.  **Query Gold:** The Gold layer uses dynamic Views, so simply query the tables to see the final modeled data:
    ```sql
    SELECT * FROM DATAWAREHOUSE.gold.fact_sales LIMIT 10;
    ```

## Future Improvements & Learnings
* **Task Automation:** I have written the SQL logic to automate this pipeline using a Snowflake Task DAG (a root task for Bronze, and a dependent child task for Silver). Due to `EXECUTE TASK` permission restrictions in my current lab environment, I am running the pipeline manually, but scheduling automation is the immediate next step.
* **Data Quality Checks:** Implement basic data quality checks (like checking for duplicate IDs) before allowing data to pass from Bronze to Silver.

## Acknowledgments
Thanks to Google's Gemini AI, which acted as my programming assistant throughout this project as I am a very beginner in Snowflake. Gemini was incredibly helpful for brainstorming the Medallion architecture, drafting and refining the Snowflake SQL scripts, and debugging environment configurations along the way. Please feel free to point out any errors or areas for improvement; I always welcome the opportunity to learn from your feedback and ensure the script is optimized.

**Original repository base:** [sql-data-warehouse-project](https://github.com/rgdeekshith/sql-data-warehouse-project)
