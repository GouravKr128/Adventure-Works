CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Pass_@33346';

CREATE DATABASE SCOPED CREDENTIAL _gk_
    WITH IDENTITY = 'Managed Identity'


CREATE EXTERNAL DATA SOURCE silver_layer
WITH (
    LOCATION = 'https://gk111datalake.dfs.core.windows.net/silver',
    CREDENTIAL = _gk_ 
    )


CREATE EXTERNAL DATA SOURCE gold_layer
WITH (
    LOCATION = 'https://gk111datalake.dfs.core.windows.net/gold',
    CREDENTIAL = _gk_ 
    )


CREATE EXTERNAL FILE FORMAT parquet_file
WITH (
         FORMAT_TYPE = PARQUET,
         DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );


----------------
-- CETAS Query
----------------

CREATE EXTERNAL TABLE gold.tbl_Calendar
    WITH (
            LOCATION = 'Calendar',
            DATA_SOURCE = gold_layer,
            FILE_FORMAT = parquet_file
            ) 
    AS
    SELECT * FROM gold.Calendar;



CREATE EXTERNAL TABLE gold.tbl_Customers
    WITH (
            LOCATION = 'Customers',
            DATA_SOURCE = gold_layer,
            FILE_FORMAT = parquet_file
            ) 
    AS
    SELECT * FROM gold.Customers;



CREATE EXTERNAL TABLE gold.tbl_Product_Categories
    WITH (
            LOCATION = 'Product_Categories',
            DATA_SOURCE = gold_layer,
            FILE_FORMAT = parquet_file
            ) 
    AS
    SELECT * FROM gold.Product_Categories;



CREATE EXTERNAL TABLE gold.tbl_Products
    WITH (
            LOCATION = 'Products',
            DATA_SOURCE = gold_layer,
            FILE_FORMAT = parquet_file
            ) 
    AS
    SELECT * FROM gold.Products;



CREATE EXTERNAL TABLE gold.tbl_Returns
    WITH (
            LOCATION = 'Returns',
            DATA_SOURCE = gold_layer,
            FILE_FORMAT = parquet_file
            ) 
    AS
    SELECT * FROM gold.Returns;



CREATE EXTERNAL TABLE gold.tbl_Sales
    WITH (
            LOCATION = 'Sales',
            DATA_SOURCE = gold_layer,
            FILE_FORMAT = parquet_file
            ) 
    AS
    SELECT * FROM gold.Sales;



CREATE EXTERNAL TABLE gold.tbl_SubCategories
    WITH (
            LOCATION = 'SubCategories',
            DATA_SOURCE = gold_layer,
            FILE_FORMAT = parquet_file
            ) 
    AS
    SELECT * FROM gold.SubCategories;



CREATE EXTERNAL TABLE gold.tbl_Territories
    WITH (
            LOCATION = 'Territories',
            DATA_SOURCE = gold_layer,
            FILE_FORMAT = parquet_file
            ) 
    AS
    SELECT * FROM gold.Territories;

----------------------------------------

select * from gold.tbl_Territories









