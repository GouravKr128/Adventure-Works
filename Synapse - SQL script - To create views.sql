create schema gold;

------------------------
-- CREATE VIEW -> CALENDAR
------------------------
CREATE VIEW gold.calendar
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://gk111datalake.dfs.core.windows.net/silver/Calendar/',
            FORMAT = 'PARQUET'
        ) as q1;


------------------------
-- CREATE VIEW -> Customers
------------------------
CREATE VIEW gold.Customers
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://gk111datalake.dfs.core.windows.net/silver/Customers/',
            FORMAT = 'PARQUET'
        ) as q2;


-----------------------
-- CREATE VIEW -> Product_Categories
------------------------
CREATE VIEW gold.Product_Categories
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://gk111datalake.dfs.core.windows.net/silver/Product_Categories/',
            FORMAT = 'PARQUET'
        ) as q3;


-----------------------
-- CREATE VIEW -> Products
------------------------
CREATE VIEW gold.Products
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://gk111datalake.dfs.core.windows.net/silver/Products/',
            FORMAT = 'PARQUET'
        ) as q4;


-----------------------
-- CREATE VIEW -> Returns
------------------------
CREATE VIEW gold.Returns
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://gk111datalake.dfs.core.windows.net/silver/Returns/',
            FORMAT = 'PARQUET'
        ) as q5;


-----------------------
-- CREATE VIEW -> Sales
------------------------
CREATE VIEW gold.Sales
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://gk111datalake.dfs.core.windows.net/silver/Sales/',
            FORMAT = 'PARQUET'
        ) as q6;


-----------------------
-- CREATE VIEW -> SubCategories
------------------------
CREATE VIEW gold.SubCategories
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://gk111datalake.dfs.core.windows.net/silver/SubCategories/',
            FORMAT = 'PARQUET'
        ) as q7;


-----------------------
-- CREATE VIEW -> Territories
------------------------
CREATE VIEW gold.Territories
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://gk111datalake.dfs.core.windows.net/silver/Territories/',
            FORMAT = 'PARQUET'
        ) as q8;


----------------------------------------------------

select * from gold.Territories 



