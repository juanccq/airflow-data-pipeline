CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS gold;

-- Create the staging table structure Spark writes to
CREATE TABLE IF NOT EXISTS staging.sales_by_city (
    load_date DATE,
    city VARCHAR(100),
    total_sales DECIMAL(10, 2),
    transaction_count INT
);
