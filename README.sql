-- Task4 Develop the queries to retrieve information from the OLAP operations performed and to gain a deeper understanding of the sales data through different dimensions, aggregations, and filters.
-- Project: OLAP Operations (using Redshift or PostgreSQL)
-- Objective: Perform OLAP operations (Drill Down, Rollup, Cube, Slice, and Dice) on the"sales_sample" table to analyze sales data. The project will include the following tasks:

-- 1.Database Creation

-- Create a database to store the sales data (Redshift or PostgreSQL).

CREATE DATABASE sales_olap;

Create a table named "sales_sample" with the specified columns:
Product_Id (Integer)
Region (varchar(50))-like East ,West etc
Date (Date)
Sales_Amount (int/numeric)

CREATE TABLE sales_sample (
    Product_Id INTEGER,
    Region VARCHAR(50),
    Date DATE,
    Sales_Amount NUMERIC(10,2)
);


-- 2.Data Creation Insert 10 sample records into the "sales_sample" table, representing sales data.


INSERT INTO sales_sample (Product_Id, Region, Date, Sales_Amount) VALUES
(101, 'East', '2024-01-15', 5000.00),
(102, 'West', '2024-01-15', 6000.00),
(101, 'North', '2024-02-10', 4500.00),
(103, 'South', '2024-02-15', 3500.00),
(102, 'East', '2024-03-01', 5500.00),
(104, 'West', '2024-03-15', 7000.00),
(103, 'North', '2024-04-01', 4000.00),
(101, 'South', '2024-04-15', 3800.00),
(104, 'East', '2024-05-01', 6500.00),
(102, 'West', '2024-05-15', 5800.00);

SELECT * FROM sales_sample;


-- 3.Perform OLAP operations
-- a)Drill Down-Analyze sales data at a more detailed level. Write a query to perform drill down from region to product level to understand sales performance.


-- Region Level 
SELECT 
    Region,
    SUM(Sales_Amount) as Total_Sales
FROM sales_sample
GROUP BY Region;

-- Drill Down to Product Level 
SELECT 
    Region,
    Product_Id,
    SUM(Sales_Amount) as Total_Sales
FROM sales_sample
GROUP BY Region, Product_Id
ORDER BY Total_Sales DESC;


-- b)Rollup- To summarize sales data at different levels of granularity. Write a query to perform roll up from product to region level to view total sales by region.


SELECT 
    COALESCE(region, 'All Regions') as region,
    COALESCE(product_id, 'All Products') as product_id,
    SUM(sales_amount) AS total_sales
FROM 
    sales_sample s
GROUP BY 
    ROLLUP (region, product_id)
ORDER BY 
    region, product_id;



-- c)Cube - To analyze sales data from multiple dimensions simultaneously. Write a query to Explore sales data from different perspectives, such as product, region, and date.

-- MySql does not support CUBE function and as an alternative same query was run on Postgresql

SELECT
    COALESCE(region, 'All Regions') AS region,
    COALESCE(CAST(product_id AS VARCHAR), 'All Products') AS product_id,
    COALESCE(CAST(sale_date AS VARCHAR), 'All Dates') AS sale_date,
    SUM(sales_amount) AS total_sales
FROM
    sales_sample
GROUP BY
    CUBE (region, product_id, sale_date)
ORDER BY
    region,
    product_id,
    sale_date;



-- d)Slice- To extract a subset of data based on specific criteria. Write a query to slice the data to view sales for a particular region or date range.



SELECT * 
from sales_sample
where region = 'East' and date > '2024-02-15'; -- Edit values as per requirement



-- e)Dice - To extract data based on multiple criteria. Write a query to view sales for specific combinations of product, region, and date


SELECT *
FROM sales_sample
WHERE region IN ('East', 'West')
  AND product_id IN (101, 102)
  AND date BETWEEN '2024-01-01' AND '2024-01-31';


