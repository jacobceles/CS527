-- 5. OLAP Operations in MySQL
-- ProductWise sales for each quarter
--  STATIC APPROACH
SELECT
  Product,
  SUM(IF(ThisQuarter = 'Q1', Sales, 0)) AS Q1,
  SUM(IF(ThisQuarter = 'Q2', Sales, 0)) AS Q2,
  SUM(IF(ThisQuarter = 'Q3', Sales, 0)) AS Q3,
  SUM(IF(ThisQuarter = 'Q4', Sales, 0)) AS Q4
FROM
  MyCube
GROUP BY
  Product;

-- DYNAMIC APPROACH
CREATE PROCEDURE productwise_sales()
   BEGIN
     SET @sql = NULL;

     SELECT
       GROUP_CONCAT(DISTINCT
         CONCAT(
           'sum(if(ThisQuarter = ''',
           ThisQuarter,
           ''', Sales, 0)) AS `',
           ThisQuarter ,'`'
         )
       ) INTO @sql
     FROM MyCube;

     SET @sql = CONCAT('SELECT Product, ', @sql, ' FROM MyCube group by Product');
     PREPARE stmt FROM @sql;
     EXECUTE stmt;
     DEALLOCATE PREPARE stmt;
   END;

CALL productwise_sales();
DROP PROCEDURE productwise_sales;

------------------------------------------------------------------------------

-- ProductWise sales for each quarter
--  STATIC APPROACH
SELECT
  Region,
  SUM(IF(ThisQuarter = 'Q1', Sales, 0)) AS Q1,
  SUM(IF(ThisQuarter = 'Q2', Sales, 0)) AS Q2,
  SUM(IF(ThisQuarter = 'Q3', Sales, 0)) AS Q3,
  SUM(IF(ThisQuarter = 'Q4', Sales, 0)) AS Q4
FROM
  MyCube
GROUP BY
  Region;

-- DYNAMIC APPROACH
CREATE PROCEDURE regionwise_sales()
   BEGIN
    SET @sql = NULL;

    SELECT
      GROUP_CONCAT(DISTINCT
        CONCAT(
          'sum(if(ThisQuarter = ''',
          ThisQuarter,
          ''', Sales, 0)) AS `',
          ThisQuarter ,'`'
        )
      ) INTO @sql
    FROM MyCube;

    SET @sql = CONCAT('SELECT Region, ', @sql, ' FROM MyCube group by Region');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END;

 CALL regionwise_sales();
 DROP PROCEDURE regionwise_sales;

------------------------------------------------------------------------------


-- ProductWise Regionwise sales for each quarter
--  STATIC APPROACH
SELECT
  Product,
  Region,
  SUM(IF(ThisQuarter = 'Q1', Sales, 0)) AS Q1,
  SUM(IF(ThisQuarter = 'Q2', Sales, 0)) AS Q2,
  SUM(IF(ThisQuarter = 'Q3', Sales, 0)) AS Q3,
  SUM(IF(ThisQuarter = 'Q4', Sales, 0)) AS Q4
FROM
  MyCube
GROUP BY
  Product,
  Region;

-- DYNAMIC APPROACH
CREATE PROCEDURE product_regionwise_sales()
   BEGIN
    SET @sql = NULL;

    SELECT
      GROUP_CONCAT(DISTINCT
        CONCAT(
          'sum(if(ThisQuarter = ''',
          ThisQuarter,
          ''', Sales, 0)) AS `',
          ThisQuarter ,'`'
        )
      ) INTO @sql
    FROM MyCube;

    SET @sql = CONCAT('SELECT Product,Region, ', @sql, ' FROM MyCube group by Product,Region');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END;

CALL product_regionwise_sales();
DROP PROCEDURE product_regionwise_sales;

------------------------------------------------------------------------------

-- slicing
select * from MyCube where ThisQuarter='Q1';


------------------------------------------------------------------------------

-- dicing
select * from MyCube where ThisQuarter='Q1' and Region ='UK';


------------------------------------------------------------------------------

-- group by with rollup
SELECT ThisQuarter, Region, Product, SUM(Sales) as TotalSales
FROM MyCube
GROUP BY ThisQuarter, Region, Product with rollup
ORDER BY 1,2,3

------------------------------------------------------------------------------

-- group by with cube
SELECT ThisQuarter, Region, Product, SUM(Sales)as TotalSales
FROM MyCube
GROUP BY ThisQuarter, Region, Product
UNION
SELECT NULL, Region, Product, SUM(Sales)as TotalSales
FROM MyCube
GROUP BY Region, Product
UNION
SELECT ThisQuarter, NULL, Product, SUM(Sales)as TotalSales
FROM MyCube
GROUP BY ThisQuarter,Product
UNION
SELECT ThisQuarter, Region, NULL, SUM(Sales)as TotalSales
FROM MyCube
GROUP BY ThisQuarter, Region
UNION
SELECT NULL, NULL, Product, SUM(Sales)as TotalSales
FROM MyCube
GROUP BY Product
UNION
SELECT ThisQuarter, NULL, NULL, SUM(Sales)as TotalSales
FROM MyCube
GROUP BY ThisQuarter
UNION
SELECT NULL, Region, NULL, SUM(Sales)as TotalSales
FROM MyCube
GROUP BY Region
UNION
SELECT NULL, NULL, NULL, SUM(Sales)as TotalSales
FROM MyCube
order by 1,2,3

------------------------------------------------------------------------------


-- group by grouping sets
SELECT ThisQuarter, NULL as Region, SUM(Sales) as TotalSales
FROM MyCube
GROUP BY ThisQuarter
UNION ALL
SELECT NULL, Region, SUM(Sales) as TotalSales
FROM MyCube
GROUP BY Region
ORDER BY 1,2

------------------------------------------------------------------------------

-- Ranking
SELECT
	Product, Sales
	, RANK() OVER (ORDER BY Sales ASC) as RANK_SALES
	, DENSE_RANK() OVER (ORDER BY Sales ASC) as DENSE_RANK_SALES
	, PERCENT_RANK() OVER (ORDER BY Sales ASC) as PERC_RANK_SALES
	, CUME_DIST() OVER (ORDER BY Sales ASC) as CUM_DIST_SALES
FROM
	MyCube
ORDER BY
	RANK_SALES ASC

------------------------------------------------------------------------------

-- Windowing
SELECT
	ThisQuarter, Region, Sales
	, AVG(Sales) OVER (PARTITION BY Region ORDER BY ThisQuarter) AS Sales_Avg
FROM
	MyCube
ORDER BY
	Region, ThisQuarter, Sales_Avg;

------------------------------------------------------------------------------

-- Windowing
SELECT
	ThisQuarter, Region, Sales
	, AVG(Sales) OVER (PARTITION BY Region ORDER BY ThisQuarter ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS Sales_Avg
FROM
	MyCube
ORDER BY
	Region, ThisQuarter, Sales_Avg;


------------------------------------------------------------------------------
