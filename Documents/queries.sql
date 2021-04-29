--SIMPLE READ
SELECT * FROM departments;
--SIMPLE READ
SELECT department FROM departments;

--AGGREGATION
SELECT order_dow, count(order_dow) 
FROM orders 
GROUP BY order_dow 
ORDER BY order_dow ASC
LIMIT 10;

--JOIN
SELECT * FROM 
departments d 
INNER JOIN 
products  p
ON d.department_id=p.department_id;

--CREATE + READ
CREATE TABLE test (name varchar(50));
SELECT * FROM test;

--INSERT + READ
INSERT INTO test('name') VALUES ('Data Stars');
SELECT * FROM test;

--UPDATE + READ
UPDATE test SET name='Changed' WHERE name='Data Stars';
SELECT * FROM test;

--DELETE (with a function) + READ
DELETE FROM test WHERE LOWER(name)='changed';

--DROP + READ
DROP TABLE test;
SELECT * FROM test;
