-- Higher Order Functions

SELECT * FROM orders;

----------+------------+-------------------------------------------------------+
-- 1- Lateral Flatten:
----------+------------+-------------------------------------------------------+

SELECT order_id, order_date, ARRAY_AGG(value) WITHIN GROUP (ORDER BY seq)
FROM orders o,
LATERAL FLATTEN(input => o.order_detail)
WHERE value:subtotal >= 1500
GROUP BY ALL;

----------+------------+-------------------------------------------------------+
-- 2- UDF:
----------+------------+-------------------------------------------------------+

CREATE OR REPLACE FUNCTION order_filter(ORDER_DETAIL ARRAY, ITEM_SUBTOTAL float)
RETURNS ARRAY
LANGUAGE JAVASCRIPT
AS
$$
    return ORDER_DETAIL.filter((ITEM) => ITEM.subtotal >= ITEM_SUBTOTAL);
$$;

SELECT order_id, order_date, order_filter(order_detail, 1500)
FROM orders o; 

----------+------------+-------------------------------------------------------+
-- Higher-order Functions:
----------+------------+-------------------------------------------------------+

SELECT order_id, order_date, FILTER(o.order_detail, i -> i:subtotal >= 1500)
FROM orders o ; 

-- Insert new elements into the array

SELECT order_id, order_date, 
TRANSFORM(o.order_detail, i -> OBJECT_INSERT(i, 'unit_price', (i:subtotal / i:quantity)::NUMERIC(10,2)))
FROM orders o;

-- Delete the key “quantity” from each order
SELECT order_id, order_date, 
TRANSFORM(o.order_detail, i -> OBJECT_DELETE(i, 'quantity'))
FROM orders o;

-- Find out the item whose purchase subtotal matches the largest purchase subtotal

SELECT order_id, order_date, FILTER(order_detail, i -> i:subtotal = (
   SELECT MAX(ARRAY_MAX(TRANSFORM(order_detail, i -> i:subtotal))) 
   FROM orders)) AS largest_item
FROM orders 
WHERE ARRAY_SIZE(largest_item) > 0;

------ LEAST/GREATEST IGNORE NULLS

select * from test_ignore_nulls ; 

SELECT col_1, col_2, col_3, col_4,
GREATEST(col_1, col_2, col_3, col_4) AS existing_greatest, 
GREATEST_IGNORE_NULLS(col_1, col_2, col_3, col_4) AS greatest_ignore_nulls
FROM test_ignore_nulls;

SELECT col_1, col_2, col_3, col_4,
LEAST(col_1, col_2, col_3, col_4) AS existing_least,
LEAST_IGNORE_NULLS(col_1, col_2, col_3, col_4) AS least_ignore_nulls
FROM test_ignore_nulls;

-- Dynamic Pivot 

SELECT fis_year, fis_quarter, region, total_sales 
FROM sales_data ;

-- Static Pivot

SELECT *
FROM sales_data
PIVOT (
SUM(total_sales)
FOR fis_quarter IN ('Q1', 'Q2', 'Q3', 'Q4')
) AS PivotTable
WHERE fis_year IN (2023)
ORDER BY region;

-- "old" 2-step Dynamic Pivoting

SELECT 'SELECT * FROM sales_data 
PIVOT (SUM(total_sales) FOR fis_quarter 
IN ('||LISTAGG(DISTINCT ''''||fis_quarter||'''', ',') ||')) 
WHERE fis_year IN (2023) ORDER BY region;' AS QUERY
FROM sales_data;

SELECT * FROM sales_data 
PIVOT (SUM(total_sales) FOR fis_quarter 
IN ('Q1','Q2','Q3','Q4')) 
WHERE fis_year IN (2023) ORDER BY region;

-- New Dynamic Pivot

SELECT * FROM sales_data
PIVOT ( SUM(total_sales) FOR fis_quarter IN (ANY)) AS PivotTable
ORDER BY region;

-- Dynamic with Sub-Query

SELECT * FROM sales_data
PIVOT ( SUM(total_sales) FOR fis_quarter IN 
         (SELECT DISTINCT fis_quarter 
          FROM sales_data 
          GROUP BY fis_quarter 
          HAVING sum(total_sales) > 200000 
          ORDER BY fis_quarter DESC)
) AS PivotTable
ORDER BY region;

-- Trailiing Comma

SELECT 
  fis_quarter, 
  region, 
  total_sales ,
  fis_year
FROM sales_data
WHERE fis_quarter = 'Q1' ; 

-- Time Series 
SELECT * FROM sensor_data_ts;

SELECT
TIME_SLICE(timestamp, 1, 'MINUTE') minute_slice,
device_id,
COUNT(*),
AVG(temperature) avg_temp
FROM sensor_data_ts
GROUP BY 1,2
ORDER BY 1,2;

SELECT
TIME_SLICE(timestamp, 1, 'HOUR') minute_slice,
device_id,
COUNT(*),
AVG(temperature) avg_temp
FROM sensor_data_ts
GROUP BY 1,2
ORDER BY 1,2;

-- AS-OF JOIN

SELECT * FROM quotes;

select * from trades ; 

-- For each Trade find the most recent prior quote

SELECT t.stock_symbol, t.trade_time, t.quantity, q.quote_time, q.price
FROM trades t ASOF JOIN quotes q
MATCH_CONDITION(t.trade_time >= quote_time)
ON t.stock_symbol=q.stock_symbol
ORDER BY t.stock_symbol;

-- Window functions

select * from sales ; 

   -- LAG
SELECT 
  emp_id, 
  year, 
  revenue, 
  revenue - LAG(revenue, 1, 0) OVER (PARTITION BY emp_id ORDER BY year) AS diff_to_prev 
FROM sales 
ORDER BY emp_id, year;


-- RANGE_BETWEEN

select * from heavy_weather 
where city in ('South Lake Tahoe','Big Bear City')
order by city;  

SELECT 
  city, 
  start_time, 
  precip,
  SUM(precip) OVER( PARTITION BY city ORDER BY start_time  
      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) moving_sum_ROWS,
  SUM(precip) OVER( PARTITION BY city ORDER BY start_time 
      RANGE BETWEEN INTERVAL '12 hours' PRECEDING AND CURRENT ROW) moving_sum_RANGE
FROM heavy_weather
WHERE city IN('South Lake Tahoe','Big Bear City')
GROUP BY city, start_time, precip
ORDER BY city;

