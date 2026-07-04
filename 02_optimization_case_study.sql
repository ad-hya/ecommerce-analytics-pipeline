USE DATABASE portfolio_db;
USE WAREHOUSE learn_wh;

SELECT customer_id, SUM(price)
FROM marts.fact_orders
WHERE order_date BETWEEN '2017-06-01' AND '2017-06-30'
GROUP BY customer_id;

CREATE WAREHOUSE IF NOT EXISTS learn_wh_m
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

USE WAREHOUSE learn_wh_m;

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

SELECT customer_id, SUM(price)
FROM marts.fact_orders
WHERE order_date BETWEEN '2017-06-01' AND '2017-06-30'
GROUP BY customer_id;



USE WAREHOUSE learn_wh;
USE DATABASE portfolio_db;

-- Add a clustering key on order_date
ALTER TABLE marts.fact_orders CLUSTER BY (order_date);

-- Check clustering health/depth
SELECT SYSTEM$CLUSTERING_INFORMATION('marts.fact_orders', '(order_date)');

-- Re-run the same baseline query with cache disabled
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

SELECT customer_id, SUM(price)
FROM marts.fact_orders
WHERE order_date BETWEEN '2017-06-01' AND '2017-06-30'
GROUP BY customer_id;


ALTER SESSION SET USE_CACHED_RESULT = TRUE;

SELECT customer_id, SUM(price)
FROM marts.fact_orders
WHERE order_date BETWEEN '2017-06-01' AND '2017-06-30'
GROUP BY customer_id;