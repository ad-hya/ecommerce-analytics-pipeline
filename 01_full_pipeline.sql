-- ============================================
-- STEP 0: Clean slate — drop everything downstream of raw stage
-- ============================================
USE DATABASE portfolio_db;

DROP TABLE IF EXISTS raw.orders;
DROP TABLE IF EXISTS raw.order_items;
DROP TABLE IF EXISTS raw.customers;
DROP TABLE IF EXISTS raw.products;
DROP TABLE IF EXISTS raw.payments;
DROP TABLE IF EXISTS raw.sellers;

DROP VIEW IF EXISTS staging.stg_orders;
DROP VIEW IF EXISTS staging.stg_order_items;
DROP VIEW IF EXISTS staging.stg_customers;
DROP VIEW IF EXISTS staging.stg_products;
DROP VIEW IF EXISTS staging.stg_payments;

DROP TABLE IF EXISTS marts.fact_orders;
DROP TABLE IF EXISTS marts.dim_customer;
DROP TABLE IF EXISTS marts.dim_product;
DROP TABLE IF EXISTS marts.dim_date;

-- ============================================
-- STEP 1: Confirm files are actually in the stage
-- ============================================
LIST @portfolio_db.raw.raw_stage;

-- ============================================
-- STEP 2: Recreate raw tables + load from stage
-- ============================================
CREATE OR REPLACE TABLE portfolio_db.raw.orders (
  order_id STRING,
  customer_id STRING,
  order_status STRING,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP
);

COPY INTO portfolio_db.raw.orders
  FROM @portfolio_db.raw.raw_stage/olist_orders_dataset.csv
  FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
  ON_ERROR = 'CONTINUE';

CREATE OR REPLACE TABLE portfolio_db.raw.order_items (
  order_id STRING,
  order_item_id INT,
  product_id STRING,
  seller_id STRING,
  shipping_limit_date TIMESTAMP,
  price FLOAT,
  freight_value FLOAT
);

COPY INTO portfolio_db.raw.order_items
  FROM @portfolio_db.raw.raw_stage/olist_order_items_dataset.csv
  FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
  ON_ERROR = 'CONTINUE';

CREATE OR REPLACE TABLE portfolio_db.raw.customers (
  customer_id STRING,
  customer_unique_id STRING,
  customer_zip_code_prefix STRING,
  customer_city STRING,
  customer_state STRING
);

COPY INTO portfolio_db.raw.customers
  FROM @portfolio_db.raw.raw_stage/olist_customers_dataset.csv
  FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
  ON_ERROR = 'CONTINUE';

CREATE OR REPLACE TABLE portfolio_db.raw.products (
  product_id STRING,
  product_category_name STRING,
  product_name_lenght FLOAT,
  product_description_lenght FLOAT,
  product_photos_qty FLOAT,
  product_weight_g FLOAT,
  product_length_cm FLOAT,
  product_height_cm FLOAT,
  product_width_cm FLOAT
);

COPY INTO portfolio_db.raw.products
  FROM @portfolio_db.raw.raw_stage/olist_products_dataset.csv
  FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
  ON_ERROR = 'CONTINUE';

CREATE OR REPLACE TABLE portfolio_db.raw.payments (
  order_id STRING,
  payment_sequential INT,
  payment_type STRING,
  payment_installments INT,
  payment_value FLOAT
);

COPY INTO portfolio_db.raw.payments
  FROM @portfolio_db.raw.raw_stage/olist_order_payments_dataset.csv
  FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
  ON_ERROR = 'CONTINUE';

CREATE OR REPLACE TABLE portfolio_db.raw.sellers (
  seller_id STRING,
  seller_zip_code_prefix STRING,
  seller_city STRING,
  seller_state STRING
);

COPY INTO portfolio_db.raw.sellers
  FROM @portfolio_db.raw.raw_stage/olist_sellers_dataset.csv
  FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
  ON_ERROR = 'CONTINUE';

-- ============================================
-- STEP 3: Verify raw load
-- ============================================
SELECT 'orders' AS tbl, COUNT(*) AS row_count FROM portfolio_db.raw.orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM portfolio_db.raw.order_items
UNION ALL
SELECT 'customers', COUNT(*) FROM portfolio_db.raw.customers
UNION ALL
SELECT 'products', COUNT(*) FROM portfolio_db.raw.products
UNION ALL
SELECT 'payments', COUNT(*) FROM portfolio_db.raw.payments
UNION ALL
SELECT 'sellers', COUNT(*) FROM portfolio_db.raw.sellers;

-- ============================================
-- STEP 4: Staging views
-- ============================================
CREATE OR REPLACE VIEW portfolio_db.staging.stg_orders AS
SELECT
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp::TIMESTAMP AS purchase_ts,
  order_approved_at::TIMESTAMP AS approved_ts,
  order_delivered_customer_date::TIMESTAMP AS delivered_ts,
  order_estimated_delivery_date::TIMESTAMP AS estimated_delivery_ts,
  DATEDIFF('day', order_purchase_timestamp, order_delivered_customer_date) AS delivery_days
FROM portfolio_db.raw.orders
WHERE order_id IS NOT NULL;

CREATE OR REPLACE VIEW portfolio_db.staging.stg_order_items AS
SELECT
  order_id,
  order_item_id,
  product_id,
  seller_id,
  price,
  freight_value
FROM portfolio_db.raw.order_items
WHERE order_id IS NOT NULL;

CREATE OR REPLACE VIEW portfolio_db.staging.stg_customers AS
SELECT DISTINCT
  customer_id,
  customer_unique_id,
  customer_city,
  customer_state
FROM portfolio_db.raw.customers
WHERE customer_id IS NOT NULL;

CREATE OR REPLACE VIEW portfolio_db.staging.stg_products AS
SELECT DISTINCT
  product_id,
  COALESCE(product_category_name, 'unknown') AS product_category_name
FROM portfolio_db.raw.products
WHERE product_id IS NOT NULL;

CREATE OR REPLACE VIEW portfolio_db.staging.stg_payments AS
SELECT
  order_id,
  payment_type,
  payment_installments,
  payment_value
FROM portfolio_db.raw.payments
WHERE order_id IS NOT NULL;

-- ============================================
-- STEP 5: Star schema (marts)
-- ============================================
CREATE OR REPLACE TABLE portfolio_db.marts.dim_date AS
SELECT
  DATEADD(day, seq4(), '2016-01-01') AS date_day,
  YEAR(date_day) AS year,
  MONTH(date_day) AS month,
  DAY(date_day) AS day,
  DAYNAME(date_day) AS day_name,
  MONTHNAME(date_day) AS month_name,
  QUARTER(date_day) AS quarter
FROM TABLE(GENERATOR(ROWCOUNT => 2000));

CREATE OR REPLACE TABLE portfolio_db.marts.dim_customer AS
SELECT
  customer_id,
  customer_unique_id,
  customer_city,
  customer_state
FROM portfolio_db.staging.stg_customers;

CREATE OR REPLACE TABLE portfolio_db.marts.dim_product AS
SELECT
  product_id,
  product_category_name
FROM portfolio_db.staging.stg_products;

CREATE OR REPLACE TABLE portfolio_db.marts.fact_orders AS
SELECT
  oi.order_id,
  o.customer_id,
  oi.product_id,
  oi.seller_id,
  o.purchase_ts::DATE AS order_date,
  oi.price,
  oi.freight_value,
  o.delivery_days,
  o.order_status
FROM portfolio_db.staging.stg_orders o
JOIN portfolio_db.staging.stg_order_items oi ON o.order_id = oi.order_id;

-- ============================================
-- STEP 6: Verify star schema
-- ============================================
SELECT COUNT(*) AS fact_orders_count FROM portfolio_db.marts.fact_orders;

SELECT COUNT(*) AS orphaned_customers FROM portfolio_db.marts.fact_orders f
LEFT JOIN portfolio_db.marts.dim_customer c ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

SELECT COUNT(*) AS orphaned_products FROM portfolio_db.marts.fact_orders f
LEFT JOIN portfolio_db.marts.dim_product p ON f.product_id = p.product_id
WHERE p.product_id IS NULL;