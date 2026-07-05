from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "adhya",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_analytics_pipeline",
    default_args=default_args,
    description="Orchestrates the Olist e-commerce Snowflake ELT pipeline (staging -> marts)",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["snowflake", "portfolio"],
) as dag:

    # ---- Staging layer: rebuild all 5 staging views ----
    refresh_stg_orders = SnowflakeOperator(
        task_id="refresh_stg_orders",
        snowflake_conn_id="snowflake_conn",
        sql="""
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
        """,
    )

    refresh_stg_order_items = SnowflakeOperator(
        task_id="refresh_stg_order_items",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE VIEW portfolio_db.staging.stg_order_items AS
            SELECT order_id, order_item_id, product_id, seller_id, price, freight_value
            FROM portfolio_db.raw.order_items
            WHERE order_id IS NOT NULL;
        """,
    )

    refresh_stg_customers = SnowflakeOperator(
        task_id="refresh_stg_customers",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE VIEW portfolio_db.staging.stg_customers AS
            SELECT DISTINCT customer_id, customer_unique_id, customer_city, customer_state
            FROM portfolio_db.raw.customers
            WHERE customer_id IS NOT NULL;
        """,
    )

    refresh_stg_products = SnowflakeOperator(
        task_id="refresh_stg_products",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE VIEW portfolio_db.staging.stg_products AS
            SELECT DISTINCT product_id, COALESCE(product_category_name, 'unknown') AS product_category_name
            FROM portfolio_db.raw.products
            WHERE product_id IS NOT NULL;
        """,
    )

    refresh_stg_payments = SnowflakeOperator(
        task_id="refresh_stg_payments",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE VIEW portfolio_db.staging.stg_payments AS
            SELECT order_id, payment_type, payment_installments, payment_value
            FROM portfolio_db.raw.payments
            WHERE order_id IS NOT NULL;
        """,
    )

    # ---- Marts layer: rebuild dimensions ----
    rebuild_dim_customer = SnowflakeOperator(
        task_id="rebuild_dim_customer",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE portfolio_db.marts.dim_customer AS
            SELECT customer_id, customer_unique_id, customer_city, customer_state
            FROM portfolio_db.staging.stg_customers;
        """,
    )

    rebuild_dim_product = SnowflakeOperator(
        task_id="rebuild_dim_product",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE portfolio_db.marts.dim_product AS
            SELECT product_id, product_category_name
            FROM portfolio_db.staging.stg_products;
        """,
    )

    # ---- Marts layer: rebuild fact table ----
    rebuild_fact_orders = SnowflakeOperator(
        task_id="rebuild_fact_orders",
        snowflake_conn_id="snowflake_conn",
        sql="""
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
        """,
    )

    # ---- Data quality checks ----
    def check_row_count(**context):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        result = hook.get_first("SELECT COUNT(*) FROM portfolio_db.marts.fact_orders;")
        row_count = result[0]
        print(f"fact_orders row count: {row_count}")
        if row_count == 0:
            raise ValueError("fact_orders is empty - upstream pipeline may have failed")

    def check_orphaned_customers(**context):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        sql = """
            SELECT COUNT(*) FROM portfolio_db.marts.fact_orders f
            LEFT JOIN portfolio_db.marts.dim_customer c ON f.customer_id = c.customer_id
            WHERE c.customer_id IS NULL;
        """
        result = hook.get_first(sql)
        orphaned = result[0]
        print(f"Orphaned customer references: {orphaned}")
        if orphaned > 0:
            raise ValueError(f"{orphaned} orders reference a customer_id missing from dim_customer")

    def check_orphaned_products(**context):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        sql = """
            SELECT COUNT(*) FROM portfolio_db.marts.fact_orders f
            LEFT JOIN portfolio_db.marts.dim_product p ON f.product_id = p.product_id
            WHERE p.product_id IS NULL;
        """
        result = hook.get_first(sql)
        orphaned = result[0]
        print(f"Orphaned product references: {orphaned}")
        if orphaned > 0:
            raise ValueError(f"{orphaned} orders reference a product_id missing from dim_product")

    dq_row_count = PythonOperator(task_id="dq_check_row_count", python_callable=check_row_count)
    dq_orphaned_customers = PythonOperator(task_id="dq_check_orphaned_customers", python_callable=check_orphaned_customers)
    dq_orphaned_products = PythonOperator(task_id="dq_check_orphaned_products", python_callable=check_orphaned_products)

    # ---- Dependencies ----
    [refresh_stg_orders, refresh_stg_order_items] >> rebuild_fact_orders
    refresh_stg_customers >> rebuild_dim_customer
    refresh_stg_products >> rebuild_dim_product
    refresh_stg_payments  # standalone, feeds future payment analysis - no downstream dependency yet

    [rebuild_dim_customer, rebuild_dim_product, rebuild_fact_orders] >> dq_row_count
    dq_row_count >> [dq_orphaned_customers, dq_orphaned_products]