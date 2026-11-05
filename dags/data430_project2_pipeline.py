
from datetime import datetime

import os
import psycopg2

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# --------------------------------------------------
# DATABASE VALIDATION
# --------------------------------------------------

def validate_database():

    connection = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    try:
        with connection.cursor() as cursor:

            # Expected records from Spark output
            expected_counts = {
                "customers": 99900,
                "products": 99861,
                "orders": 98457,
                "customer_metrics": 62580,
                "top_10_customers": 10,
                "customer_type_metrics": 3,
                "category_metrics": 8,
                "inventory_metrics": 99861,
                "monthly_sales": 24,
                "daily_sales": 730,
                "order_status_metrics": 3,
                "payment_metrics": 5,
                "discount_metrics": 1,
                "revenue_by_customer_type": 3,
                "revenue_by_city": 29103,
            }

            print("\nPOSTGRESQL TABLE VALIDATION")

            for table, expected in expected_counts.items():

                cursor.execute(
                    f"SELECT COUNT(*) FROM {table}"
                )

                actual = cursor.fetchone()[0]

                print(
                    f"{table}: {actual:,} records "
                    f"(expected: {expected:,})"
                )

                if actual != expected:
                    raise ValueError(
                        f"{table}: expected {expected}, found {actual}"
                    )

            # Check customer foreign-key relationships
            cursor.execute("""
                SELECT COUNT(*)
                FROM orders o
                LEFT JOIN customers c
                ON o.customer_id = c.customer_id
                WHERE c.customer_id IS NULL
            """)

            orphan_orders = cursor.fetchone()[0]

            print(f"Orphan orders: {orphan_orders}")

            if orphan_orders != 0:
                raise ValueError(
                    "Orphan customer references found."
                )

        print("\nALL 15 TABLES VALIDATED SUCCESSFULLY")
        print("DATABASE VALIDATION SUCCESSFUL")

    finally:
        connection.close()



# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------

default_args = {
    "owner": "data430",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data430_project2_pipeline",
    default_args=default_args,
    description="DATA430 Project 2 Spark ETL and Analytics Pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["DATA430", "Spark", "PostgreSQL"],
) as dag:

    # 1. Validate existing generated CSV files
    file_validation = BashOperator(
        task_id="file_validation",
        bash_command=(
            "python /opt/airflow/scripts/verify_data.py"
        ),
    )

    # 2. Clean all three datasets
    spark_cleanse = BashOperator(
        task_id="spark_cleanse",
        bash_command=(
            "python /opt/airflow/scripts/spark_cleanse.py"
        ),
    )

    # 3. Generate analytics
    spark_analytics = BashOperator(
        task_id="spark_analytics",
        bash_command=(
            "python /opt/airflow/scripts/spark_analytics.py"
        ),
    )

    # 4. Load cleaned data into PostgreSQL
    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=(
            "python /opt/airflow/scripts/load_to_postgres.py"
        ),
    )

    # 5. Verify database results
    data_validation = PythonOperator(
        task_id="data_validation",
        python_callable=validate_database,
    )

    # 6. Pipeline completion
    completion = BashOperator(
        task_id="completion",
        bash_command=(
            "echo 'DATA430 PROJECT 2 PIPELINE COMPLETED SUCCESSFULLY'"
        ),
    )

    # Pipeline execution order
    (
        file_validation
        >> spark_cleanse
        >> spark_analytics
        >> load_to_postgres
        >> data_validation
        >> completion
    )
