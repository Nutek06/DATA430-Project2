
import os
import sys
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

POSTGRES_CONFIG = {
    "host": os.environ["POSTGRES_HOST"],
    "port": os.environ["POSTGRES_PORT"],
    "dbname": os.environ["POSTGRES_DB"],
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}

CLEANED_BASE = "/opt/spark-data/cleaned"
ANALYTICS_BASE = "/opt/spark-data/analytics"
BATCH_SIZE = 1000


# --------------------------------------------------
# CLEANED DATASET DEFINITIONS
# --------------------------------------------------

DATASETS = {
    "customers": {
        "path": f"{CLEANED_BASE}/customers",
        "columns": [
            "customer_id", "first_name", "last_name", "email",
            "phone", "city", "registration_date", "customer_type",
        ],
        "primary_key": "customer_id",
    },
    "products": {
        "path": f"{CLEANED_BASE}/products",
        "columns": [
            "product_id", "product_name", "category", "brand",
            "unit_price", "stock_quantity", "is_active",
        ],
        "primary_key": "product_id",
    },
    "orders": {
        "path": f"{CLEANED_BASE}/orders",
        "columns": [
            "order_id", "customer_id", "order_date", "order_status",
            "payment_method", "total_amount", "discount_amount",
            "shipping_cost",
        ],
        "primary_key": "order_id",
    },
}


# --------------------------------------------------
# ANALYTICS DATASET DEFINITIONS
# --------------------------------------------------

ANALYTICS_DATASETS = {
    "customer_metrics": [
        "customer_id", "lifetime_value", "order_frequency",
        "average_order_value", "customer_type",
    ],
    "top_10_customers": [
        "customer_id", "lifetime_value", "order_frequency",
        "average_order_value", "customer_type",
    ],
    "customer_type_metrics": [
        "customer_type", "customer_count",
        "average_lifetime_value", "average_order_frequency",
    ],
    "category_metrics": [
        "category", "product_count", "average_product_price",
        "total_stock", "active_product_count",
    ],
    "inventory_metrics": [
        "product_id", "product_name", "category", "brand",
        "unit_price", "stock_quantity", "is_active",
    ],
    "monthly_sales": [
        "month", "total_sales", "order_count",
        "average_order_value",
    ],
    "daily_sales": [
        "date", "total_sales", "order_count",
    ],
    "order_status_metrics": [
        "order_status", "order_count", "total_sales",
    ],
    "payment_metrics": [
        "payment_method", "order_count", "total_sales",
    ],
    "discount_metrics": [
        "total_discounts", "average_discount",
        "total_shipping_cost", "total_sales",
    ],
    "revenue_by_customer_type": [
        "customer_type", "total_revenue", "order_count",
    ],
    "revenue_by_city": [
        "city", "total_revenue", "order_count",
    ],
}


# --------------------------------------------------
# POSTGRESQL CONNECTION
# --------------------------------------------------

def get_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


# --------------------------------------------------
# AUDIT LOGGING
# --------------------------------------------------

def write_audit(connection, entity, status, count, message):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO etl_audit (
                pipeline_name,
                entity_name,
                status,
                records_processed,
                message,
                completed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                "DATA430_Project2",
                entity,
                status,
                count,
                message,
                datetime.now() if status in ("SUCCESS", "FAILED") else None,
            ),
        )
    connection.commit()


# --------------------------------------------------
# LOAD CLEANED DATA INTO POSTGRESQL
# --------------------------------------------------

def load_dataset(spark, connection, table_name, config):

    print(f"\nStarting PostgreSQL load: {table_name}")

    dataframe = spark.read.parquet(config["path"])
    columns = config["columns"]
    primary_key = config["primary_key"]

    total_records = dataframe.count()
    print(f"Source records: {total_records:,}")

    column_sql = ", ".join(columns)

    update_columns = [
        column for column in columns
        if column != primary_key
    ]

    update_sql = ", ".join(
        f"{column} = EXCLUDED.{column}"
        for column in update_columns
    )

    insert_sql = f"""
        INSERT INTO {table_name} ({column_sql})
        VALUES %s
        ON CONFLICT ({primary_key})
        DO UPDATE SET {update_sql}
    """

    batch = []
    processed = 0

    try:
        with connection.cursor() as cursor:

            for row in dataframe.select(*columns).toLocalIterator():

                values = [row[column] for column in columns]

                if table_name == "customers":
                    type_index = columns.index("customer_type")

                    customer_type_map = {
                        "regular": "Regular",
                        "premium": "Premium",
                        "vip": "VIP",
                    }

                    original_type = values[type_index]

                    if original_type is not None:
                        values[type_index] = customer_type_map.get(
                            str(original_type).strip().lower(),
                            original_type,
                        )

                batch.append(tuple(values))

                if len(batch) >= BATCH_SIZE:
                    execute_values(
                        cursor, insert_sql, batch,
                        page_size=BATCH_SIZE,
                    )
                    processed += len(batch)
                    batch.clear()

            if batch:
                execute_values(
                    cursor, insert_sql, batch,
                    page_size=BATCH_SIZE,
                )
                processed += len(batch)

        connection.commit()

        print(f"Loaded {processed:,} records into {table_name}")

        write_audit(
            connection, table_name, "SUCCESS", processed,
            f"Successfully loaded {processed} records.",
        )

        return processed

    except Exception as error:
        connection.rollback()
        write_audit(
            connection, table_name, "FAILED", processed, str(error)
        )
        raise


# --------------------------------------------------
# REFRESH ANALYTICS TABLES
# --------------------------------------------------

def load_analytics_dataset(spark, connection, table_name, columns):

    print(f"\nRefreshing analytics table: {table_name}")

    path = f"{ANALYTICS_BASE}/{table_name}"
    dataframe = spark.read.parquet(path)

    total_records = dataframe.count()
    print(f"Analytics source records: {total_records:,}")

    column_sql = ", ".join(columns)

    insert_sql = f"""
        INSERT INTO {table_name} ({column_sql})
        VALUES %s
    """

    batch = []
    processed = 0

    try:
        with connection.cursor() as cursor:

            # Delete old results and insert the latest results
            # in one transaction.
            cursor.execute(f"DELETE FROM {table_name}")

            for row in dataframe.select(*columns).toLocalIterator():

                values = tuple(row[column] for column in columns)
                batch.append(values)

                if len(batch) >= BATCH_SIZE:
                    execute_values(
                        cursor, insert_sql, batch,
                        page_size=BATCH_SIZE,
                    )
                    processed += len(batch)
                    batch.clear()

            if batch:
                execute_values(
                    cursor, insert_sql, batch,
                    page_size=BATCH_SIZE,
                )
                processed += len(batch)

        connection.commit()

        print(f"Refreshed {processed:,} records in {table_name}")

        write_audit(
            connection, table_name, "SUCCESS", processed,
            f"Successfully refreshed {processed} analytics records.",
        )

        return processed

    except Exception as error:
        connection.rollback()

        write_audit(
            connection, table_name, "FAILED", processed, str(error)
        )

        raise


# --------------------------------------------------
# MAIN PIPELINE
# --------------------------------------------------

def main():

    spark = (
        SparkSession.builder
        .appName("DATA430_LoadToPostgreSQL")
        .getOrCreate()
    )

    connection = None

    try:
        connection = get_connection()
        print("SUCCESS: Connected to PostgreSQL")

        total_loaded = 0

        # Load core datasets first.
        # Customers must be loaded before orders.
        for table_name in ["customers", "products", "orders"]:

            loaded = load_dataset(
                spark,
                connection,
                table_name,
                DATASETS[table_name],
            )

            total_loaded += loaded

        # Load all 12 analytics datasets.
        for table_name, columns in ANALYTICS_DATASETS.items():

            loaded = load_analytics_dataset(
                spark,
                connection,
                table_name,
                columns,
            )

            total_loaded += loaded

        print("\n--------------------------------")
        print("POSTGRESQL LOAD COMPLETED")
        print(f"Total records processed: {total_loaded:,}")
        print("--------------------------------")

    except Exception as error:
        print(f"\nPOSTGRESQL LOAD FAILED: {error}")
        sys.exit(1)

    finally:
        if connection is not None:
            connection.close()

        spark.stop()


if __name__ == "__main__":
    main()
