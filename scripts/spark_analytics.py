
from pyspark.sql import functions as F

from spark_utils import get_spark, show_data_summary

INPUT_DIR = "/opt/spark-data/cleaned"
OUTPUT_DIR = "/opt/spark-data/analytics"


def save_analytics(df, name):
    """Save an analytics DataFrame as Parquet."""
    path = f"{OUTPUT_DIR}/{name}"
    df.write.mode("overwrite").parquet(path)
    show_data_summary(df, name)
    print(f"Saved analytics to: {path}")


def run_customer_analytics(customers, orders):
    print("\nRunning customer analytics...")

    customer_metrics = (
        orders.groupBy("customer_id")
        .agg(
            F.sum("total_amount").alias("lifetime_value"),
            F.countDistinct("order_id").alias("order_frequency"),
            F.avg("total_amount").alias("average_order_value")
        )
        .join(
            customers.select("customer_id", "customer_type"),
            "customer_id"
        )
    )

    top_customers = (
        customer_metrics
        .orderBy(F.desc("lifetime_value"))
        .limit(10)
    )

    customer_type_metrics = (
        customer_metrics.groupBy("customer_type")
        .agg(
            F.count("*").alias("customer_count"),
            F.avg("lifetime_value").alias("average_lifetime_value"),
            F.avg("order_frequency").alias("average_order_frequency")
        )
    )

    save_analytics(customer_metrics, "customer_metrics")
    save_analytics(top_customers, "top_10_customers")
    save_analytics(customer_type_metrics, "customer_type_metrics")


def run_product_analytics(products):
    print("\nRunning product and inventory analytics...")

    category_metrics = (
        products.groupBy("category")
        .agg(
            F.countDistinct("product_id").alias("product_count"),
            F.avg("unit_price").alias("average_product_price"),
            F.sum("stock_quantity").alias("total_stock"),
            F.sum(
                F.when(F.col("is_active") == True, 1).otherwise(0)
            ).alias("active_product_count")
        )
        .orderBy(F.desc("product_count"))
    )

    inventory_metrics = products.select(
        "product_id",
        "product_name",
        "category",
        "brand",
        "unit_price",
        "stock_quantity",
        "is_active"
    )

    save_analytics(category_metrics, "category_metrics")
    save_analytics(inventory_metrics, "inventory_metrics")


def run_sales_analytics(orders):
    print("\nRunning sales analytics...")

    monthly_sales = (
        orders.withColumn(
            "month", F.date_format("order_date", "yyyy-MM")
        )
        .groupBy("month")
        .agg(
            F.sum("total_amount").alias("total_sales"),
            F.countDistinct("order_id").alias("order_count"),
            F.avg("total_amount").alias("average_order_value")
        )
        .orderBy("month")
    )

    daily_sales = (
        orders.withColumn("date", F.to_date("order_date"))
        .groupBy("date")
        .agg(
            F.sum("total_amount").alias("total_sales"),
            F.countDistinct("order_id").alias("order_count")
        )
        .orderBy("date")
    )

    status_metrics = (
        orders.groupBy("order_status")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("total_amount").alias("total_sales")
        )
    )

    payment_metrics = (
        orders.groupBy("payment_method")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("total_amount").alias("total_sales")
        )
    )

    discount_metrics = orders.agg(
        F.sum("discount_amount").alias("total_discounts"),
        F.avg("discount_amount").alias("average_discount"),
        F.sum("shipping_cost").alias("total_shipping_cost"),
        F.sum("total_amount").alias("total_sales")
    )

    save_analytics(monthly_sales, "monthly_sales")
    save_analytics(daily_sales, "daily_sales")
    save_analytics(status_metrics, "order_status_metrics")
    save_analytics(payment_metrics, "payment_metrics")
    save_analytics(discount_metrics, "discount_metrics")


def run_cross_entity_analytics(customers, orders):
    print("\nRunning cross-entity analytics...")

    customer_revenue = orders.join(
        customers.select("customer_id", "customer_type", "city"),
        "customer_id"
    )

    revenue_by_customer_type = (
        customer_revenue.groupBy("customer_type")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count")
        )
    )

    revenue_by_city = (
        customer_revenue.groupBy("city")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count")
        )
        .orderBy(F.desc("total_revenue"))
    )

    save_analytics(
        revenue_by_customer_type,
        "revenue_by_customer_type"
    )
    save_analytics(revenue_by_city, "revenue_by_city")


def main():
    spark = get_spark("DATA430_Project2_Analytics")

    try:
        customers = spark.read.parquet(f"{INPUT_DIR}/customers")
        products = spark.read.parquet(f"{INPUT_DIR}/products")
        orders = spark.read.parquet(f"{INPUT_DIR}/orders")

        print("\nINPUT DATA SUMMARY")
        show_data_summary(customers, "Customers")
        show_data_summary(products, "Products")
        show_data_summary(orders, "Orders")

        run_customer_analytics(customers, orders)
        run_product_analytics(products)
        run_sales_analytics(orders)
        run_cross_entity_analytics(customers, orders)

        print("\nALL ANALYTICS COMPLETED SUCCESSFULLY")
        print(f"Output folder: {OUTPUT_DIR}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
