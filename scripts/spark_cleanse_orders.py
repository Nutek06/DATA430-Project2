from spark_utils import get_spark
from spark_cleanse import clean_orders

CUSTOMERS_PATH = "/opt/spark-data/cleaned/customers"

def main():
    spark = get_spark("DATA430_Clean_Orders")
    try:
        customers = spark.read.parquet(CUSTOMERS_PATH)
        clean_orders(spark, customers)
        print("ORDERS CLEANSING COMPLETED")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
