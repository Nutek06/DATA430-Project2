from spark_utils import get_spark
from spark_cleanse import clean_customers

def main():
    spark = get_spark("DATA430_Clean_Customers")
    try:
        clean_customers(spark)
        print("CUSTOMER CLEANSING COMPLETED")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
