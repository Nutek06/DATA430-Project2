from spark_utils import get_spark
from spark_cleanse import clean_products

def main():
    spark = get_spark("DATA430_Clean_Products")
    try:
        clean_products(spark)
        print("PRODUCT CLEANSING COMPLETED")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
