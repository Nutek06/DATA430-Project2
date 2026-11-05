from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DATA430_Project2_Test")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

data = [
    (1, "Alice", 500),
    (2, "Bob", 700),
    (3, "Charlie", 900)
]

columns = ["customer_id", "name", "sales"]

df = spark.createDataFrame(data, columns)

print("DATA430 PROJECT 2 - SPARK TEST")
df.show()

print("Total sales:")
df.selectExpr("SUM(sales) AS total_sales").show()

spark.stop()