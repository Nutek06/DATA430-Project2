
from pyspark.sql import SparkSession


def get_spark(app_name="DATA430_Project2"):
    """Create or retrieve a Spark session."""
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def read_csv(spark, file_path):
    """Read a CSV file with its header."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(file_path)
    )


def write_parquet(dataframe, output_path):
    """Save a Spark DataFrame as Parquet."""
    dataframe.write.mode("overwrite").parquet(output_path)
    print(f"Saved Parquet data to: {output_path}")


def show_data_summary(dataframe, name):
    """Display basic record and column information."""
    print(f"\n--- {name} ---")
    print(f"Records: {dataframe.count():,}")
    print(f"Columns: {len(dataframe.columns)}")
    print(f"Column names: {dataframe.columns}")
