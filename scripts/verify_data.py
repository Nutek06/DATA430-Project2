import csv
from pathlib import Path

folder = Path("/opt/airflow/data/spark_raw_v2")

for filename in ["customers.csv", "products.csv", "orders.csv"]:
    with open(folder / filename, encoding="utf-8", newline="") as file:
        count = sum(1 for row in csv.DictReader(file))
    print(f"{filename}: {count:,} records")
