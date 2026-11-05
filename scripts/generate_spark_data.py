
import csv
import random
from pathlib import Path
from datetime import datetime, timedelta

from faker import Faker

# Reproducible data generation
fake = Faker()
Faker.seed(430)
random.seed(430)

# Save new Spark data separately from existing Project 1 files
DATA_DIR = Path("data") / "spark_raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

RECORD_COUNT = 100_000


def write_csv(filename, records, columns):
    """Write records to a CSV file."""
    path = DATA_DIR / filename

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)

    print(f"Created: {path} ({RECORD_COUNT:,} records)")


def generate_customers():
    print("\nGenerating customers...")

    records = []
    columns = [
        "customer_id", "first_name", "last_name",
        "email", "city", "registration_date", "customer_type"
    ]

    customer_types = ["Regular", "Premium", "VIP"]

    for i in range(1, RECORD_COUNT + 1):
        customer_id = i

        # Duplicate IDs while keeping exactly 100,000 rows
        if 1001 <= i <= 1100:
            customer_id = i - 1000

        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"customer{i}@example.com"
        city = fake.city()
        customer_type = random.choice(customer_types)

        registration_date = fake.date_between(
            start_date="-5y", end_date="today"
        ).isoformat()

        # Inject quality problems
        if i % 5000 == 0:
            first_name = None

        if i % 7000 == 0:
            email = None

        if i % 3000 == 0:
            city = " " + city.upper() + " "

        if i % 4000 == 0:
            customer_type = customer_type.lower()

        records.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "city": city,
            "registration_date": registration_date,
            "customer_type": customer_type
        })

    write_csv("customers.csv", records, columns)


def generate_products():
    print("\nGenerating products...")

    records = []
    columns = [
        "product_id", "product_name", "category",
        "brand", "unit_price", "stock_quantity", "is_active"
    ]

    categories = [
        "Electronics", "Clothing", "Home",
        "Books", "Beauty", "Sports", "Toys"
    ]

    brands = [
        "Northstar", "BluePeak", "Everline",
        "Oak & Stone", "BrightPath", "SummitWorks"
    ]

    for i in range(1, RECORD_COUNT + 1):
        product_id = i

        # Duplicate product IDs
        if 1001 <= i <= 1100:
            product_id = i - 1000

        category = random.choice(categories)
        unit_price = round(random.uniform(5, 1500), 2)
        stock_quantity = random.randint(0, 500)

        # Inject quality problems
        if i % 5000 == 0:
            unit_price = -25.00

        if i % 6000 == 0:
            stock_quantity = -10

        if i % 7000 == 0:
            category = None

        if i % 3000 == 0:
            category = category.upper() if category else None

        if i % 8000 == 0:
            unit_price = None

        records.append({
            "product_id": product_id,
            "product_name": fake.catch_phrase(),
            "category": category,
            "brand": random.choice(brands),
            "unit_price": unit_price,
            "stock_quantity": stock_quantity,
            "is_active": random.choice([True, False])
        })

    write_csv("products.csv", records, columns)


def generate_orders():
    print("\nGenerating orders...")

    records = []
    columns = [
        "order_id", "customer_id", "order_date",
        "order_status", "payment_method",
        "total_amount", "discount_amount", "shipping_cost"
    ]

    statuses = ["Pending", "Shipped", "Delivered"]
    payments = [
        "Credit Card", "Debit Card",
        "PayPal", "Bank Transfer"
    ]

    start_date = datetime.now() - timedelta(days=730)

    for i in range(1, RECORD_COUNT + 1):
        order_id = i

        # Duplicate order IDs
        if 1001 <= i <= 1100:
            order_id = i - 1000

        customer_id = random.randint(1, RECORD_COUNT)

        order_date = start_date + timedelta(
            seconds=random.randint(0, 730 * 24 * 60 * 60)
        )

        order_date_text = order_date.strftime("%Y-%m-%d %H:%M:%S")

        order_status = random.choice(statuses)
        payment_method = random.choice(payments)

        total_amount = round(random.uniform(20, 2500), 2)
        discount_amount = round(random.uniform(0, 100), 2)
        shipping_cost = round(random.uniform(0, 30), 2)

        # Inject quality problems
        if i % 2000 == 0:
            customer_id = RECORD_COUNT + i

        if i % 5000 == 0:
            total_amount = -100.00

        if i % 6000 == 0:
            total_amount = None

        if i % 3000 == 0:
            order_status = order_status.lower()

        if i % 4000 == 0:
            payment_method = " " + payment_method.upper() + " "

        if i % 7000 == 0:
            order_date_text = order_date.strftime("%m/%d/%Y")

        records.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date_text,
            "order_status": order_status,
            "payment_method": payment_method,
            "total_amount": total_amount,
            "discount_amount": discount_amount,
            "shipping_cost": shipping_cost
        })

    write_csv("orders.csv", records, columns)


def main():
    print("=" * 55)
    print("DATA 430 PROJECT 2 - SPARK DATA GENERATION")
    print("=" * 55)

    generate_customers()
    generate_products()
    generate_orders()

    print("\nDATA GENERATION COMPLETED")
    print("Customers: 100,000")
    print("Products:  100,000")
    print("Orders:    100,000")
    print("Total:     300,000 records")
    print(f"Output folder: {DATA_DIR.resolve()}")
    print("\nData-quality issues have been injected for Spark cleaning.")


if __name__ == "__main__":
    main()
