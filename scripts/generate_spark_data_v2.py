import csv
import random
from pathlib import Path
from datetime import datetime, timedelta

from faker import Faker

# Reproducible data generation
fake = Faker()
Faker.seed(430)
random.seed(430)

# IMPORTANT: This version writes to a NEW folder.
# It does not overwrite the existing data/spark_raw files.
DATA_DIR = Path("data") / "spark_raw_v2"
DATA_DIR.mkdir(parents=True, exist_ok=True)

RECORD_COUNT = 100_000


def write_csv(filename, records, columns):
    """Write records to a CSV file."""
    path = DATA_DIR / filename

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)

    print(f"Created: {path} ({len(records):,} records)")


def format_date(value, row_number):
    """Return one of the three required date formats."""
    if row_number % 3 == 0:
        return value.strftime("%Y-%m-%d")
    if row_number % 3 == 1:
        return value.strftime("%m/%d/%Y")
    return value.strftime("%b %d, %Y")


def generate_customers():
    print("\nGenerating customers...")

    records = []
    columns = [
        "customer_id", "first_name", "last_name", "email", "phone",
        "city", "registration_date", "customer_type"
    ]

    customer_types = ["Regular", "Premium", "VIP"]

    for i in range(1, RECORD_COUNT + 1):
        customer_id = i

        # Duplicate IDs 1-100 with slight variations.
        if 1001 <= i <= 1100:
            customer_id = i - 1000

        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"customer{i}@example.com"
        phone = fake.phone_number()
        city = fake.city()
        customer_type = random.choice(customer_types)

        registration_date = fake.date_between(
            start_date="-5y", end_date="today"
        )
        registration_date_text = format_date(registration_date, i)

        # Required data-quality issues.
        if i % 5000 == 0:
            first_name = None
        if i % 7000 == 0:
            email = None
        if i % 9000 == 0:
            phone = None
        if i % 3000 == 0:
            city = " " + city.upper() + " "
        if i % 4000 == 0:
            customer_type = customer_type.lower()
        if i % 2500 == 0:
            last_name = " " + last_name + " "
        if i % 6000 == 0:
            first_name = (" " + first_name + " ") if first_name else first_name

        records.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "city": city,
            "registration_date": registration_date_text,
            "customer_type": customer_type
        })

    write_csv("customers.csv", records, columns)


def generate_products():
    print("\nGenerating products...")

    records = []
    columns = [
        "product_id", "product_name", "category", "brand",
        "unit_price", "stock_quantity", "is_active"
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

        if 1001 <= i <= 1100:
            product_id = i - 1000

        product_name = fake.catch_phrase()
        category = random.choice(categories)
        brand = random.choice(brands)
        unit_price = round(random.uniform(5, 1500), 2)
        stock_quantity = random.randint(0, 500)

        if i % 5000 == 0:
            unit_price = -25.00
        if i % 6000 == 0:
            stock_quantity = -10
        if i % 7000 == 0:
            category = None
        elif i % 3000 == 0:
            category = category.upper()
        if i % 8000 == 0:
            unit_price = None
        if i % 9000 == 0:
            product_name = None
        if i % 2500 == 0:
            brand = " " + brand + " "
        if i % 3500 == 0:
            product_name = " " + product_name + " " if product_name else product_name

        records.append({
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "brand": brand,
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
        "Credit Card", "Debit Card", "PayPal", "Bank Transfer"
    ]

    start_date = datetime(2024, 1, 1)

    for i in range(1, RECORD_COUNT + 1):
        order_id = i

        if 1001 <= i <= 1100:
            order_id = i - 1000

        customer_id = random.randint(1, RECORD_COUNT)
        order_date = start_date + timedelta(
            seconds=random.randint(0, 730 * 24 * 60 * 60)
        )
        order_date_text = format_date(order_date, i)

        # Keep a timestamp for ISO-formatted rows.
        if i % 3 == 0:
            order_date_text = order_date.strftime("%Y-%m-%d %H:%M:%S")
        elif i % 3 == 1:
            order_date_text = order_date.strftime("%m/%d/%Y")
        else:
            order_date_text = order_date.strftime("%b %d, %Y")

        order_status = random.choice(statuses)
        payment_method = random.choice(payments)
        total_amount = round(random.uniform(20, 2500), 2)
        discount_amount = round(random.uniform(0, 100), 2)
        shipping_cost = round(random.uniform(0, 30), 2)

        if i % 2000 == 0:
            customer_id = RECORD_COUNT + i  # Orphan customer reference
        if i % 5000 == 0:
            total_amount = -100.00
        if i % 6000 == 0:
            total_amount = None
        if i % 3000 == 0:
            order_status = order_status.lower()
        if i % 4000 == 0:
            payment_method = " " + payment_method.upper() + " "
        if i % 5500 == 0:
            discount_amount = round(total_amount + 50, 2) if total_amount is not None else 200.00
        if i % 6500 == 0:
            payment_method = None
        if i % 2500 == 0:
            order_status = " " + order_status + " "

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
    print("=" * 60)
    print("DATA 430 PROJECT 2 - IMPROVED SPARK DATA GENERATION")
    print("=" * 60)

    generate_customers()
    generate_products()
    generate_orders()

    print("\nDATA GENERATION COMPLETED")
    print(f"Customers: {RECORD_COUNT:,}")
    print(f"Products:  {RECORD_COUNT:,}")
    print(f"Orders:    {RECORD_COUNT:,}")
    print(f"Total:     {RECORD_COUNT * 3:,} records")
    print(f"Output folder: {DATA_DIR.resolve()}")
    print("Original data/spark_raw files were not modified.")


if __name__ == "__main__":
    main()
