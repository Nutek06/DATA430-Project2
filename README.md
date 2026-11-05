# DATA-430 Project 2: Data Processing and Analytics with Apache Spark

## 1. Project Overview

This project develops a containerized data-processing and analytics
pipeline using Apache Spark, Apache Airflow, PostgreSQL, Python, and
Docker Compose. The pipeline works with synthetic customer, product, and
order datasets; cleans and transforms the data; produces analytical
outputs; loads the results into PostgreSQL; and validates the database
contents.

The project builds on the ETL concepts from Project 1 and adds
distributed data processing with Apache Spark and a larger dataset.

## 2. Technologies Used

-   **Python** --- pipeline scripts and data-processing logic.
-   **Apache Spark 3.5.3** --- data cleansing and analytical
    transformations.
-   **Apache Airflow 2.10.5** --- workflow orchestration and task
    monitoring.
-   **PostgreSQL 16** --- storage for cleaned core data and analytics
    tables.
-   **Docker Compose** --- containerized project environment.
-   **Faker** --- synthetic data generation.
-   **Parquet** --- storage format for cleaned and analytical Spark
    outputs.

## 3. Data Sources

The pipeline processes three generated datasets:

-   **Customers**
-   **Products**
-   **Orders**

The generator produced 100,000 records for each dataset. The final
cleaned row counts verified in the project database were:

  Dataset       Cleaned records
  ----------- -----------------
  Customers              99,900
  Products               99,861
  Orders                 98,457

The differences between generated and cleaned row counts reflect the
cleansing process. Per-rule rejection counts were not captured, so the
row-count differences should not be interpreted as counts for any single
data-quality rule.

## 4. Data Cleansing and Analytics

The Spark cleansing workflow addresses data-quality concerns including
missing values, duplicate records, inconsistent text formatting, invalid
numeric values, date parsing, and orders referencing customers that do
not exist.

Cleaned data is written in Parquet format. The analytics script produces
twelve output datasets:

1.  Customer metrics
2.  Top 10 customers
3.  Customer-type metrics
4.  Category metrics
5.  Inventory metrics
6.  Monthly sales
7.  Daily sales
8.  Order-status metrics
9.  Payment metrics
10. Discount metrics
11. Revenue by customer type
12. Revenue by city

**Analytics limitation:** The available orders data does not contain a
`product_id` or order-line quantity. Therefore, the current source data
cannot support true product-level revenue, product popularity, category
revenue from sales, or category-by-customer-type sales calculations.
Category and inventory outputs describe the product catalog and stock,
not sales by product.

## 5. PostgreSQL Storage

The pipeline loads three cleaned core tables and twelve analytics tables
into PostgreSQL. The final verified row counts are:

  Table                          Rows
  -------------------------- --------
  customers                    99,900
  products                     99,861
  orders                       98,457
  customer_metrics             62,580
  top_10_customers                 10
  customer_type_metrics             3
  category_metrics                  8
  inventory_metrics            99,861
  monthly_sales                    24
  daily_sales                     730
  order_status_metrics              3
  payment_metrics                   5
  discount_metrics                  1
  revenue_by_customer_type          3
  revenue_by_city              29,103

The `etl_audit` table separately records load outcomes. Historical
failed audit records are retained as execution history; a previous
customer-type constraint issue was corrected by normalizing
customer-type values before loading.

## 6. Airflow Orchestration

The Airflow DAG is named `data430_project2_pipeline`. The verified
version contains six sequential tasks:

1.  `file_validation`
2.  `spark_cleanse`
3.  `spark_analytics`
4.  `load_to_postgres`
5.  `data_validation`
6.  `completion`

The final recorded manual run, `manual__2026-11-05T18:44:40+00:00`,
completed with all six tasks successful.

**Implementation note:** This verified DAG invokes the Spark scripts
with Python from Airflow. It is not yet the nine-task `spark-submit` DAG
structure specified in the project requirements. The Spark
`spark-submit` executable is installed in the Airflow container, and the
Spark Master service is reachable, but the verified successful DAG run
described above did not use `spark-submit`.

## 7. Validation and Audit

Database validation checked the expected row counts for all fifteen core
and analytics tables and checked for orders without a matching customer.
The final DAG run completed successfully.

The audit history includes successful load records and one earlier
failed customer load caused by a case-sensitive customer-type
constraint. The loader was updated to normalize customer-type values,
and the later pipeline run succeeded. The historical failed entry
remains in the audit table as part of the execution history.

## 8. Running the Project

Run the following commands from the project directory in PowerShell:

``` powershell
docker compose ps
```

If the project services are already configured and you need to start
them:

``` powershell
docker compose up -d
```

To inspect Airflow service logs:

``` powershell
docker compose logs --tail 100 airflow
```

To inspect PostgreSQL service logs:

``` powershell
docker compose logs --tail 100 postgres
```

The project uses a Docker Compose configuration with service-specific
settings and environment variables. Keep your local `.env` file private;
use `.env.example` as the shareable configuration template. Do not
publish passwords, tokens, or database credentials.

The Airflow web interface is configured on host port `8081` in this
project environment. Open:

`http://localhost:8081`

Locate `data430_project2_pipeline`, trigger a manual run when
appropriate, and inspect the task status and logs in Airflow.

## 9. Project Files

The project includes Docker Compose configuration, an Airflow DAG,
Python scripts for data generation, cleansing, analytics, and PostgreSQL
loading, SQL schema files, input and output data directories, and
project documentation.

Refer to the actual project directory for the current file and folder
names. Keep Project 1 documentation separate from this Project 2 README.

## 10. Conclusion

This project demonstrates a containerized data pipeline that uses Spark
for cleansing and analytics, Airflow for orchestration, and PostgreSQL
for storage and validation. The final verified six-task Airflow run
succeeded, and the database row counts for the fifteen core and
analytics tables were checked. The remaining work is to align the DAG
with the full nine-task `spark-submit` requirement and complete the
supporting submission evidence, including screenshots and the
demonstration recording.
