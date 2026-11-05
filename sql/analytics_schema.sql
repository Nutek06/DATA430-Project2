
-- DATA430 Project 2: Analytics Tables
-- Additive migration: preserves existing tables and records.

CREATE TABLE IF NOT EXISTS customer_metrics (
    id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER,
    lifetime_value NUMERIC(14,2),
    order_frequency INTEGER,
    average_order_value NUMERIC(14,2),
    customer_type VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS top_10_customers (
    id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER,
    lifetime_value NUMERIC(14,2),
    order_frequency INTEGER,
    average_order_value NUMERIC(14,2),
    customer_type VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS customer_type_metrics (
    id BIGSERIAL PRIMARY KEY,
    customer_type VARCHAR(20),
    customer_count BIGINT,
    average_lifetime_value NUMERIC(14,2),
    average_order_frequency NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS category_metrics (
    id BIGSERIAL PRIMARY KEY,
    category VARCHAR(100),
    product_count BIGINT,
    average_product_price NUMERIC(14,2),
    total_stock BIGINT,
    active_product_count BIGINT
);

CREATE TABLE IF NOT EXISTS inventory_metrics (
    id BIGSERIAL PRIMARY KEY,
    product_id INTEGER,
    product_name VARCHAR(255),
    category VARCHAR(100),
    brand VARCHAR(100),
    unit_price NUMERIC(14,2),
    stock_quantity INTEGER,
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS monthly_sales (
    id BIGSERIAL PRIMARY KEY,
    month VARCHAR(7),
    total_sales NUMERIC(14,2),
    order_count BIGINT,
    average_order_value NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS daily_sales (
    id BIGSERIAL PRIMARY KEY,
    date DATE,
    total_sales NUMERIC(14,2),
    order_count BIGINT
);

CREATE TABLE IF NOT EXISTS order_status_metrics (
    id BIGSERIAL PRIMARY KEY,
    order_status VARCHAR(20),
    order_count BIGINT,
    total_sales NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS payment_metrics (
    id BIGSERIAL PRIMARY KEY,
    payment_method VARCHAR(50),
    order_count BIGINT,
    total_sales NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS discount_metrics (
    id BIGSERIAL PRIMARY KEY,
    total_discounts NUMERIC(14,2),
    average_discount NUMERIC(14,2),
    total_shipping_cost NUMERIC(14,2),
    total_sales NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS revenue_by_customer_type (
    id BIGSERIAL PRIMARY KEY,
    customer_type VARCHAR(20),
    total_revenue NUMERIC(14,2),
    order_count BIGINT
);

CREATE TABLE IF NOT EXISTS revenue_by_city (
    id BIGSERIAL PRIMARY KEY,
    city VARCHAR(100),
    total_revenue NUMERIC(14,2),
    order_count BIGINT
);
