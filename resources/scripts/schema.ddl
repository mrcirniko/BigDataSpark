DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id bigint UNIQUE,
    first_name text NOT NULL,
    last_name text NOT NULL,
    age int,
    email text,
    country text,
    postal_code text
);

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_sk SERIAL PRIMARY KEY,
    sale_date date UNIQUE,
    year int,
    quarter int,
    month int,
    day int,
    weekday int
);

DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id bigint UNIQUE,
    name text NOT NULL,
    category text,
    weight numeric,
    color text,
    size text,
    brand text,
    material text,
    description text,
    rating numeric,
    reviews int,
    release_date date,
    expiry_date date,
    unit_price numeric
);

DROP TABLE IF EXISTS dim_seller;
CREATE TABLE dim_seller (
    seller_sk SERIAL PRIMARY KEY,
    seller_id bigint UNIQUE,
    first_name text NOT NULL,
    last_name text NOT NULL,
    email text,
    country text,
    postal_code text
);

DROP TABLE IF EXISTS dim_store;
CREATE TABLE dim_store (
    store_sk SERIAL PRIMARY KEY,
    name text UNIQUE,
    location text,
    city text,
    state text,
    country text,
    phone text,
    email text
);

DROP TABLE IF EXISTS dim_supplier;
CREATE TABLE dim_supplier (
    supplier_sk SERIAL PRIMARY KEY,
    name text UNIQUE,
    contact text,
    email text,
    phone text,
    address text,
    city text,
    country text
);

DROP TABLE IF EXISTS fact_sales;
CREATE TABLE fact_sales (
    sale_sk SERIAL PRIMARY KEY,
    date_sk int NOT NULL REFERENCES dim_date(date_sk),
    customer_sk int NOT NULL REFERENCES dim_customer(customer_sk),
    seller_sk int NOT NULL REFERENCES dim_seller(seller_sk),
    product_sk int NOT NULL REFERENCES dim_product(product_sk),
    store_sk int NOT NULL REFERENCES dim_store(store_sk),
    supplier_sk int NOT NULL REFERENCES dim_supplier(supplier_sk),
    sale_quantity int,
    sale_total_price numeric,
    unit_price numeric
);