DROP TABLE IF EXISTS mock_data;
CREATE TABLE mock_data (
	id int4 NULL,
	customer_first_name varchar(50) NULL,
	customer_last_name varchar(50) NULL,
	customer_age int4 NULL,
	customer_email varchar(50) NULL,
	customer_country varchar(50) NULL,
	customer_postal_code varchar(50) NULL,
	customer_pet_type varchar(50) NULL,
	customer_pet_name varchar(50) NULL,
	customer_pet_breed varchar(50) NULL,
	seller_first_name varchar(50) NULL,
	seller_last_name varchar(50) NULL,
	seller_email varchar(50) NULL,
	seller_country varchar(50) NULL,
	seller_postal_code varchar(50) NULL,
	product_name varchar(50) NULL,
	product_category varchar(50) NULL,
	product_price float4 NULL,
	product_quantity int4 NULL,
	sale_date varchar(50) NULL,
	sale_customer_id int4 NULL,
	sale_seller_id int4 NULL,
	sale_product_id int4 NULL,
	sale_quantity int4 NULL,
	sale_total_price float4 NULL,
	store_name varchar(50) NULL,
	store_location varchar(50) NULL,
	store_city varchar(50) NULL,
	store_state varchar(50) NULL,
	store_country varchar(50) NULL,
	store_phone varchar(50) NULL,
	store_email varchar(50) NULL,
	pet_category varchar(50) NULL,
	product_weight float4 NULL,
	product_color varchar(50) NULL,
	product_size varchar(50) NULL,
	product_brand varchar(50) NULL,
	product_material varchar(50) NULL,
	product_description varchar(1024) NULL,
	product_rating float4 NULL,
	product_reviews int4 NULL,
	product_release_date varchar(50) NULL,
	product_expiry_date varchar(50) NULL,
	supplier_name varchar(50) NULL,
	supplier_contact varchar(50) NULL,
	supplier_email varchar(50) NULL,
	supplier_phone varchar(50) NULL,
	supplier_address varchar(50) NULL,
	supplier_city varchar(50) NULL,
	supplier_country varchar(50) NULL
);

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

COPY mock_data FROM '/resources/mock_data/MOCK_DATA (1).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/resources/mock_data/MOCK_DATA (2).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/resources/mock_data/MOCK_DATA (3).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/resources/mock_data/MOCK_DATA (4).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/resources/mock_data/MOCK_DATA (5).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/resources/mock_data/MOCK_DATA (6).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/resources/mock_data/MOCK_DATA (7).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/resources/mock_data/MOCK_DATA (8).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/resources/mock_data/MOCK_DATA (9).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/resources/mock_data/MOCK_DATA.csv'
  WITH (FORMAT csv, HEADER true);