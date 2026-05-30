CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE dim_pet (
    pet_id TEXT PRIMARY KEY,
    pet_type TEXT,
    pet_category TEXT,
    pet_breed TEXT,
    pet_name TEXT,
    customer_id INT
);

CREATE TABLE dim_seller (
    seller_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE dim_supplier (
    supplier_name TEXT PRIMARY KEY,
    contact_name TEXT,
    email TEXT,
    phone TEXT,
    country TEXT,
    city TEXT,
    address TEXT
);

CREATE TABLE dim_store (
    store_name TEXT PRIMARY KEY,
    phone TEXT,
    email TEXT,
    country TEXT,
    city TEXT,
    state TEXT,
    location TEXT
);

CREATE TABLE dim_product (
    product_id INT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    price NUMERIC,
    weight NUMERIC,
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC,
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    supplier_name TEXT
);

CREATE TABLE fact_sales (
    sale_id INT PRIMARY KEY,
    sale_date DATE,
    customer_id INT,
    seller_id INT,
    product_id INT,
    store_name TEXT,
    quantity INT,
    total_price NUMERIC
);
