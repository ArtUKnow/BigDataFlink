from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

t_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    "file:///opt/flink/lib/flink-sql-connector-kafka-1.17.0.jar;"
    "file:///opt/flink/lib/flink-connector-jdbc-3.1.0-1.17.jar;"
    "file:///opt/flink/lib/postgresql-42.6.0.jar"
)

source_ddl = """
CREATE TABLE raw_kafka (
    id STRING,
    customer_first_name STRING,
    customer_last_name STRING,
    customer_age STRING,
    customer_email STRING,
    customer_country STRING,
    customer_postal_code STRING,
    customer_pet_type STRING,
    customer_pet_name STRING,
    customer_pet_breed STRING,
    seller_first_name STRING,
    seller_last_name STRING,
    seller_email STRING,
    seller_country STRING,
    seller_postal_code STRING,
    product_name STRING,
    product_category STRING,
    product_price STRING,
    product_quantity STRING,
    sale_date STRING,
    sale_customer_id STRING,
    sale_seller_id STRING,
    sale_product_id STRING,
    sale_quantity STRING,
    sale_total_price STRING,
    store_name STRING,
    store_location STRING,
    store_city STRING,
    store_state STRING,
    store_country STRING,
    store_phone STRING,
    store_email STRING,
    pet_category STRING,
    product_weight STRING,
    product_color STRING,
    product_size STRING,
    product_brand STRING,
    product_material STRING,
    product_description STRING,
    product_rating STRING,
    product_reviews STRING,
    product_release_date STRING,
    product_expiry_date STRING,
    supplier_name STRING,
    supplier_contact STRING,
    supplier_email STRING,
    supplier_phone STRING,
    supplier_address STRING,
    supplier_city STRING,
    supplier_country STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw_data_topic',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink_group',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
)
"""
t_env.execute_sql(source_ddl)

jdbc_sink_options = """
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/analytics',
    'username' = 'user',
    'password' = 'password'
"""

t_env.execute_sql(f"""
CREATE TABLE dim_customer (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    age INT,
    email STRING,
    country STRING,
    postal_code STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH ({jdbc_sink_options}, 'table-name' = 'dim_customer')
""")

t_env.execute_sql(f"""
CREATE TABLE dim_supplier (
    supplier_name STRING,
    contact_name STRING,
    email STRING,
    phone STRING,
    country STRING,
    city STRING,
    address STRING,
    PRIMARY KEY (supplier_name) NOT ENFORCED
) WITH ({jdbc_sink_options}, 'table-name' = 'dim_supplier')
""")

t_env.execute_sql(f"""
CREATE TABLE dim_product (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    weight DECIMAL(10, 2),
    color STRING,
    `size` STRING,
    brand STRING,
    material STRING,
    description STRING,
    rating DECIMAL(3, 2),
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    supplier_name STRING,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH ({jdbc_sink_options}, 'table-name' = 'dim_product')
""")

t_env.execute_sql(f"""
CREATE TABLE dim_seller (
    seller_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    country STRING,
    postal_code STRING,
    PRIMARY KEY (seller_id) NOT ENFORCED
) WITH ({jdbc_sink_options}, 'table-name' = 'dim_seller')
""")

t_env.execute_sql(f"""
CREATE TABLE dim_store (
    store_name STRING,
    phone STRING,
    email STRING,
    country STRING,
    city STRING,
    state STRING,
    location STRING,
    PRIMARY KEY (store_name) NOT ENFORCED
) WITH ({jdbc_sink_options}, 'table-name' = 'dim_store')
""")

t_env.execute_sql(f"""
CREATE TABLE fact_sales (
    sale_id INT,
    sale_date DATE,
    customer_id INT,
    seller_id INT,
    product_id INT,
    store_name STRING,
    quantity INT,
    total_price DECIMAL(10, 2),
    PRIMARY KEY (sale_id) NOT ENFORCED
) WITH ({jdbc_sink_options}, 'table-name' = 'fact_sales')
""")

statement_set = t_env.create_statement_set()

statement_set.add_insert_sql("""
INSERT INTO dim_customer
SELECT CAST(sale_customer_id AS INT), customer_first_name, customer_last_name, CAST(customer_age AS INT), customer_email, customer_country, customer_postal_code
FROM raw_kafka WHERE sale_customer_id IS NOT NULL AND sale_customer_id <> ''
""")

statement_set.add_insert_sql("""
INSERT INTO dim_supplier
SELECT supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_country, supplier_city, supplier_address
FROM raw_kafka WHERE supplier_name IS NOT NULL AND supplier_name <> ''
""")

statement_set.add_insert_sql("""
INSERT INTO dim_product
SELECT CAST(sale_product_id AS INT), product_name, product_category, CAST(product_price AS DECIMAL(10, 2)), CAST(product_weight AS DECIMAL(10, 2)), product_color, product_size, product_brand, product_material, product_description, CAST(product_rating AS DECIMAL(3, 2)), CAST(product_reviews AS INT), TO_DATE(product_release_date, 'MM/dd/yyyy'), TO_DATE(product_expiry_date, 'MM/dd/yyyy'), supplier_name
FROM raw_kafka WHERE sale_product_id IS NOT NULL AND sale_product_id <> ''
""")

statement_set.add_insert_sql("""
INSERT INTO dim_seller
SELECT CAST(sale_seller_id AS INT), seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code
FROM raw_kafka WHERE sale_seller_id IS NOT NULL AND sale_seller_id <> ''
""")

statement_set.add_insert_sql("""
INSERT INTO dim_store
SELECT store_name, store_phone, store_email, store_country, store_city, store_state, store_location
FROM raw_kafka WHERE store_name IS NOT NULL AND store_name <> ''
""")

statement_set.add_insert_sql("""
INSERT INTO fact_sales
SELECT CAST(id AS INT), TO_DATE(sale_date, 'MM/dd/yyyy'), CAST(sale_customer_id AS INT), CAST(sale_seller_id AS INT), CAST(sale_product_id AS INT), store_name, CAST(sale_quantity AS INT), CAST(sale_total_price AS DECIMAL(10, 2))
FROM raw_kafka WHERE id IS NOT NULL AND id <> ''
""")

statement_set.execute()
