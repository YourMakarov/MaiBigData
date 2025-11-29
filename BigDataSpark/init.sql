CREATE TABLE IF NOT EXISTS raw_mock (
    id INTEGER,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INTEGER,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC,
    product_quantity INTEGER,
    sale_date DATE,
    sale_customer_id INTEGER,
    sale_seller_id INTEGER,
    sale_product_id INTEGER,
    sale_quantity INTEGER,
    sale_total_price NUMERIC,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    pet_category TEXT,
    product_weight NUMERIC,
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC,
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

COPY raw_mock FROM '/data/mock_data1.csv' DELIMITER ',' CSV HEADER;
COPY raw_mock FROM '/data/mock_data2.csv' DELIMITER ',' CSV HEADER;
COPY raw_mock FROM '/data/mock_data3.csv' DELIMITER ',' CSV HEADER;
COPY raw_mock FROM '/data/mock_data4.csv' DELIMITER ',' CSV HEADER;
COPY raw_mock FROM '/data/mock_data5.csv' DELIMITER ',' CSV HEADER;
COPY raw_mock FROM '/data/mock_data6.csv' DELIMITER ',' CSV HEADER;
COPY raw_mock FROM '/data/mock_data7.csv' DELIMITER ',' CSV HEADER;
COPY raw_mock FROM '/data/mock_data8.csv' DELIMITER ',' CSV HEADER;
COPY raw_mock FROM '/data/mock_data9.csv' DELIMITER ',' CSV HEADER;
COPY raw_mock FROM '/data/mock_data10.csv' DELIMITER ',' CSV HEADER;