-- Под-измерения для продуктов
CREATE TABLE IF NOT EXISTS dim_product_category (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_pet_category (
    pet_category_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_brand (
    brand_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_material (
    material_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_color (
    color_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_size (
    size_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

-- Основные измерения
CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    contact VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(50),
    address VARCHAR(200),
    city VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    phone VARCHAR(50),
    email VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    email VARCHAR(100),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    pet_type VARCHAR(50),
    pet_name VARCHAR(50),
    pet_breed VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    country VARCHAR(50),
    postal_code VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2),
    quantity INT,
    weight DECIMAL(10,2),
    description TEXT,
    rating DECIMAL(3,1),
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    supplier_id INT REFERENCES dim_supplier(supplier_id),
    category_id INT REFERENCES dim_product_category(category_id),
    pet_category_id INT REFERENCES dim_pet_category(pet_category_id),
    brand_id INT REFERENCES dim_brand(brand_id),
    material_id INT REFERENCES dim_material(material_id),
    color_id INT REFERENCES dim_color(color_id),
    size_id INT REFERENCES dim_size(size_id)
);

-- Факт-таблица
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id INT PRIMARY KEY,
    sale_date DATE,
    customer_id INT REFERENCES dim_customer(customer_id),
    seller_id INT REFERENCES dim_seller(seller_id),
    product_id INT REFERENCES dim_product(product_id),
    store_id INT REFERENCES dim_store(store_id),
    quantity INT,
    total_price DECIMAL(10,2)
);