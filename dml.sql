-- Заполнение под-измерений
INSERT INTO dim_product_category (name)
SELECT DISTINCT product_category FROM mock_data WHERE product_category IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO dim_pet_category (name)
SELECT DISTINCT pet_category FROM mock_data WHERE pet_category IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO dim_brand (name)
SELECT DISTINCT product_brand FROM mock_data WHERE product_brand IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO dim_material (name)
SELECT DISTINCT product_material FROM mock_data WHERE product_material IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO dim_color (name)
SELECT DISTINCT product_color FROM mock_data WHERE product_color IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO dim_size (name)
SELECT DISTINCT product_size FROM mock_data WHERE product_size IS NOT NULL
ON CONFLICT DO NOTHING;

-- Заполнение dim_supplier
INSERT INTO dim_supplier (name, contact, email, phone, address, city, country)
SELECT DISTINCT supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country
FROM mock_data
ON CONFLICT DO NOTHING;

-- Заполнение dim_store
INSERT INTO dim_store (name, location, city, state, country, phone, email)
SELECT DISTINCT store_name, store_location, store_city, store_state, store_country, store_phone, store_email
FROM mock_data
ON CONFLICT DO NOTHING;

-- Заполнение dim_customer
INSERT INTO dim_customer (customer_id, first_name, last_name, age, email, country, postal_code, pet_type, pet_name, pet_breed)
SELECT DISTINCT sale_customer_id, customer_first_name, customer_last_name, customer_age, customer_email, customer_country, customer_postal_code, customer_pet_type, customer_pet_name, customer_pet_breed
FROM mock_data
ON CONFLICT (customer_id) DO NOTHING;

-- Заполнение dim_seller
INSERT INTO dim_seller (seller_id, first_name, last_name, email, country, postal_code)
SELECT DISTINCT sale_seller_id, seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code
FROM mock_data
ON CONFLICT (seller_id) DO NOTHING;

-- Заполнение dim_product (удалены TO_DATE)
INSERT INTO dim_product (product_id, name, price, quantity, weight, description, rating, reviews, release_date, expiry_date, supplier_id, category_id, pet_category_id, brand_id, material_id, color_id, size_id)
SELECT DISTINCT m.sale_product_id, m.product_name, m.product_price, m.product_quantity, m.product_weight, m.product_description, m.product_rating, m.product_reviews,
       m.product_release_date, m.product_expiry_date,  -- Просто поля, без TO_DATE
       s.supplier_id, pc.category_id, petc.pet_category_id, b.brand_id, mat.material_id, col.color_id, sz.size_id
FROM mock_data m
JOIN dim_supplier s ON s.name = m.supplier_name AND s.contact = m.supplier_contact AND s.email = m.supplier_email AND s.phone = m.supplier_phone AND s.address = m.supplier_address AND s.city = m.supplier_city AND s.country = m.supplier_country
JOIN dim_product_category pc ON pc.name = m.product_category
JOIN dim_pet_category petc ON petc.name = m.pet_category
JOIN dim_brand b ON b.name = m.product_brand
JOIN dim_material mat ON mat.name = m.product_material
JOIN dim_color col ON col.name = m.product_color
JOIN dim_size sz ON sz.name = m.product_size
ON CONFLICT (product_id) DO NOTHING;

-- Заполнение fact_sales (удален TO_DATE)
INSERT INTO fact_sales (sale_id, sale_date, customer_id, seller_id, product_id, store_id, quantity, total_price)
SELECT m.id, m.sale_date,  -- Просто поле, без TO_DATE
       m.sale_customer_id, m.sale_seller_id, m.sale_product_id,
       s.store_id, m.sale_quantity, m.sale_total_price
FROM mock_data m
JOIN dim_store s ON s.name = m.store_name AND s.location = m.store_location AND s.city = m.store_city AND s.state = m.store_state AND s.country = m.store_country AND s.phone = m.store_phone AND s.email = m.store_email
ON CONFLICT (sale_id) DO NOTHING;