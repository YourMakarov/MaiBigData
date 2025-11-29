CREATE TABLE IF NOT EXISTS sales_by_products (
    product_name String,
    product_category String,
    total_revenue Float64,
    total_quantity Int64,
    avg_rating Float64,
    review_count Int64
) ENGINE = MergeTree()
ORDER BY (product_name);

CREATE TABLE IF NOT EXISTS sales_by_customers (
    customer_name String,
    customer_country String,
    total_purchases Float64,
    num_orders Int64,
    avg_order_value Float64,
    pet_types_count Int64
) ENGINE = MergeTree()
ORDER BY (customer_name);

CREATE TABLE IF NOT EXISTS sales_by_time (
    year Int32,
    month Int32,
    monthly_revenue Float64,
    num_orders Int64,
    avg_order_amount Float64
) ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS sales_by_stores (
    store_name String,
    store_city String,
    store_country String,
    total_revenue Float64,
    num_orders Int64,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY (store_name);

CREATE TABLE IF NOT EXISTS sales_by_suppliers (
    supplier_name String,
    supplier_country String,
    total_revenue Float64,
    avg_price Float64
) ENGINE = MergeTree()
ORDER BY (supplier_name);

CREATE TABLE IF NOT EXISTS product_quality (
    product_name String,
    avg_rating Float64,
    review_count Int64,
    sales_volume Int64,
    rating_sales_correlation Float64
) ENGINE = MergeTree()
ORDER BY (product_name);