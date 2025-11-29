from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, concat, lit, coalesce, monotonically_increasing_id

spark = SparkSession.builder \
    .appName("ETL_Star_Optimized") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/bigdata"
prop = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

raw = spark.read.jdbc(jdbc_url, "raw_mock", properties=prop) \
    .withColumn("sale_date", col("sale_date").cast("date")) \
    .withColumn("product_price", col("product_price").cast("double")) \
    .withColumn("sale_quantity", col("sale_quantity").cast("int")) \
    .withColumn("sale_total_price", coalesce(col("sale_total_price").cast("double"), col("product_price") * col("sale_quantity"))) \
    .withColumn("product_rating", col("product_rating").cast("double")) \
    .withColumn("customer_age", col("customer_age").cast("int")) \
    .repartition(200)  

# dim_customer
dim_customer = raw.select(
    "customer_first_name", "customer_last_name", "customer_age", "customer_email", "customer_country", "customer_postal_code",
    "customer_pet_type", "customer_pet_name", "customer_pet_breed"
).distinct() \
    .withColumn("customer_name", concat(col("customer_first_name"), lit(" "), col("customer_last_name"))) \
    .withColumn("customer_key", monotonically_increasing_id())
dim_customer.write.jdbc(jdbc_url, "dim_customer", mode="overwrite", properties=prop)

# dim_seller
dim_seller = raw.select(
    "seller_first_name", "seller_last_name", "seller_email", "seller_country", "seller_postal_code"
).distinct() \
    .withColumn("seller_name", concat(col("seller_first_name"), lit(" "), col("seller_last_name"))) \
    .withColumn("seller_key", monotonically_increasing_id())
dim_seller.write.jdbc(jdbc_url, "dim_seller", mode="overwrite", properties=prop)

# dim_product
dim_product = raw.select(
    "product_name", "product_category", "product_price", "product_quantity", "pet_category", "product_weight", "product_color",
    "product_size", "product_brand", "product_material", "product_description", "product_rating", "product_reviews",
    "product_release_date", "product_expiry_date"
).distinct() \
    .withColumn("product_key", monotonically_increasing_id())
dim_product.write.jdbc(jdbc_url, "dim_product", mode="overwrite", properties=prop)

# dim_supplier
dim_supplier = raw.select(
    "supplier_name", "supplier_contact", "supplier_email", "supplier_phone", "supplier_address", "supplier_city", "supplier_country"
).distinct() \
    .withColumn("supplier_key", monotonically_increasing_id())
dim_supplier.write.jdbc(jdbc_url, "dim_supplier", mode="overwrite", properties=prop)

# dim_store
dim_store = raw.select(
    "store_name", "store_location", "store_city", "store_state", "store_country", "store_phone", "store_email"
).distinct() \
    .withColumn("store_key", monotonically_increasing_id())
dim_store.write.jdbc(jdbc_url, "dim_store", mode="overwrite", properties=prop)

# dim_date
dim_date = raw.select("sale_date").distinct() \
    .withColumn("date_key", monotonically_increasing_id()) \
    .withColumn("year", year("sale_date")) \
    .withColumn("month", month("sale_date")) \
    .withColumn("day", dayofmonth("sale_date"))
dim_date.write.jdbc(jdbc_url, "dim_date", mode="overwrite", properties=prop)

# fact_sales
df = raw
for dim, keys, key_col in [
    (dim_customer, ["customer_first_name", "customer_last_name", "customer_email"], "customer_key"),
    (dim_seller, ["seller_first_name", "seller_last_name", "seller_email"], "seller_key"),
    (dim_product, ["product_name", "product_category"], "product_key"),
    (dim_supplier, ["supplier_name", "supplier_email"], "supplier_key"),
    (dim_store, ["store_name", "store_city", "store_country"], "store_key"),
    (dim_date, ["sale_date"], "date_key")
]:
    df = df.join(dim.alias("dim"), keys, "left") \
           .select(df["*"], col(f"dim.{key_col}").alias(key_col))

fact = df.select(
    col("id").alias("sale_id"),
    "customer_key", "seller_key", "product_key", "supplier_key", "store_key", "date_key",
    "sale_quantity", "sale_total_price", "product_rating"
)
fact.write.jdbc(jdbc_url, "fact_sales", mode="overwrite", properties=prop)

spark.stop()
print("Star schema готова!")