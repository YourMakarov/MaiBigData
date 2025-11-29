from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, sum, count, countDistinct, corr, broadcast

spark = SparkSession.builder \
    .appName("Reports_ClickHouse_Optimized") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

jdbc_pg = "jdbc:postgresql://postgres:5432/bigdata"
prop_pg = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

jdbc_ch = "jdbc:clickhouse:http://clickhouse:8123/default?ssl=false&compress=0"
prop_ch = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "socket_timeout": "300000",
    "connect_timeout": "30000",
    "batchsize": "1000"
}

fact = spark.read.jdbc(jdbc_pg, "fact_sales", properties=prop_pg).repartition(50).alias("fact").persist()
dim_customer = spark.read.jdbc(jdbc_pg, "dim_customer", properties=prop_pg).repartition(50).alias("dim_customer").persist()
dim_seller = spark.read.jdbc(jdbc_pg, "dim_seller", properties=prop_pg).repartition(50).alias("dim_seller").persist()
dim_product = spark.read.jdbc(jdbc_pg, "dim_product", properties=prop_pg).repartition(50).alias("dim_product").persist()
dim_supplier = spark.read.jdbc(jdbc_pg, "dim_supplier", properties=prop_pg).repartition(50).alias("dim_supplier").persist()
dim_store = spark.read.jdbc(jdbc_pg, "dim_store", properties=prop_pg).repartition(50).alias("dim_store").persist()
dim_date = spark.read.jdbc(jdbc_pg, "dim_date", properties=prop_pg).repartition(50).alias("dim_date").persist()

dim_product_broadcast = broadcast(dim_product)
dim_customer_broadcast = broadcast(dim_customer)
dim_date_broadcast = broadcast(dim_date)
dim_store_broadcast = broadcast(dim_store)
dim_supplier_broadcast = broadcast(dim_supplier)

v1 = fact.join(dim_product_broadcast, col("fact.product_key") == col("dim_product.product_key")) \
    .groupBy(col("dim_product.product_name"), col("dim_product.product_category")) \
    .agg(sum(col("fact.sale_total_price")).alias("total_revenue"),
         sum(col("fact.sale_quantity")).alias("total_quantity"),
         avg(col("fact.product_rating")).alias("avg_rating"),
         count(when(col("fact.product_rating").isNotNull(), 1)).alias("review_count"))
v1.write.jdbc(jdbc_ch, "sales_by_products", mode="append", properties=prop_ch)

v2 = fact.join(dim_customer_broadcast, col("fact.customer_key") == col("dim_customer.customer_key")) \
    .groupBy(col("dim_customer.customer_name"), col("dim_customer.customer_country")) \
    .agg(sum(col("fact.sale_total_price")).alias("total_purchases"),
         count("*").alias("num_orders"),
         avg(col("fact.sale_total_price")).alias("avg_order_value"),
         countDistinct(col("dim_customer.customer_pet_type")).alias("pet_types_count"))
v2.write.jdbc(jdbc_ch, "sales_by_customers", mode="append", properties=prop_ch)

v3 = fact.join(dim_date_broadcast, col("fact.date_key") == col("dim_date.date_key")) \
    .groupBy(col("dim_date.year"), col("dim_date.month")) \
    .agg(sum(col("fact.sale_total_price")).alias("monthly_revenue"),
         count("*").alias("num_orders"),
         avg(col("fact.sale_total_price")).alias("avg_order_amount"))
v3.write.jdbc(jdbc_ch, "sales_by_time", mode="append", properties=prop_ch)

v4 = fact.join(dim_store_broadcast, col("fact.store_key") == col("dim_store.store_key")) \
    .groupBy(col("dim_store.store_name"), col("dim_store.store_city"), col("dim_store.store_country")) \
    .agg(sum(col("fact.sale_total_price")).alias("total_revenue"),
         count("*").alias("num_orders"),
         avg(col("fact.sale_total_price")).alias("avg_order_value"))
v4.write.jdbc(jdbc_ch, "sales_by_stores", mode="append", properties=prop_ch)

v5 = fact.join(dim_supplier_broadcast, col("fact.supplier_key") == col("dim_supplier.supplier_key")) \
    .groupBy(col("dim_supplier.supplier_name"), col("dim_supplier.supplier_country")) \
    .agg(sum(col("fact.sale_total_price")).alias("total_revenue"),
         (sum(col("fact.sale_total_price")) / sum(col("fact.sale_quantity"))).alias("avg_price"))
v5.write.jdbc(jdbc_ch, "sales_by_suppliers", mode="append", properties=prop_ch)

v6 = fact.join(dim_product_broadcast, col("fact.product_key") == col("dim_product.product_key")) \
    .groupBy(col("dim_product.product_name")) \
    .agg(avg(col("fact.product_rating")).alias("avg_rating"),
         count(when(col("fact.product_rating").isNotNull(), 1)).alias("review_count"),
         sum(col("fact.sale_quantity")).alias("sales_volume"),
         corr(col("fact.product_rating"), col("fact.sale_quantity")).alias("rating_sales_correlation"))
v6.write.jdbc(jdbc_ch, "product_quality", mode="append", properties=prop_ch)

fact.unpersist()
dim_customer.unpersist()
dim_seller.unpersist()
dim_product.unpersist()
dim_supplier.unpersist()
dim_store.unpersist()
dim_date.unpersist()

spark.stop()
print("6 витрин в ClickHouse готово!")