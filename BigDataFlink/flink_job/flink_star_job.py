import os
import json
import time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.datastream.connectors.jdbc import (
    JdbcSink,
    JdbcExecutionOptions,
    JdbcConnectionOptions
)
from datetime import datetime
from enriched_record import EnrichedRecord
from pyflink.common import Row, Types

class EnrichmentMapFunction(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        from dimension_enrichment import DimensionEnrichment
        self.enricher = DimensionEnrichment(
            os.getenv("POSTGRES_URL"),
            os.getenv("POSTGRES_USER"),
            os.getenv("POSTGRES_PASSWORD"),
        )
    def map(self, value: str) -> EnrichedRecord:
        record = json.loads(value)
        sale_date = record["sale_date"]
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                dt = datetime.strptime(sale_date, fmt)
                break
            except ValueError:
                pass
        else:
            raise ValueError(f"Unknown date format: {sale_date!r}")
        sale_date = dt.strftime("%Y-%m-%d")

        date_sk = self.enricher.upsert_and_get_sk(
            "dim_date",
            ("sale_date",),
            {
                "sale_date": sale_date,
                "year": dt.year,
                "quarter": (dt.month - 1) // 3 + 1,
                "month": dt.month,
                "day": dt.day,
                "weekday": dt.isoweekday(),
            },
        )

        customer_sk = self.enricher.upsert_and_get_sk(
            "dim_customer",
            ("customer_id",),
            {
                "customer_id": int(record["sale_customer_id"]),
                "first_name": record["customer_first_name"],
                "last_name": record["customer_last_name"],
                "age": int(record["customer_age"]),
                "email": record["customer_email"],
                "country": record["customer_country"],
                "postal_code": record["customer_postal_code"],
            },
        )
        seller_sk = self.enricher.upsert_and_get_sk(
            "dim_seller",
            ("seller_id",),
            {
                "seller_id": int(record["sale_seller_id"]),
                "first_name": record["seller_first_name"],
                "last_name": record["seller_last_name"],
                "email": record["seller_email"],
                "country": record["seller_country"],
                "postal_code": record["seller_postal_code"],
            },
        )
        product_sk = self.enricher.upsert_and_get_sk(
            "dim_product",
            ("product_id",),
            {
                "product_id": int(record["sale_product_id"]),
                "name":       record["product_name"],
                "category":   record["product_category"],
                "unit_price": float(record["product_price"]),  
                "weight":     float(record["product_weight"]),
                "color":      record["product_color"],
                "size":       record["product_size"],
                "brand":      record["product_brand"],
                "material":   record["product_material"],
                "description":record["product_description"],
                "rating":     float(record["product_rating"]),
                "reviews":    int(record["product_reviews"]),
                "release_date":record["product_release_date"],
                "expiry_date": record["product_expiry_date"],
            },
        )

        store_sk = self.enricher.upsert_and_get_sk(
            "dim_store",
            ("name",),
            {
                "name": record["store_name"],
                "location": record["store_location"],
                "city": record["store_city"],
                "state": record["store_state"],
                "country": record["store_country"],
                "phone": record["store_phone"],
                "email": record["store_email"],
            },
        )

        supplier_sk = self.enricher.upsert_and_get_sk(
            "dim_supplier",
            ("name",),                       
            {
                "name":     record["supplier_name"],
                "contact":  record["supplier_contact"],
                "email":    record["supplier_email"],
                "phone":    record["supplier_phone"],
                "address":  record["supplier_address"],
                "city":     record["supplier_city"],
                "country":  record["supplier_country"],
            },
        )

        return EnrichedRecord(
            date_sk=date_sk,
            customer_sk=customer_sk,
            seller_sk=seller_sk,
            product_sk=product_sk,
            store_sk=store_sk,
            supplier_sk=supplier_sk,
            sale_quantity=int(record["sale_quantity"]),
            sale_total_price=float(record["sale_total_price"]),
            unit_price=float(record["product_price"]),
        )

def _build_jdbc_url(pg_url: str) -> str:
    if pg_url.startswith("jdbc:"):
        return pg_url
    if "://" in pg_url:
        pg_url = pg_url.split("://", 1)[1]
    if "@" in pg_url:
        pg_url = pg_url.split("@", 1)[1]
    return f"jdbc:postgresql://{pg_url}"


kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "raw-mock")
pg_url = os.getenv("POSTGRES_URL")
pg_user = os.getenv("POSTGRES_USER")
pg_pass = os.getenv("POSTGRES_PASSWORD")

env = StreamExecutionEnvironment.get_execution_environment()

consumer = FlinkKafkaConsumer(
    topics=[kafka_topic],
    deserialization_schema=SimpleStringSchema(),
    properties={
        "bootstrap.servers": kafka_bootstrap,
        "group.id": f"flink-star-group-{int(time.time())}",
        "auto.offset.reset": "earliest"
    },
)
consumer.set_start_from_earliest()
ds = env.add_source(consumer)

enriched_ds = ds.map(EnrichmentMapFunction())

row_ds = enriched_ds.map(
    lambda e: Row(
        e.date_sk,
        e.customer_sk,
        e.seller_sk,
        e.product_sk,
        e.store_sk,
        e.supplier_sk,
        e.sale_quantity,
        e.sale_total_price,
        e.unit_price,
    ),
    output_type=Types.ROW(
        [
            Types.LONG(),   # date_sk
            Types.LONG(),   # customer_sk
            Types.LONG(),   # seller_sk
            Types.LONG(),   # product_sk
            Types.LONG(),   # store_sk
            Types.LONG(),   # supplier_sk
            Types.INT(),    # sale_quantity
            Types.DOUBLE(), # sale_total_price
            Types.DOUBLE()  # unit_price
        ]
    ),
)

insert_sql = """
    INSERT INTO fact_sales(
        date_sk, customer_sk, seller_sk,
        product_sk, store_sk, supplier_sk,
        sale_quantity, sale_total_price, unit_price
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""".strip()

row_type = Types.ROW([
    Types.LONG(), Types.LONG(), Types.LONG(),
    Types.LONG(), Types.LONG(), Types.LONG(),
    Types.INT(),  Types.DOUBLE(), Types.DOUBLE(),
])

row_ds.map(lambda r: (print(">>> GOT ROW:", r), r)[1], output_type=Types.ROW([Types.LONG(), Types.LONG(), Types.LONG(), Types.LONG(), Types.LONG(), Types.LONG(), Types.INT(), Types.DOUBLE(), Types.DOUBLE()]))

jdbc_sink = JdbcSink.sink(
    insert_sql,
    row_type,                              
    JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(_build_jdbc_url(pg_url))
        .with_driver_name("org.postgresql.Driver")
        .with_user_name(pg_user)
        .with_password(pg_pass)
        .build(),
    JdbcExecutionOptions.builder()
        .with_batch_size(1000)
        .with_batch_interval_ms(200)
        .build(),
)

row_ds.add_sink(jdbc_sink)   


env.execute("PyFlink Star Schema Job")



