from dataclasses import dataclass

@dataclass
class EnrichedRecord:
    date_sk: int
    customer_sk: int
    seller_sk: int
    product_sk: int
    store_sk: int
    supplier_sk: int
    sale_quantity: int
    sale_total_price: float
    unit_price: float
