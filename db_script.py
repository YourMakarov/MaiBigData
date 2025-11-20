import psycopg2
import pandas as pd
import os
from sqlalchemy import create_engine

# Параметры подключения
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'mydatabase'
DB_USER = 'myuser'
DB_PASSWORD = 'mypassword'

# Путь к CSV
CSV_DIR = 'data/'
CSV_FILES = [f'mock_data{i}.csv' for i in range(1, 11)]

# Engine для SQLAlchemy
ENGINE = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

def connect_db():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn

def create_mock_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mock_data (
                id SERIAL PRIMARY KEY,
                customer_first_name VARCHAR(50),
                customer_last_name VARCHAR(50),
                customer_age INT,
                customer_email VARCHAR(100),
                customer_country VARCHAR(50),
                customer_postal_code VARCHAR(20),
                customer_pet_type VARCHAR(50),
                customer_pet_name VARCHAR(50),
                customer_pet_breed VARCHAR(50),
                seller_first_name VARCHAR(50),
                seller_last_name VARCHAR(50),
                seller_email VARCHAR(100),
                seller_country VARCHAR(50),
                seller_postal_code VARCHAR(20),
                product_name VARCHAR(100),
                product_category VARCHAR(50),
                product_price DECIMAL(10,2),
                product_quantity INT,
                sale_date DATE,
                sale_customer_id INT,
                sale_seller_id INT,
                sale_product_id INT,
                sale_quantity INT,
                sale_total_price DECIMAL(10,2),
                store_name VARCHAR(100),
                store_location VARCHAR(200),
                store_city VARCHAR(50),
                store_state VARCHAR(50),
                store_country VARCHAR(50),
                store_phone VARCHAR(50),
                store_email VARCHAR(100),
                pet_category VARCHAR(50),
                product_weight DECIMAL(10,2),
                product_color VARCHAR(50),
                product_size VARCHAR(50),
                product_brand VARCHAR(50),
                product_material VARCHAR(50),
                product_description TEXT,
                product_rating DECIMAL(3,1),
                product_reviews INT,
                product_release_date DATE,
                product_expiry_date DATE,
                supplier_name VARCHAR(100),
                supplier_contact VARCHAR(100),
                supplier_email VARCHAR(100),
                supplier_phone VARCHAR(50),
                supplier_address VARCHAR(200),
                supplier_city VARCHAR(50),
                supplier_country VARCHAR(50)
            );
        """)
    conn.commit()
    print("Таблица mock_data создана.")

def import_csv_to_mock():
    for csv_file in CSV_FILES:
        full_path = os.path.join(CSV_DIR, csv_file)
        if not os.path.exists(full_path):
            print(f"Файл {full_path} не найден. Пропускаю.")
            continue
        
        try:
            dtype_dict = {
                'customer_age': 'Int64',
                'product_price': 'float64',
                'product_quantity': 'Int64',
                'sale_customer_id': 'Int64',
                'sale_seller_id': 'Int64',
                'sale_product_id': 'Int64',
                'sale_quantity': 'Int64',
                'sale_total_price': 'float64',
                'product_weight': 'float64',
                'product_rating': 'float64',
                'product_reviews': 'Int64',
            }
            df = pd.read_csv(full_path, dtype=dtype_dict, quotechar='"', escapechar='\\', on_bad_lines='warn')
            
            # Удаляем колонку 'id', если она есть в CSV (чтобы SERIAL генерировал уникальные id)
            if 'id' in df.columns:
                df = df.drop(columns=['id'])
            
            date_cols = ['sale_date', 'product_release_date', 'product_expiry_date']
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce')
            
            df.to_sql('mock_data', ENGINE, if_exists='append', index=False)
            print(f"Данные из {csv_file} импортированы. Строк: {len(df)}")
        except Exception as e:
            print(f"Ошибка при импорте {csv_file}: {e}")

def execute_sql_from_file(conn, file_path):
    with open(file_path, 'r') as f:
        sql_script = f.read()
    with conn.cursor() as cur:
        cur.execute(sql_script)
    conn.commit()
    print(f"Скрипт {file_path} выполнен.")

def clear_tables(conn):
    tables = [
        'fact_sales', 'dim_product', 'dim_customer', 'dim_seller', 'dim_store', 'dim_supplier',
        'dim_size', 'dim_color', 'dim_material', 'dim_brand', 'dim_pet_category', 'dim_product_category'
    ]
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")
    conn.commit()
    print("Все таблицы снежинки очищены.")

if __name__ == "__main__":
    conn = connect_db()
    try:
        create_mock_table(conn)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE mock_data RESTART IDENTITY;")
            conn.commit()
        import_csv_to_mock()
        clear_tables(conn)
        execute_sql_from_file(conn, 'ddl.sql')  
        execute_sql_from_file(conn, 'dml.sql')  
    except Exception as e:
        print(f"Общая ошибка: {e}")
    finally:
        conn.close()