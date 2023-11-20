##Task 4

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

# Membuat koneksi ke postgres
def conn_postgres():
    pg_user = "postgres"
    pg_password = "pass"
    pg_host = "localhost"
    pg_port = 5439
    pg_database = "store"
    conn_str = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}'
    engine = create_engine(conn_str)
    return engine


# Membuat koneksi ke citus
def conn_citus():
    citus_user = "postgres"
    citus_password = "pass"
    citus_host = "localhost"
    citus_port = 15432
    citus_database = "store"
    conn_str = f"postgresql+psycopg2://{citus_user}:{citus_password}@{citus_host}:{citus_port}/{citus_database}"
    citus_engine = create_engine(conn_str)
    return citus_engine

# Eksekusi SQL yang dikembalikan ke bentuk DataFrame
def execute_sql_query(engine, query):
    with engine.connect() as conn:
        result = conn.execute(text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df


# Load to sql
if __name__ == "__main__":
    pg_engine = conn_postgres()
    citus_engine = conn_citus()

    query_brands = "SELECT * FROM brands"
    df_brands = execute_sql_query(pg_engine, query_brands)

    query_orders = "SELECT * FROM orders"
    df_orders = execute_sql_query(pg_engine, query_orders)

    query_order_details = "SELECT * FROM order_details"
    df_od = execute_sql_query(pg_engine, query_order_details)

    query_products = "SELECT * from products"
    df_products = execute_sql_query(pg_engine, query_products)

    df_brands.to_sql("brands", citus_engine, if_exists="replace", index=False)
    df_orders.to_sql("orders", citus_engine, if_exists="replace", index=False)
    df_od.to_sql("order_details", citus_engine, if_exists="replace", index=False)
    df_products.to_sql("products", citus_engine, if_exists="replace", index=False)