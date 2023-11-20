##TASK 2

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, JSON, DateTime, Boolean, Float, Integer
from fastparquet import ParquetFile

#  Read Data
pd.set_option('display.max_columns', None)
def parq ():
    pf = ParquetFile('dataset/yellow_tripdata_2023-01.parquet')
    df = pf.to_pandas()
    return df


#  Clean data
def get_manipulate(df):
    df.dropna(inplace=True)
    df['passenger_count'] = df['passenger_count'].astype('int8')
    df['VendorID'] = df['VendorID'].astype('int8')
    df['PULocationID'] = df['PULocationID'].astype('int8')
    df['DOLocationID'] = df['DOLocationID'].astype('int8')
    df['payment_type'] = df['payment_type'].astype('int8')
    df['RatecodeID'] = df['RatecodeID'].astype('int8')

    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].replace (['N', 'Y'], [False, True])
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype('boolean')

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    return df

# Create Connect
def get_postgres():
    user = 'postgres'
    password = 'admin'
    host = 'localhost'
    database = 'mydb'
    port = 5432

    conn_str = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(conn_str)
    return engine

# Schema
def load_to_postgres(engine):
    df_schema = {
        'VendorID':BigInteger,
        'tpep_pickup_datetime': DateTime,
        'tpep_dropoff_datetime': DateTime,
        'passenger_count': BigInteger,
        'trip_distance': Float,
        'RatecodeID':Float,
        'store_and_fwd_flag':Boolean,
        'PULocationID':BigInteger,
        'DOLocationID':BigInteger,
        'payment_type':BigInteger,
        'fare_amount':Float,
        'extra':Float,
        'mta_tax':Float,
        'tip_amount':Float,
        'tolls_amount':Float,
        'improvement_surcharge':Float,
        'airport_fee':Float
    }

    df.to_sql(name='tugas_day2',con=engine, if_exists='replace', index=False, schema='public', dtype=df_schema, method=None, chunksize=100)

df = parq()
man = get_manipulate(df)
postgres_con = get_postgres()

load_to_postgres(postgres_con)