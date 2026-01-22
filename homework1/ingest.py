#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]


# For testing purposes

# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[14]:
@click.command()
@click.option('--user', default='postgres', help='PostgreSQL user')
@click.option('--password', default='postgres', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5433, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default='2025', type=int, help='Data year')
@click.option('--month', default='11', type=int, help='Data month')
@click.option('--chunksize', default='100000', type=int, help='Chunk size for data ingestion')
@click.option('--table', default='green_taxi_data', help='Target table name')

def run(user, password, host, port, db, year, month, chunksize, table):    
    
    pg_user = user
    pg_pass = password
    pg_host = host
    pg_port = port
    pg_db   = db
   
    target_table = table

    
    # prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df = pd.read_parquet(url, engine='pyarrow')
    # 1. convert non-datetime columns
    df = df.astype(dtype)

    # 2. convert datetime columns
    for col in parse_dates:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    print(df.info())
    df_iter = [df[i:i+chunksize] for i in range(0, len(df), chunksize)]

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')
            first = False
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')
        #print(len(df))

    zones_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

    zones_df = pd.read_csv(zones_url)
    zones_df.to_sql(name='zones', con=engine, if_exists='replace')

if __name__ == '__main__':
    run()   


# uv run python ingest.py --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --year=2021 --month=1 --chunksize=100000 --table=yellow_taxi_data

# docker run -it --rm --network=pg-network taxi_ingest:v001 --user=root --password=root --host=pgdatabase --port=5432 -
# -db=ny_taxi --year=2021 --month=1 --chunksize=100000 --table=yellow_taxi_data

# docker run -it --rm \
#   -e POSTGRES_USER="root" \
#   -e POSTGRES_PASSWORD="root" \
#   -e POSTGRES_DB="ny_taxi" \
#   -v ny_taxi_postgres_data:/var/lib/postgresql \
#   -p 5432:5432 \
#   --network=pg-network \
#   --name pgdatabase
#   postgres:18