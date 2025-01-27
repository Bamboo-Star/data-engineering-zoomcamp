#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

def loadZoneData(engine, table, url):
    csv_name = 'output.csv'
    os.system(f"wget '{url}' -O '{csv_name}'")
    
    df = pd.read_csv(csv_name)
    df.to_sql(name=table, con=engine, if_exists='replace')
    return
    
def loadTaxiData(engine, table, url):
    csv_name = 'output.csv.gz'
    os.system(f"wget '{url}' -O '{csv_name}'")
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, compression='gzip')
    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')
    df.to_sql(name=table, con=engine, if_exists='append')
    
    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.to_sql(name=table, con=engine, if_exists='append')
            t_end = time()
            print("inserted another chunk..., took %.3f seconds" % (t_end-t_start))
        except Exception as e:
            print("An error occurred:", e)
            break
    return

def main(params):
    user = params.username
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table1 = params.table1
    url1 = params.url1
    table2 = params.table2
    url2 = params.url2

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    loadZoneData(engine, table1, url1)
    loadTaxiData(engine, table2, url2)
    return



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--username', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table1', help='table name where we will write the results to')
    parser.add_argument('--url1', help='url of the data file')
    parser.add_argument('--table2', help='table name where we will write the results to')
    parser.add_argument('--url2', help='url of the data file')    
    args = parser.parse_args()

    main(args)
