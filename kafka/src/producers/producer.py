import dataclasses
import json
import time

import pandas as pd
from kafka import KafkaProducer

import sys
sys.path.append('/workspaces/DE_Zoomcamp/kafka/src')
from models import Ride, ride_from_row


def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')


server = 'localhost:9092'
topic_name = 'rides'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']
df = pd.read_parquet(url, columns=columns).head(1000)

t0 = time.time()

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")
    time.sleep(0.01)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')