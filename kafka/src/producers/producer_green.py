import json
import pandas as pd
from kafka import KafkaProducer
from time import time

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz'
columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

df = pd.read_csv(url, usecols=columns).head(20000)

topic_name = 'green-trips'

t0 = time()

for _, row in df.iterrows():
    message = row.to_dict()
    producer.send(topic_name, value=message)

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')

#took 108.81 secs
