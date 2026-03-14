# import json
# import pandas as pd
# from kafka import KafkaProducer
# from time import time

# def json_serializer(data):
#     return json.dumps(data).encode('utf-8')

# server = 'localhost:9092'

# producer = KafkaProducer(
#     bootstrap_servers=[server],
#     value_serializer=json_serializer
# )

# url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz'
# columns = [
#     'lpep_pickup_datetime',
#     'lpep_dropoff_datetime',
#     'PULocationID',
#     'DOLocationID',
#     'passenger_count',
#     'trip_distance',
#     'tip_amount'
# ]

# df = pd.read_csv(url, usecols=columns).head(20000)

# topic_name = 'green-trips'

# t0 = time()

# for _, row in df.iterrows():
#     message = row.to_dict()
#     producer.send(topic_name, value=message)

# producer.flush()

# t1 = time()
# print(f'took {(t1 - t0):.2f} seconds')

# #took 108.81 secs

# # docker compose exec postgres psql -U postgres -d postgres -c "SELECT PULocationID, DOLocationID, num_trips, window_start, window_end FROM processed_events_aggregated ORDER BY num_trips DESC LIMIT 5;"


# # pulocationid | dolocationid | num_trips |    window_start     |     window_end      
# # --------------+--------------+-----------+---------------------+---------------------
# #           129 |          129 |        29 | 2019-10-01 00:05:42 | 2019-10-01 05:23:33
# #            82 |           82 |        14 | 2019-10-01 00:05:42 | 2019-10-01 05:23:33
# #            82 |          129 |        10 | 2019-10-01 00:05:42 | 2019-10-01 05:23:33
# #            82 |          173 |         8 | 2019-10-01 00:05:42 | 2019-10-01 05:23:33
# #           129 |          260 |         8 | 2019-10-01 00:05:42 | 2019-10-01 05:23:33


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

columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]

df = pd.read_parquet('/tmp/green_tripdata_2025-10.parquet', columns=columns)

# convert datetime columns to strings for JSON serialization
df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].astype(str)
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(str)

topic_name = 'green-trips'

t0 = time()

for _, row in df.iterrows():
    message = row.to_dict()
    producer.send(topic_name, value=message)

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
print(f'total rows sent: {len(df)}')