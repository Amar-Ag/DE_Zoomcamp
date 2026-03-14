import sys
sys.path.append('/workspaces/DE_Zoomcamp/kafka/src')

from kafka import KafkaConsumer
import json

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-counter',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=10000  # stop after 10 seconds of no messages
)

count = 0
total = 0

for message in consumer:
    trip = message.value
    total += 1
    if trip['trip_distance'] > 5.0:
        count += 1

consumer.close()
print(f'Total trips: {total}')
print(f'Trips with distance > 5.0: {count}')