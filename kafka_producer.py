import csv
import json
import time
import os
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'raw_data_topic'
data_dir = 'исходные данные'

# List all csv files
files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]

for file in files:
    with open(os.path.join(data_dir, file), 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.send(topic, row)
            time.sleep(0.01) # Small sleep to simulate streaming
            
producer.flush()
print("All data sent to Kafka")
