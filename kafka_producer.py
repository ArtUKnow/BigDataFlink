import csv
import json
import time
import os
import re
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'raw_data_topic'
data_dir = 'исходные данные'

files = sorted([f for f in os.listdir(data_dir) if f.endswith('.csv')])

for file in files:
    match = re.search(r'\((\d+)\)', file)
    file_num = int(match.group(1)) if match else 0
    
    print(f"Streaming file: {file} with offset {file_num * 10000}")
    
    with open(os.path.join(data_dir, file), 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_data = dict(row)
            
            # Сдвиг ID для обеспечения уникальности данных между файлами
            if row_data.get('id'):
                row_data['id'] = str(file_num * 10000 + int(row_data['id']))
            if row_data.get('sale_customer_id'):
                row_data['sale_customer_id'] = str(file_num * 10000 + int(row_data['sale_customer_id']))
            if row_data.get('sale_seller_id'):
                row_data['sale_seller_id'] = str(file_num * 10000 + int(row_data['sale_seller_id']))
            if row_data.get('sale_product_id'):
                row_data['sale_product_id'] = str(file_num * 10000 + int(row_data['sale_product_id']))
            
            # Генерация уникального pet_id для dim_pet
            if row_data.get('sale_customer_id') and row_data.get('customer_pet_name'):
                row_data['pet_id'] = f"{row_data['sale_customer_id']}_{row_data['customer_pet_name']}"
            else:
                row_data['pet_id'] = None
                
            producer.send(topic, row_data)
            time.sleep(0.001) # Небольшая задержка для имитации стриминга

producer.flush()
print("All data sent to Kafka")
