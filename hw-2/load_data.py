import os
from kafka import KafkaConsumer
import json

def create_consumer():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='scoring-service'
    )

    return consumer
