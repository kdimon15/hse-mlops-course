import os
import json
from kafka import KafkaProducer

def create_producer():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    return producer

def send_batch_results(producer, transactions, scores, fraud_flags):
    results = []
    for transaction, score, flag in zip(transactions, scores, fraud_flags):
        result = transaction.copy()
        result['score'] = score
        result['fraud_flag'] = flag
        results.append(result)
    producer.send('scores', value={'batch': results})
