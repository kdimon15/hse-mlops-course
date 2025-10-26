import pandas as pd
import json
import time
import os
from kafka import KafkaProducer

def main():
    print("=== PRODUCER ЗАПУЩЕН ===")

    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    df = pd.read_csv('/app/data/test.csv', parse_dates=['transaction_time'])
    print(f"Загружено {len(df)} транзакций")

    batch_size = 1000
    total_batches = (len(df) + batch_size - 1) // batch_size

    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(df))
        batch = df.iloc[start_idx:end_idx]

        transactions = []
        for idx, row in batch.iterrows():
            transaction = row.to_dict()
            transaction['transaction_id'] = int(idx)
            transaction['transaction_time'] = str(transaction['transaction_time'])
            transactions.append(transaction)

        producer.send('transactions', value={'batch': transactions})
        producer.flush()

        print(f"Отправлен батч {batch_num + 1}/{total_batches} ({len(transactions)} транзакций)")

        # Не мгновенно отправляем батчи
        if batch_num < total_batches - 1:
            import random
            time.sleep(random.uniform(0.01, 0.1))

    print("=== ВСЕ ТРАНЗАКЦИИ ОТПРАВЛЕНЫ ===")
    producer.close()

if __name__ == "__main__":
    main()
