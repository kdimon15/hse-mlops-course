import os
import json
import psycopg2
from kafka import KafkaConsumer

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id INTEGER PRIMARY KEY,
            score FLOAT NOT NULL,
            fraud_flag INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cursor.close()
    print("Таблица создана")

def main():
    print("=== DB WRITER ЗАПУЩЕН ===")

    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

    conn_params = {
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'fraud_user'),
        'password': os.getenv('POSTGRES_PASSWORD', 'fraud_pass'),
        'database': os.getenv('POSTGRES_DB', 'fraud_db')
    }

    consumer = KafkaConsumer(
        'scores',
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='db-writer'
    )

    conn = psycopg2.connect(**conn_params)
    create_table(conn)
    cursor = conn.cursor()

    print("Ожидание батчей результатов скоринга...")

    saved_count = 0

    try:
        for message in consumer:
            batch_data = message.value
            results = batch_data['batch']

            for result in results:
                cursor.execute(
                    """
                    INSERT INTO transactions (transaction_id, score, fraud_flag)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                    """,
                    (result['transaction_id'], result['score'], result['fraud_flag'])
                )
            conn.commit()

            saved_count += len(results)
            print(f"Сохранено {len(results)} результатов")

    except KeyboardInterrupt:
        print(f"\n=== ЗАВЕРШЕНО: сохранено {saved_count} результатов ===")
    finally:
        cursor.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
