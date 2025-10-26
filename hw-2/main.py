from load_data import create_consumer
from preprocess import Preprocessor
from score import FraudScorer
from save_results import create_producer, send_batch_results

def main():
    print("=== SCORING SERVICE ЗАПУЩЕН ===")

    consumer = create_consumer()
    producer = create_producer()
    preprocessor = Preprocessor()
    scorer = FraudScorer()

    print("Модель загружена, ожидание батчей транзакций...")

    processed_batches = 0
    processed_count = 0

    try:
        for message in consumer:
            batch_data = message.value
            transactions = batch_data['batch']

            # В случае если от producer приходит больше 1000 транзакций, то мы обрабатываем их порциями по 1000
            model_batch_size = 1000
            for i in range(0, len(transactions), model_batch_size):
                chunk = transactions[i:i + model_batch_size]
                
                features, transaction_ids = preprocessor.preprocess_batch(chunk)
                scores, fraud_flags = scorer.score_batch(features)
                send_batch_results(producer, chunk, scores, fraud_flags)

            producer.flush()

            processed_batches += 1
            processed_count += len(transactions)
            print(f"Обработан батч {processed_batches} ({len(transactions)} транзакций, всего {processed_count})")

    except KeyboardInterrupt:
        print(f"\n=== ЗАВЕРШЕНО: обработано {processed_batches} батчей, {processed_count} транзакций ===")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()
