import pandas as pd
import os

def load_data():
    print("=== ЗАГРУЗКА ДАННЫХ ===")
    input_path = "/app/input/test.csv"
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Файл не найден: {input_path}")
    
    df = pd.read_csv(input_path, parse_dates=['transaction_time'])
    print(f"Загружено {len(df)} записей")
    
    # Сохраняем во временную директорию
    df.to_pickle("/tmp/data.pkl")
    print("Данные сохранены")
    return df

if __name__ == "__main__":
    load_data()

