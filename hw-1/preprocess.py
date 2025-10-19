import pandas as pd
import numpy as np
import json

def preprocess_data():
    print("=== ПРЕПРОЦЕССИНГ ===")
    
    # Загружаем данные
    df = pd.read_pickle("/tmp/data.pkl")
    
    # Временные признаки
    df['hour'] = df['transaction_time'].dt.hour
    df['day_of_week'] = df['transaction_time'].dt.dayofweek
    
    # Географические признаки
    df['diff_lat'] = df['lat'] - df['merchant_lat']
    df['diff_lon'] = df['lon'] - df['merchant_lon']
    df['name_join'] = df['name_1'] + ' ' + df['name_2']
    
    # Haversine расстояние
    def haversine_vec(lat1, lon1, lat2, lon2):
        phi1 = np.radians(lat1)
        phi2 = np.radians(lat2)
        dphi = np.radians(lat2 - lat1)
        dlambda = np.radians(lon2 - lon1)
        a = np.sin(dphi/2.0)**2 + np.cos(phi1)*np.cos(phi2)*(np.sin(dlambda/2.0)**2)
        return np.arctan2(np.sqrt(a), np.sqrt(1-a))
    
    df['haversine_km'] = haversine_vec(df['lat'], df['lon'], df['merchant_lat'], df['merchant_lon'])
    
    # Загружаем статистики
    with open("/app/models/stats.json", "r", encoding='utf-8') as f:
        stats = json.load(f)
    
    merchant_count_dict = stats["merchant_count_dict"]
    merchant_jobs_count_dict = stats["merchant_jobs_count_dict"]
    name_count_dict = stats["name_count_dict"]
    name_merchant_count_dict = {eval(k): v for k, v in stats["name_merchant_count_dict"].items()}
    
    # Формируем фичи
    categorical_features = ["name_1", "gender", "us_state", 'cat_id']
    numerical_features = ["amount", "haversine_km", 'population_city']
    temp_cols = ['merch', 'jobs', 'name_join']
    
    X = df[categorical_features + numerical_features + temp_cols]
    X['merch_count'] = X['merch'].map(merchant_count_dict)
    X['merch_jobs_count'] = X['merch'].map(merchant_jobs_count_dict)
    X['name_count'] = X['name_join'].map(name_count_dict)
    X['name_merchant_count'] = X.set_index(['name_join', 'merch']).index.map(name_merchant_count_dict)
    X.drop(columns=temp_cols, inplace=True)
    
    print(f"Подготовлено {len(X.columns)} признаков")
    
    # Сохраняем
    X.to_pickle("/tmp/features.pkl")
    print("Признаки сохранены")

if __name__ == "__main__":
    preprocess_data()

