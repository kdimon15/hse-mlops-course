import pandas as pd
import numpy as np
import json

class Preprocessor:
    def __init__(self):
        with open("/app/models/stats.json", "r", encoding='utf-8') as f:
            stats = json.load(f)

        self.merchant_count_dict = stats["merchant_count_dict"]
        self.merchant_jobs_count_dict = stats["merchant_jobs_count_dict"]
        self.name_count_dict = stats["name_count_dict"]
        self.name_merchant_count_dict = {eval(k): v for k, v in stats["name_merchant_count_dict"].items()}

        self.categorical_features = ["name_1", "gender", "us_state", 'cat_id']
        self.numerical_features = ["amount", "haversine_km", 'population_city']

    def haversine_vec(self, lat1, lon1, lat2, lon2):
        phi1 = np.radians(lat1)
        phi2 = np.radians(lat2)
        dphi = np.radians(lat2 - lat1)
        dlambda = np.radians(lon2 - lon1)
        a = np.sin(dphi/2.0)**2 + np.cos(phi1)*np.cos(phi2)*(np.sin(dlambda/2.0)**2)
        return np.arctan2(np.sqrt(a), np.sqrt(1-a))

    def preprocess_batch(self, transactions):
        df = pd.DataFrame(transactions)

        df['transaction_time'] = pd.to_datetime(df['transaction_time'])
        df['hour'] = df['transaction_time'].dt.hour
        df['day_of_week'] = df['transaction_time'].dt.dayofweek

        df['diff_lat'] = df['lat'] - df['merchant_lat']
        df['diff_lon'] = df['lon'] - df['merchant_lon']
        df['name_join'] = df['name_1'] + ' ' + df['name_2']

        df['haversine_km'] = self.haversine_vec(
            df['lat'], df['lon'],
            df['merchant_lat'], df['merchant_lon']
        )

        X = df[self.categorical_features + self.numerical_features].copy()
        X['merch_count'] = df['merch'].map(self.merchant_count_dict).fillna(0)
        X['merch_jobs_count'] = df['merch'].map(self.merchant_jobs_count_dict).fillna(0)
        X['name_count'] = df['name_join'].map(self.name_count_dict).fillna(0)
        X['name_merchant_count'] = df.set_index(['name_join', 'merch']).index.map(
            self.name_merchant_count_dict
        ).fillna(0)

        return X, df['transaction_id'].tolist()
