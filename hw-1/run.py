import catboost as cb
import pandas as pd
import numpy as np

model = cb.CatBoostClassifier()
model.load_model("../data/models/catboost_model.cbm")

def get_feature_importance_dict(model):
    feature_names = model.feature_names_
    importances = model.get_feature_importance()
    feature_importance_dict = dict(zip(feature_names, importances))
    feature_importance_dict = dict(sorted(feature_importance_dict.items(), key=lambda x: x[1], reverse=True))
    return feature_importance_dict

test_df = pd.read_csv("../data/test.csv", parse_dates=['transaction_time'])

test_df['hour'] = test_df['transaction_time'].dt.hour
test_df['day_of_week'] = test_df['transaction_time'].dt.dayofweek
test_df['diff_lat'] = test_df['lat'] - test_df['merchant_lat']
test_df['diff_lon'] = test_df['lon'] - test_df['merchant_lon']
test_df['name_join'] = test_df['name_1'] + ' ' + test_df['name_2']

def haversine_vec(lat1, lon1, lat2, lon2):
    phi1 = np.radians(lat1); phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1); dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2.0)**2 + np.cos(phi1)*np.cos(phi2)*(np.sin(dlambda/2.0)**2)
    return np.arctan2(np.sqrt(a), np.sqrt(1-a))

test_df['haversine_km'] = haversine_vec(test_df['lat'], test_df['lon'], test_df['merchant_lat'], test_df['merchant_lon'])

categorical_features = ["name_1", "gender", "us_state", 'cat_id']
numerical_features = ["amount", "haversine_km", 'population_city']
temp_cols = ['merch', 'jobs', 'name_join']

import json

# Загрузка словарей с фичами из файла, сохранённого на этапе EDA
with open("../data/models/stats.json", "r", encoding='utf-8') as f:
    stats = json.load(f)

merchant_count_dict = stats["merchant_count_dict"]
merchant_jobs_count_dict = stats["merchant_jobs_count_dict"]
name_count_dict = stats["name_count_dict"]
# Ключи в name_merchant_count_dict были сериализованы в строки ("('name', 'merch')"), нужно приводить обратно к tuple
name_merchant_count_dict = {eval(k): v for k, v in stats["name_merchant_count_dict"].items()}

X_test = test_df[categorical_features + numerical_features + temp_cols]
X_test['merch_count'] = X_test['merch'].map(merchant_count_dict)
X_test['merch_jobs_count'] = X_test['merch'].map(merchant_jobs_count_dict)
X_test['name_count'] = X_test['name_join'].map(name_count_dict)
X_test['name_merchant_count'] = X_test.set_index(['name_join', 'merch']).index.map(name_merchant_count_dict)

X_test.drop(columns=temp_cols, inplace=True)

def get_feature_importance_dict(model):
    feature_names = model.feature_names_
    importances = model.get_feature_importance()
    feature_importance_dict = dict(zip(feature_names, importances))
    feature_importance_dict = dict(sorted(feature_importance_dict.items(), key=lambda x: x[1], reverse=True))
    return feature_importance_dict


threshold = 0.42

preds = model.predict_proba(X_test)[:, 1]
test_df['prediction'] = (preds > threshold).astype(int)

test_df[['prediction']].to_csv('../data/submission.csv', index_label='index')