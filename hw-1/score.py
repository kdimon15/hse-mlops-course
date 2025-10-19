import catboost as cb
import pandas as pd

def score_model():
    print("=== СКОРИНГ ===")
    
    # Загружаем модель
    model = cb.CatBoostClassifier()
    model.load_model("/app/models/catboost_model.cbm")
    print("Модель загружена")
    
    # Загружаем признаки
    X = pd.read_pickle("/tmp/features.pkl")
    
    # Предсказания
    threshold = 0.42 # Подобранный threshold по f1 мере (train_model/inference.ipynb)
    preds = model.predict_proba(X)[:, 1]
    predictions = (preds > threshold).astype(int)
    
    print(f"Выполнено {len(predictions)} предсказаний")
    
    # Сохраняем
    pd.to_pickle(predictions, "/tmp/predictions.pkl")
    pd.to_pickle(preds, "/tmp/probabilities.pkl")
    
    # Сохраняем важность признаков
    feature_names = model.feature_names_
    importances = model.get_feature_importance()
    feature_importance_dict = dict(zip(feature_names, importances))
    feature_importance_dict = dict(sorted(feature_importance_dict.items(), key=lambda x: x[1], reverse=True))
    
    pd.to_pickle(feature_importance_dict, "/tmp/feature_importance.pkl")
    print("Результаты скоринга сохранены")

if __name__ == "__main__":
    score_model()

