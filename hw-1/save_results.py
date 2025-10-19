import pandas as pd
import json
import matplotlib.pyplot as plt
import seaborn as sns

def save_results():
    print("=== СОХРАНЕНИЕ РЕЗУЛЬТАТОВ ===")
    
    # Загружаем результаты
    predictions = pd.read_pickle("/tmp/predictions.pkl")
    probabilities = pd.read_pickle("/tmp/probabilities.pkl")
    feature_importance_dict = pd.read_pickle("/tmp/feature_importance.pkl")
    
    # 1. Сохраняем submission
    submission = pd.DataFrame({'prediction': predictions})
    submission.to_csv('/app/output/submission.csv', index_label='index')
    print("✓ submission.csv сохранен")
    
    # 2. Сохраняем топ-5 feature importance
    top5_features = dict(list(feature_importance_dict.items())[:5])
    with open('/app/output/feature_importance.json', 'w', encoding='utf-8') as f:
        json.dump(top5_features, f, indent=2, ensure_ascii=False)
    print("✓ feature_importance.json сохранен")
    
    # 3. Сохраняем график распределения предсказаний
    plt.figure(figsize=(10, 6))
    sns.kdeplot(probabilities, fill=True, color='blue', alpha=0.6)
    plt.title('Распределение предсказанных вероятностей', fontsize=14)
    plt.xlabel('Вероятность', fontsize=12)
    plt.ylabel('Плотность', fontsize=12)
    plt.grid(alpha=0.3)
    plt.savefig('/app/output/predictions_distribution.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("✓ predictions_distribution.png сохранен")
    
    print(f"\nВсего предсказаний: {len(predictions)}")

if __name__ == "__main__":
    save_results()

