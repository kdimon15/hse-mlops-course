import sys
from load_data import load_data
from preprocess import preprocess_data
from score import score_model
from save_results import save_results

def main():
    print("\n" + "="*50)
    print("ML INFERENCE PIPELINE")
    print("="*50 + "\n")
    
    try:
        # Этап 1: Загрузка данных
        load_data()
        print()
        
        # Этап 2: Препроцессинг
        preprocess_data()
        print()
        
        # Этап 3: Скоринг
        score_model()
        print()
        
        # Этап 4: Сохранение результатов
        save_results()
        print()
        
        print("="*50)
        print("✓ PIPELINE ЗАВЕРШЕН УСПЕШНО")
        print("="*50)
        
    except Exception as e:
        print(f"\n✗ ОШИБКА: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

