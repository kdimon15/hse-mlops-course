# ML Inference Service

Сервис для inference модели CatBoost из kaggle-соревнования.

> Пока что модельки (`catboost_model.cbm` и `stats.json`) просто лежат в репозитории на GitHub, отдельного хранения или удобного скачивания для них пока нет — мне было лень всё это красиво оформить:)

> **Оценка на Kaggle-лидерборде:**
>
> - **Private Score:** `0.762096`
> - **Public Score:** `0.726315`
>


## Структура

```
hw-1/
├── main.py              # Orchestrator всех этапов
├── load_data.py         # Загрузка данных
├── preprocess.py        # Препроцессинг
├── score.py             # Скоринг модели
├── save_results.py      # Сохранение результатов
├── requirements.txt     # Зависимости
├── Dockerfile           # Docker образ
├── docker-compose.yml   # Конфигурация
└── README.md            # Эта инструкция
```

## Использование

### 1. Подготовка

Создайте директории для входных и выходных данных:

```bash
cd hw-1
mkdir -p input output
```

### 2. Добавьте тестовый файл

Скопируйте файл `test.csv` в директорию `input/`:

```bash
cp ../data/test.csv input/
```

### 3. Запуск сервиса

Сборка и запуск через docker-compose:

```bash
docker-compose up --build
```

### 4. Результаты

После завершения в директории `output/` будут созданы:
- `submission.csv` - предсказания модели
- `feature_importance.json` - топ-5 важных признаков
- `predictions_distribution.png` - график распределения предсказаний

## Этапы pipeline

1. **Загрузка данных** - чтение test.csv
2. **Препроцессинг** - создание признаков
3. **Скоринг** - применение модели
4. **Сохранение** - выгрузка результатов

## Требования

- Docker
- docker-compose
- Модель и статистики в `../data/models/`

