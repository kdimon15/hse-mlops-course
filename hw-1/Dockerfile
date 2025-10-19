FROM python:3.12-slim

WORKDIR /app

# Копируем requirements и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем скрипты
COPY *.py ./

# Создаем директорию для временных файлов
RUN mkdir -p /tmp

CMD ["python", "main.py"]

