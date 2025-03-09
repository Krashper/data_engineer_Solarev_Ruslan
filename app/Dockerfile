FROM python:3.10

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем requirements.txt в контейнер
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем остальные файлы проекта
COPY . .

# Команда для запуска приложения
CMD ["python", "main.py"]