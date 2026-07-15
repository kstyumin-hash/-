FROM python:3.11-slim

WORKDIR /app

# Устанавливаем зависимости для компиляции
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код
COPY . .

# Создаём папку для WireGuard конфигов (если нужно)
RUN mkdir -p /etc/wireguard

# Открываем порт
EXPOSE 8080

# Запускаем бота
CMD ["python", "main.py"]