# Використовуємо офіційний образ Python
FROM python:3.9-slim

# Встановлюємо робочу директорію
WORKDIR /app

# Встановлюємо Poetry
RUN pip install poetry

# Копіюємо файли конфігурації та встановлюємо залежності
# Це прискорить збірку, оскільки залежності кешуються в окремому шарі
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-ansi --no-root

# Копіюємо вихідний код у контейнер
# Тепер пакет kafka_producer буде знаходитись безпосередньо в /app
COPY src/ .

ENV KAFKA_BROKER_URL=kafka:29092
ENV TOPIC_NAME=tweets
ENV MESSAGES_PER_SECOND=15
ENV PYTHONUNBUFFERED=1

# Встановлюємо точку входу для запуску продюсера
# Ця команда запускає модуль kafka_producer.producer як скрипт
CMD ["poetry", "run", "python", "-m", "kafka_producer.producer"]