FROM python:3.11-slim

WORKDIR /app
ENV POETRY_VIRTUALENVS_IN_PROJECT=true

RUN pip install poetry

# 1. Копіюємо тільки файли залежностей
COPY pyproject.toml poetry.lock* ./

# 2. Копіюємо весь код (до встановлення залежностей)
COPY src /app/src

# 3. Встановлюємо залежності і реєструємо package (щоб poetry знав про analyze_ads_nosql)
RUN poetry install --no-ansi --no-root

# Якщо треба .env
COPY .env /app/.env
