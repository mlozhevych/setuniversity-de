# Процесор потокових даних на Spark

## Опис

Цей проєкт є компонентом системи обробки потокових даних, який використовує Apache Spark для обробки повідомлень з
Apache Kafka. Процесор зчитує повідомлення з topic `input`, обробляє їх та зберігає у topic `processed`.

---

## Структура проєкту

```
spark-streaming-processor/
│
├── Dockerfile        # Файл для створення Docker-образу
├── poetry.lock       # Lock-файл для залежностей Poetry
├── pyproject.toml    # Конфігурація Poetry та метадані проєкту
├── README.md         # Документацією
│
└── src/              # Вихідний код проєкту
    ├── __init__.py
    └── processor.py       # Головний скрипт програми
```

---

## Функціональність

Процесор виконує наступні функції:

1. Зчитує повідомлення з теми Kafka `tweets`.
2. Вилучає з кожного повідомлення поля: `user_id`, `domain`, `created_at` та `page_title`.
3. Фільтрує повідомлення за доменами "en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org" та
   `user_is_bot = False`.
4. Зберігає відфільтровані дані у topic `processed`.

---

## Вимоги

- Python 3.9+
- Poetry
- Docker
- Apache Spark
- Apache Kafka

---

## Встановлення

### За допомогою Poetry

```bash
# Встановіть залежності
poetry install

# Активуйте віртуальне середовище
poetry shell
```

### За допомогою Docker

```bash
# Створіть Docker-образ
docker build -t spark-streaming-processor .

# Запустіть контейнер
docker run --network=host spark-streaming-processor
```

---

## Запуск

### Локально

```bash
poetry run run-processor
```

### У складі docker-compose

Використовуйте файл docker-compose.yml з кореневої директорії проєкту:

```bash
docker-compose up -d
```

---

## Конфігурація

Конфігурація процесора задається через змінні середовища або файл конфігурації:

- `KAFKA_BOOTSTRAP_SERVERS` - адреси серверів Kafka (за замовчуванням: "localhost:9092")
- `KAFKA_TOPIC` - topic Kafka для зчитування повідомлень
- `KAFKA_OUTPUT_TOPIC` - topic Kafka для збереження відфільтрованих повідомлень
- `SPARK_MASTER` - адреса Spark Master (за замовчуванням: "local[*]")
- `CHECKPOINT_LOCATION` - шлях до директорії для збереження контрольних точок

---

## Ліцензія

Copyright (c) 2025
