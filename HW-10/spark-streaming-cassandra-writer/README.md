# Записувач Потокових Даних у Cassandra

## Опис

Цей проєкт є частиною системи обробки потокових даних, який використовує Apache Spark Streaming для зчитування
оброблених даних з Apache Kafka та їх запису у базу даних Cassandra. Компонент спеціалізується на обробці подій,
пов'язаних зі створенням сторінок у Вікіпедії.

---

## Структура проєкту

```
spark-streaming-cassandra-writer/
│
├── Dockerfile        # Файл для створення Docker-образу
├── poetry.lock       # Lock-файл для залежностей Poetry
├── pyproject.toml    # Конфігурація Poetry та метадані проєкту
├── README.md         # Цей файл з документацією
│
└── src/              # Вихідний код проєкту
    └── spark_cassandra_writer/
        ├── __init__.py
        └── writer.py  # Основний модуль запису даних у Cassandra
```

---

## Функціональність

Реалізовані наступні функції:

1. Створення сесії Spark для потокової обробки даних.
2. Ініціалізація з'єднання з базою даних Cassandra, створення keyspace та таблиці, якщо їх не існують.
3. Зчитування даних з topic Kafka (за замовчуванням "processed").
4. Парсинг JSON-даних згідно зі схемою з полями: `user_id`, `domain`, `created_at`, `page_title`.
5. Збереження відфільтрованих даних у таблицю Cassandra.

---

## Вимоги

- Python 3.9+
- Poetry
- Docker
- Apache Spark
- Apache Kafka
- Apache Cassandra

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
docker build -t spark-streaming-cassandra-writer .

# Запустіть контейнер
docker run --network=host spark-streaming-cassandra-writer
```

---

## Запуск

### Локально

```bash
poetry run run-writer
```

### У складі docker-compose

Використовуйте файл `docker-compose.yml` з кореневої директорії проєкту:

```bash
docker-compose up -d
```

---

## Конфігурація

Конфігурація записувача здійснюється через змінні середовища:

- `KAFKA_BOOTSTRAP_SERVERS` - адреси серверів Kafka (за замовчуванням: "kafka:29092")
- `KAFKA_PROCESSED_TOPIC` - topic Kafka для зчитування (за замовчуванням: "processed")
- `CHECKPOINT_LOCATION` - директорія для зберігання контрольних точок Spark Streaming (за замовчуванням: "
  /opt/spark/work-dir/checkpoints/writer")
- `SPARK_MASTER_URL` - URL головного вузла Spark (за замовчуванням: "spark://spark-master:7077")
- `CASSANDRA_HOST` - хост Cassandra (за замовчуванням: "cassandra")
- `CASSANDRA_KEYSPACE` - простір ключів Cassandra (за замовчуванням: "wikipedia")
- `CASSANDRA_TABLE` - таблиця Cassandra (за замовчуванням: "page_creations")

---

## Схема даних Cassandra

Записувач створює наступну схему таблиці в Cassandra:

```cql
CREATE TABLE IF NOT EXISTS page_creations (
    user_id TEXT,
    domain TEXT,
    created_at TEXT,
    page_title TEXT,
    PRIMARY KEY (user_id, created_at)
)
```

---

## Ліцензія

Copyright (c) 2025
