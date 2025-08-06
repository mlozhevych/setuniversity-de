# Генератор потоку даних

## Опис

Цей проєкт є частиною завдання з обробки потокових даних. Генератор потоку даних відповідає за створення повідомлень та
їх публікацію в Apache Kafka.
---

## Структура проєкту

```
stream-generator/
│
├── Dockerfile        # Файл для створення Docker-образу
├── poetry.lock       # Lock-файл для залежностей Poetry
├── pyproject.toml    # Конфігурація Poetry та метадані проєкту
├── README.md         # Документацією
│
└── src/              # Вихідний код проєкту
```

---

## Вимоги

- Python 3.9+
- Poetry
- Docker
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
docker build -t stream-generator .

# Запустіть контейнер
docker run --network=host stream-generator
```

---

## Запуск

### Локально

```bash
poetry run run-generator
```

### У складі docker-compose

Використовуйте файл docker-compose.yml з кореневої директорії проєкту:

```bash
docker-compose up -d
```

---

## Конфігурація

Конфігурація генератора потоку задається через змінні середовища або файл конфігурації.

---

## Ліцензія

Copyright (c) 2025
