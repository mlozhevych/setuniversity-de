# Analyze Ads Cassandra

Цей проєкт є ETL-конвеєром для аналізу рекламних даних з використанням Apache Cassandra. Він завантажує дані про
кампанії, користувачів та події з CSV-файлів, обробляє їх і зберігає в денормалізованих таблицях Cassandra для
ефективних аналітичних запитів.

---

## 🚀 Початок роботи

### Передумови

- Python 3.9+
- Poetry ([інструкція зі встановлення](https://python-poetry.org/docs/#installing-with-pipx))
- Docker та Docker Compose

### Кроки встановлення

1. Клонуйте репозиторій:
    ```bash
   git clone https://github.com/mlozhevych/setuniversity-de.git
   cd HW-4/analyze-ads-cassandra
   ```

2. Налаштуйте змінні середовища:

   Створіть файл `.env` у кореневій директорії проєкту. Це необхідно для завантаження даних та підключення до Cassandra.
    ```
    GDRIVE_CAMPAIGNS_FILE_ID=<ID_ФАЙЛУ_GDRIVE_З_КАМПАНІЯМИ>
    GDRIVE_USERS_FILE_ID=<ID_ФАЙЛУ_GDRIVE_З_КОРИСТУВАЧАМИ>
    GDRIVE_EVENTS_FILE_ID=<ID_ФАЙЛУ_GDRIVE_З_ПОДІЯМИ>
   
    # Налаштування Cassandra (стандартні для Docker)
    CASSANDRA_HOST=127.0.0.1
    CASSANDRA_PORT=9042
    CASSANDRA_KEYSPACE=adtech
    ```
3. Встановіть залежності за допомогою Poetry:

   Poetry автоматично створить віртуальне середовище та встановить усі пакети, вказані у `pyproject.toml`.
   ```bash
   poetry install
   ```

## ⚙️ Використання

### 1. Запустіть Cassandra:

Проєкт містить файл docker-compose.yml для легкого запуску бази даних.

```bash  
docker-compose up -d
```

### 2. Запустіть скрипти ETL:

У файлі `pyproject.toml` визначені спеціальні скрипти для запуску основних етапів конвеєра через Poetry.

- Імпорт "сирих" даних:

  Ця команда завантажить дані з Google Drive і збереже їх у проміжних таблицях Cassandra.
   ```bash
    poetry run import_data
   ```
- Запуск окремих ETL-скриптів для аналітичних таблиць:

  Ви також можете запускати кожен ETL-скрипт окремо для оновлення конкретних аналітичних таблиць.
   ```bash
    poetry run load_analytics_campaign_daily_metrics
    poetry run load_analytics_advertiser_spend
    poetry run load_analytics_user_engagement
    poetry run load_analytics_active_users
    poetry run load_analytics_advertiser_spend_by_region
   ```

_Примітка: poetry run виконує команди у віртуальному середовищі проєкту._

## 🗂️ Структура проєкту та схема БД

### Структура

```
.
├── docker-compose.yml        # Конфігурація Docker для Cassandra
├── pyproject.toml            # Основний файл конфігурації Poetry
├── requirements.txt          # Список залежностей
└── src
    └── analyze_ads_cassandra
        ├── cassandra_queries
        │   ├── ddl_queries.cql # DDL-запити для таблиць
        │   └── dml_queries.cql # Приклади DML-запитів
        ├── etl_scripts           # ETL-скрипти для аналітичних таблиць
        │   ├── load_analytics_active_users.py
        │   ├── load_analytics_advertiser_spend.py
        │   ├── load_analytics_advertiser_spend_by_region.py
        │   ├── load_analytics_campaign_daily_metrics.py
        │   └── load_analytics_user_engagement.py
        ├── import_data
        │   └── load_raw.py     # Скрипт, що виконується через `import_data`
        └── utils.py              # Допоміжні функції
```

### Схема Бази Даних

Проєкт використовує Cassandra для зберігання даних у декількох денормалізованих таблицях для швидкого доступу до
аналітичних зрізів. Ключовий простір adtech створюється автоматично.

#### Основні аналітичні таблиці:
![adtech.png](../../../docs/adtech.png)

`campaign_daily_metrics`

`top_advertisers_by_spend`

`user_engagement_history`

`top_users_by_clicks`

`top_advertisers_by_spend`