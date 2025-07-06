import json
import os
from pathlib import Path
from urllib.parse import quote_plus

import gdown
import pandas as pd
import pymongo


def build_mongo_uri() -> str:
    """Складає URI, якщо повністю не вказаний у .env."""
    if uri := os.getenv("MONGO_URI"):
        return uri

    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    user = os.getenv("MONGO_USER")
    pwd = os.getenv("MONGO_PASSWORD")

    if user and pwd:
        # URL-encode, щоб спецсимволи не зламали рядок
        user_enc = quote_plus(user)
        pwd_enc = quote_plus(pwd)
        return f"mongodb://{user_enc}:{pwd_enc}@{host}:{port}"
    return f"mongodb://{host}:{port}"


def get_db_connection(uri: str, db_name: str):
    """Встановлює з'єднання з MongoDB та повертає об'єкт бази даних."""
    try:
        client = pymongo.MongoClient(uri)
        # Перевірка з'єднання
        client.admin.command('ping')
        print("✅ Підключення до MongoDB успішне.")
        return client, client[db_name]
    except pymongo.errors.ConnectionFailure as e:
        print(f"Помилка підключення до MongoDB: {e}")
        return None


def gdrive_download(file_id: str, dst: Path) -> None:
    url = f"https://drive.google.com/uc?id={file_id}"
    dst.parent.mkdir(parents=True, exist_ok=True)
    gdown.download(url, str(dst), quiet=False)


# --- Функції для збереження результатів ---

def save_results(results: list, filename: str, file_format: str):
    """Зберігає результати у вказаному форматі (json або csv)."""
    if not results:
        print("Немає даних для збереження.")
        return

    try:
        if file_format == 'json':
            if not filename.endswith('.json'):
                filename += '.json'
            with open(filename, 'w', encoding='utf-8') as f:
                # default=str для обробки ObjectId та datetime
                json.dump(results, f, indent=4, default=str)
        elif file_format == 'csv':
            if not filename.endswith('.csv'):
                filename += '.csv'
            df = pd.DataFrame(results)
            df.to_csv(filename, index=False, encoding='utf-8-sig')

        print(f"✔ Результати успішно збережено у файл '{filename}'")

    except Exception as e:
        print(f"Помилка під час збереження файлу: {e}")
