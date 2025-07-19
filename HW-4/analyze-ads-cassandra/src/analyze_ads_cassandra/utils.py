import json
import os
from pathlib import Path

import gdown
import pandas as pd
from cassandra.cluster import Cluster


# --- Функції для роботи з Cassandra ---
def get_db_connection() -> 'cassandra.cluster.Session':
    """Establishes connection to Cassandra and returns the session.
       Works for both Docker (using environment vars) and local execution."""
    host = os.getenv("CASSANDRA_HOST")
    if not host:
        # Default to localhost when not running in Docker
        host = "127.0.0.1"
    port = os.getenv("CASSANDRA_PORT", "9042")
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "adtech")
    try:
        print(f"🔗 Connecting to Cassandra at {host}:{port} (keyspace: {keyspace}) ...")
        cluster = Cluster([host], port=int(port))
        session = cluster.connect(keyspace)
        # Check connection by executing a simple query
        session.execute("SELECT now() FROM system.local")
        print("✅ Successfully connected to Cassandra.")
        return session
    except Exception as e:
        print(f"❌ Cassandra connection error: {e}")
        return None


# --- Функції для завантаження даних з Google Drive ---
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
