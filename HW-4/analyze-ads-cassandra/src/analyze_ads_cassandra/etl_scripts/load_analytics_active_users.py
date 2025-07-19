# load_analytics_active_users.py
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Цей скрипт розраховує загальну кількість кліків для кожного користувача
# за останні 30 днів відносно останньої дати в історичних даних
# і завантажує результати в таблицю `top_users_by_clicks`.

import os
from collections import defaultdict
from datetime import datetime, timedelta

from cassandra.query import SimpleStatement
from dotenv import load_dotenv

from analyze_ads_cassandra.utils import get_db_connection


def ensure_analytics_table_exists(session):
    """
    Перевіряє існування таблиці `top_users_by_clicks` і створює її, якщо потрібно.
    """
    keyspace = os.environ.get("CASSANDRA_KEYSPACE", "adtech")
    table_name = "top_users_by_clicks"

    table_check = session.execute(f"""
        SELECT table_name FROM system_schema.tables
        WHERE keyspace_name = %s AND table_name = %s
    """, (keyspace, table_name))

    if not table_check.one():
        print(f"Table {table_name} not found. Creating it...")
        ddl_query = """
        CREATE TABLE top_users_by_clicks (
            time_bucket text,
            total_clicks int,
            user_id int,
            PRIMARY KEY (time_bucket, total_clicks, user_id)
        ) WITH CLUSTERING ORDER BY (total_clicks DESC)
        """
        session.execute(ddl_query)
        print(f"Table {table_name} created successfully.")
    else:
        print(f"Table {table_name} already exists.")


def process_and_load_data(session):
    """
    Обробляє події, знаходить останню дату, та завантажує агреговані дані про кліки.
    """
    print("Starting data processing for top active users...")

    # 1. Знайти останню мітку часу в наборі даних для визначення часового вікна
    print("Finding the latest timestamp in raw_events...")
    all_events_query = SimpleStatement("SELECT ts FROM raw_events", fetch_size=5000)
    rows = session.execute(all_events_query)

    latest_timestamp = None
    for event in rows:
        event_time = datetime.fromisoformat(event.ts.replace(" ", "T"))
        if latest_timestamp is None or event_time > latest_timestamp:
            latest_timestamp = event_time

    if not latest_timestamp:
        print("No events found in raw_events. Exiting.")
        return

    print(f"Latest event timestamp found: {latest_timestamp}")

    # 2. Визначити часове вікно (останні 30 днів відносно останньої події)
    thirty_days_ago_from_latest = latest_timestamp - timedelta(days=30)
    print(f"Calculating clicks from {thirty_days_ago_from_latest} to {latest_timestamp}")

    # 3. Прочитати дані з raw_events та агрегувати кліки для кожного користувача
    query = SimpleStatement("SELECT user_id, ts, wasclicked FROM raw_events", fetch_size=1000)
    rows = session.execute(query)

    clicks_by_user = defaultdict(int)
    processed_count = 0

    for event in rows:
        try:
            event_time = datetime.fromisoformat(event.ts.replace(" ", "T"))
            # Враховуємо лише події з кліками за останні 30 днів
            if event.wasclicked and event_time >= thirty_days_ago_from_latest:
                clicks_by_user[event.user_id] += 1

            processed_count += 1
            if processed_count % 10000 == 0:
                print(f"Scanned {processed_count} events...")
        except Exception as e:
            print(f"Could not process event for user '{event.user_id}': {e}")

    print(f"Finished scanning events. Found click data for {len(clicks_by_user)} users.")

    # 4. Очистити таблицю та завантажити нові агреговані дані
    time_bucket_name = 'last_30_days_historical'
    print(f"Truncating and loading data into `top_users_by_clicks` for time_bucket='{time_bucket_name}'...")

    session.execute("TRUNCATE adtech.top_users_by_clicks")

    insert_stmt = session.prepare("""
        INSERT INTO top_users_by_clicks (time_bucket, user_id, total_clicks)
        VALUES (?, ?, ?)
    """)

    for user_id, total_clicks in clicks_by_user.items():
        session.execute(insert_stmt, (time_bucket_name, user_id, total_clicks))

    print("ETL process finished.")


def main():
    """
    Головна функція для запуску ETL-процесу.
    """
    load_dotenv()
    session = get_db_connection()
    if not session:
        print("❌ Failed to connect to Cassandra. Exiting.")
        return

    try:
        ensure_analytics_table_exists(session)
        process_and_load_data(session)
        print("✅ Analytics data for top active users loaded successfully.")
    finally:
        session.shutdown()
        print("Cassandra connection closed.")


if __name__ == "__main__":
    main()
