# load_analytics_user_engagement.py
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Цей скрипт читає дані з raw_events і заповнює денормалізовану
# таблицю `user_engagement_history` для швидкого доступу до історії користувача.

import os
from datetime import datetime
from cassandra.query import SimpleStatement
from dotenv import load_dotenv

from analyze_ads_cassandra.utils import get_db_connection

def ensure_analytics_table_exists(session):
    """
    Перевіряє існування таблиці `user_engagement_history` і створює її, якщо потрібно.
    """
    keyspace = os.environ.get("CASSANDRA_KEYSPACE", "adtech")
    table_name = "user_engagement_history"

    table_check = session.execute(f"""
        SELECT table_name FROM system_schema.tables
        WHERE keyspace_name = %s AND table_name = %s
    """, (keyspace, table_name))

    if not table_check.one():
        print(f"Table {table_name} not found. Creating it...")
        ddl_query = """
        CREATE TABLE user_engagement_history (
            user_id int,
            event_time timestamp,
            campaign_name text,
            advertiser_name text,
            was_clicked boolean,
            PRIMARY KEY (user_id, event_time)
        ) WITH CLUSTERING ORDER BY (event_time DESC)
        """
        session.execute(ddl_query)
        print(f"Table {table_name} created successfully.")
    else:
        print(f"Table {table_name} already exists.")


def process_and_load_data(session):
    """
    Читає дані з raw_events та завантажує їх у user_engagement_history.
    """
    print("Starting data processing from raw_events for user engagement...")

    # Очищення таблиці перед новим завантаженням для ідемпотентності
    print("Truncating user_engagement_history to ensure fresh data...")
    session.execute("TRUNCATE adtech.user_engagement_history")

    insert_stmt = session.prepare("""
        INSERT INTO user_engagement_history (user_id, event_time, campaign_name, advertiser_name, was_clicked)
        VALUES (?, ?, ?, ?, ?)
    """)

    query = SimpleStatement("SELECT user_id, ts, campaign_name, advertiser_name, wasclicked FROM raw_events",
                            fetch_size=1000)
    rows = session.execute(query)

    processed_count = 0
    for event in rows:
        try:
            # Конвертуємо рядок з часом у тип timestamp
            event_timestamp = datetime.fromisoformat(event.ts.replace(" ", "T"))

            session.execute(insert_stmt, (
                event.user_id,
                event_timestamp,
                event.campaign_name,
                event.advertiser_name,
                event.wasclicked
            ))

            processed_count += 1
            if processed_count % 5000 == 0:
                print(f"Processed {processed_count} events for user history...")

        except Exception as e:
            print(f"Could not process event for user '{event.user_id}': {e}")

    print(f"ETL process finished. Total events loaded into user_engagement_history: {processed_count}.")


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
        print("✅ Analytics data for user engagement loaded successfully.")
    finally:
        session.shutdown()
        print("Cassandra connection closed.")


if __name__ == "__main__":
    main()
