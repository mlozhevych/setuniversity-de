# load_analytics_campaign_daily_metrics.py
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Цей скрипт агрегує дані з raw_events та raw_campaigns для розрахунку
# щоденних показників (покази, кліки, CTR) для кожної кампанії.
# Результати завантажуються в таблицю `campaign_daily_metrics`.

import os
from collections import defaultdict
from datetime import datetime

from cassandra.query import BatchStatement, SimpleStatement
from dotenv import load_dotenv

from analyze_ads_cassandra.utils import get_db_connection


def ensure_analytics_table_exists(session):
    """
    Перевіряє існування таблиці `campaign_daily_metrics` і створює її, якщо потрібно.
    """
    keyspace = os.environ.get("CASSANDRA_KEYSPACE", "adtech")
    table_name = "campaign_daily_metrics"

    table_check = session.execute(f"""
        SELECT table_name FROM system_schema.tables
        WHERE keyspace_name = %s AND table_name = %s
    """, (keyspace, table_name))

    if not table_check.one():
        print(f"Table {table_name} not found. Creating it...")
        ddl_query = """
        CREATE TABLE campaign_daily_metrics (
            campaign_id int,
            event_date date,
            impressions bigint,
            clicks bigint,
            ctr double,
            PRIMARY KEY (campaign_id, event_date)
        ) WITH CLUSTERING ORDER BY (event_date DESC)
        """
        session.execute(ddl_query)
        print(f"Table {table_name} created successfully.")
    else:
        print(f"Table {table_name} already exists.")


def process_and_load_data(session):
    """
    Виконує повний ETL-процес: читання, агрегацію та завантаження даних.
    """
    print("Starting data processing for campaign daily metrics...")
    keyspace = os.environ.get("CASSANDRA_KEYSPACE", "adtech")

    # 1. Створити словник для відповідності campaign_name -> campaign_id
    print("Building campaign name-to-ID lookup...")
    name_to_id_lookup = {}
    rows = session.execute(f"SELECT campaign_id, campaign_name FROM {keyspace}.raw_campaigns")
    for r in rows:
        name_to_id_lookup[r.campaign_name] = r.campaign_id
    print(f"Lookup created with {len(name_to_id_lookup)} campaigns.")

    # 2. Агрегувати події (покази та кліки)
    print("Aggregating events from raw_events...")
    metrics = defaultdict(lambda: {'impressions': 0, 'clicks': 0})

    query = SimpleStatement(f"SELECT campaign_name, ts, wasclicked FROM {keyspace}.raw_events", fetch_size=1000)
    events = session.execute(query)

    processed_count = 0
    for event in events:
        campaign_id = name_to_id_lookup.get(event.campaign_name)
        if campaign_id is None:
            continue  # Пропустити, якщо кампанія не знайдена в довіднику

        try:
            # Отримати лише дату з мітки часу
            event_date = datetime.fromisoformat(event.ts.replace(" ", "T")).date()
            key = (campaign_id, event_date)

            metrics[key]['impressions'] += 1
            if event.wasclicked:
                metrics[key]['clicks'] += 1

            processed_count += 1
            if processed_count % 10000 == 0:
                print(f"Scanned {processed_count} events...")
        except Exception as e:
            print(f"Could not process event for campaign '{event.campaign_name}': {e}")

    print(f"Finished aggregation. Found metrics for {len(metrics)} campaign/day pairs.")

    # 3. Завантажити агреговані дані в аналітичну таблицю
    print("Truncating and loading data into `campaign_daily_metrics`...")
    session.execute(f"TRUNCATE {keyspace}.campaign_daily_metrics")

    insert_stmt = session.prepare(f"""
        INSERT INTO {keyspace}.campaign_daily_metrics (campaign_id, event_date, impressions, clicks, ctr)
        VALUES (?, ?, ?, ?, ?)
    """)

    batch = BatchStatement()
    batch_size = 100
    counter = 0

    for (campaign_id, event_date), values in metrics.items():
        impressions = values['impressions']
        clicks = values['clicks']
        ctr = (clicks / impressions) if impressions > 0 else 0.0

        batch.add(insert_stmt, (campaign_id, event_date, impressions, clicks, ctr))
        counter += 1

        if counter % batch_size == 0:
            session.execute(batch)
            batch.clear()

    if len(batch) > 0:
        session.execute(batch)

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
        print("✅ Analytics data for daily campaign metrics loaded successfully.")
    finally:
        session.shutdown()
        print("Cassandra connection closed.")


if __name__ == "__main__":
    main()
