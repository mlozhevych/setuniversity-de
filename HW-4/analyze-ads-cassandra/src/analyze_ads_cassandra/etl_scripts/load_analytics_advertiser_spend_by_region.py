# load_analytics_advertiser_spend_by_region.py
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Цей скрипт розраховує загальні витрати кожного рекламодавця за регіонами
# за останні 30 днів відносно останньої дати в історичних даних
# і завантажує результати в таблицю `top_advertisers_by_region`.

import os
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

from cassandra.query import SimpleStatement
from dotenv import load_dotenv

from analyze_ads_cassandra.utils import get_db_connection


def ensure_analytics_table_exists(session):
    """
    Перевіряє існування таблиці `top_advertisers_by_region` і створює її, якщо потрібно.
    """
    keyspace = os.environ.get("CASSANDRA_KEYSPACE", "adtech")
    table_name = "top_advertisers_by_region"

    table_check = session.execute(f"""
        SELECT table_name FROM system_schema.tables
        WHERE keyspace_name = %s AND table_name = %s
    """, (keyspace, table_name))

    if not table_check.one():
        print(f"Table {table_name} not found. Creating it...")
        ddl_query = """
        CREATE TABLE top_advertisers_by_region (
            region text,
            total_spend decimal,
            advertiser_name text,
            PRIMARY KEY (region, total_spend, advertiser_name)
        ) WITH CLUSTERING ORDER BY (total_spend DESC)
        """
        session.execute(ddl_query)
        print(f"Table {table_name} created successfully.")
    else:
        print(f"Table {table_name} already exists.")


def process_and_load_data(session):
    """
    Обробляє події, знаходить останню дату, та завантажує агреговані дані про витрати за регіонами.
    """
    print("Starting data processing for top advertisers by region...")

    # 1. Знайти останню мітку часу в наборі даних
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
    print(f"Calculating spend from {thirty_days_ago_from_latest} to {latest_timestamp}")

    # 3. Прочитати дані з raw_events та агрегувати витрати
    # Ключ словника - кортеж (регіон, рекламодавець)
    query = SimpleStatement("SELECT campaign_targeting_country, advertiser_name, ts, adcost FROM raw_events",
                            fetch_size=1000)
    rows = session.execute(query)

    spend_by_region_advertiser = defaultdict(Decimal)
    processed_count = 0

    for event in rows:
        try:
            event_time = datetime.fromisoformat(event.ts.replace(" ", "T"))
            # Враховуємо лише події за останні 30 днів
            if event_time >= thirty_days_ago_from_latest:
                region = event.campaign_targeting_country.strip() if event.campaign_targeting_country else None
                advertiser = event.advertiser_name.strip() if event.advertiser_name else None
                if region and advertiser:  # Ігноруємо записи без регіону або рекламодавця
                    spend_by_region_advertiser[(region, advertiser)] += Decimal(str(event.adcost))

            processed_count += 1
            if processed_count % 10000 == 0:
                print(f"Scanned {processed_count} events...")
        except Exception as e:
            print(f"Could not process event for advertiser '{event.advertiser_name}': {e}")

    print(f"Finished scanning events. Found spend data for {len(spend_by_region_advertiser)} region/advertiser pairs.")

    # 4. Очистити таблицю та завантажити нові агреговані дані
    print("Truncating and loading data into `top_advertisers_by_region`...")

    session.execute("TRUNCATE adtech.top_advertisers_by_region")

    insert_stmt = session.prepare("""
        INSERT INTO top_advertisers_by_region (region, advertiser_name, total_spend)
        VALUES (?, ?, ?)
    """)

    for (region, advertiser), total_spend in spend_by_region_advertiser.items():
        session.execute(insert_stmt, (region, advertiser, total_spend))

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
        print("✅ Analytics data for top advertisers by region loaded successfully.")
    finally:
        session.shutdown()
        print("Cassandra connection closed.")


if __name__ == "__main__":
    main()
