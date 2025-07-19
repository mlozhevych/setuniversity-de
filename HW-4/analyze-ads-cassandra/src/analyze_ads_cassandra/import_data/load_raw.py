"""
load_raw.py
~~~~~~~~~~~
Читає CSV-файли у staging-таблиці:
    raw_campaigns, raw_users, raw_events
"""
import csv
import os
import uuid
from pathlib import Path

from cassandra.query import BatchStatement
from dotenv import load_dotenv

from analyze_ads_cassandra.utils import gdrive_download, get_db_connection

# --- Константи та налаштування ---
# Define project root and data directory
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"

CSV_SEP = os.getenv("CSV_SEPARATOR", ",")


def load_raw_campaigns(session):
    # 1. Download campaigns CSV
    gdrive_campaigns_file_id = os.environ["GDRIVE_CAMPAIGNS_FILE_ID"]
    campaigns_csv_file = DATA_DIR / "campaigns.csv"
    print(f"⬇️  Downloading CSV to {campaigns_csv_file} …")
    gdrive_download(gdrive_campaigns_file_id, campaigns_csv_file)

    # 2. Check if table exists, then truncate or create
    keyspace = os.environ.get("CASSANDRA_KEYSPACE", "adtech")
    table_name = "raw_campaigns"

    table_check = session.execute(f"""
            SELECT table_name FROM system_schema.tables
            WHERE keyspace_name = %s AND table_name = %s
        """, (keyspace, table_name))

    if table_check.one():
        print(f"Table {table_name} exists. Truncating it...")
        session.execute(f"TRUNCATE {table_name}")
    else:
        print(f"Table {table_name} not found. Creating it...")
        ddl_users = f"""
        CREATE TABLE {table_name} (
            campaign_id int PRIMARY KEY,
            advertiser_name text,
            campaign_name text,
            campaign_start_date text,
            campaign_end_date text,
            targeting_criteria text,
            adslotsize text,
            budget float,
            remainingbudget float
        )
        """
        session.execute(ddl_users)

    print("Table raw_campaigns ensured, now inserting data...")

    # 3. Load campaigns CSV into raw_campaigns table
    stmt = session.prepare("""
    INSERT INTO raw_campaigns (
        campaign_id, advertiser_name, campaign_name,
        campaign_start_date, campaign_end_date,
        targeting_criteria, adslotsize, budget, remainingbudget
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    with open(campaigns_csv_file, newline="") as fh:
        rdr = csv.DictReader(fh, delimiter=CSV_SEP)
        for row in rdr:
            session.execute(stmt, (
                int(row["CampaignID"]),
                row["AdvertiserName"],
                row["CampaignName"],
                row["CampaignStartDate"],
                row["CampaignEndDate"],
                row["TargetingCriteria"],
                row["AdSlotSize"],
                float(row["Budget"]),
                float(row["RemainingBudget"]),
            ))


def load_raw_users(session):
    # 1. Download users CSV
    gdrive_users_file_id = os.environ["GDRIVE_USERS_FILE_ID"]
    users_csv_file = DATA_DIR / "users.csv"
    print(f"⬇️  Downloading CSV to {users_csv_file} …")
    gdrive_download(gdrive_users_file_id, users_csv_file)

    # 2. Check if table exists, then truncate or create
    keyspace = os.environ.get("CASSANDRA_KEYSPACE", "adtech")
    table_name = "raw_users"

    table_check = session.execute(f"""
               SELECT table_name FROM system_schema.tables
               WHERE keyspace_name = %s AND table_name = %s
           """, (keyspace, table_name))

    if table_check.one():
        print(f"Table {table_name} exists. Truncating it...")
        session.execute(f"TRUNCATE {table_name}")
    else:
        print(f"Table {table_name} not found. Creating it...")
        ddl_users = f"""
        CREATE TABLE {table_name} (
            user_id int PRIMARY KEY,
            age int,
            gender text,
            location text,
            interests set<text>,
            signup_date text
        )
        """
        session.execute(ddl_users)

    print("Table raw_users ensured, now inserting data...")

    # 3. Load users CSV into raw_users table
    stmt = session.prepare("""
    INSERT INTO raw_users (
        user_id, age, gender, location, interests, signup_date
    ) VALUES (?, ?, ?, ?, ?, ?)
    """)

    # Створюємо пакет
    batch = BatchStatement()
    batch_size = 100  # Максимальний розмір пакету
    counter = 0  # Лічильник для відстеження розміру пакету

    with open(users_csv_file, newline="") as fh:
        rdr = csv.DictReader(fh, delimiter=CSV_SEP)
        for row in rdr:
            # Додаємо запит до пакета
            batch.add(stmt, (
                int(row["UserID"]),
                int(row["Age"]),
                row["Gender"],
                row["Location"],
                set(row["Interests"].split(",")),
                row["SignupDate"],
            ))
            counter += 1

            # Виконуємо пакет, коли він досягає потрібного розміру
            if counter % batch_size == 0:
                session.execute(batch)
                batch.clear()  # Очищуємо пакет для наступної ітерації
                print(f"Processed {counter} records...")

    # Виконуємо залишок пакету, якщо він не порожній
    if len(batch) > 0:
        session.execute(batch)
        print(f"Processed final {len(batch)} records.")

    print(f"Total records processed: {counter}")


def load_raw_events(session):
    # 1. Download events CSV
    gdrive_events_file_id = os.environ["GDRIVE_EVENTS_FILE_ID"]
    events_csv_file = DATA_DIR / "events.csv"
    print(f"⬇️  Downloading CSV to {events_csv_file} …")
    gdrive_download(gdrive_events_file_id, events_csv_file)

    # 2. Check if table exists, then truncate or create
    keyspace = os.environ.get("CASSANDRA_KEYSPACE", "adtech")
    table_name = "raw_events"

    table_check = session.execute(f"""
                   SELECT table_name FROM system_schema.tables
                   WHERE keyspace_name = %s AND table_name = %s
               """, (keyspace, table_name))
    if table_check.one():
        print(f"Table {table_name} exists. Truncating it...")
        session.execute(f"TRUNCATE {table_name}")
    else:
        print(f"Table {table_name} not found. Creating it...")
        ddl_users = f"""
        CREATE TABLE {table_name} (
            event_id uuid PRIMARY KEY,
            advertiser_name text,
            campaign_name text,
            campaign_start_date text,
            campaign_end_date text,
            campaign_targeting_criteria text,
            campaign_targeting_interest text,
            campaign_targeting_country text,
            adslotsize text,
            user_id int,
            device text,
            location text,
            ts text,
            bidamount float,
            adcost float,
            wasclicked boolean,
            clickts text,
            adrevenue float,
            budget float,
            remainingbudget float
        )
        """
        session.execute(ddl_users)

    print("Table raw_events ensured, now inserting data...")

    # 3. Load events CSV into raw_events table
    stmt = session.prepare("""
        INSERT INTO raw_events (
            event_id, advertiser_name, campaign_name,
            campaign_start_date, campaign_end_date, campaign_targeting_criteria,
            campaign_targeting_interest, campaign_targeting_country,
            adslotsize, user_id, device, location,
            ts, bidamount, adcost, wasclicked,
            clickts, adrevenue, budget, remainingbudget
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """)

    # Створюємо пакет
    batch = BatchStatement()
    batch_size = 50  # Максимальний розмір пакету
    counter = 0  # Лічильник для відстеження розміру пакету

    with open(events_csv_file, newline="") as fh:
        rdr = csv.DictReader(fh, delimiter=CSV_SEP)
        for r in rdr:
            # Додаємо запит до пакета
            batch.add(stmt, (
                uuid.UUID(r["EventID"]),  # Використовуємо UUID для унікального ідентифікатора події
                r["AdvertiserName"],
                r["CampaignName"],
                r["CampaignStartDate"],
                r["CampaignEndDate"],
                r["CampaignTargetingCriteria"],
                r["CampaignTargetingInterest"],
                r["CampaignTargetingCountry"],
                r["AdSlotSize"],
                int(r["UserID"]),
                r["Device"],
                r["Location"],
                r["Timestamp"],
                float(r["BidAmount"]),
                float(r["AdCost"]),
                r["WasClicked"].lower() == "true",
                r["ClickTimestamp"] or None,
                float(r["AdRevenue"]),
                float(r["Budget"]),
                float(r["RemainingBudget"]),
            ))
            counter += 1

            # Виконуємо пакет, коли він наповниться
            if counter % batch_size == 0:
                session.execute(batch)
                batch.clear()  # Очищуємо пакет для наступної ітерації
                print(f"Processed {counter} event records...")

    # Виконуємо залишок пакету, якщо він не порожній
    if len(batch) > 0:
        session.execute(batch)
        print(f"Processed final {len(batch)} records.")

    print(f"Total records processed: {counter}")


def main():
    load_dotenv()
    session = get_db_connection()
    if not session:
        print("❌ Failed to connect to Cassandra. Exiting.")
        return
    print("Starting RAW ingest ...")
    # load_raw_campaigns(session)
    # load_raw_users(session)
    load_raw_events(session)
    print("RAW ingest finished 👍")
    session.shutdown()


if __name__ == "__main__":
    main()
