"""
migrate_denormalized.py
~~~~~~~~~~~~~~~~~~~~~~~
ETL із raw_* → денормалізовані проекції під аналітичні запити.
Запускати ad-hoc або по крону/airflow після надходження «сирих» подій.
"""

import decimal
import os
from collections import defaultdict
from datetime import datetime

from cassandra.query import SimpleStatement, ConsistencyLevel, BatchStatement
from cassandra.util import uuid_from_time
from dotenv import load_dotenv

from analyze_ads_cassandra.utils import get_db_connection


# --- Ініціалізація таблиць у Cassandra ---
def init_cassandra_tables(session, keyspace_name: str):
    print(f"🔗 Initializing Cassandra tables in keyspace '{keyspace_name}' ...")
    # Check if keyspace exists; if not, create it
    if keyspace_name.lower() not in session.cluster.metadata.keyspaces:
        session.execute(
            "CREATE KEYSPACE {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}};"
            .format(keyspace_name)
        )

    session.set_keyspace(keyspace_name)

    # Define table creation statements
    table_statements = {
        "campaign_performance_by_day": """
        CREATE TABLE AdTech.campaign_performance_by_day(
            campaign_name   text,
            impressions     counter,
            clicks          counter,
            event_date      date,
            PRIMARY KEY (campaign_name, event_date)
        );
        """,
        "top_users_by_clicks": """
        CREATE TABLE AdTech.top_users_by_clicks (
            event_date  date,
            click_count bigint,
            user_id     int,
            PRIMARY KEY (event_date, click_count, user_id)
        ) WITH CLUSTERING ORDER BY (click_count DESC);
        """,
        "user_engagement": """
        CREATE TABLE AdTech.user_engagement(
            user_id           int,
            event_timestamp   timeuuid,
            campaign_name     text,
            advertiser_name   text,
            was_clicked       boolean,
            PRIMARY KEY (user_id, event_timestamp)
        ) WITH CLUSTERING ORDER BY (event_timestamp DESC);
        """,
        "advertiser_spend_by_region": """
        CREATE TABLE AdTech.advertiser_spend_by_region(
            region            text,
            event_date        date,
            total_spend       decimal,
            advertiser_name   text,
            PRIMARY KEY ((region, event_date), total_spend, advertiser_name)
        ) WITH CLUSTERING ORDER BY (total_spend DESC);
        """,
        "top_advertisers_by_spend": """
        CREATE TABLE AdTech.top_advertisers_by_spend (
            event_date        date,
            total_spend       decimal,
            advertiser_name   text,
            PRIMARY KEY (event_date, total_spend, advertiser_name)
        ) WITH CLUSTERING ORDER BY (total_spend DESC);
        """
    }

    # Get existing tables in keyspace
    current_tables = session.cluster.metadata.keyspaces[keyspace_name.lower()].tables.keys()

    # Iterate over tables and drop then recreate
    for table_name, create_stmt in table_statements.items():
        if table_name in current_tables:
            session.execute("DROP TABLE {}.{}".format(keyspace_name, table_name))
        session.execute(create_stmt)
    print("✅  All tables initialized or truncated successfully.")


def run_migration(session):
    print("🔄 Starting migration from raw_events to denormalized projections ...")
    select_raw = SimpleStatement(
        "SELECT event_id, advertiser_name, campaign_name, adslotsize, "
        "       user_id, device, location, ts, adcost, wasclicked "
        "FROM   raw_events",
        fetch_size=10_000,
        consistency_level=ConsistencyLevel.ONE
    )

    # ---------- prepared statements ----------
    ins_user_eng = session.prepare("""
        INSERT INTO user_engagement (user_id, event_timestamp, 
        campaign_name, advertiser_name, was_clicked)
        VALUES (?, ?, ?, ?, ?)
    """)

    upd_campaign_perf = session.prepare("""
        UPDATE campaign_performance_by_day
        SET impressions = impressions + ?,
            clicks      = clicks      + ?
        WHERE campaign_name = ? AND event_date = ?
    """)

    ins_user_clicks = session.prepare("""
        INSERT INTO top_users_by_clicks (event_date, click_count, user_id)
        VALUES (?, ?, ?)
    """)

    ins_adv_reg = session.prepare("""
        INSERT INTO advertiser_spend_by_region (region, event_date,
                                                total_spend, advertiser_name)
        VALUES (?, ?, ?, ?)
    """)

    ins_adv_day = session.prepare("""
        INSERT INTO top_advertisers_by_spend (event_date, total_spend, advertiser_name)
        VALUES (?, ?, ?)
    """)

    # ---------- in-memory accumulators ----------
    camp_perf = defaultdict(lambda: [0, 0])
    user_day_clicks = defaultdict(int)
    adv_reg_spend = defaultdict(decimal.Decimal)
    adv_day_spend = defaultdict(decimal.Decimal)
    user_engagement_rows = []

    # ---------------------------------------------------------------------------
    print("⏳  Scanning raw_events …")
    for ev in session.execute(select_raw):
        # 1) базові поля
        ts_dt = datetime.fromisoformat(ev.ts)
        ev_date = ts_dt.date()

        spend = decimal.Decimal(str(ev.adcost or 0.0))
        is_click = bool(ev.wasclicked)

        # -------- 1. user_engagement (insert on every event) --------
        timeuuid = uuid_from_time(ts_dt)
        session.execute_async(ins_user_eng, (
            ev.user_id, timeuuid, ev.campaign_name,
            ev.advertiser_name, is_click
        ))

        # -------- 2. aggregate for campaign_performance_by_day --------
        impr_click = camp_perf[(ev.campaign_name, ev_date)]
        impr_click[0] += 1
        if is_click:
            impr_click[1] += 1

        # -------- 3. aggregate for top_users_by_clicks --------
        if is_click:
            user_day_clicks[(ev_date, ev.user_id)] += 1

        # -------- 4. aggregate for advertiser_spend_by_region --------
        adv_reg_spend[(ev.location, ev_date, ev.advertiser_name)] += spend

        # -------- 5. aggregate for top_advertisers_by_spend --------
        adv_day_spend[(ev_date, ev.advertiser_name)] += spend

    print("✅  Raw scan finished, writing aggregates …")

    # ------------------- phase A: counters --------------------------
    for (camp_name, ev_date), (impr, clk) in camp_perf.items():
        session.execute_async(upd_campaign_perf, (impr, clk, camp_name, ev_date))

    # ------------------- phase B: leaderboard tables ---------------
    batch_size = 100
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)

    def flush_batch():
        nonlocal batch
        if len(batch) > 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)

    # --- top_users_by_clicks ---
    for (ev_date, uid), clicks in user_day_clicks.items():
        batch.add(ins_user_clicks, (ev_date, clicks, uid))
        if len(batch) >= batch_size:
            flush_batch()
    flush_batch()

    # --- advertiser_spend_by_region ---
    for (region, ev_date, adv_name), spend in adv_reg_spend.items():
        batch.add(ins_adv_reg, (
            region, ev_date, spend, adv_name
        ))
        if len(batch) >= batch_size:
            flush_batch()
    flush_batch()

    # --- top_advertisers_by_spend ---
    for (ev_date, adv_name), spend in adv_day_spend.items():
        batch.add(ins_adv_day, (
            ev_date, spend, adv_name
        ))
        if len(batch) >= batch_size:
            flush_batch()
    flush_batch()

    print("🎉  Migration completed!")


def main():
    load_dotenv()
    keyspace = os.environ.get("CASSANDRA_KEYSPACE", "adtech")
    session = get_db_connection()
    if not session:
        print("❌ Failed to connect to Cassandra. Exiting.")
        return
    try:
        # Initialize Cassandra tables.
        init_cassandra_tables(session, keyspace)
        # Execute migration.
        run_migration(session)
    finally:
        session.shutdown()


if __name__ == "__main__":
    main()
