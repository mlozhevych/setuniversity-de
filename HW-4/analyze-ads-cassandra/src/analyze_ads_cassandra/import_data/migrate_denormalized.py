#!/usr/bin/env python
"""
migrate_denormalized.py
~~~~~~~~~~~~~~~~~~~~~~~
ETL із raw_* → денормалізовані проекції під аналітичні запити.
Запускати ad-hoc або по крону/airflow після надходження «сирих» подій.
"""

import uuid, decimal, math
from collections import defaultdict
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query  import SimpleStatement, BatchStatement, ConsistencyLevel
from cassandra.util   import uuid_from_time

KEYSPACE       = "adtech"     # ваш KS
CONTACT_POINTS = ["127.0.0.1"]

# ---------- deterministic UUID helpers ----------
def uuid5_ns(text: str, ns=uuid.NAMESPACE_DNS) -> uuid.UUID:
    """Стабільний UUID на основі тексту (ім'я кампанії, рекламодавця ...)."""
    return uuid.uuid5(ns, text)

# ---------- connect ----------
cluster   = Cluster(CONTACT_POINTS)
session   = cluster.connect(KEYSPACE)

# ---------- select raw_events (token-range scan можна замінити Spark'ом) ----------
select_raw = SimpleStatement(
    "SELECT event_id, advertiser_name, campaign_name, adslotsize, "
    "       user_id, device, location, ts, adcost, wasclicked "
    "FROM   raw_events",
    fetch_size = 10_000,
    consistency_level = ConsistencyLevel.ONE
)

# ---------- prepared statements ----------
ins_user_eng = session.prepare("""
    INSERT INTO user_engagement (user_id, event_timestamp, campaign_id,
                                 campaign_name, advertiser_id, was_clicked)
    VALUES (?, ?, ?, ?, ?, ?)
""")

upd_campaign_perf = session.prepare("""
    UPDATE campaign_performance_by_day
    SET impressions = impressions + ?,
        clicks      = clicks      + ?
    WHERE campaign_id = ? AND event_date = ?
""")

ins_user_clicks = session.prepare("""
    INSERT INTO top_users_by_clicks (year_month, click_count, user_id)
    VALUES (?, ?, ?)
""")

ins_adv_reg = session.prepare("""
    INSERT INTO advertiser_spend_by_region (region, event_date,
                                            total_spend, advertiser_id, advertiser_name)
    VALUES (?, ?, ?, ?, ?)
""")

ins_adv_day = session.prepare("""
    INSERT INTO top_advertisers_by_spend (event_date, total_spend,
                                          advertiser_id, advertiser_name)
    VALUES (?, ?, ?, ?)
""")

# ---------- in-memory accumulators ----------
#   (campaign_id, date) -> [impr, clicks]
camp_perf = defaultdict(lambda: [0, 0])

#   (year_month, user_id) -> clicks
user_month_clicks = defaultdict(int)

#   (region, date, advertiser_uuid) -> spend Decimal
adv_reg_spend = defaultdict(decimal.Decimal)

#   (date, advertiser_uuid) -> spend Decimal
adv_day_spend = defaultdict(decimal.Decimal)

# ---------------------------------------------------------------------------
print("⏳  Scanning raw_events …")
for ev in session.execute(select_raw):
    # 1) базові поля
    ts_dt   = datetime.fromisoformat(ev.ts)              # '2024-10-31T06:56:39'
    ev_date = ts_dt.date()
    yyyymm  = ev_date.strftime("%Y-%m")                  # 'YYYY-MM'

    campaign_uuid   = uuid5_ns(ev.campaign_name)
    advertiser_uuid = uuid5_ns(ev.advertiser_name)

    spend = decimal.Decimal(str(ev.adcost or 0.0))
    is_click = bool(ev.wasclicked)

    # -------- 1. user_engagement (insert on every event) --------
    timeuuid = uuid_from_time(ts_dt)
    session.execute_async(ins_user_eng, (
        ev.user_id, timeuuid, campaign_uuid,
        ev.campaign_name, advertiser_uuid, is_click
    ))

    # -------- 2. aggregate for campaign_performance_by_day --------
    impr_click = camp_perf[(campaign_uuid, ev_date)]
    impr_click[0] += 1
    if is_click:
        impr_click[1] += 1

    # -------- 3. aggregate for top_users_by_clicks --------
    if is_click:
        user_month_clicks[(yyyymm, ev.user_id)] += 1

    # -------- 4. aggregate for advertiser_spend_by_region --------
    adv_reg_spend[(ev.location, ev_date, advertiser_uuid)] += spend

    # -------- 5. aggregate for top_advertisers_by_spend --------
    adv_day_spend[(ev_date, advertiser_uuid)] += spend

print("✅  Raw scan finished, writing aggregates …")

# ------------------- phase A: counters --------------------------
for (camp_id, ev_date), (impr, clk) in camp_perf.items():
    session.execute_async(upd_campaign_perf, (impr, clk, camp_id, ev_date))

# ------------------- phase B: leaderboard tables ---------------
batch_size = 100
batch      = BatchStatement(consistency_level=ConsistencyLevel.ONE)

def flush_batch():
    global batch
    if len(batch) > 0:
        session.execute(batch)
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)

# --- top_users_by_clicks ---
for (yyyymm, uid), clicks in user_month_clicks.items():
    batch.add(ins_user_clicks, (yyyymm, clicks, uid))
    if len(batch) >= batch_size:
        flush_batch()
flush_batch()

# --- advertiser_spend_by_region ---
for (region, ev_date, adv_uid), spend in adv_reg_spend.items():
    batch.add(ins_adv_reg, (
        region, ev_date, spend, adv_uid,
        None       # advertiser_name необов'язковий; можна lookup-ом
    ))
    if len(batch) >= batch_size:
        flush_batch()
flush_batch()

# --- top_advertisers_by_spend ---
for (ev_date, adv_uid), spend in adv_day_spend.items():
    batch.add(ins_adv_day, (
        ev_date, spend, adv_uid,
        None        # advertiser_name
    ))
    if len(batch) >= batch_size:
        flush_batch()
flush_batch()

print("🎉  Migration completed!")

cluster.shutdown()
