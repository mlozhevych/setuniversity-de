"""
Перетворення flat-CSV з логами реклами у вкладені документи MongoDB
   • sessions – сесії з імпресіями та кліками
"""

import os
from datetime import timedelta
from pathlib import Path
from typing import List, Dict

import pandas as pd
from analyze_ads_nosql.utils import gdrive_download, build_mongo_uri
from dotenv import load_dotenv
from pymongo import MongoClient, InsertOne, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError


def build_campaign(r) -> Dict:
    """Знімок кампанії на момент імпресії (immutable)."""
    return dict(
        campaignId=int(r.CampaignName.split("_")[-1]),  # прим.: витягаємо ID з назви
        name=r.CampaignName,
        advertiserName=r.AdvertiserName,
        startDate=r.CampaignStartDate,
        endDate=r.CampaignEndDate,
        targetingCriteria=r.CampaignTargetingCriteria,
        targetingInterest=r.CampaignTargetingInterest,
        targetingCountry=r.CampaignTargetingCountry,
        adSlotSize={"width": r.SlotW, "height": r.SlotH},
    )


def build_impression(r) -> Dict:
    imp = dict(
        impressionId=r.EventID,
        timestamp=r.Timestamp,
        device=r.Device,
        location=r.Location,
        campaign=build_campaign(r),
        bidAmount=float(r.BidAmount),
        adCost=float(r.AdCost),
        clicks=[]
    )
    if r.WasClicked:
        imp["clicks"].append(dict(
            clickTimestamp=r.ClickTimestamp,
            adRevenue=float(r.AdRevenue)
        ))
    return imp


def flush_session(bag: List[Dict], user_id: int,
                  start_ts, end_ts, out: List[Dict]):
    """Формує завершений документ сесії та додає до колекції для bulk-insert."""
    if not bag:
        return
    out.append(dict(
        userId=user_id,
        sessionStart=start_ts,
        sessionEnd=end_ts,
        impressionsCount=len(bag),
        clicksCount=sum(len(i["clicks"]) for i in bag),
        impressions=bag
    ))


def insert_batches(collection, docs: List[Dict], batch_size=1000):
    for i in range(0, len(docs), batch_size):
        batch = docs[i:i + batch_size]
        try:
            collection.bulk_write([InsertOne(d) for d in batch], ordered=False)
        except BulkWriteError as bwe:
            print(f"Bulk insert error in batch {i // batch_size}: {bwe.details}")


def create_indexes(collection):
    collection.create_index([("userId", ASCENDING), ("sessionStart", DESCENDING)])
    collection.create_index([("impressions.campaign.advertiserName", ASCENDING),
                             ("impressions.clicks.clickTimestamp", DESCENDING)])
    collection.create_index([("impressions.campaign.campaignId", ASCENDING)])
    collection.create_index([("impressions.campaign.targetingInterest", ASCENDING)])


def import_sessions() -> None:
    session_timeout = timedelta(minutes=int(os.getenv("SESSION_TIMEOUT_MINUTES", "30")))
    gdrive_id = os.environ["GDRIVE_EVENTS_FILE_ID"]
    db_name = os.getenv("MONGO_DB", "AdTech")
    coll_name = "sessions"
    csv_sep = os.getenv("CSV_SEPARATOR", ",")

    project_root = Path(__file__).resolve().parent
    data_dir = project_root / "data"
    csv_file = data_dir / "sessions.csv"
    print(f"⬇️  Downloading CSV to {csv_file} …")
    gdrive_download(gdrive_id, csv_file)

    dtype_map = {
        "EventID": "string", "AdvertiserName": "string",
        "CampaignName": "string", "AdSlotSize": "string",
        "Device": "string", "Location": "string", "WasClicked": "bool"
    }
    date_cols = ["Timestamp", "ClickTimestamp", "CampaignStartDate", "CampaignEndDate"]

    print("📖  Processing CSV in chunks …")
    chunk_size = 100_000
    uri = build_mongo_uri(use_docker=True)
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[coll_name]

    for chunk in pd.read_csv(csv_file, dtype=dtype_map, parse_dates=date_cols, sep=csv_sep, chunksize=chunk_size):
        chunk[["SlotW", "SlotH"]] = chunk["AdSlotSize"].str.split("x", expand=True).astype(int)

        sessions_bulk: List[Dict] = []
        for user_id, grp in chunk.sort_values(["UserID", "Timestamp"]).groupby("UserID"):
            bag, start_ts, last_ts = [], None, None
            for r in grp.itertuples(index=False):
                if last_ts is None or r.Timestamp - last_ts > session_timeout:
                    flush_session(bag, user_id, start_ts, last_ts, sessions_bulk)
                    bag, start_ts = [], r.Timestamp
                bag.append(build_impression(r))
                last_ts = r.Timestamp
            flush_session(bag, user_id, start_ts, last_ts, sessions_bulk)

        if sessions_bulk:
            insert_batches(collection, sessions_bulk, batch_size=1000)

    create_indexes(collection)
    client.close()
    print("✅ All sessions imported and indexed.")


def main() -> None:
    load_dotenv()
    import_sessions()


if __name__ == "__main__":
    main()
