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


def import_sessions() -> None:
    session_timeout = timedelta(
        minutes=int(os.getenv("SESSION_TIMEOUT_MINUTES", "30"))
    )
    gdrive_id = os.environ["GDRIVE_EVENTS_FILE_ID"]
    db_name = os.getenv("MONGO_DB", "AdTech")
    coll_name = "sessions"
    csv_sep = os.getenv("CSV_SEPARATOR", ",")

    # 1. Download
    project_root = Path(__file__).resolve().parent
    data_dir = project_root / "data"
    csv_file = data_dir / "sessions.csv"
    print(f"⬇️  Downloading CSV to {csv_file} …")
    gdrive_download(gdrive_id, csv_file)

    # 2. Read
    print("📖  Reading file …")
    dtype_map = {
        "EventID": "string", "AdvertiserName": "string",
        "CampaignName": "string", "AdSlotSize": "string",
        "Device": "string", "Location": "string", "WasClicked": "bool"
    }
    date_cols = ["Timestamp", "ClickTimestamp",
                 "CampaignStartDate", "CampaignEndDate"]

    df = pd.read_csv(csv_file, dtype=dtype_map, parse_dates=date_cols, sep=csv_sep)

    df[["SlotW", "SlotH"]] = (
        df["AdSlotSize"].str.split("x", expand=True).astype(int)
    )

    # 3. Make a sessions
    sessions_bulk: List[Dict] = []
    users_stub: Dict[int, Dict] = {}

    for user_id, grp in df.sort_values(["UserID", "Timestamp"]) \
            .groupby("UserID"):

        bag, start_ts, last_ts = [], None, None
        for r in grp.itertuples(index=False):
            if last_ts is None or r.Timestamp - last_ts > session_timeout:
                flush_session(bag, user_id, start_ts, last_ts, sessions_bulk)
                bag, start_ts = [], r.Timestamp  # початок нової сесії
            bag.append(build_impression(r))
            last_ts = r.Timestamp

        flush_session(bag, user_id, start_ts, last_ts, sessions_bulk)

        # мінімальний юзер-профіль
        # users_stub[user_id] = dict(
        #     _id=user_id,
        #     userId=user_id,
        #     firstSeen=grp.Timestamp.min(),
        #     lastSeen=grp.Timestamp.max()
        # )

    # 4. Insert
    uri = build_mongo_uri()
    safe_uri_for_log = uri.replace(f":{os.getenv('MONGO_PASSWORD', '')}@", ":***@")
    print(f"Connecting to {safe_uri_for_log} …")

    # Connect and insert
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[coll_name]

    if sessions_bulk:
        db.sessions.bulk_write([InsertOne(s) for s in sessions_bulk],
                               ordered=False)
    # if users_stub:
    #     db.users_profile.bulk_write([InsertOne(u) for u in users_stub.values()],
    #                                 ordered=False)

    print(f"Loaded {len(users_stub)} users / {len(sessions_bulk)} sessions from {csv_file}")

    # Create index
    collection.create_index([("userId", ASCENDING), ("sessionStart", DESCENDING)])


def main() -> None:
    load_dotenv()
    import_sessions()


if __name__ == "__main__":
    main()
