"""
Імпорт користувачів у MongoDB із CSV, розміщеного на Google Drive.
"""

import os
from pathlib import Path

import pandas as pd
from analyze_ads_nosql.utils import build_mongo_uri, gdrive_download, get_db_connection
from dateutil import parser
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING


def row_to_document(row):
    """Перетворення рядків на BSON-документи"""
    interests = [x.strip() for x in str(row["Interests"]).split(",") if x]
    signup_dt = parser.parse(str(row["SignupDate"]))

    return {
        "userId": int(row["UserID"]),
        "age": int(row["Age"]),
        "gender": str(row["Gender"]),
        "location": {"country": str(row["Location"])},
        "interests": interests,
        "signUpDate": signup_dt,
    }


def import_users() -> None:
    gdrive_id = os.environ["GDRIVE_USERS_FILE_ID"]
    db_name = os.getenv("MONGO_DB", "AdTech")
    coll_name = "users"
    csv_sep = os.getenv("CSV_SEPARATOR", ",")

    # 1. Download
    project_root = Path(__file__).resolve().parent
    data_dir = project_root / "data"
    csv_file = data_dir / "users.csv"
    print(f"⬇️  Downloading CSV to {csv_file} …")
    gdrive_download(gdrive_id, csv_file)

    # 2. Read
    print("📖  Reading file …")
    df = pd.read_csv(csv_file, sep=csv_sep)

    # 3. Transform
    documents = [row_to_document(r) for _, r in df.iterrows()]

    # 4. Insert
    uri = build_mongo_uri(use_docker=True)
    safe_uri_for_log = uri.replace(f":{os.getenv('MONGO_PASSWORD', '')}@", ":***@")
    print(f"Connecting to {safe_uri_for_log} …")

    # Connect and insert
    client, db = get_db_connection(uri, db_name)
    collection = db[coll_name]
    result = client[db_name][coll_name].insert_many(documents)

    print(f"Inserted {len(result.inserted_ids)} documents into '{coll_name}'.")

    # Create unique index
    collection.create_index(
        [("userId", ASCENDING)],
        unique=True)


def main() -> None:
    load_dotenv()
    import_users()


if __name__ == "__main__":
    main()
