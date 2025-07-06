"""
clicks_per_hour.py

Пайплайн: Кількість кліків по годинах у розрізі кампаній для рекламодавця за останні 24 години.
"""

from datetime import timedelta


def execute_query_3(sessions_collection, advertiser_name: str):
    """3. Кількість кліків за годину для кампаній певного рекламодавця за останню добу."""
    print(f"Шукаємо кліки для рекламодавця: {advertiser_name}")

    # Крок 1: Знайти час останнього кліку
    last_click_pipeline = [
        {"$unwind": "$impressions"},
        {"$unwind": "$impressions.clicks"},
        {"$match": {"impressions.campaign.advertiserName": advertiser_name}},
        {"$sort": {"impressions.clicks.clickTimestamp": -1}},
        {"$limit": 1},
        {"$project": {"_id": 0, "lastClickTimestamp": "$impressions.clicks.clickTimestamp"}}
    ]

    last_click_result = list(sessions_collection.aggregate(last_click_pipeline))

    if not last_click_result:
        print(f"Не знайдено кліків для рекламодавця '{advertiser_name}'.")
        return []

    end_date = last_click_result[0]['lastClickTimestamp']
    start_date = end_date - timedelta(hours=24)

    print(f"Часове вікно: {start_date.isoformat()} ... {end_date.isoformat()}")

    # Крок 2: Основний агрегаційний запит
    main_pipeline = [
        {"$unwind": "$impressions"},
        {"$unwind": "$impressions.clicks"},
        {
            "$match": {
                "impressions.campaign.advertiserName": advertiser_name,
                "impressions.clicks.clickTimestamp": {"$gte": start_date, "$lte": end_date}
            }
        },
        {
            "$group": {
                "_id": {
                    "campaignId": "$impressions.campaign.campaignId",
                    "campaignName": "$impressions.campaign.name",
                    "hour": {
                        "$dateToString": {"format": "%Y-%m-%dT%H:00:00Z", "date": "$impressions.clicks.clickTimestamp"}}
                },
                "clicksCount": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "campaignId": "$_id.campaignId",
                "campaignName": "$_id.campaignName",
                "hour": "$_id.hour",
                "clicksCount": 1
            }
        },
        {"$sort": {"campaignId": 1, "hour": 1}}
    ]

    return list(sessions_collection.aggregate(main_pipeline))
