"""
ad_fatigue.py

Пайплайн: Виявлення ad fatigue — користувач бачив кампанію 5+ рази.
"""


def get_query_4():
    """4. Виявлення 'втоми від реклами' (багато показів, мало кліків)."""
    return [
        {"$unwind": "$impressions"},
        {
            "$group": {
                "_id": {
                    "userId": "$userId",
                    "campaignId": "$impressions.campaign.campaignId"
                },
                "impressionsCount": {"$sum": 1},
                "clicksCount": {
                    "$sum": {
                        "$cond": [{"$gt": [{"$size": {"$ifNull": ["$impressions.clicks", []]}}, 0]}, 1, 0]
                    }
                }
            }
        },
        {
            "$match": {
                "impressionsCount": {"$gte": 5},  # Мінімум 5 покази
                "clicksCount": 0  # Жодного кліку
            }
        },
        {
            "$project": {
                "_id": 0,
                "userId": "$_id.userId",
                "campaignId": "$_id.campaignId",
                "impressionsCount": 1
            }
        }
    ]
