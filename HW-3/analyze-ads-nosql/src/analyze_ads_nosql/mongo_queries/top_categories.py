"""
top_categories.py

Пайплайн: Топ-3 категорії/інтереси користувача за кількістю кліків.
"""


def get_query_5(user_id: int):
    """5. Топ-3 категорії, за якими користувач найчастіше клікає."""
    return [
        {"$match": {"userId": user_id}},
        {"$unwind": "$impressions"},
        {"$unwind": "$impressions.clicks"},
        {
            "$group": {
                "_id": "$impressions.campaign.targetingInterest",
                "clicks": {"$sum": 1}
            }
        },
        {"$sort": {"clicks": -1}},
        {"$limit": 3},
        {
            "$project": {
                "_id": 0,
                "category": "$_id",
                "clicks": 1
            }
        }
    ]
