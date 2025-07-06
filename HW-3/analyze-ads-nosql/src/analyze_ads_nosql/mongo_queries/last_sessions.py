"""
last_sessions.py

Пайплайн: Останні 5 ad-сесій користувача з інформацією про кліки та час.
"""


def get_query_2(user_id: int):
    """2. Отримати останні 5 рекламних сесій користувача."""
    return [
        {"$match": {"userId": user_id}},
        {"$sort": {"sessionStart": -1}},
        {"$limit": 5},
        {
            "$project": {
                "_id": 0,
                "userId": 1,
                "sessionStart": 1,
                "sessionEnd": 1,
                "impressionsCount": 1,
                "clicksCount": 1,
                "impressions": {
                    "impressionId": 1,
                    "timestamp": 1,
                    "device": 1,
                    "clicks": 1
                }
            }
        }
    ]
