"""
ad_interactions.py

Пайплайн: Всі ad-інтеракції для користувача.
"""


def get_query_1(user_id: int):
    """1. Отримати всі рекламні взаємодії для конкретного користувача."""
    return [
        {"$match": {"userId": user_id}},
        {
            "$lookup": {
                "from": "users",
                "localField": "userId",
                "foreignField": "userId",
                "as": "user"
            }
        },
        {"$unwind": "$user"},
        {
            "$project": {
                "_id": 0,
                "userId": 1,
                "sessionStart": 1,
                "sessionEnd": 1,
                "impressionsCount": 1,
                "clicksCount": 1,
                "impressions": 1,
                "user": {
                    "age": "$user.age",
                    "gender": "$user.gender",
                    "location": "$user.location",
                    "interests": "$user.interests",
                    "signUpDate": "$user.signUpDate"
                }
            }
        }
    ]
