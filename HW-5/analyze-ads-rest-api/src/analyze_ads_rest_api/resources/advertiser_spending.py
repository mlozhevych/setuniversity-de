from datetime import timedelta

import redis
from flask import jsonify
from flask.views import MethodView
from flask_smorest import Blueprint
from sqlalchemy import func

from analyze_ads_rest_api.cache import cache_get, cache_set
from analyze_ads_rest_api.db import db
from analyze_ads_rest_api.models.advertiser import AdvertiserModel
from analyze_ads_rest_api.models.campaign import CampaignModel
from analyze_ads_rest_api.models.event import EventModel
from analyze_ads_rest_api.schemas import AdvertiserSpendingSchema

blp = Blueprint("AdvertiserSpending", __name__, description="Advertiser Spending")


@blp.route("/advertiser/<int:advertiser_id>/spending")
class AdvertiserSpendingResource(MethodView):

    @blp.response(200, AdvertiserSpendingSchema)
    def get(self, advertiser_id):
        """
        Returns an advertiser’s total ad spend.
        Implements a read-through cache with a 5-minute TTL.
        """
        cache_key = f"advertiser:{advertiser_id}:spending"

        # 1. Check Redis cache
        try:
            cached_data = cache_get(cache_key)
            if cached_data:
                print(f"CACHE HIT for advertiser {advertiser_id}")
                return jsonify(cached_data)
        except (redis.exceptions.ConnectionError, redis.exceptions.RedisError) as e:
            print(f"Redis connection error: {e}")

        print(f"CACHE MISS for advertiser {advertiser_id}")
        max_ts = db.session.query(func.max(EventModel.Timestamp)).scalar()
        if not max_ts:
            return {"AdvertiserID": advertiser_id, "TotalSpend": 0.0}

        start_ts = max_ts - timedelta(days=30)

        result = db.session.query(
            AdvertiserModel.AdvertiserID,
            func.sum(EventModel.AdCost).label("TotalSpend")
        ).join(CampaignModel, AdvertiserModel.AdvertiserID == CampaignModel.AdvertiserID) \
            .join(EventModel, CampaignModel.CampaignID == EventModel.CampaignID) \
            .filter(
            AdvertiserModel.AdvertiserID == advertiser_id,
            EventModel.Timestamp.between(start_ts, max_ts)
        ).group_by(AdvertiserModel.AdvertiserID) \
            .first()

        if result:
            result = result._asdict()  # Convert SQLAlchemy result to dict

            # 3. Store the result in Redis with a 30-second TTL
            try:
                cache_set(cache_key, result, 30)
            except (redis.exceptions.ConnectionError, redis.exceptions.RedisError) as e:
                print(f"Could not write to Redis: {e}")

            return {
                "AdvertiserID": result["AdvertiserID"],
                "TotalSpend": float(result["TotalSpend"] or 0.0)
            }
        else:
            return {
                "AdvertiserID": advertiser_id,
                "TotalSpend": 0.0
            }
