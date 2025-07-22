from datetime import timedelta

from flask.views import MethodView
from flask_smorest import Blueprint
from sqlalchemy import func

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
        """Returns advertiser's total ad spend over the last 30 days"""
        max_ts = db.session.query(func.max(EventModel.Timestamp)).scalar()
        if not max_ts:
            return {"AdvertiserID": advertiser_id, "TotalSpend": 0.0}

        start_ts = max_ts - timedelta(days=30)

        total_spend = db.session.query(
            AdvertiserModel.AdvertiserID,
            func.sum(EventModel.AdCost).label("TotalSpend")
        ).join(CampaignModel, AdvertiserModel.AdvertiserID == CampaignModel.AdvertiserID) \
            .join(EventModel, CampaignModel.CampaignID == EventModel.CampaignID) \
            .filter(
            AdvertiserModel.AdvertiserID == advertiser_id,
            EventModel.Timestamp.between(start_ts, max_ts)
        ).group_by(AdvertiserModel.AdvertiserID) \
            .first()

        if total_spend:
            return {
                "AdvertiserID": total_spend.AdvertiserID,
                "TotalSpend": float(total_spend.TotalSpend or 0.0)
            }
        else:
            return {
                "AdvertiserID": advertiser_id,
                "TotalSpend": 0.0
            }
