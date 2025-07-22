from datetime import timedelta

from flask.views import MethodView
from flask_smorest import Blueprint
from sqlalchemy import func

from analyze_ads_rest_api.db import db
from analyze_ads_rest_api.models.campaign import CampaignModel
from analyze_ads_rest_api.models.click import ClickModel
from analyze_ads_rest_api.models.event import EventModel
from analyze_ads_rest_api.schemas import CampaignPerformanceSchema

blp = Blueprint("CampaignPerformance", __name__, description="Campaign Performance Endpoints")


@blp.route("/campaign/<string:campaign_id>/performance")
class CampaignPerformanceResource(MethodView):

    @blp.response(200, CampaignPerformanceSchema)
    def get(self, campaign_id):
        """Retrieve campaign performance by ID (last 30 days)"""
        max_ts = db.session.query(func.max(EventModel.Timestamp)).scalar()
        if not max_ts:
            return {"message": "No events found"}, 404

        start_range = max_ts - timedelta(days=30)

        # Subquery with filter
        events_query = db.session.query(
            EventModel.EventID,
            EventModel.AdCost,
            EventModel.CampaignID
        ).filter(
            EventModel.CampaignID == campaign_id,
            EventModel.Timestamp.between(start_range, max_ts)
        ).subquery()

        # Clicks subquery
        clicks_query = db.session.query(
            ClickModel.EventID.label('ClickedEventID')
        ).join(events_query, ClickModel.EventID == events_query.c.EventID).subquery()

        # Final aggregation
        result = db.session.query(
            CampaignModel.CampaignID,
            CampaignModel.CampaignName,
            func.count(events_query.c.EventID).label("Impressions"),
            func.count(clicks_query.c.ClickedEventID).label("Clicks"),
            func.sum(events_query.c.AdCost).label("TotalCost"),
        ).join(events_query, CampaignModel.CampaignID == events_query.c.CampaignID) \
            .outerjoin(clicks_query, events_query.c.EventID == clicks_query.c.ClickedEventID) \
            .group_by(CampaignModel.CampaignID, CampaignModel.CampaignName) \
            .first()

        if result is None:
            return {"message": "Campaign not found or no events in range"}, 404

        ctr = (result.Clicks / result.Impressions) * 100 if result.Impressions else 0

        return {
            "CampaignID": result.CampaignID,
            "CampaignName": result.CampaignName,
            "Impressions": result.Impressions,
            "Clicks": result.Clicks,
            "TotalCost": float(result.TotalCost or 0),
            "CTR": round(ctr, 4)
        }
