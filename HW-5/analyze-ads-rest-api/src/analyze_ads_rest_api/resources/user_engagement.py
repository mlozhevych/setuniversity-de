from datetime import timedelta

from flask.views import MethodView
from flask_smorest import Blueprint
from sqlalchemy import func

from analyze_ads_rest_api.db import db
from analyze_ads_rest_api.models.click import ClickModel
from analyze_ads_rest_api.models.event import EventModel
from analyze_ads_rest_api.models.user import UserModel
from analyze_ads_rest_api.schemas import UserEngagementSchema

blp = Blueprint("UserEngagement", __name__, description="User Engagement Endpoints")


@blp.route("/user/<int:user_id>/engagements")
class UserEngagementResource(MethodView):

    @blp.response(200, UserEngagementSchema)
    def get(self, user_id):
        """
        Returns ads a user engaged with (clicked).
        No caching is applied to this endpoint as per the requirements.
        """
        max_ts = db.session.query(func.max(EventModel.Timestamp)).scalar()
        if not max_ts:
            return {"UserID": user_id, "TotalClicks": 0, "TotalRevenueGenerated": 0.0}

        start_ts = max_ts - timedelta(days=30)

        result = db.session.query(
            UserModel.UserID,
            func.count(ClickModel.EventID).label("TotalClicks"),
            func.sum(ClickModel.AdRevenue).label("TotalRevenueGenerated")
        ).join(EventModel, EventModel.UserID == UserModel.UserID) \
            .join(ClickModel, ClickModel.EventID == EventModel.EventID) \
            .filter(
            UserModel.UserID == user_id,
            EventModel.Timestamp.between(start_ts, max_ts)
        ).group_by(UserModel.UserID) \
            .first()

        if result:
            return {
                "UserID": result.UserID,
                "TotalClicks": result.TotalClicks or 0,
                "TotalRevenueGenerated": float(result.TotalRevenueGenerated or 0.0)
            }
        else:
            return {
                "UserID": user_id,
                "TotalClicks": 0,
                "TotalRevenueGenerated": 0.0
            }
