from analyze_ads_rest_api.db import db


class ClickModel(db.Model):
    __tablename__ = 'Clicks'

    EventID = db.Column(db.Integer, db.ForeignKey('Events.EventID'), primary_key=True)
    ClickTimestamp = db.Column(db.DateTime)
    AdRevenue = db.Column(db.Float)
