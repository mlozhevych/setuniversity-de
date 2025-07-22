from analyze_ads_rest_api.db import db


class EventModel(db.Model):
    __tablename__ = 'Events'

    EventID = db.Column(db.String(36), primary_key=True)
    CampaignID = db.Column(db.String(36), db.ForeignKey('Campaigns.CampaignID'), nullable=False)
    UserID = db.Column(db.Integer, db.ForeignKey('Users.UserID'), nullable=False)
    Timestamp = db.Column(db.DateTime, nullable=False)
    AdCost = db.Column(db.Float)

    campaign = db.relationship("CampaignModel", back_populates="events")
    click = db.relationship("ClickModel", backref="event", uselist=False)
