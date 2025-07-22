from analyze_ads_rest_api.db import db


class CampaignModel(db.Model):
    __tablename__ = 'Campaigns'

    CampaignID = db.Column(db.String(36), primary_key=True)
    CampaignName = db.Column(db.String, nullable=False)
    Budget = db.Column(db.Float)
    RemainingBudget = db.Column(db.Float)
    AdvertiserID = db.Column(db.Integer, db.ForeignKey('Advertisers.AdvertiserID'), nullable=False)

    events = db.relationship("EventModel", back_populates="campaign")
    advertiser = db.relationship("AdvertiserModel", back_populates="campaigns")
