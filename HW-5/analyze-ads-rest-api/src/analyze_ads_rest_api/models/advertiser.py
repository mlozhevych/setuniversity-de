from analyze_ads_rest_api.db import db


class AdvertiserModel(db.Model):
    __tablename__ = "Advertisers"

    AdvertiserID = db.Column(db.Integer, primary_key=True)
    AdvertiserName = db.Column(db.String(255), nullable=False)

    campaigns = db.relationship("CampaignModel", back_populates="advertiser")
