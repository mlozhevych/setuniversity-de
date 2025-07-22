from analyze_ads_rest_api.db import db


class UserModel(db.Model):
    __tablename__ = "Users"

    UserID = db.Column(db.Integer, primary_key=True)
    Age = db.Column(db.Integer)
    Gender = db.Column(db.String(10))
    LocationID = db.Column(db.Integer, db.ForeignKey('Locations.LocationID'))

    events = db.relationship("EventModel", backref="user")
