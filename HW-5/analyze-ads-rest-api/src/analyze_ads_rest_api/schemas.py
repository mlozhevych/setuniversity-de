from marshmallow import Schema, fields


class CampaignPerformanceSchema(Schema):
    CampaignID = fields.Str()
    CampaignName = fields.Str()
    Impressions = fields.Int()
    Clicks = fields.Int()
    TotalCost = fields.Float()
    CTR = fields.Float()


class AdvertiserSpendingSchema(Schema):
    AdvertiserID = fields.Integer(required=True)
    TotalSpend = fields.Float(allow_none=True)


class UserEngagementSchema(Schema):
    UserID = fields.Integer(required=True)
    TotalClicks = fields.Integer()
    TotalRevenueGenerated = fields.Float()
