WITH MaxDate AS (
    SELECT MAX(Timestamp) AS max_ts FROM Events
),
AdvertiserSpend AS (
    SELECT
        a.AdvertiserID,
        SUM(e.AdCost) AS TotalSpend
    FROM Advertisers a
    JOIN Campaigns c ON a.AdvertiserID = c.AdvertiserID
    JOIN Events e ON c.CampaignID = e.CampaignID
    JOIN MaxDate m ON e.Timestamp BETWEEN m.max_ts - INTERVAL 30 DAY AND m.max_ts
--     WHERE a.AdvertiserID = :advertiser_id
    GROUP BY a.AdvertiserID
)
SELECT AdvertiserID, TotalSpend
FROM AdvertiserSpend;
