-- advertiser_summary.sql
WITH MaxDate AS (
    SELECT MAX(Timestamp) AS max_ts FROM Events
),
AdvertiserMetrics AS (
    SELECT
        a.AdvertiserID,
        a.AdvertiserName,
        COUNT(DISTINCT e.EventID) AS TotalImpressions,
        SUM(e.AdCost) AS TotalSpend,
        COUNT(DISTINCT cl.EventID) AS TotalClicks
    FROM Advertisers a
    JOIN Campaigns c
        ON a.AdvertiserID = c.AdvertiserID
    JOIN Events e
        ON c.CampaignID = e.CampaignID
    LEFT JOIN Clicks cl
        ON e.EventID = cl.EventID
    JOIN MaxDate m
        ON e.Timestamp BETWEEN m.max_ts - INTERVAL 30 DAY AND m.max_ts
    GROUP BY a.AdvertiserID, a.AdvertiserName
)
SELECT
    AdvertiserName,
    TotalSpend,
    TotalClicks,
    TotalImpressions,
    ROUND(100 * TotalClicks / NULLIF(TotalImpressions, 0), 4) AS CTR
FROM AdvertiserMetrics
ORDER BY TotalSpend DESC;