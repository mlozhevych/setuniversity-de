WITH MaxDate AS (
    SELECT MAX(Timestamp) AS max_ts FROM Events
),
CampaignMetrics AS (
    SELECT
        c.CampaignID,
        c.CampaignName,
        COUNT(DISTINCT e.EventID) AS Impressions,
        SUM(e.AdCost) AS TotalCost,
        COUNT(DISTINCT cl.EventID) AS Clicks
    FROM Campaigns c
    JOIN Events e
        ON c.CampaignID = e.CampaignID
    LEFT JOIN Clicks cl
        ON e.EventID = cl.EventID
    JOIN MaxDate m
        ON e.Timestamp BETWEEN m.max_ts - INTERVAL 30 DAY AND m.max_ts
    GROUP BY c.CampaignID, c.CampaignName
)
SELECT
    CampaignID,
    CampaignName,
    Impressions,
    Clicks,
    TotalCost,
    ROUND(100 * Clicks / NULLIF(Impressions, 0), 4) AS CTR
FROM CampaignMetrics
ORDER BY CTR DESC;
