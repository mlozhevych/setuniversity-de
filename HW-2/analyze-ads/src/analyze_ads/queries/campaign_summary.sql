-- campaign_summary.sql
WITH MaxDate AS (
    SELECT MAX(Timestamp) AS max_ts FROM Events
),
CampaignMetrics AS (
    SELECT
        c.CampaignID,
        c.CampaignName,
        c.Budget,
        c.RemainingBudget,
        COUNT(DISTINCT e.EventID) AS Impressions,
        SUM(e.AdCost) AS TotalCost,
        COUNT(DISTINCT cl.EventID) AS Clicks,
        SUM(cl.AdRevenue) AS TotalRevenue
    FROM Campaigns c
    JOIN Events e
        ON c.CampaignID = e.CampaignID
    LEFT JOIN Clicks cl
        ON e.EventID = cl.EventID
    JOIN MaxDate m
        ON e.Timestamp BETWEEN m.max_ts - INTERVAL 30 DAY AND m.max_ts
    GROUP BY c.CampaignID, c.CampaignName, c.Budget, c.RemainingBudget
)
SELECT
    CampaignID,
    CampaignName,
    Impressions,
    Clicks,
    TotalCost,
    TotalRevenue,
    Budget,
    (Budget - RemainingBudget) AS BudgetSpent,
    ROUND(100 * (Budget - RemainingBudget) / NULLIF(Budget, 0), 2) AS BudgetConsumptionPercentage,
    ROUND(100 * Clicks / NULLIF(Impressions, 0), 4) AS CTR,
    ROUND(TotalCost / NULLIF(Clicks, 0), 2) AS CPC,
    ROUND(TotalCost / NULLIF(Impressions, 0) * 1000, 2) AS CPM,
    ROUND(TotalRevenue / NULLIF(TotalCost, 0), 2) AS ROI
FROM CampaignMetrics
ORDER BY ROI DESC;