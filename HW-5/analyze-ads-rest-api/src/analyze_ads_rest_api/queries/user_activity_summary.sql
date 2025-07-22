WITH MaxDate AS (
    SELECT MAX(Timestamp) AS max_ts FROM Events
)
SELECT
    u.UserID,
    COUNT(c.EventID) AS TotalClicks,
    SUM(c.AdRevenue) AS TotalRevenueGenerated
FROM Clicks c
JOIN Events e ON c.EventID = e.EventID
JOIN Users u ON e.UserID = u.UserID
JOIN MaxDate m ON e.Timestamp BETWEEN m.max_ts - INTERVAL 30 DAY AND m.max_ts
WHERE u.UserID = :user_id
GROUP BY u.UserID;
