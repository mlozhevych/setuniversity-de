-- user_activity_summary.sql
WITH MaxDate AS (
    SELECT MAX(Timestamp) AS max_ts FROM Events
)
SELECT
    u.UserID,
    u.Age,
    u.Gender,
    l.CountryName,
    COUNT(c.EventID) AS TotalClicks,
    SUM(c.AdRevenue) AS TotalRevenueGenerated
FROM Clicks c
JOIN Events e
    ON c.EventID = e.EventID
JOIN Users u
    ON e.UserID = u.UserID
LEFT JOIN Locations l
    ON u.LocationID = l.LocationID
JOIN MaxDate m
    ON e.Timestamp BETWEEN m.max_ts - INTERVAL 30 DAY AND m.max_ts
GROUP BY u.UserID, u.Age, u.Gender, l.CountryName
ORDER BY TotalClicks DESC
LIMIT 10;