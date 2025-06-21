-- location_summary.sql
WITH MaxDate AS (
    SELECT MAX(Timestamp) AS max_ts FROM Events
)
SELECT
    l.CountryName,
    COUNT(c.EventID) AS Clicks,
    SUM(c.AdRevenue) AS TotalRevenue,
    ROUND(AVG(c.AdRevenue), 2) AS AvgRevenuePerClick
FROM Clicks c
JOIN Events e
    ON c.EventID = e.EventID
JOIN Locations l
    ON e.LocationID = l.LocationID
JOIN MaxDate m
    ON e.Timestamp BETWEEN m.max_ts - INTERVAL 30 DAY AND m.max_ts
GROUP BY l.CountryName
ORDER BY TotalRevenue DESC;