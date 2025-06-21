-- device_summary.sql
WITH MaxDate AS (
    SELECT MAX(Timestamp) AS max_ts FROM Events
)
SELECT
    dt.DeviceName,
    COUNT(DISTINCT e.EventID) AS Impressions,
    COUNT(DISTINCT c.EventID) AS Clicks,
    ROUND(100 * COUNT(DISTINCT c.EventID) / COUNT(DISTINCT e.EventID), 4) AS CTR
FROM Events e
JOIN DeviceTypes dt
    ON e.DeviceTypeID = dt.DeviceTypeID
LEFT JOIN Clicks c
    ON e.EventID = c.EventID
JOIN MaxDate m
    ON e.Timestamp BETWEEN m.max_ts - INTERVAL 30 DAY AND m.max_ts
GROUP BY dt.DeviceName
ORDER BY CTR DESC;