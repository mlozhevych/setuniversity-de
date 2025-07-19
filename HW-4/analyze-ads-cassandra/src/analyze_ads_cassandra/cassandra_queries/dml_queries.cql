USE AdTech;

-- 1) Отримує щоденну статистику (покази, кліки, CTR) для заданої кампанії.
-- Запит ефективний, оскільки він звертається до вже агрегованих даних.

-- Замініть `101` на ID потрібної кампанії.
SELECT
    event_date,
    impressions,
    clicks,
    ctr
FROM
    adtech.campaign_daily_metrics
WHERE
    campaign_id = 101
ORDER BY
    event_date DESC;

-- 2) Отримує топ-5 рекламодавців за загальними витратами.
-- Запит працює миттєво, оскільки дані вже агреговані та відсортовані в таблиці.
SELECT
    advertiser_name,
    total_spend
FROM
    adtech.top_advertisers_by_spend
WHERE
    time_bucket = 'last_30_days_historical'
LIMIT 5;

-- 3) Отримує останні 10 рекламних оголошень, які бачив користувач.
-- Запит використовує `LIMIT 10` для отримання останніх подій,
-- оскільки дані вже відсортовані за часом у зворотному порядку.

-- Замініть `302602` на реальний UserID
SELECT
    event_time,
    campaign_name,
    advertiser_name,
    was_clicked
FROM
    adtech.user_engagement_history
WHERE
    user_id = 302602
LIMIT 10;


-- Отримує топ-10 найактивніших користувачів за кількістю кліків.
-- Запит використовує `LIMIT 10` для отримання перших записів,
-- оскільки дані вже відсортовані за спаданням кількості кліків.

SELECT
    user_id,
    total_clicks
FROM
    adtech.top_users_by_clicks
WHERE
    time_bucket = 'last_30_days_historical'
LIMIT 10;


-- Отримує топ-5 рекламодавців за витратами в конкретному регіоні.
-- Запит використовує `LIMIT 5` для отримання перших записів,
-- оскільки дані вже відсортовані за спаданням витрат.

-- Замініть 'USA' на потрібний регіон (наприклад, 'Canada', 'Germany').
SELECT
    advertiser_name,
    total_spend
FROM
    adtech.top_advertisers_by_region
WHERE
    region = 'USA'
LIMIT 5;
