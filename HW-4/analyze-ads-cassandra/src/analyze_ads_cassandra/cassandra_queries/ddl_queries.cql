CREATE KEYSPACE AdTech
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE AdTech;

--
-- 1) Створює денормалізовану таблицю `campaign_daily_metrics`
-- для зберігання щоденної агрегації показників по кампаніях.

-- Ключ партиціонування: campaign_id
--    - Усі дані для однієї кампанії групуються разом.
-- Ключ кластеризації: event_date
--    - Дані всередині партиції сортуються за датою у зворотному порядку,
--      що дозволяє легко отримувати останні показники.

CREATE TABLE IF NOT EXISTS adtech.campaign_daily_metrics (
    campaign_id int,
    event_date date,
    impressions bigint,
    clicks bigint,
    ctr double,
    PRIMARY KEY (campaign_id, event_date)
) WITH CLUSTERING ORDER BY (event_date DESC);


-- 2) Створює таблицю `top_advertisers_by_spend` для зберігання агрегованих даних
-- про витрати рекламодавців. Таблиця оптимізована для запиту "топ-N".

-- Ключ партиціонування: time_bucket
--    - Статичне значення (наприклад, 'last_30_days'), щоб усі дані були в одній партиції.
--    - Це дозволяє Cassandra сортувати всіх рекламодавців разом.
-- Ключі кластеризації: total_spend, advertiser_name
--    - `total_spend` сортується за спаданням, тому найдорожчі рекламодавці будуть першими.
--    - `advertiser_name` додано для унікальності первинного ключа.

CREATE TABLE IF NOT EXISTS adtech.top_advertisers_by_spend (
    time_bucket text,
    total_spend decimal,
    advertiser_name text,
    PRIMARY KEY (time_bucket, total_spend, advertiser_name)
) WITH CLUSTERING ORDER BY (total_spend DESC);


-- 3) Створює таблицю `user_engagement_history` для зберігання історії
-- взаємодії користувачів з рекламою.

-- Ключ партиціонування: user_id
--    - Усі події для одного користувача зберігаються в одній партиції,
--      що забезпечує швидкий доступ.
-- Ключ кластеризації: event_time
--    - Події всередині партиції сортуються за часом у зворотному порядку.
--    - Це дозволяє миттєво отримувати останні події за допомогою LIMIT.

CREATE TABLE IF NOT EXISTS adtech.user_engagement_history (
    user_id int,
    event_time timestamp,
    campaign_name text,
    advertiser_name text,
    was_clicked boolean,
    PRIMARY KEY (user_id, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);


-- Створює таблицю `top_users_by_clicks` для зберігання агрегованих даних
-- про кліки користувачів. Таблиця оптимізована для запиту "топ-N".

-- Ключ партиціонування: time_bucket
--    - Статичне значення, щоб усі дані були в одній партиції для глобального сортування.
-- Ключі кластеризації: total_clicks, user_id
--    - `total_clicks` сортується за спаданням, тому найактивніші користувачі будуть першими.
--    - `user_id` додано для забезпечення унікальності первинного ключа.

CREATE TABLE IF NOT EXISTS adtech.top_users_by_clicks (
    time_bucket text,
    total_clicks int,
    user_id int,
    PRIMARY KEY (time_bucket, total_clicks, user_id)
) WITH CLUSTERING ORDER BY (total_clicks DESC);

-- Створює таблицю `top_advertisers_by_region` для зберігання агрегованих
-- даних про витрати рекламодавців за регіонами.

-- Ключ партиціонування: region
--    - Усі дані для одного регіону групуються разом, що дозволяє
--      ефективно фільтрувати за регіоном.
-- Ключі кластеризації: total_spend, advertiser_name
--    - `total_spend` сортується за спаданням, щоб рекламодавці з
--      найбільшими витратами були першими.
--    - `advertiser_name` додано для унікальності первинного ключа.

CREATE TABLE IF NOT EXISTS adtech.top_advertisers_by_region (
    region text,
    total_spend decimal,
    advertiser_name text,
    PRIMARY KEY (region, total_spend, advertiser_name)
) WITH CLUSTERING ORDER BY (total_spend DESC);
