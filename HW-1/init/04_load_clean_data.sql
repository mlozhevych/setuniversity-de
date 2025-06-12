-- Крок 0: Підготовка
-- Тимчасово вимикаємо перевірку зовнішніх ключів для можливості завантаження даних у таблиці
-- у потрібному нам порядку, а не в порядку залежностей.
SET FOREIGN_KEY_CHECKS = 0;

/*
--------------------------------------------------------------------
Крок 1: Заповнення довідкових таблиць (Lookup/Dimension Tables)
--------------------------------------------------------------------
Ми вибираємо унікальні значення з сирих таблиць і вставляємо їх у
відповідні нормалізовані довідники. `INSERT IGNORE` використовується
для уникнення помилок при спробі вставити дублікати.
*/

-- 1.1. Заповнення `Advertisers`
-- Вибираємо унікальні імена рекламодавців з `RawCampaigns`.
INSERT IGNORE INTO `AdTech`.`Advertisers` (AdvertiserName)
SELECT DISTINCT AdvertiserName FROM `AdTech`.`RawCampaigns`;

-- 1.2. Заповнення `DeviceTypes`
-- Вибираємо унікальні типи пристроїв з `RawEvents`.
INSERT IGNORE INTO `AdTech`.`DeviceTypes` (DeviceName)
SELECT DISTINCT Device FROM `AdTech`.`RawEvents`;

-- 1.3. Заповнення `Locations`
-- Збираємо унікальні локації з таблиць користувачів та подій, оскільки вони можуть містити різні дані.
INSERT IGNORE INTO `AdTech`.`Locations` (CountryName)
SELECT DISTINCT Location FROM `AdTech`.`RawUsers` WHERE Location IS NOT NULL AND Location != ''
UNION
SELECT DISTINCT Location FROM `AdTech`.`RawEvents` WHERE Location IS NOT NULL AND Location != '';

-- 1.4. Заповнення `AdSlotSizes`
-- Розбираємо рядок 'WidthxHeight' на два окремих числових стовпці.
-- `SUBSTRING_INDEX` ідеально підходить для цієї задачі.
INSERT IGNORE INTO `AdTech`.`AdSlotSizes` (Width, Height)
SELECT DISTINCT
  CAST(SUBSTRING_INDEX(AdSlotSize, 'x', 1) AS UNSIGNED),
  CAST(SUBSTRING_INDEX(AdSlotSize, 'x', -1) AS UNSIGNED)
FROM `AdTech`.`RawCampaigns`;

-- 1.5. Заповнення `Interests`
-- Ми розбиваємо рядок з інтересами (напр., 'Gaming,Sports,Health') на окремі записи.
-- Для цього використовується рекурсивний запит (CTE), який "відрізає" по одному інтересу за ітерацію.
INSERT IGNORE INTO `AdTech`.`Interests` (InterestName)
WITH RECURSIVE InterestCTE AS (
  SELECT
    UserID,
    TRIM(SUBSTRING_INDEX(Interests, ',', 1)) AS InterestName,
    IF(LOCATE(',', Interests) > 0, SUBSTRING(Interests, LOCATE(',', Interests) + 1), NULL) AS RemainingInterests
  FROM `AdTech`.`RawUsers`
  WHERE Interests IS NOT NULL AND Interests != ''
  UNION ALL
  SELECT
    UserID,
    TRIM(SUBSTRING_INDEX(RemainingInterests, ',', 1)),
    IF(LOCATE(',', RemainingInterests) > 0, SUBSTRING(RemainingInterests, LOCATE(',', RemainingInterests) + 1), NULL)
  FROM InterestCTE
  WHERE RemainingInterests IS NOT NULL
)
SELECT DISTINCT InterestName FROM InterestCTE;

/*
--------------------------------------------------------------------
Крок 2: Заповнення таблиць основних сутностей (Users, Campaigns)
--------------------------------------------------------------------
Тепер, коли довідники заповнені, ми можемо заповнювати основні таблиці,
приєднуючи довідники для отримання їх `ID` (зовнішніх ключів).
*/

-- 2.1. Заповнення `Users`
-- Вставляємо користувачів, приєднуючи таблицю `Locations`, щоб отримати `LocationID`.
INSERT IGNORE INTO `AdTech`.`Users` (UserID, Age, Gender, LocationID, SignupDate)
SELECT
  raw_u.UserID,
  raw_u.Age,
  raw_u.Gender,
  l.LocationID,
  raw_u.SignupDate
FROM `AdTech`.`RawUsers` raw_u
LEFT JOIN `AdTech`.`Locations` l ON raw_u.Location = l.CountryName;

-- 2.2. Заповнення `Campaigns`
-- Генеруємо новий UUID для `CampaignID`, оскільки в сирих даних його немає в потрібному форматі.
-- Приєднуємо `Advertisers` та `AdSlotSizes`, щоб отримати відповідні `ID`.
INSERT IGNORE INTO `AdTech`.`Campaigns` (CampaignID, AdvertiserID, CampaignName, StartDate, EndDate, TargetingCriteria, AdSlotSizeID, Budget, RemainingBudget)
SELECT
    UUID(), -- Генеруємо новий унікальний ID для кампанії
    adv.AdvertiserID,
    rc.CampaignName,
    rc.CampaignStartDate,
    rc.CampaignEndDate,
    rc.TargetingCriteria,
    sz.AdSlotSizeID,
    rc.Budget,
    rc.RemainingBudget
FROM `AdTech`.`RawCampaigns` rc
JOIN `AdTech`.`Advertisers` adv ON rc.AdvertiserName = adv.AdvertiserName
JOIN `AdTech`.`AdSlotSizes` sz ON
    CAST(SUBSTRING_INDEX(rc.AdSlotSize, 'x', 1) AS UNSIGNED) = sz.Width AND
    CAST(SUBSTRING_INDEX(rc.AdSlotSize, 'x', -1) AS UNSIGNED) = sz.Height;

/*
--------------------------------------------------------------------
Крок 3: Заповнення зв'язуючої таблиці `UserInterests` (M:N)
--------------------------------------------------------------------
Використовуємо той самий рекурсивний підхід, що й для заповнення `Interests`,
але тепер ми вставляємо пари (UserID, InterestID).
*/
INSERT IGNORE INTO `AdTech`.`UserInterests` (UserID, InterestID)
WITH RECURSIVE InterestCTE AS (
  SELECT
    UserID,
    TRIM(SUBSTRING_INDEX(Interests, ',', 1)) AS InterestName,
    IF(LOCATE(',', Interests) > 0, SUBSTRING(Interests, LOCATE(',', Interests) + 1), NULL) AS RemainingInterests
  FROM `AdTech`.`RawUsers`
  WHERE Interests IS NOT NULL AND Interests != ''
  UNION ALL
  SELECT
    UserID,
    TRIM(SUBSTRING_INDEX(RemainingInterests, ',', 1)),
    IF(LOCATE(',', RemainingInterests) > 0, SUBSTRING(RemainingInterests, LOCATE(',', RemainingInterests) + 1), NULL)
  FROM InterestCTE
  WHERE RemainingInterests IS NOT NULL
)
SELECT
  cte.UserID,
  i.InterestID
FROM InterestCTE cte
JOIN `AdTech`.`Interests` i ON cte.InterestName = i.InterestName;


/*
--------------------------------------------------------------------
Крок 4: Заповнення таблиці фактів `Events` та `Clicks`
--------------------------------------------------------------------
Це фінальний етап завантаження, де ми збираємо всі ID з довідників
та основних таблиць, щоб сформувати один запис про подію.
*/

-- 4.0 Одноразово пишемо числові ключі у RawEvents
ALTER TABLE `AdTech`.`RawEvents`
  ADD COLUMN DeviceTypeID INT,
  ADD COLUMN LocationID   INT;

UPDATE `AdTech`.`RawEvents` re
JOIN `AdTech`.`DeviceTypes` dt
    ON re.Device = dt.DeviceName
    SET re.DeviceTypeID = dt.DeviceTypeID;

UPDATE `AdTech`.`RawEvents` re
JOIN `AdTech`.`Locations` l
    ON re.Location = l.CountryName
    SET re.LocationID = l.LocationID;

-- Вставляємо без «дорогих» JOIN-ів
ALTER TABLE Events DISABLE KEYS;

-- Створюємо процедуру для пакетного завантаження даних в Events
DROP PROCEDURE IF EXISTS `AdTech`.`LoadEventsInBatches`;

DELIMITER $$
CREATE PROCEDURE `AdTech`.`LoadEventsInBatches`(IN batch_size INT)
BEGIN
    DECLARE offset_val INT DEFAULT 0;
    DECLARE rows_affected INT DEFAULT 0;

    -- Виконуємо цикл, доки вставка не перестане додавати нові рядки
    batch_loop: LOOP
        -- Вставляємо наступну порцію даних
        INSERT IGNORE INTO `AdTech`.`Events`
            (EventID, CampaignID, UserID, DeviceTypeID, LocationID, Timestamp, BidAmount, AdCost)
        SELECT
            re.EventID,
            c.CampaignID,
            re.UserID,
            re.DeviceTypeID,
            re.LocationID,
            re.Timestamp,
            re.BidAmount,
            re.AdCost
FROM `AdTech`.`RawEvents` re
JOIN `AdTech`.`Campaigns` c ON re.CampaignName = c.CampaignName
        ORDER BY re.EventID
        LIMIT batch_size OFFSET offset_val;

        -- Отримуємо кількість фактично вставлених рядків
        SET rows_affected = ROW_COUNT();

        -- Якщо рядків більше не було вставлено, виходимо з циклу
        IF rows_affected = 0 THEN
            LEAVE batch_loop;
        END IF;

        -- Збільшуємо зміщення для наступної ітерації
        SET offset_val = offset_val + batch_size;

        -- Повідомлення про прогрес (опціонально)
        SELECT CONCAT('Імпортовано рядків: ', offset_val) AS Progress;

    END LOOP;

    SELECT 'Завантаження в Events завершено.' AS Status;
END$$
DELIMITER ;

-- 4.1. Заповнення `Events`
-- Ми приєднуємо `RawEvents` до всіх необхідних таблиць (`Campaigns`, `DeviceTypes`, `Locations`),
-- щоб отримати правильні зовнішні ключі.
CALL `AdTech`.`LoadEventsInBatches`(50000);
-- INSERT IGNORE INTO `AdTech`.`Events` (EventID, CampaignID, UserID, DeviceTypeID, LocationID, Timestamp, BidAmount, AdCost)
-- SELECT
--     re.EventID,
--     c.CampaignID,
--     re.UserID,
--     re.DeviceTypeID,
--     re.LocationID,
--     re.Timestamp,
--     re.BidAmount,
--     re.AdCost
-- FROM `AdTech`.`RawEvents` re
-- JOIN `AdTech`.`Campaigns` c ON re.CampaignName = c.CampaignName;
-- LEFT JOIN `AdTech`.`DeviceTypes` dt ON re.Device = dt.DeviceName
-- LEFT JOIN `AdTech`.`Locations` l ON re.Location = l.CountryName;

ALTER TABLE Events ENABLE KEYS;

-- 4.2. Заповнення `Clicks`
-- Вибираємо лише ті події, де був клік.
-- Перетворюємо текстовий формат дати кліку на стандартний `DATETIME` за допомогою `STR_TO_DATE`.
INSERT IGNORE INTO `AdTech`.`Clicks` (EventID, ClickTimestamp, AdRevenue)
SELECT
    EventID,
    -- Формат '%Y-%m-%dT%H:%i:%s' відповідає рядку типу '2024-11-13T03:01:37'
    STR_TO_DATE(ClickTimestamp, '%Y-%m-%dT%H:%i:%s'),
    AdRevenue
FROM `AdTech`.`RawEvents`
WHERE WasClicked = 'True' AND ClickTimestamp IS NOT NULL AND ClickTimestamp != '';
