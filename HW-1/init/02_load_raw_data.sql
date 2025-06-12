-- SET GLOBAL local_infile = 1;
--
CREATE DATABASE IF NOT EXISTS AdTech;

-- RawCampaigns Table
DROP TABLE IF EXISTS `AdTech`.`RawCampaigns`;

-- Створення таблиці RawCampaigns
CREATE TABLE `AdTech`.`RawCampaigns` (
    `CampaignID`        BIGINT NOT NULL,
    `AdvertiserName`    VARCHAR(255)  NOT NULL,
    `CampaignName`      VARCHAR(255)  NOT NULL,
    `CampaignStartDate` DATE          NOT NULL,
    `CampaignEndDate`   DATE          NOT NULL,
    `TargetingCriteria` TEXT          NULL,          
    `AdSlotSize`        VARCHAR(50)   NOT NULL,      
    `Budget`            DECIMAL(14,2) NOT NULL,      
    `RemainingBudget`   DECIMAL(14,2) NOT NULL
);

-- На той випадок якщо на сервері заборонено завантаження (LOAD DATA LOCAL INFILE) файлів (під root).
-- SHOW GLOBAL VARIABLES LIKE 'local_infile'; -- Перевірка
-- SET GLOBAL local_infile = 1;
-- https://drive.google.com/file/d/1I54DEoDfPohEPbLmHWWW-iED8MsdFeId/view
-- Load Campaign data
-- LOAD DATA INFILE '/docker-entrypoint-initdb.d/data/campaigns.csv' -- '/Users/user/Downloads/campaigns.csv'
LOAD DATA INFILE '/var/lib/mysql-files/campaigns.csv'
INTO TABLE `AdTech`.`RawCampaigns`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','  OPTIONALLY ENCLOSED BY '"'
LINES  TERMINATED BY '\n'
IGNORE 1 ROWS
(CampaignID, AdvertiserName, CampaignName, CampaignStartDate, CampaignEndDate,
 TargetingCriteria, AdSlotSize, Budget, RemainingBudget);

-- https://drive.google.com/file/d/18gEty-UqAd0UkuVwL4V2rdTCDaBK8406/view
-- RawUsers Table
DROP TABLE IF EXISTS `AdTech`.`RawUsers`;

-- Створення таблиці RawUsers
CREATE TABLE `AdTech`.`RawUsers` (
    `UserID`      BIGINT NOT NULL,
    `Age`         INT,
    `Gender`      VARCHAR(50)       NOT NULL,
    `Location`    VARCHAR(255)      NOT NULL,
    `Interests`   TEXT              NULL,   
    `SignupDate`  DATETIME          NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Load Users data
LOAD DATA INFILE '/var/lib/mysql-files/users.csv'
INTO TABLE `AdTech`.`RawUsers`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES  TERMINATED BY '\n'
IGNORE 1 ROWS
(UserID, Age, Gender, Location, Interests, SignupDate);

-- RawEvents Table
DROP TABLE IF EXISTS `AdTech`.`RawEvents`;

CREATE TABLE `AdTech`.`RawEvents` (
    `EventID`                    CHAR(36)         NOT NULL,
    `AdvertiserName`             VARCHAR(255)     NOT NULL,
    `CampaignName`               VARCHAR(255)     NOT NULL,
    `CampaignStartDate`          DATE             NOT NULL,
    `CampaignEndDate`            DATE             NOT NULL,
    `CampaignTargetingCriteria`  TEXT             NULL,        
    `CampaignTargetingInterest`  VARCHAR(255)     NULL,
    `CampaignTargetingCountry`   VARCHAR(100)     NULL,    
    `AdSlotSize`                 VARCHAR(50)      NOT NULL,            
    `UserID`                     BIGINT UNSIGNED  NOT NULL,
    `Device`                     VARCHAR(100)     NOT NULL,    
    `Location`                   VARCHAR(255)     NULL,    
    `Timestamp`                  DATETIME         NOT NULL,
    `BidAmount`                  DECIMAL(12,4)    NOT NULL,
    `AdCost`                     DECIMAL(12,4)    NOT NULL,
    `WasClicked`                 VARCHAR(5)       NOT NULL,
    `ClickTimestamp`             VARCHAR(50)      NULL,
    `AdRevenue`                  DECIMAL(12,4)    NULL,        
    `Budget`                     DECIMAL(14,2)    NOT NULL,
    `RemainingBudget`            DECIMAL(14,2)    NOT NULL
);

-- https://drive.google.com/file/d/1B7GmvhSeLA8rot3-_0mBCE1bxhyFZ65L/view
-- Load Events data
LOAD DATA INFILE '/var/lib/mysql-files/events.csv'
INTO TABLE `AdTech`.`RawEvents`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES  TERMINATED BY '\n'
IGNORE 1 ROWS
(EventID, AdvertiserName, CampaignName, CampaignStartDate, CampaignEndDate,
 CampaignTargetingCriteria, CampaignTargetingInterest, CampaignTargetingCountry,
 AdSlotSize, UserID, Device, Location, Timestamp,
 BidAmount, AdCost, WasClicked, ClickTimestamp, AdRevenue,
 Budget, RemainingBudget);

-- RawCampaigns indexes 
-- Індекс для швидкого пошуку по CampaignName (JOIN з Campaigns)
CREATE INDEX idx_campaign_name ON `AdTech`.`RawCampaigns` (CampaignName);
-- Індекс для пошуку/фільтрації по AdvertiserName
CREATE INDEX idx_advertiser_name ON `AdTech`.`RawCampaigns` (AdvertiserName);
-- Унікальний індекс 
CREATE UNIQUE INDEX idx_unique_campaign_id ON `AdTech`.`RawCampaigns` (CampaignID);

-- RawUsers indexes 
-- Унікальний або первинний ключ
CREATE UNIQUE INDEX idx_user_id ON `AdTech`.`RawUsers` (UserID);
-- Індекс по Location
CREATE INDEX idx_location ON `AdTech`.`RawUsers` (Location);

-- RawEvents indexes 
-- Унікальний індекс по EventID 
CREATE UNIQUE INDEX idx_event_id ON `AdTech`.`RawEvents` (EventID);
-- Індекс для зв'язку з Campaigns або фільтрації по кампаніях
CREATE INDEX idx_campaign_name ON `AdTech`.`RawEvents` (CampaignName);
-- Індекс по Device
CREATE INDEX idx_device ON `AdTech`.`RawEvents` (Device);
-- Індекс по Location
CREATE INDEX idx_location ON `AdTech`.`RawEvents` (Location);
