
-- 1. Drop tables with foreign keys first
DROP TABLE IF EXISTS `AdTech`.`Clicks`;
DROP TABLE IF EXISTS `AdTech`.`UserInterests`;
DROP TABLE IF EXISTS `AdTech`.`Events`;

-- 2. Then drop core tables
DROP TABLE IF EXISTS `AdTech`.`Users`;
DROP TABLE IF EXISTS `AdTech`.`Campaigns`;

-- 3. Finally, drop lookup/reference tables
DROP TABLE IF EXISTS `AdTech`.`Locations`;
DROP TABLE IF EXISTS `AdTech`.`Interests`;
DROP TABLE IF EXISTS `AdTech`.`DeviceTypes`;
DROP TABLE IF EXISTS `AdTech`.`Advertisers`;
DROP TABLE IF EXISTS `AdTech`.`AdSlotSizes`;

/* -----------------------------------------------------------------
   Факт-таблиця подій показу / кліку
----------------------------------------------------------------- */

-- Create table Events
CREATE TABLE `AdTech`.`Events` (
    `EventID` CHAR(36)      NOT NULL,               -- UUID
    `CampaignID` CHAR(36)   NOT NULL,
    `UserID`                BIGINT UNSIGNED,
    `DeviceTypeID`          TINYINT UNSIGNED,
    `LocationID`            INT UNSIGNED,
    `Timestamp`             DATETIME(3) NOT NULL,
    `BidAmount`             DECIMAL(10,4),
    `AdCost`                DECIMAL(10,4),
    PRIMARY KEY (`EventID`)
) ENGINE=InnoDB COMMENT='Подія показу (impression) + необов’язковий клік';

/* -----------------------------------------------------------------
   Основні сутності
----------------------------------------------------------------- */

-- Create table UserInterests
CREATE TABLE `AdTech`.`UserInterests` (
    `UserID`        BIGINT UNSIGNED NOT NULL,
    `InterestID`    INT UNSIGNED NOT NULL,
    PRIMARY KEY (`UserID`, `InterestID`)
) ENGINE=InnoDB COMMENT='M:N міст для інтересів';

-- Create table Users
CREATE TABLE `AdTech`.`Users` (
    `UserID`        BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `Age`           SMALLINT,
    `Gender`        VARCHAR(20),
    `LocationID`    INT UNSIGNED,
    `SignupDate`    DATE
) ENGINE=InnoDB COMMENT='Зареєстровані користувачі';

-- Create table Campaigns
CREATE TABLE `AdTech`.`Campaigns` (
    `CampaignID`        CHAR(36) NOT NULL,  -- UUID як текст
    `AdvertiserID`      INT UNSIGNED NOT NULL,
    `CampaignName`      VARCHAR(255) NOT NULL,
    `StartDate`         DATE,
    `EndDate`           DATE,
    `TargetingCriteria` TEXT,
    `AdSlotSizeID`      INT UNSIGNED,
    `Budget`            DECIMAL(12,2),
    `RemainingBudget`   DECIMAL(12,2),
    PRIMARY KEY (`CampaignID`)
) ENGINE=InnoDB COMMENT='Статичні дані кампаній';

-- Create table Clicks
CREATE TABLE `AdTech`.`Clicks` (
    `EventID` CHAR(36)  PRIMARY KEY,
    `ClickTimestamp`    DATETIME(6) NOT NULL,
    `AdRevenue`         DECIMAL(10, 4)
);

/* -----------------------------------------------------------------
   Базові (довідкові) таблиці
----------------------------------------------------------------- */

-- Create table Locations
CREATE TABLE `AdTech`.`Locations` (
    `LocationID`    INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `CountryName`   VARCHAR(100) NOT NULL,
    UNIQUE KEY (`LocationID`)
) ENGINE=InnoDB COMMENT='Країни';

-- Create table Interests
CREATE TABLE `AdTech`.`Interests` (
    `InterestID`    INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `InterestName`  VARCHAR(100) NOT NULL,
     UNIQUE KEY (`InterestName`)
) ENGINE=InnoDB COMMENT='Каталог інтересів';

-- Create table DeviceTypes
CREATE TABLE `AdTech`.`DeviceTypes` (
    `DeviceTypeID`  TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `DeviceName`    VARCHAR(20) NOT NULL,
    PRIMARY KEY (`DeviceTypeID`),
    UNIQUE KEY `uq_device` (`DeviceName`)
) ENGINE=InnoDB COMMENT='Mobile / Desktop / Tablet';

-- Create table Advertisers
CREATE TABLE `AdTech`.`Advertisers` (
    `AdvertiserID`      INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `AdvertiserName`    VARCHAR(255) NOT NULL,
    PRIMARY KEY (`AdvertiserID`),
    UNIQUE KEY `uq_advertiser_name` (`AdvertiserName`)
) ENGINE=InnoDB COMMENT='1 advertiser → n campaigns';

-- Create table AdSlotSizes
CREATE TABLE `AdTech`.`AdSlotSizes` (
    `AdSlotSizeID`  INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `Width`         SMALLINT UNSIGNED NOT NULL,
    `Height`        SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (`AdSlotSizeID`),
    UNIQUE KEY `uq_slot` (`Width`, `Height`)
) ENGINE=InnoDB COMMENT='Унікальні розміри слотів';


-- Keys
ALTER TABLE `AdTech`.`Users`
ADD CONSTRAINT `fk_user_location`
FOREIGN KEY (`LocationID`) REFERENCES `AdTech`.`Locations`(`LocationID`);

-- 
ALTER TABLE `AdTech`.`Campaigns`
ADD CONSTRAINT `fk_campaign_advertiser`
FOREIGN KEY (`AdvertiserID`) REFERENCES `AdTech`.`Advertisers`(`AdvertiserID`),
ADD CONSTRAINT `fk_campaign_slot`
FOREIGN KEY (`AdSlotSizeID`) REFERENCES `AdTech`.`AdSlotSizes`(`AdSlotSizeID`);
ALTER TABLE `AdTech`.`Campaigns` ADD INDEX (CampaignName);

-- 
ALTER TABLE `AdTech`.`Clicks`
ADD CONSTRAINT `fk_clicks_event`
FOREIGN KEY (`EventID`) REFERENCES `AdTech`.`Events`(`EventID`);

--
ALTER TABLE `AdTech`.`UserInterests`
ADD CONSTRAINT `fk_ui_user`
FOREIGN KEY (`UserID`) REFERENCES `AdTech`.`Users`(`UserID`);

ALTER TABLE `AdTech`.`UserInterests`
ADD CONSTRAINT `fk_ui_interest`
FOREIGN KEY (`InterestID`) REFERENCES `AdTech`.`Interests`(`InterestID`);

-- 
ALTER TABLE `AdTech`.`Events`
ADD CONSTRAINT `fk_event_campaign`
FOREIGN KEY (`CampaignID`) REFERENCES `AdTech`.`Campaigns`(`CampaignID`);

ALTER TABLE `AdTech`.`Events`
ADD CONSTRAINT `fk_event_user`
FOREIGN KEY (`UserID`) REFERENCES `AdTech`.`Users`(`UserID`);

ALTER TABLE `AdTech`.`Events`
ADD CONSTRAINT `fk_event_device`
FOREIGN KEY (`DeviceTypeID`) REFERENCES `AdTech`.`DeviceTypes`(`DeviceTypeID`);

ALTER TABLE `AdTech`.`Events`
ADD CONSTRAINT `fk_event_location`
FOREIGN KEY (`LocationID`) REFERENCES `AdTech`.`Locations`(`LocationID`);

--
ALTER TABLE `AdTech`.`DeviceTypes` ADD INDEX (DeviceName);
ALTER TABLE `AdTech`.`Locations` ADD INDEX (CountryName);
