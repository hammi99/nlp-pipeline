-- mysql --local-infile=1 -u root -ppassword
-- mysql --local-infile=1 -u root -ppassword < temp.sql

CREATE DATABASE IF NOT EXISTS `default`;
USE default;

CREATE TABLE IF NOT EXISTS `Tweet` (
    `id`        INT             PRIMARY KEY,
    `number`    INT             NOT NULL,
    
    `createdAt` DATETIME        NOT NULL,
    -- `createdAt` VARCHAR(255)    NOT NULL,
    
    `flag`      VARCHAR(255)    NOT NULL,
    `userName`  VARCHAR(255)    NOT NULL,
    `text`      VARCHAR(255)    NOT NULL
);


