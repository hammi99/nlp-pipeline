-- mysql --local-infile=1 -u root -ppassword
-- mysql --local-infile=1 -u root -ppassword < /app/sql/2-dump.sql

SET GLOBAL local_infile=1;

USE `default`;

TRUNCATE TABLE `Tweet`;

LOAD DATA LOCAL
    INFILE      './app/data/ProjectTweets.csv' 
    INTO TABLE  `Tweet`
    FIELDS 
        TERMINATED BY ',' 
        OPTIONALLY ENCLOSED   BY '"'
    LINES 
        TERMINATED BY '\r\n'

    (`id`, `number`, @var_createdAt, `flag`, `userName`, `text`)
    SET `createdAt` = STR_TO_DATE(@var_createdAt, '%a %b %d %H:%i:%S PDT %Y')
;

select * from `Tweet` limit 10;