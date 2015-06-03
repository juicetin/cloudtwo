CREATE DATABASE IF NOT EXISTS ssid8890;
set db_name=ssid8890;
use ssid8890;

DROP TABLE IF EXISTS place_lookup;
DROP VIEW IF EXISTS user_lookup;
DROP VIEW IF EXISTS user_visits;

CREATE TABLE place_lookup (place_id STRING, country STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/ssid8890/hiveData/placeLookup';

INSERT OVERWRITE TABLE place_lookup
SELECT place_id, split(place_url, "/")[1]
FROM share.place
ORDER BY place_id;

CREATE VIEW user_lookup AS
SELECT owner, unix_timestamp(date_taken) AS start_date, country FROM share.photo a JOIN place_lookup b ON  (a.place_id = b.place_id) ORDER BY owner, start_date, country;

CREATE VIEW user_visits AS
SELECT owner, country, MIN(start_date) FROM user_lookup GROUP BY owner, country;

SELECT owner, country, COUNT(*) FROM user_visits GROUP BY owner, country ORDER BY owner, country LIMIT 5;