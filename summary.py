-- Enables concurrent GC
SET mapred.child.java.opts="-Xmx1G -XX:+UseConcMarkSweepGC";
-- Enables aggregation in the map stage
set hive.map.aggr=true;
-- Creates the databases/tables that are necessary
CREATE DATABASE IF NOT EXISTS ssid8890;
set db_name=ssid8890;
use ssid8890;
ADD FILE /home/ssid8890/summary.py;
DROP TABLE IF EXISTS place_lookup;
DROP TABLE IF EXISTS photo_lookup;
DROP TABLE IF EXISTS results;
DROP VIEW IF EXISTS user_lookup;
DROP VIEW IF EXISTS user_visits;
DROP VIEW IF EXISTS duration_lookup;

-- Place Lookup
CREATE TABLE place_lookup (place_id STRING, country STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/ssid8890/hiveData/placeLookup';

INSERT OVERWRITE TABLE place_lookup
SELECT place_id, regexp_replace(split(place_url, "/")[1], '\\+', ' ')
FROM share.place
ORDER BY place_id;

-- Photo Lookup Table, external table accesses HDFS file
CREATE EXTERNAL TABLE photo_lookup (
        photo_id        string,
        owner   string,
        tags    string,
        date_taken      string,
        place_id        string,
        accuracy        int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/share/small/';

-- Table to store the answers
CREATE TABLE IF NOT EXISTS results (
answer string
);

-- Join photos and places, and sort
CREATE VIEW user_lookup AS
SELECT /*+ MAPJOIN(b) */ owner, country, unix_timestamp(date_taken) AS start_date FROM photo_lookup a JOIN place_lookup b ON  (a.place_id = b.place_id) DISTRIBUTE BY owner SORT BY owner, start_date, country;

-- Duration Lookup
CREATE VIEW duration_lookup AS
SELECT TRANSFORM(owner, country, start_date) using 'python summary.py' AS (owner STRING, country STRING, duration FLOAT) FROM user_lookup;

-- Insert the results into the table
INSERT OVERWRITE TABLE results
SELECT concat(owner, " ", concat_ws(", ", collect_list(record)))
FROM (
        SELECT owner, concat(country, "(", COUNT(*), ",", ROUND(MAX(duration),1), ",",  ROUND(MIN(duration),1), ",", ROUND(AVG(duration),1), ",", ROUND(SUM(duration),1), ")") AS record FROM duration_lookup GROUP BY owner, country
) AS recorded GROUP BY owner;