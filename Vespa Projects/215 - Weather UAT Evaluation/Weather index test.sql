CREATE TABLE WEATHER_INDEX_TEST_D (district varchar(5), ID int IDENTITY)
commit
-------------------------------------------------
INSERT INTO WEATHER_INDEX_TEST_D (district)
SELECT DISTINCT district
FROM WEATHER_JOIN_TEST_v2
-------------------------------------------------
SELECT top 30 * from WEATHER_INDEX_TEST_D
-------------------------------------------------
CREATE VIEW WEATHER_INDEX_TEST_SAMPLE AS
SELECT
          weather_score
        , v.district
        , weather_date
FROM WEATHER_JOIN_TEST_v2 as v
JOIN WEATHER_INDEX_TEST_D as d ON d.district = v.district AND year(v.weather_date) = 2013 AND d.ID <=30
commit

--SELECT DISTINCT district from WEATHER_INDEX_TEST_SAMPLE
