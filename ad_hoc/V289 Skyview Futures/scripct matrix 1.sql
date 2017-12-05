------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
------------------------------ BARB Matrix
CREATE TABLE PIV_Matrix_1 
	(row_id INT IDENTITY  
	, day_of_week VARCHAR (2) DEFAULT NULL
	, daypart VARCHAR 	(2) DEFAULT NULL
	, person_count 	INT DEFAULT NULL
	, Male_4_9 	INT DEFAULT NULL
	, Male_10_15 	INT DEFAULT NULL
	, Male_16_19 	INT DEFAULT NULL
	, Male_20_24 	INT DEFAULT NULL
	, Male_25_34 	INT DEFAULT NULL
	, Male_35_44 	INT DEFAULT NULL
	, Male_45_64 	INT DEFAULT NULL
	, Male_65 	INT DEFAULT NULL
	, Female_4_9 	INT DEFAULT NULL
	, Female_10_15 	INT DEFAULT NULL
	, Female_16_19 	INT DEFAULT NULL
	, Female_20_24 	INT DEFAULT NULL
	, Female_25_34 	INT DEFAULT NULL
	, Female_35_44 	INT DEFAULT NULL
	, Female_45_64 	INT DEFAULT NULL
	, Female_65 	INT DEFAULT NULL
	, channel_name  varchar	(40) DEFAULT NULL
	, genre_description varchar	(20) DEFAULT NULL
	, hH_count int DEFAULT NULL
	, SKY_HH_count int DEFAULT NULL
	, Sky_indicator bit DEFAULT 0)
	COMMIT

CREATE TABLE PIV_day_of_Week
    ( row_id INT IDENTITY
    , day_of_week VARCHAR (2) DEFAULT NULL
    , description VARCHAR (10) DEFAULT NULL
    , f_value int DEFAULT NULL)
COMMIT

CREATE TABLE PIV_daypart 
    ( row_id INT IDENTITY
    , daypart VARCHAR (2) DEFAULT NULL
    , description VARCHAR (10) DEFAULT NULL
    , from_time TIME DEFAULT NULL)
	, to_time TIME DEFAULT NULL)
COMMIT
	
CREATE TABLE PIV_genre 
    ( row_id INT IDENTITY
    , genre VARCHAR (20) DEFAULT NULL
    , f_value int DEFAULT NULL)
COMMIT

CREATE TABLE PIV_Channel_packs 
    ( row_id INT IDENTITY
    , channel_pack VARCHAR (10) DEFAULT NULL
    , description VARCHAR (30) DEFAULT NULL
    , f_value int DEFAULT NULL)
COMMIT

INSERT INTO PIV_day_of_Week (day_of_week, description, f_value)
VALUES	 ('su', 'Sunday', 1)
		,('mo', 'Monday', 2)
		,('tu', 'Tuesday', 3)
		,('we', 'Wednesday', 4)		
		,('th', 'Thursday', 5)
		,('fr', 'Friday', 6)
		,('sa', 'Saturday', 7)
COMMIT
 
CREATE lf INDEX id1 ON PIV_day_of_Week(day_of_week)
COMMIT
		
INSERT INTO PIV_daypart (	description, from_time, to_time)	
VALUES  ('breakfast', '06:00:00', '08:59:00'),
		('morning', '09:00:00', '11:59:00'),
		('lunch', '12:00:00', '14:59:00'),
		('early prime', '15:00:00', '17:59:00'),
		('prime', '18:00:00', '20:59:00'),
		('late night', '21:00:00', '23:59:00'),
		('night', ' 24:00:00', '05:59:00'),
		('na', null, null)
COMMIT

UPDATE PIV_daypart 
SET daypart = left(description, 2)

CREATE lf INDEX id2 ON PIV_daypart(daypart)
COMMIT

CREATE TABLE PIV_person_count
	(row_id INT IDENTITY
    , person_count int DEFAULT NULL
	,  description VARCHAR (20) DEFAULT NULL)
COMMIT

INSERT INTO     PIV_person_count (description)
SELECT DISTINCT CAST (person_count as VARCHAR) as asas
 from angeld.barbview
 ORDER BY asas
	
INSERT INTO PIV_genre (genre)
SELECT DISTINCT genre_description
 from angeld.barbview
 ORDER BY genre_description

COMMIT
CREATE lf INDEX id3 ON PIV_genre (genre)
COMMIT
INSERT INTO PIV_Channel_packs (description)
VALUES('Diginets')
	,('Diginets non-commercial')
	,('Other')
	,('Other non-commercial')
	,('Terrestrial')
	,('Terrestrial non-commercial')

INSERT INTO PIV_Matrix_1
SELECT
      day_of_week
    , daypart
    , person_count
    , channel_pack
    , genre
FROM   		PIV_Channel_packs
JOIN        PIV_genre AS a ON 1=1
JOIN        PIV_daypart AS b ON 1=1
JOIN        PIV_day_of_Week AS c ON 1=1
JOIN        PIV_person_count AS d ON 1=1
-----------------------------------------------------------------
-----------------------------------------------------------------
------- POPULATING the Matrix
------- All count
UPDATE PIV_Matrix_1
SET hh_count = HH
FROM PIV_Matrix_1 as a
JOIN (SELECT day_of_week = CASE datepart(dw, vw.Barb_date_of_activity)      WHEN 1 THEN 'su'
                                                                WHEN 2 THEN 'mo'
                                                                WHEN 3 THEN 'tu'
                                                                WHEN 4 THEN 'we'
                                                                WHEN 5 THEN 'th'
                                                                WHEN 6 THEN 'fr'
                                                                WHEN 7 THEN 'sa'
                                                                ELSE null END
        , dy.daypart
        , vw.person_count
        , cp.channel_pack
        , genre_description      
        , COUNT (DISTINCT household_number) HH
    FROM angeld.barbview vw
    INNER JOIN PIV_daypart as dy ON CAST(vw.start_time_of_session AS TIME) BETWEEN from_time AND to_time
    INNER JOIN vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES as  ska ON  vw.service_key         = ska.service_key AND active_channel = 'Y'
	INNER JOIN PIV_Channel_packs AS cp ON cp.description = ska.channel_pack
	INNER JOIN angeld.skybarb as sk ON house_id = vw. household_number    GROUP BY      day_of_week
            , dy.daypart
            , vw.person_count
            , genre_description
            , cp.channel_pack
        ) as b ON   a.day_of_week = b.day_of_week
                AND a.daypart = b.daypart
                AND a.person_count = b.person_count
                AND a.channel_name = b.channel_pack
                AND a.genre_description = b.genre_description

------- Sky HH count
UPDATE PIV_Matrix_1
SET SKY_HH_count = HH
FROM PIV_Matrix_1 as a
JOIN (SELECT day_of_week = CASE datepart(dw, vw.Barb_date_of_activity)      WHEN 1 THEN 'su'
                                                                WHEN 2 THEN 'mo'
                                                                WHEN 3 THEN 'tu'
                                                                WHEN 4 THEN 'we'
                                                                WHEN 5 THEN 'th'
                                                                WHEN 6 THEN 'fr'
                                                                WHEN 7 THEN 'sa'
                                                                ELSE null END
        , dy.daypart
        , vw.person_count
        , cp.channel_pack
        , genre_description      
        , COUNT (DISTINCT household_number) HH
    FROM angeld.barbview vw
    INNER JOIN PIV_daypart as dy ON CAST(vw.start_time_of_session AS TIME) BETWEEN from_time AND to_time
    INNER JOIN vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES as  ska ON  vw.service_key         = ska.service_key AND active_channel = 'Y'
	INNER JOIN PIV_Channel_packs AS cp ON cp.description = ska.channel_pack
	INNER JOIN angeld.skybarb as sk ON house_id = vw. household_number
    GROUP BY      day_of_week
            , dy.daypart
            , vw.person_count
            , genre_description
            , cp.channel_pack
        ) as b ON   a.day_of_week = b.day_of_week
                AND a.daypart = b.daypart
                AND a.person_count = b.person_count
                AND a.channel_name = b.channel_pack
                AND a.genre_description = b.genre_description

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------ VESPA MATRIX				
CREATE TABLE PIV_Matrix_1_VESPA
	(row_id INT IDENTITY  
	, day_of_week VARCHAR (2) DEFAULT NULL
	, daypart VARCHAR 	(2) DEFAULT NULL
	, person_count 	INT DEFAULT NULL
	, Male_4_9 	INT DEFAULT NULL
	, Male_10_15 	INT DEFAULT NULL
	, Male_16_19 	INT DEFAULT NULL
	, Male_20_24 	INT DEFAULT NULL
	, Male_25_34 	INT DEFAULT NULL
	, Male_35_44 	INT DEFAULT NULL
	, Male_45_64 	INT DEFAULT NULL
	, Male_65 	INT DEFAULT NULL
	, Female_4_9 	INT DEFAULT NULL
	, Female_10_15 	INT DEFAULT NULL
	, Female_16_19 	INT DEFAULT NULL
	, Female_20_24 	INT DEFAULT NULL
	, Female_25_34 	INT DEFAULT NULL
	, Female_35_44 	INT DEFAULT NULL
	, Female_45_64 	INT DEFAULT NULL
	, Female_65 	INT DEFAULT NULL
	, channel_name  varchar	(40) DEFAULT NULL
	, genre_description varchar	(20) DEFAULT NULL
	, hH_count int DEFAULT NULL
	, SKY_HH_count int DEFAULT NULL
	, Sky_indicator bit DEFAULT 0)
	COMMIT
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
------------------------------Populating the shell
INSERT INTO PIV_Matrix_1_VESPA
SELECT
      day_of_week
    , daypart
    , person_count
    , channel_pack
    , genre
FROM   		PIV_Channel_packs
JOIN        PIV_genre AS a ON 1=1
JOIN        PIV_daypart AS b ON 1=1
JOIN        PIV_day_of_Week AS c ON 1=1
JOIN        PIV_person_count AS d ON 1=1				

COMMIT

CREATE lf INDEX id3 ON PIV_daypart(day_of_week)
CREATE lf INDEX id4 ON PIV_daypart(daypart)
CREATE lf INDEX id5 ON PIV_daypart(person_count)
CREATE lf INDEX id6 ON PIV_daypart(channel_pack)
CREATE lf INDEX id6 ON PIV_daypart(genre)
COMMIT
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
------------------------------Populating the shell using hh_key and 1st address line
CREATE TABLE PIV_VESPA_HH_view
(	  row_id 		INT IDENTITY  
	, acccount_number 	varchar	(20) DEFAULT NULL
	, cb_key_household 	bigint DEFAULT NULL
	, cb_address_line_1 varchar	(200) DEFAULT NULL
	, person_count 	INT DEFAULT NULL
	, Children_count INT DEFAULT NULL
	, Children_5_11_flag 		BIT DEFAULT 0
	, Children_12_17_flag 		BIT DEFAULT 0
	, Male_18_19 	INT DEFAULT NULL
	, Male_20_24 	INT DEFAULT NULL
	, Male_25_34 	INT DEFAULT NULL
	, Male_35_44 	INT DEFAULT NULL
	, Male_45_64 	INT DEFAULT NULL
	, Male_65 		INT DEFAULT NULL
	, Female_18_19 	INT DEFAULT NULL
	, Female_20_24 	INT DEFAULT NULL
	, Female_25_34 	INT DEFAULT NULL
	, Female_35_44 	INT DEFAULT NULL
	, Female_45_64 	INT DEFAULT NULL
	, Female_65 	INT DEFAULT NULL
	)
COMMIT
--------------------------------------------------------------------------------------------------------------
-------------------------------------------------------Inserting all the accounts/HH available

INSERT INTO PIV_VESPA_HH_view(acccount_number, cb_key_household, cb_address_line_1)
SELECT DISTINCT 
	  vw.account_number
	, sav.cb_key_household
	, sav.cb_address_line_1
FROM sk_prod.vespa_dp_prog_viewed_201309  as vw
JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav ON sav.account_number = vw.account_number
WHERE vw.cb_key_household > 0
AND   vw.cb_key_household IS NOT NULL
AND   vw.account_number IS NOT NULL
AND   vw.duration_minutes >= 1


COMMIT

CREATE HG INDEX idhh ON PIV_VESPA_HH_view(cb_key_household)
CREATE HG INDEX idac ON PIV_VESPA_HH_view(account_number)
CREATE HG INDEX idal ON PIV_VESPA_HH_view(cb_address_line_1)

COMMIT
--------------------------------------------------------------------------------------------------------------
-------------------------------------------------------Updating by HH-ky and address_line
SELECT acccount_number
    , vh.cb_key_household
    , vh.cb_address_line_1
    , COUNT(DISTINCT ex.cb_key_db_person) + MAX(CAST(h_number_of_children_in_household_2011 as INT))  AS     person_count
    , Children_count        = MAX(CAST(h_number_of_children_in_household_2011 as INT))
    , Children_5_11_flag    = MAX(CASE h_presence_of_child_aged_5_11_2011 WHEN '1' THEN 1 ELSE 0 END)
    , Children_12_17_flag   = MAX(CASE h_presence_of_child_aged_12_17_2011 WHEN '1' THEN 1 ELSE 0 END)
    , Male_18_19 = COUNT(DISTINCT CASE WHEN p_gender = '0' AND p_actual_age <= 19  THEN cb_key_db_person ELSE NULL END)
    , Male_20_24 = COUNT(DISTINCT CASE WHEN p_gender = '0' AND p_actual_age BETWEEN 20 AND 24  THEN cb_key_db_person ELSE NULL END)
    , Male_25_34 = COUNT(DISTINCT CASE WHEN p_gender = '0' AND p_actual_age BETWEEN 25 AND 34  THEN cb_key_db_person ELSE NULL END)
    , Male_35_44 = COUNT(DISTINCT CASE WHEN p_gender = '0' AND p_actual_age BETWEEN 35 AND 44  THEN cb_key_db_person ELSE NULL END)
    , Male_45_64 = COUNT(DISTINCT CASE WHEN p_gender = '0' AND p_actual_age BETWEEN 45 AND 64  THEN cb_key_db_person ELSE NULL END)
    , Male_65    = COUNT(DISTINCT CASE WHEN p_gender = '0' AND p_actual_age >= 65              THEN cb_key_db_person ELSE NULL END)
    , Female_18_19 = COUNT(DISTINCT CASE WHEN p_gender = '1' AND p_actual_age <= 19             THEN cb_key_db_person ELSE NULL END)
    , Female_20_24 = COUNT(DISTINCT CASE WHEN p_gender = '1' AND p_actual_age BETWEEN 20 AND 24 THEN cb_key_db_person ELSE NULL END)
    , Female_25_34 = COUNT(DISTINCT CASE WHEN p_gender = '1' AND p_actual_age BETWEEN 25 AND 34 THEN cb_key_db_person ELSE NULL END)
    , Female_35_44 = COUNT(DISTINCT CASE WHEN p_gender = '1' AND p_actual_age BETWEEN 35 AND 44 THEN cb_key_db_person ELSE NULL END)
    , Female_45_64 = COUNT(DISTINCT CASE WHEN p_gender = '1' AND p_actual_age BETWEEN 45 AND 64 THEN cb_key_db_person ELSE NULL END)
    , Female_65    = COUNT(DISTINCT CASE WHEN p_gender = '1' AND p_actual_age >= 65             THEN cb_key_db_person ELSE NULL END)
	, rank() OVER (PARTITION by ex.cb_key_household, ex.cb_address_line_1 ORDER BY person_count DESC ) rank_1
INTO #t1
FROM PIV_VESPA_HH_view AS vh
JOIN sk_prod.EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household AND ex.cb_address_line_1 = vh.cb_address_line_1
GROUP BY acccount_number
    , vh.cb_key_household
    , vh.cb_address_line_1
COMMIT

CREATE HG INDEX idhh ON #t1(cb_key_household)
CREATE HG INDEX idac ON #t1(acccount_number)
CREATE HG INDEX idal ON #t1(cb_address_line_1)


UPDATE PIV_VESPA_HH_view
SET a.person_count              = b.person_count
    ,a.Children_count           = b.Children_count
    ,a.Children_5_11_flag       = b.Children_5_11_flag
    ,a.Children_12_17_flag      = b.Children_12_17_flag
    ,a.Male_18_19   = b.Male_18_19
    ,a.Male_20_24   = b.Male_20_24
    ,a.Male_25_34   = b.Male_25_34
    ,a.Male_35_44   = b.Male_35_44
    ,a.Male_45_64   = b.Male_45_64
    ,a.Male_65      = b.Male_65
    ,a.Female_18_19 = b.Female_18_19
    ,a.Female_20_24 = b.Female_20_24
    ,a.Female_25_34 = b.Female_25_34
    ,a.Female_35_44 = b.Female_35_44
    ,a.Female_45_64 = b.Female_45_64
    ,a.Female_65    = b.Female_65
FROM PIV_VESPA_HH_view as a
JOIN #t1 as b ON a.acccount_number = b.acccount_number  AND a.cb_key_household = b.cb_key_household AND a.cb_address_line_1 = b.cb_address_line_1 and rank_1 = 1

COMMIT
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
------------------------------------------------------- UPDATING HH that didn't match hh_key and address line
SELECT vh.acccount_number
    , vh.cb_key_household
    , vh.cb_address_line_1
	, ex.cb_address_line_1 AS linex
	, COUNT(DISTINCT ex.cb_key_db_person) + MAX(CAST(h_number_of_children_in_household_2011 as INT))  AS  persons
	, RANK () OVER (PARTITION BY vh.cb_key_household ORDER by persons DESC) as rank_1
INTO #t2
FROM PIV_VESPA_HH_view as vh
JOIN sk_prod.EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household
WHERE (vh.person_count is null OR vh.person_count = 0)
GROUP BY vh.acccount_number
    , vh.cb_key_household
    , vh.cb_address_line_1
	, linex
HAVING persons <= 10

SELECT x.acccount_number
    , x.cb_key_household
	, x.cb_address_line_1
    , x.linex
    , x.persons
    , Children_count        = MAX(CAST(h_number_of_children_in_household_2011 as INT))
    , Children_5_11_flag    = MAX(CASE h_presence_of_child_aged_5_11_2011 WHEN '1' THEN 1 ELSE 0 END)
    , Children_12_17_flag   = MAX(CASE h_presence_of_child_aged_12_17_2011 WHEN '1' THEN 1 ELSE 0 END)
    , Male_18_19 = COUNT(DISTINCT CASE WHEN p_gender = '0' AND p_actual_age <= 19  THEN cb_key_db_person ELSE NULL END)
    , Male_20_24 = COUNT(DISTINCT CASE WHEN p_gender = '0' AND p_actual_age BETWEEN 20 AND 24  THEN cb_key_db_person ELSE NULL END)
    , Male_25_34 = COUNT(DISTINCT CASE WHEN p_gender = '0' AND p_actual_age BETWEEN 25 AND 34  THEN cb_key_db_person ELSE NULL END)
    , Male_35_44 = COUNT(DISTINCT CASE WHEN p_gender = '0' AND p_actual_age BETWEEN 34 AND 44  THEN cb_key_db_person ELSE NULL END)
    , Male_45_64 = COUNT(DISTINCT CASE WHEN p_gender = '0' AND p_actual_age BETWEEN 45 AND 64  THEN cb_key_db_person ELSE NULL END)
    , Male_65    = COUNT(DISTINCT CASE WHEN p_gender = '0' AND p_actual_age >= 65              THEN cb_key_db_person ELSE NULL END)
    , Female_18_19 = COUNT(DISTINCT CASE WHEN p_gender = '1' AND p_actual_age <= 19             THEN cb_key_db_person ELSE NULL END)
    , Female_20_24 = COUNT(DISTINCT CASE WHEN p_gender = '1' AND p_actual_age BETWEEN 20 AND 24 THEN cb_key_db_person ELSE NULL END)
    , Female_25_34 = COUNT(DISTINCT CASE WHEN p_gender = '1' AND p_actual_age BETWEEN 25 AND 34 THEN cb_key_db_person ELSE NULL END)
    , Female_35_44 = COUNT(DISTINCT CASE WHEN p_gender = '1' AND p_actual_age BETWEEN 35 AND 44 THEN cb_key_db_person ELSE NULL END)
    , Female_45_64 = COUNT(DISTINCT CASE WHEN p_gender = '1' AND p_actual_age BETWEEN 45 AND 64 THEN cb_key_db_person ELSE NULL END)
    , Female_65    = COUNT(DISTINCT CASE WHEN p_gender = '1' AND p_actual_age >= 65             THEN cb_key_db_person ELSE NULL END)
INTO #t3
FROM #t2 AS x
JOIN sk_prod.EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = x.cb_key_household AND ex.cb_address_line_1 = x.linex
WHERE rank_1 = 1
GROUP BY x.acccount_number
    , x.cb_key_household
	, x.cb_address_line_1
    , x.linex
    , x.persons
COMMIT

CREATE HG INDEX idhh ON #t3(cb_key_household)
CREATE HG INDEX idac ON #t3(acccount_number)
CREATE HG INDEX idal ON #t3(cb_address_line_1)



UPDATE PIV_VESPA_HH_view
SET a.person_count              = b.persons
    ,a.Children_count           = b.Children_count
    ,a.Children_5_11_flag       = b.Children_5_11_flag
    ,a.Children_12_17_flag      = b.Children_12_17_flag
    ,a.Male_18_19   = b.Male_18_19
    ,a.Male_20_24   = b.Male_20_24
    ,a.Male_25_34   = b.Male_25_34
    ,a.Male_35_44   = b.Male_35_44
    ,a.Male_45_64   = b.Male_45_64
    ,a.Male_65      = b.Male_65
    ,a.Female_18_19 = b.Female_18_19
    ,a.Female_20_24 = b.Female_20_24
    ,a.Female_25_34 = b.Female_25_34
    ,a.Female_35_44 = b.Female_35_44
    ,a.Female_45_64 = b.Female_45_64
    ,a.Female_65    = b.Female_65
FROM PIV_VESPA_HH_view as a
JOIN #t3 as b ON a.acccount_number = b.acccount_number  AND a.cb_key_household = b.cb_key_household AND a.cb_address_line_1 = b.cb_address_line_1
WHERE a.person_count is null OR a.person_count = 0 

COMMIT
------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
---------------------------------- VESPA MATRIX UPDATE

UPDATE PIV_Matrix_1_VESPA
SET SKY_HH_count = HH
FROM PIV_Matrix_1_VESPA as a
JOIN (SELECT day_of_week = CASE datepart(dw, vw.instance_start_date_time_utc)      WHEN 1 THEN 'su'
                                                                WHEN 2 THEN 'mo'
                                                                WHEN 3 THEN 'tu'
                                                                WHEN 4 THEN 'we'
                                                                WHEN 5 THEN 'th'
                                                                WHEN 6 THEN 'fr'
                                                                WHEN 7 THEN 'sa'
                                                                ELSE null END
        , dy.daypart
        , hh.person_count
        , cp.channel_pack
        , vw.genre_description      
        , COUNT (DISTINCT vw.cb_key_household) HH
    FROM sk_prod.vespa_dp_prog_viewed_201309    as vw		--	angeld.barbview vw
    INNER JOIN PIV_daypart as dy ON CAST(vw.instance_start_date_time_utc AS TIME) BETWEEN from_time AND to_time
    INNER JOIN vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES as  ska ON  vw.service_key         = ska.					service_key AND active_channel = 'Y'
	INNER JOIN PIV_Channel_packs AS cp ON cp.description = ska.channel_pack
	INNER JOIN PIV_VESPA_HH_view as hh ON hh.acccount_number = vw.account_number
	WHERE vw.duration_minutes >= 1		-- Events duration +1 min
			AND vw.instance_start_date_time_utc BETWEEN '2013-09-11' AND '2013-09-29'
	GROUP BY      day_of_week
				, dy.daypart
				, hh.person_count
				, vw.genre_description
				, cp.channel_pack
        ) as b ON   	a.day_of_week = b.day_of_week
					AND a.daypart = b.daypart
					AND a.person_count = b.person_count
					AND a.channel_name = b.channel_pack
					AND a.genre_description = b.genre_description
------------------------------------------------------------------------------------------------------
---------------------------------- VESPA MATRIX UPDATE		- MIN HH COUNT	
																		
UPDATE PIV_Matrix_1_VESPA
SET HH_count = hh_min
FROM PIV_Matrix_1_VESPA as a
JOIN (SELECT day_of_week = CASE datepart(dw, dt)      WHEN 1 THEN 'su'
                                                                WHEN 2 THEN 'mo'
                                                                WHEN 3 THEN 'tu'
                                                                WHEN 4 THEN 'we'
                                                                WHEN 5 THEN 'th'
                                                                WHEN 6 THEN 'fr'
                                                                WHEN 7 THEN 'sa'
                                                                ELSE null END
			, daypart, person_count, channel_pack, genre_description
			, MIN (HH) as hh_min																
			FROM (SELECT DATE(vw.instance_start_date_time_utc)      as dt
						, dy.daypart
						, hh.person_count
						, cp.channel_pack
						, vw.genre_description      
						, COUNT (DISTINCT vw.cb_key_household) HH
					FROM sk_prod.vespa_dp_prog_viewed_201309    as vw		--	angeld.barbview vw
					INNER JOIN PIV_daypart as dy ON CAST(vw.instance_start_date_time_utc AS TIME) BETWEEN from_time AND to_time
					INNER JOIN vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES as  ska ON  vw.service_key         = ska.					service_key AND active_channel = 'Y'
					INNER JOIN PIV_Channel_packs AS cp ON cp.description = ska.channel_pack
					INNER JOIN PIV_VESPA_HH_view as hh ON hh.acccount_number = vw.account_number
					WHERE vw.duration_minutes >= 1		-- Events duration +1 min
					GROUP BY      dt
								, dy.daypart
								, hh.person_count
								, vw.genre_description
								, cp.channel_pack) AS v	
			GROUP BY day_of_week
					, daypart, person_count, channel_pack, genre_description
		) as b ON   	a.day_of_week = b.day_of_week
							AND a.daypart = b.daypart
							AND a.person_count = b.person_count
							AND a.channel_name = b.channel_pack
							AND a.genre_description = b.genre_description

							
------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
---------------------------------- Daily table from VESPA							
SELECT 
		  DATE(vw.instance_start_date_time_utc)      as dt
		, dy.daypart
		, hh.person_count
		, cp.channel_pack
		, vw.genre_description      
		, COUNT (DISTINCT vw.cb_key_household) HH
INTO PIV_VESPA_Daily_viewing
FROM sk_prod.vespa_dp_prog_viewed_201309    as vw		--	angeld.barbview vw
INNER JOIN PIV_daypart as dy ON CAST(vw.instance_start_date_time_utc AS TIME) BETWEEN from_time AND to_time
INNER JOIN vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES as  ska ON  vw.service_key         = ska.					service_key AND active_channel = 'Y'
INNER JOIN PIV_Channel_packs AS cp ON cp.description = ska.channel_pack
INNER JOIN PIV_VESPA_HH_view as hh ON hh.acccount_number = vw.account_number
WHERE vw.duration_minutes >= 1		-- Events duration +1 min
		AND dt BETWEEN '2013-09-11' AND '2013-09-29'
GROUP BY      dt
			, dy.daypart
			, hh.person_count
			, vw.genre_description
			, cp.channel_pack							
							
--------------------------------------
CREATE VIEW PIV_viewing_joined
AS 
SELECT 
		'BARB' AS source
		, row_id
		, day_of_week
		, daypart
		, person_count
		, channel_name
		, genre_description
		, SKY_HH_count
FROM PIV_Matrix_1
UNION ALL 
SELECT 
		'VESPA' AS source
		, row_id
		, day_of_week
		, daypart
		, person_count
		, channel_name
		, genre_description
		, SKY_HH_count
FROM PIV_Matrix_1_VESPA

COMMIT
