CREATE VIEW pitteloudj.CUST_CONSOLIDATED
AS

SET OPTION Query_Temp_Space_Limit = 0
DROP TABLE CUST_CONSOLIDATED_2015
DROP TABLE CUST_CONSOLIDATED_2016
GO 
SELECT a.subs_week_and_year
	, a.end_date
	, a.BB_Enter_SysCan
	, a.BB_Enter_CusCan
	, a.BB_Enter_HM
	, a.BB_Enter_3rd_Party
	, a.account_number
	, agg.country
	, CASE 	WHEN d.node IN (22, 46, 49, 70, 75, 71) THEN 1 
			WHEN d.node IN (83, 53, 43, 82, 73, 57) THEN 2 
			WHEN d.node IN (63, 47, 68, 42, 62, 12, 39, 11, 35) THEN 3 
			WHEN d.node IN (21, 74, 72) THEN 4 
			WHEN d.node IN (40, 36, 66, 60, 65) THEN 5 
			WHEN d.node IN (77, 31, 84, 56, 76) THEN 6 
			WHEN d.node IN (10, 41, 67) THEN 7 
			WHEN d.node IN (61, 51, 64, 24, 50) THEN 8 
			WHEN d.node IN (27, 55, 85, 81, 79, 80, 54) THEN 9 
			WHEN d.node IN (9) THEN 10 
			ELSE 0 END AS segment
	, CAST(0 AS BIT) AS sky_plus
INTO 		CUST_CONSOLIDATED_2015	
FROM pitteloudj.DTV_FCAST_WEEKLY_BASE AS agg
INNER JOIN citeam.CUST_FCAST_WEEKLY_BASE AS a ON a.account_number = agg.account_number AND a.end_date = agg.end_date
LEFT JOIN pitteloudj.BB_SABB_Churn_segments_lookup AS d ON agg.BB_offer_rem_and_end = d.BB_offer_rem_and_end AND agg.BB_tenure = d.BB_tenure AND agg.my_sky_login_3m = d.my_sky_login_3m AND agg.talk_type = d.talk_type AND agg.home_owner_status = d.home_owner_status AND agg.BB_all_calls_1m = d.BB_all_calls_1m
WHERE end_date BETWEEN '2015-01-01' AND '2015-12-31' 


INSERT INTO 		CUST_CONSOLIDATED_2017_2
SELECT a.subs_week_and_year
	, a.end_date
	, a.BB_Enter_SysCan
	, a.BB_Enter_CusCan
	, a.BB_Enter_HM
	, a.BB_Enter_3rd_Party
	, a.account_number
	, agg.country
	, CASE 	WHEN d.node IN (22, 46, 49, 70, 75, 71) THEN 1 
			WHEN d.node IN (83, 53, 43, 82, 73, 57) THEN 2 
			WHEN d.node IN (63, 47, 68, 42, 62, 12, 39, 11, 35) THEN 3 
			WHEN d.node IN (21, 74, 72) THEN 4 
			WHEN d.node IN (40, 36, 66, 60, 65) THEN 5 
			WHEN d.node IN (77, 31, 84, 56, 76) THEN 6 
			WHEN d.node IN (10, 41, 67) THEN 7 
			WHEN d.node IN (61, 51, 64, 24, 50) THEN 8 
			WHEN d.node IN (27, 55, 85, 81, 79, 80, 54) THEN 9 
			WHEN d.node IN (9) THEN 10 
			ELSE 0 END AS segment
	, CAST(0 AS BIT) AS sky_plus
	, CAST( NULL AS INT) AS PL_week
	, DATEADD (Week, 1 , end_Date ) AS  PL_end_date
FROM pitteloudj.DTV_FCAST_WEEKLY_BASE AS agg
INNER JOIN citeam.CUST_FCAST_WEEKLY_BASE AS a ON a.account_number = agg.account_number AND a.end_date = agg.end_date
LEFT JOIN pitteloudj.BB_SABB_Churn_segments_lookup AS d ON agg.BB_offer_rem_and_end = d.BB_offer_rem_and_end AND agg.BB_tenure = d.BB_tenure AND agg.my_sky_login_3m = d.my_sky_login_3m AND agg.talk_type = d.talk_type AND agg.home_owner_status = d.home_owner_status AND agg.BB_all_calls_1m = d.BB_all_calls_1m
WHERE end_date BETWEEN '2017-01-01' AND '2017-12-31' 



UPDATE CUST_CONSOLIDATED_2017
SET sky_plus = 1 
FROM CUST_CONSOLIDATED_2017 AS a 
JOIN CUST_SUBS_HIST AS b ON a.account_number= b.account_number AND a.end_date BETWEEN b.effective_from_dt AND b.effective_to_dt 
						AND b.subscription_sub_type = 'DTV Sky+'
						AND        	status_code='AC'
						
WHERE 		a.account_number is not null
			AND        	a.account_number <> '?'
GO 
UPDATE CUST_CONSOLIDATED_2016
SET sky_plus = 1 
FROM CUST_CONSOLIDATED_2016 AS a 
JOIN CUST_SUBS_HIST AS b ON a.account_number= b.account_number AND a.end_date BETWEEN b.effective_from_dt AND b.effective_to_dt 
						AND b.subscription_sub_type = 'DTV Sky+'
						AND        	status_code='AC'
						
WHERE 		a.account_number is not null
			AND        	a.account_number <> '?'


ALTER TABLE CUST_CONSOLIDATED_2017_2
ADD ( PL_week INT DEFAULT NULL
    , PL_end_date DATE DEFAULT NULL )
    
    UPDATE CUST_CONSOLIDATED_2017_2
    SET PL_end_date = DATEADD (Week, 1 , end_Date )
    
    UPDATE CUST_CONSOLIDATED_2017_2
    SET PL_week = b.subs_week_and_year
    FROM CUST_CONSOLIDATED_2017_2 as a
    JOIN sky_calendar AS b ON a.pl_end_date = calendar_date 
    



