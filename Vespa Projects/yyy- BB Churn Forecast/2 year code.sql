CREATE TABLE BB_CHURN_attachement_2yr
    ( Year      INT
    , Q         tinyint
    , Segment   tinyint
    , BASE      bigint
    , Calls     BIGINT) 
    COMMIT 
    CREATE LF index i1 ON BB_CHURN_attachement_2yr(year)
    CREATE LF index i2 ON BB_CHURN_attachement_2yr(Q)
    CREATE LF index i3 ON BB_CHURN_attachement_2yr(segment)
    COMMIT 			  
	
	DECLARE @y INT
	DECLARE @Q INT
	DECLARE @w1 INT
	DECLARE @wf INT
	DECLARE @dt DATE
	SET @y = 2015
	SET @q = 1
	-- SET @w1 = 1
	-- SET @wf = 13
	SET @dt = '2015-01-01'
	
SELECT a.account_number
	, a.product_holding
	, a.bb_type
	, a.h_AGE_coarse_description      	AS age
	, COALESCE(a.h_fss_v3_group, 'U')    	AS fss
	, CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, @dt) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, @dt) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, @dt) BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, @dt) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, @dt) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, @dt)  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 	AS bb_tenure
	, COALESCE(LEFT (a.affluence,2), 'U') AS affluence
	, COALESCE(LEFT (a.life_stage,2),'U') AS life_stage
     , TA_call  = CASE WHEN b.account_number is not null then 1 ELSE 0 END
	 , CAST (null AS VARCHAR(30) )AS offer 
	 , CAST (null AS tinyint) AS segment
INTO base 
FROM sharmaa.View_attachments_201501 AS a 
LEFT JOIN (SELECT account_number, v_year, q, count(*) hits 
			FROM BB_CHURN_append 
			GROUP BY account_number, v_year, q ) AS b ON a.account_number = b.account_number AND b.v_year = @y AND q = @q --- Q1 2015
WHERE broadband = 1 

COMMIT 
CREATE HG INDEX id1 ON base(account_number) 	
CREATE LF INDEX id2 ON base(bb_tenure) 
CREATE LF INDEX id3 ON base(offer) 
CREATE LF INDEX id4 ON base(segment) 
CREATE LF INDEX id5 ON base(product_holding) 
CREATE LF INDEX id6 ON base(bb_type) 
COMMIT

EXECUTE on_offer @dt
   
UPDATE base 
SET segment= (CASE WHEN (T0.offer = 'BBT Exp +90') THEN 1 
					WHEN (((T0.offer = 'BBT Exp -30') OR (T0.offer = 'Multi Live 31-90')) OR (T0.offer = 'OTHER Live -30')) THEN 2 
					WHEN ((T0.offer = 'BBT Exp 31-90') OR (T0.offer = 'OTHER Exp -30')) THEN 2 
					WHEN ((((T0.offer = 'BBT Live -30') OR (T0.offer = 'BBT Live 31-90')) OR (T0.offer = 'Multi Exp +90')) OR (T0.offer = 'Multi Exp 31-90')) THEN 4
					WHEN (((T0.offer = 'Multi Exp -30') OR (T0.offer = 'Multi Live -30')) OR (T0.offer = 'OTHER Live 31-90')) THEN 5
					WHEN (T0.offer = 'Multi Live +90') THEN (CASE WHEN ((((((((T0.bb_type = 'Sky Broadband 12GB') OR (T0.bb_type = 'Sky Broadband Everyday')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																		OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) 
																		OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 6 
																	ELSE (CASE WHEN ((((((T0.fss = 'A') OR (T0.fss = 'B')) OR (T0.fss = 'F')) OR (T0.fss = 'I')) OR (T0.fss = 'J')) OR (T0.fss = 'L')) THEN 7 
																				ELSE 8 END) END) 
					WHEN (((T0.offer = 'No Offer Ever') OR (T0.offer = 'OTHER Exp +90')) OR (T0.offer = 'OTHER Exp 31-90')) THEN (CASE WHEN (T0.product_holding = 'E. SABB') THEN 9 
																																			ELSE (CASE WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) OR (T0.age = 'Age 36-45')) THEN 10 
																																						WHEN ((T0.age = 'Age 66+') OR (T0.age = 'Unclassified')) THEN 11
																																						ELSE 12 END) END) 
					WHEN (T0.offer = 'OTHER Live +90') THEN (CASE WHEN (((((T0.bb_type = 'Sky Broadband Unlimited Fibre') OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre Lite')) 
																			OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 13 
																	ELSE (CASE WHEN ((T0.bb_tenure LIKE '%1 year%') OR (T0.bb_tenure  LIKE '%Less 1 year%')) THEN 14 
																				ELSE (CASE WHEN ((((T0.age = 'Age 18-25') OR (T0.age = 'Age 46-55')) OR (T0.age = 'Age 56-65')) OR (T0.age = 'Unclassified')) THEN 15 
																							ELSE 16 END) END) END) 
					ELSE (CASE 	WHEN (T0.bb_tenure  LIKE '%1 year%') THEN 17
								WHEN ((T0.bb_tenure  LIKE '%2 years%') OR (T0.bb_tenure  LIKE '%3 years%')) THEN (CASE WHEN ((T0.product_holding = 'B. DTV + Triple play') OR (T0.product_holding = 'C. DTV + BB Only')) THEN 18 
																																		ELSE 19 END) 
								WHEN (T0.bb_tenure  LIKE '%Less 1 year%') THEN 20 
								ELSE (CASE WHEN ((((T0.bb_type = 'Sky Broadband Unlimited Fibre') OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) THEN 21 
					ELSE 22 END) END) END) 
FROM base T0     
   

INSERT INTO BB_CHURN_attachement_2yr 
SELECT @y, @q, segment, count(*) hits, SUM(TA_call)
FROM base
group by segment

DROP TABLE BASE 



SELECT 