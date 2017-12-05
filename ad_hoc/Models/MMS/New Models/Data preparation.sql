

-------------- Copying the table form Mark's Schema
SELECT * 
INTO Propensity_Base 
FROM menziesm.Propensity_Base 
COMMIT
CREATE HG INDEX id1 ON Propensity_Base (account_number) 
CREATE DATE INDEX iddt ON Propensity_Base (base_dt) 

------------ Adding TA variable
Call Decisioning_Procs.Add_Turnaround_Attempts_To_Base('Propensity_base_1','Base_Dt','TA Events');


------------	Adding target, eligibility  and random flags 
ALTER TABLE Propensity_base
ADD (Up_BB         BIT DEFAULT 0 
    , Up_Fibre      BIT DEFAULT 0 
    , Regrade_Fibre     BIT DEFAULT 0 
    , Up_Box_Sets          BIT DEFAULT 0 
        , random1 tinyint DEFAULT null
        , random2 tinyint DEFAULT null
        , random3 tinyint DEFAULT null
        , random4 tinyint DEFAULT null
	, BB_eligible BIT DEFAULT 0 
	, Fibre_UP_eligible BIT DEFAULT 0 
	, Fibre_RE_eligible BIT DEFAULT 0 
	, Boxset_eligible BIT DEFAULT 0 
    ) 

------------ Adding random group flags 
-- Random flags are based on a middle character of the account number. Each flag will flag 10% of the base 

UPDATE Propensity_base
SET random1 = 1 
WHERE  (base_dt = '2017-01-31' AND SUBSTRING(account_number, 8,1) = '2')
    OR  (base_dt = '2017-06-30' AND SUBSTRING(account_number, 8,1) = '5')
    OR  (base_dt = '2017-02-28' AND SUBSTRING(account_number, 8,1) = '6')
    OR  (base_dt = '2017-05-31' AND SUBSTRING(account_number, 8,1) = '1')
    OR  (base_dt = '2017-04-30' AND SUBSTRING(account_number, 8,1) = '0')
    OR  (base_dt = '2017-03-31' AND SUBSTRING(account_number, 8,1) = '3')
    OR  (base_dt = '2016-12-31' AND SUBSTRING(account_number, 8,1) = '9')
    
    
UPDATE Propensity_base
SET random2 = 1 
WHERE  (base_dt = '2017-01-31' AND SUBSTRING(account_number, 9,1) = '8')
    OR  (base_dt = '2017-06-30' AND SUBSTRING(account_number, 9,1) = '3')
    OR  (base_dt = '2017-02-28' AND SUBSTRING(account_number, 9,1) = '9')
    OR  (base_dt = '2017-05-31' AND SUBSTRING(account_number, 9,1) = '0')
    OR  (base_dt = '2017-04-30' AND SUBSTRING(account_number, 9,1) = '2')
    OR  (base_dt = '2017-03-31' AND SUBSTRING(account_number, 9,1) = '3')
    OR  (base_dt = '2016-12-31' AND SUBSTRING(account_number, 9,1) = '5')
    

UPDATE Propensity_base
SET random3 = 1 
WHERE  (base_dt = '2017-01-31' AND SUBSTRING(account_number, 7,1) = '7')
    OR  (base_dt = '2017-06-30' AND SUBSTRING(account_number, 7,1) = '6')
    OR  (base_dt = '2017-02-28' AND SUBSTRING(account_number, 7,1) = '5')
    OR  (base_dt = '2017-05-31' AND SUBSTRING(account_number, 7,1) = '4')
    OR  (base_dt = '2017-04-30' AND SUBSTRING(account_number, 7,1) = '3')
    OR  (base_dt = '2017-03-31' AND SUBSTRING(account_number, 7,1) = '2')
    OR  (base_dt = '2016-12-31' AND SUBSTRING(account_number, 7,1) = '1')



UPDATE Propensity_base
SET random4 = 1 
WHERE  (base_dt = '2017-01-31' AND SUBSTRING(account_number, 10,1) = '7')
    OR  (base_dt = '2017-06-30' AND SUBSTRING(account_number, 10,1) = '8')
    OR  (base_dt = '2017-02-28' AND SUBSTRING(account_number, 10,1) = '9')
    OR  (base_dt = '2017-05-31' AND SUBSTRING(account_number, 10,1) = '0')
    OR  (base_dt = '2017-04-30' AND SUBSTRING(account_number, 10,1) = '4')
    OR  (base_dt = '2017-03-31' AND SUBSTRING(account_number, 10,1) = '5')
    OR  (base_dt = '2016-12-31' AND SUBSTRING(account_number, 10,1) = '6')

commit

--- Checks 
--SELECT random1,random2,random3,random4, count(*) hits FROM Propensity_base group by random1,random2,random3,random4
------------------------------------------------------
-- Exploring DTV Packages 
/*
SELECT CASE WHEN a.DTV_product_holding  LIKE 'Variety%'  THEN 'Variety'  
            WHEN a.DTV_product_holding  LIKE 'Original%'  THEN 'Original'  
            WHEN a.DTV_product_holding  LIKE 'Sky Q%'  THEN 'Sky Q'  
            WHEN a.DTV_product_holding  LIKE 'Box Sets%'  THEN 'Box Sets'  
            ELSE a.DTV_product_holding END dtv_05
    , CASE WHEN b.DTV_product_holding  LIKE 'Variety%'  THEN 'Variety'  
            WHEN b.DTV_product_holding  LIKE 'Original%'  THEN 'Original'  
            WHEN b.DTV_product_holding  LIKE 'Sky Q%'  THEN 'Sky Q'  
            WHEN b.DTV_product_holding  LIKE 'Box Sets%'  THEN 'Box Sets'  
            ELSE b.DTV_product_holding END dtv_06
    , count(*) hits 

FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = 
WHERE  a.base_dt = '2017-05-31' AND dtv_05<>dtv_06
GROUP BY dtv_05, dtv_06
CREATE LF INDEX idx ON Propensity_base (DTV_product_holding)
*/

-------------  Updating Boxset target flag
------ The way the flags works is by checking if the accounts enabled the specific item (BB, Fibre, Boxset) in the next month snapshot while it doesn't have it in the current month
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0

SELECT a.account_number, a.base_dt
INTO #t1
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt =  '2017-01-31' 
WHERE  a.base_dt = '2016-12-31'
        AND (a.DTV_product_holding  LIKE 'Original%' OR a.DTV_product_holding  LIKE 'Variety%' ) 		--- Only upgrades from Variety or Original are counted 
        AND b.DTV_product_holding  LIKE 'Box Sets%'														--- Boxset enabled in the next month
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-02-28' 
WHERE  a.base_dt = '2017-01-31'
        AND (a.DTV_product_holding  LIKE 'Original%' OR a.DTV_product_holding  LIKE 'Variety%' ) 
        AND b.DTV_product_holding  LIKE 'Box Sets%'
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-03-31' 
WHERE  a.base_dt = '2017-02-28'
        AND (a.DTV_product_holding  LIKE 'Original%' OR a.DTV_product_holding  LIKE 'Variety%' ) 
        AND b.DTV_product_holding  LIKE 'Box Sets%'
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-04-30'
WHERE  a.base_dt =  '2017-03-31'
        AND (a.DTV_product_holding  LIKE 'Original%' OR a.DTV_product_holding  LIKE 'Variety%' ) 
        AND b.DTV_product_holding  LIKE 'Box Sets%'
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-05-31' 
WHERE  a.base_dt = '2017-04-30'
        AND (a.DTV_product_holding  LIKE 'Original%' OR a.DTV_product_holding  LIKE 'Variety%' ) 
        AND b.DTV_product_holding  LIKE 'Box Sets%'
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-06-30'
WHERE  a.base_dt = '2017-05-31' 
        AND (a.DTV_product_holding  LIKE 'Original%' OR a.DTV_product_holding  LIKE 'Variety%' ) 
        AND b.DTV_product_holding  LIKE 'Box Sets%'

COMMIT
CREATE HG INDEX id1 ON #t1 (account_number)
CREATE DATE INDEX id2 ON #t1 (base_dt)
COMMIT 

UPDATE Propensity_base
SET Up_Box_Sets = 1 
FROM Propensity_base AS a 
JOIN #t1 AS b ON a.base_dt = b.base_dt AND a.account_number = b.account_number 
COMMIT 

DROP TABLE #t1 
/*
-- Checking
SELECT base_dt, count(*) hits FROM #t1
GROUP BY base_dt

SELECT Up_Box_Sets,base_dt, random4 ,  count(*) hits 
FROM Propensity_base
GROUP BY Up_Box_Sets,base_dt, random4  
*/
----------------===========================***********************=======================---------------------
----------------===========================***********************=======================---------------------
----------------===========================***********************=======================---------------------
--- UPDATING BB Upsell flag

	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0
SELECT a.account_number, a.base_dt
INTO #t1
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt =  '2017-01-31' 
WHERE  a.base_dt = '2016-12-31'
        AND (a.bb_active = 0 OR a.bb_product_holding is NULL ) 						-- No BB subscription at the snapshot date 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)  NOT LIKE '%FIBRE%') 	-- BB non-Fibre package active at the end of next month
        
      
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-02-28' 
WHERE  a.base_dt = '2017-01-31'
        AND (a.bb_active = 0 OR a.bb_product_holding is NULL ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)  NOT LIKE '%FIBRE%') 
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-03-31' 
WHERE  a.base_dt = '2017-02-28'
        AND (a.bb_active = 0 OR a.bb_product_holding is NULL ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)  NOT LIKE '%FIBRE%') 
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-04-30'
WHERE  a.base_dt =  '2017-03-31'
        AND (a.bb_active = 0 OR a.bb_product_holding is NULL ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)  NOT LIKE '%FIBRE%') 
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-05-31' 
WHERE  a.base_dt = '2017-04-30'
        AND (a.bb_active = 0 OR a.bb_product_holding is NULL ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)  NOT LIKE '%FIBRE%') 
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-06-30'
WHERE  a.base_dt = '2017-05-31' 
        AND (a.bb_active = 0 OR a.bb_product_holding is NULL ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)  NOT LIKE '%FIBRE%') 

COMMIT
CREATE HG INDEX id1 ON #t1 (account_number)
CREATE DATE INDEX id2 ON #t1 (base_dt)
COMMIT 


UPDATE Propensity_base
SET Up_BB = 1 
FROM Propensity_base AS a 
JOIN #t1 AS b ON a.base_dt = b.base_dt AND a.account_number = b.account_number 
DROP TABLE #t1 
COMMIT 

/*
-- Checking
SELECT base_dt, count(*) hits FROM #t1
GROUP BY base_dt

SELECT Up_BB,base_dt, random1 ,  count(*) hits 
FROM Propensity_base
GROUP BY Up_BB,base_dt, random1  
*/
----------------===========================***********************=======================---------------------
----------------===========================***********************=======================---------------------
----------------===========================***********************=======================---------------------
--- UPDATING Fibre UPSell flag

	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0
SELECT a.account_number, a.base_dt
INTO #t1
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt =  '2017-01-31' 
WHERE  a.base_dt = '2016-12-31'
        AND (a.bb_active = 0 OR a.bb_product_holding is NULL ) 									-- No BB subscription at the snapshot date 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)   LIKE '%FIBRE%') 					-- BB Fibre package active at the end of next month
   
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-02-28' 
WHERE  a.base_dt = '2017-01-31'
        AND (a.bb_active = 0 OR a.bb_product_holding is NULL ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)   LIKE '%FIBRE%') 
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-03-31' 
WHERE  a.base_dt = '2017-02-28'
        AND (a.bb_active = 0 OR a.bb_product_holding is NULL ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)   LIKE '%FIBRE%') 
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-04-30'
WHERE  a.base_dt =  '2017-03-31'
        AND (a.bb_active = 0 OR a.bb_product_holding is NULL ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)   LIKE '%FIBRE%') 
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-05-31' 
WHERE  a.base_dt = '2017-04-30'
        AND (a.bb_active = 0 OR a.bb_product_holding is NULL ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)   LIKE '%FIBRE%') 
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-06-30'
WHERE  a.base_dt = '2017-05-31' 
        AND (a.bb_active = 0 OR a.bb_product_holding is NULL ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)   LIKE '%FIBRE%') 

COMMIT
CREATE HG INDEX id1 ON #t1 (account_number)
CREATE DATE INDEX id2 ON #t1 (base_dt)
COMMIT 


UPDATE Propensity_base
SET Up_Fibre = 1 
FROM Propensity_base AS a 
JOIN #t1 AS b ON a.base_dt = b.base_dt AND a.account_number = b.account_number 

DROP TABLE #t1
COMMIT 

/*
--	Checking 
SELECT base_dt, count(*) hits FROM #t1
GROUP BY base_dt
COMMIT 
SELECT Up_Fibre,base_dt, random1 ,  count(*) hits 
FROM Propensity_base
GROUP BY Up_Fibre,base_dt, random1  
*/

----------------===========================***********************=======================---------------------
----------------===========================***********************=======================---------------------
----------------===========================***********************=======================---------------------
--- UPDATING Fibre Regrade flag

	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0
SELECT a.account_number, a.base_dt
INTO #t1
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt =  '2017-01-31' 
WHERE  a.base_dt = '2016-12-31'
        AND (a.bb_active = 1 AND UPPER(a.bb_product_holding) NOT  LIKE '%FIBRE%' ) 	--- BB non-fibre subs active at the current snapshot
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)   LIKE '%FIBRE%') 		--- BB Fibre subs active at the next snapshot
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-02-28' 
WHERE  a.base_dt = '2017-01-31'
        AND (a.bb_active = 1 AND UPPER(a.bb_product_holding) NOT  LIKE '%FIBRE%' ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)   LIKE '%FIBRE%') 
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-03-31' 
WHERE  a.base_dt = '2017-02-28'
        AND (a.bb_active = 1 AND UPPER(a.bb_product_holding) NOT  LIKE '%FIBRE%' ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)   LIKE '%FIBRE%') 
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-04-30'
WHERE  a.base_dt =  '2017-03-31'
        AND (a.bb_active = 1 AND UPPER(a.bb_product_holding) NOT  LIKE '%FIBRE%' ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)   LIKE '%FIBRE%') 
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-05-31' 
WHERE  a.base_dt = '2017-04-30'
        AND (a.bb_active = 1 AND UPPER(a.bb_product_holding) NOT  LIKE '%FIBRE%' ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)   LIKE '%FIBRE%') 
UNION
SELECT a.account_number, a.base_dt
FROM Propensity_base AS a 
LEFT JOIN Propensity_base AS b ON a.account_number = b.account_number AND b.base_dt = '2017-06-30'
WHERE  a.base_dt = '2017-05-31' 
        AND (a.bb_active = 1 AND UPPER(a.bb_product_holding) NOT  LIKE '%FIBRE%' ) 
        AND (b.bb_active = 1 AND UPPER(b.bb_product_holding)   LIKE '%FIBRE%') 

COMMIT
CREATE HG INDEX id1 ON #t1 (account_number)
CREATE DATE INDEX id2 ON #t1 (base_dt)
COMMIT 


UPDATE Propensity_base
SET Regrade_Fibre = 1 
FROM Propensity_base AS a 
JOIN #t1 AS b ON a.base_dt = b.base_dt AND a.account_number = b.account_number 

COMMIT 


/*
--Checking
SELECT base_dt, count(*) hits FROM #t1
GROUP BY base_dt

SELECT Regrade_Fibre,base_dt, random2 ,  count(*) hits 
FROM Propensity_base
GROUP BY Regrade_Fibre,base_dt, random2  

*/

----------------===========================***********************=======================---------------------
----------------===========================***********************=======================---------------------
----------------===========================***********************=======================---------------------
----------------===========================***********************=======================---------------------

----- Eligiblility flags
--- Bosxset flag
UPDATE Propensity_base
SET Boxset_eligible = 1 
WHERE dtv_active = 1 
	AND (  a.DTV_product_holding  LIKE 'Original%' 
		OR a.DTV_product_holding  LIKE 'Variety%')														--- Variety or Original Bundle

--- BB Upgrade Flag 		
UPDATE Propensity_base
SET BB_eligible = 1 
WHERE  dtv_active = 1 																					--- Active DTV subs 
	AND bb_active = 0 																					--- No BB subs 
	AND Exchange_Status = 'ONNET'																		--- Bundled Exchange

--- Fibre Upgrade Flag 		
UPDATE Propensity_base
SET Fibre_UP_eligible = 1 
WHERE dtv_active = 1 																					--- Active DTV Subs 
		AND bb_active = 0 																				--- No active BB subs
		AND (skyfibre_enabled = 'Y' 																	--- Already in a Fibre Area
			OR skyfibre_estimated_enabled_date BETWEEN GETDATE() AND DATEADD (DAY, 28, GETDATE()) )		--- OR Soon to be in a Fibre area - 28 days 

--- Fibre Regrade Flag 		
UPDATE Propensity_base
SET Fibre_RE_eligible = 1 
WHERE dtv_active = 1 																					--- Active DTV Subs 
	AND bb_active = 1 																					--- Active BB subs 
	AND UPPER(bb_product_holding) NOT  LIKE '%FIBRE%'													--- Currenlty holding a Non-Fibre BB subs 
	AND ( 	skyfibre_enabled = 'Y' 																		--- Already in a Fibre Area
		OR 	skyfibre_estimated_enabled_date BETWEEN GETDATE() AND DATEADD (DAY, 28, GETDATE()) )		--- OR Soon to be in a Fibre area - 28 days 


----------------===========================***********************=======================---------------------
----						CREATING WORKING VIEW 
----------------===========================***********************=======================---------------------
		
create OR REPLACE  view "pitteloudj"."Propensity_base_boxset"
  as 
  select *  
            
            , DATEDIFF(DAY, DTV_Last_CusCan_Churn_Dt, base_dt) AS DTV_Last_cuscan_churn
            , DATEDIFF(DAY, DTV_Last_Activation_Dt, base_dt) AS DTV_Last_Activation
            , DATEDIFF(DAY, DTV_Curr_Contract_Intended_End_Dt, base_dt) AS DTV_Curr_Contract_Intended_End
            , DATEDIFF(DAY, DTV_Curr_Contract_Start_Dt, base_dt) AS DTV_Curr_Contract_Start
            , DATEDIFF(DAY, DTV_Last_SysCan_Churn_Dt, base_dt) AS DTV_Last_SysCan_Churn
            , DATEDIFF(DAY, Curr_Offer_Start_Dt_DTV, base_dt) AS Curr_Offer_Start_DTV
            , DATEDIFF(DAY, Curr_Offer_Actual_End_Dt_DTV, base_dt) AS Curr_Offer_Actual_End_DTV
            , DATEDIFF(DAY, DTV_1st_Activation_Dt, base_dt) AS DTV_1st_Activation
            , DATEDIFF(DAY, BB_Curr_Contract_Intended_End_Dt, base_dt) AS BB_Curr_Contract_Intended_End
            , DATEDIFF(DAY, BB_Curr_Contract_Start_Dt, base_dt) AS BB_Curr_Contract_Start
            , DATEDIFF(DAY, DTV_Last_Active_Block_Dt, base_dt) AS DTV_Last_Active_Block
            , DATEDIFF(DAY, DTV_Last_Pending_Cancel_Dt, base_dt) AS DTV_Last_Pending_Cancel
            , DATEDIFF(DAY, BB_Last_Activation_Dt, base_dt) AS BB_Last_Activation
			, 
from pitteloudj.Propensity_base
where Propensity_base_1.random4 = 1 				--- Random sample flag
and Propensity_base_1.Boxset_eligible = 1			--- Eligible Flag = 1 
		
		
		
		
		
		
		
----------------===========================***********************=======================---------------------
----------------===========================***********************=======================---------------------
----------------===========================***********************=======================---------------------

ALTER TABLE Propensity_base_1
ADD (num_sports_events INT DEFAULT NULL
	, sports_downgrade_date DATE DEFAULT NULL 
	, Sports_Tenure VARCHAR (20)  DEFAULT NULL 
	, movies_downgrade_date DATE DEFAULT NULL 
	, Movies_Tenure  VARCHAR (20)  DEFAULT NULL 
	)
-- PPV Sports Events
SELECT a.account_number
	, a.basE_dt
      ,sum(case when ppv_viewed_dt between dateadd(mm,-12,@endDate) and @endDate and ppv_service='EVENT'
                      --and  ppv_genre = 'BOXING, FOOTBALL or WRESTLING'
                      and ppv_cancelled_dt = '9999-09-09' then 1 else 0 end) as num_sport_events_12m
   INTO --drop table
        #temp_ppv
FROM Propensity_base_1 a
inner join CUST_PRODUCT_CHARGES_PPV b
on a.account_number=b.account_number
WHERE b.ppv_cancelled_dt='9999-09-09'
   and b.ppv_viewed_dt 	<= 	base_dt 
   and b.ppv_viewed_dt	>=	(base_dt-365)
GROUP BY a.account_number, a.basE_dt

UPDATE Propensity_base_1 as a
SET  a.num_sports_events = b.num_sport_events_12m
FROM #temp_ppv as b
WHERE a.account_number = b.account_number		AND a.basE_dt = b.base_dt


	
UPDATE Propensity_base_1
SET a.sports_downgrade_date = b.sports_downgrade_date 
	, a.Sports_Tenure = b.Sports_Tenure 
	, a.movies_downgrade_date = b.movies_downgrade_date 
	, a.Movies_Tenure = b.Movies_Tenure
FROM 	 Propensity_base_1 As a 
JOIN citeam.CUST_FCAST_WEEKLY_BASE AS b ON a.account_number = b.account_number 
                                    AND end_date BETWEEN DATEADD(DAY, -6, a.base_dt ) AND a.base_dt 
                                    
----------------===========================***********************=======================---------------------
----------------===========================***********************=======================---------------------

ALTER TABLE Propensity_base_1
ADD (OD_Last_3M  	INT DEFAULT NULL
	, OD_Last_12M 	INT DEFAULT NULL 
	, OD_Months_since_Last 	INT DEFAULT NULL 
	)
GO
	
	
SELECT a.account_number
	, base_dt
	, MAX(last_modified_dt) 		AS date_last_od
	, OD_Months_since_Last = CASE 	WHEN DATEDIFF(MONTH, date_last_od , base_dt ) > 15 THEN 16 ELSE  DATEDIFF(MONTH, date_last_od , base_dt )  END 
	, SUM(CASE WHEN cast(last_modified_dt AS DATE) BETWEEN dateadd(mm, - 3, base_dt) 	AND base_dt THEN 1 ELSE 0 END) AS OD_Last_3M
	, SUM(CASE WHEN cast(last_modified_dt AS DATE) BETWEEN dateadd(mm, - 12, base_dt) 	AND base_dt THEN 1 ELSE 0 END) AS OD_Last_12M
	
INTO #temp_od
FROM Propensity_base_1 a
INNER JOIN CUST_ANYTIME_PLUS_DOWNLOADS b ON a.account_number = b.account_number
WHERE b.last_modified_dt <= base_dt
GROUP BY a.account_number, base_dt
COMMIT 
CREATE HG Index id1 ON #temp_od(account_number )
CREATE DATE Index id2 ON #temp_od(base_dt)


UPDATE Propensity_base_1 a
SET a.OD_Last_3M = b.OD_Last_3M
	, a.OD_Last_12M = b.OD_Last_12M
	, a.OD_Months_since_Last = b.OD_Months_since_Last
FROM #temp_od b
WHERE a.account_number = b.account_number



----------------===========================***********************=======================---------------------

	
ALTER TABLE propensity_base_1 
ADD DTV_product_holding_recode VARCHAR(40)
GO
COMMIT 
UPDATE propensity_base_1 
SET DTV_product_holding_recode = CASE 	WHEN DTV_Product_Holding = 'Box Sets' THEN 'Box Sets'
										WHEN DTV_Product_Holding = 'Box Sets with Cinema' THEN 'Box Sets with Cinema'
										WHEN DTV_Product_Holding = 'Box Sets with Cinema 1' THEN 'Box Sets with Cinema'
										WHEN DTV_Product_Holding = 'Box Sets with Cinema 2' THEN 'Box Sets with Cinema'
										WHEN DTV_Product_Holding = 'Box Sets with Sports' THEN 'Box Sets with Sports'
										WHEN DTV_Product_Holding = 'Box Sets with Sports & Cinema' THEN 'Box Sets with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Box Sets with Sports & Cinema 1' THEN 'Box Sets with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Box Sets with Sports & Cinema 2' THEN 'Box Sets with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Box Sets with Sports 1' THEN 'Box Sets with Sports'
										WHEN DTV_Product_Holding = 'Box Sets with Sports 1 & Cinema' THEN 'Box Sets with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Box Sets with Sports 2' THEN 'Box Sets with Sports'
										WHEN DTV_Product_Holding = 'Box Sets with Sports 2 & Cinema' THEN 'Box Sets with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Original' THEN 'Original'
										WHEN DTV_Product_Holding = 'Original (Legacy 2017)' THEN 'Original'
										WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Cinema' THEN 'Original with Cinema'
										WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Sports' THEN 'Original with Sports'
										WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Sports & Cinema' THEN 'Original with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Sports 1' THEN 'Original with Sports'
										WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Sports 1 & Cinema' THEN 'Original with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Sports 2' THEN 'Original with Sports'
										WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Sports 2 & Cinema' THEN 'Original with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Original (Legacy)' THEN 'Original'
										WHEN DTV_Product_Holding = 'Original (Legacy) with Cinema' THEN 'Original with Cinema'
										WHEN DTV_Product_Holding = 'Original (Legacy) with Cinema 1' THEN 'Original with Cinema'
										WHEN DTV_Product_Holding = 'Original (Legacy) with Cinema 2' THEN 'Original with Cinema'
										WHEN DTV_Product_Holding = 'Original (Legacy) with Sports' THEN 'Original with Sports'
										WHEN DTV_Product_Holding = 'Original (Legacy) with Sports & Cinema' THEN 'Original with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Original (Legacy) with Sports & Cinema 1' THEN 'Original with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Original (Legacy) with Sports 1' THEN 'Original with Sports'
										WHEN DTV_Product_Holding = 'Original (Legacy) with Sports 1 & Cinema' THEN 'Original with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Original (Legacy) with Sports 1 & Cinema 1' THEN 'Original with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Original (Legacy) with Sports 1 & Cinema 2' THEN 'Original with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Original (Legacy) with Sports 2' THEN 'Original with Sports'
										WHEN DTV_Product_Holding = 'Original (Legacy) with Sports 2 & Cinema' THEN 'Original with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Original with Cinema' THEN 'Original with Cinema'
										WHEN DTV_Product_Holding = 'Original with Sports' THEN 'Original with Sports'
										WHEN DTV_Product_Holding = 'Original with Sports & Cinema' THEN 'Original with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Original with Sports 1' THEN 'Original with Sports'
										WHEN DTV_Product_Holding = 'Original with Sports 1 & Cinema' THEN 'Original with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Original with Sports 2' THEN 'Original with Sports'
										WHEN DTV_Product_Holding = 'Original with Sports 2 & Cinema' THEN 'Original with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Sky Q' THEN 'Sky Q'
										WHEN DTV_Product_Holding = 'Sky Q with Cinema' THEN 'Sky Q with Cinema'
										WHEN DTV_Product_Holding = 'Sky Q with Sports' THEN 'Sky Q with Sports'
										WHEN DTV_Product_Holding = 'Sky Q with Sports & Cinema' THEN 'Sky Q with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Sky Q with Sports 1' THEN 'Sky Q with Sports'
										WHEN DTV_Product_Holding = 'Sky Q with Sports 2' THEN 'Sky Q with Sports'
										WHEN DTV_Product_Holding = 'Variety' THEN 'Variety'
										WHEN DTV_Product_Holding = 'Variety with Cinema' THEN 'Variety with Cinema'
										WHEN DTV_Product_Holding = 'Variety with Cinema 1' THEN 'Variety with Cinema'
										WHEN DTV_Product_Holding = 'Variety with Cinema 2' THEN 'Variety with Cinema'
										WHEN DTV_Product_Holding = 'Variety with Sports' THEN 'Variety with Sports'
										WHEN DTV_Product_Holding = 'Variety with Sports & Cinema' THEN 'Variety with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Variety with Sports & Cinema 1' THEN 'Variety with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Variety with Sports & Cinema 2' THEN 'Variety with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Variety with Sports 1' THEN 'Variety with Sports'
										WHEN DTV_Product_Holding = 'Variety with Sports 1 & Cinema' THEN 'Variety with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Variety with Sports 1 & Cinema 1' THEN 'Variety with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Variety with Sports 1 & Cinema 2' THEN 'Variety with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Variety with Sports 2' THEN 'Variety with Sports'
										WHEN DTV_Product_Holding = 'Variety with Sports 2 & Cinema' THEN 'Variety with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Variety with Sports 2 & Cinema 1' THEN 'Variety with Sports & Cinema'
										WHEN DTV_Product_Holding = 'Variety with Sports 2 & Cinema 2' THEN 'Variety with Sports & Cinema'
											ELSE DTV_Product_Holding  END 
	
----------------===========================***********************=======================---------------------
	
UPDATE propensity_base_1 
SET curr_Offer_Length_DTV	= Case WHEN curr_Offer_Length_DTV	IN('900M',	'921M',	'941M',	'961M',	'981M',
																	'901M',	'922M',	'942M',	'962M',	'982M',
																	'902M',	'923M',	'943M',	'963M',	'983M',
																	'903M',	'924M',	'944M',	'964M',	'984M',
																	'904M',	'925M',	'945M',	'965M',	'985M',
																	'905M',	'926M',	'946M',	'966M',	'986M',
																	'906M',	'927M',	'947M',	'967M',	'987M',
																	'907M',	'928M',	'948M',	'968M',	'988M',
																	'908M',	'929M',	'949M',	'969M',	'989M',
																	'909M',	'930M',	'950M',	'970M',	'990M',
																	'910M',	'931M',	'951M',	'971M',	'991M',
																	'911M',	'932M',	'952M',	'972M',	'992M',
																	'912M',	'933M',	'953M',	'973M',	'993M',
																	'913M',	'934M',	'954M',	'974M',	'994M',
																	'914M',	'935M',	'955M',	'975M',	'995M',
																	'915M',	'936M',	'956M',	'976M',	'996M',
																	'916M',	'937M',	'957M',	'977M',	'997M',
																	'917M',	'938M',	'958M',	'978M',	'998M',
																	'918M',	'939M',	'959M',	'979M',	'999M',
																	'919M',	'940M',	'960M',	'980M',	'1000M',
																	'920M',				
																	)
                            THEN '999+'
                                WHEN curr_Offer_Length_DTV	IN('14M',	'36M',	'58M',	'80M',
																'15M',	'37M',	'59M',	'81M',
																'16M',	'38M',	'60M',	'82M',
																'17M',	'39M',	'61M',	'83M',
																'18M',	'40M',	'62M',	'84M',
																'19M',	'41M',	'63M',	'85M',
																'20M',	'42M',	'64M',	'86M',
																'21M',	'43M',	'65M',	'87M',
																'22M',	'44M',	'66M',	'88M',
																'23M',	'45M',	'67M',	'89M',
																'24M',	'46M',	'68M',	'90M',
																'25M',	'47M',	'69M',	'91M',
																'26M',	'48M',	'70M',	'92M',
																'27M',	'49M',	'71M',	'93M',
																'28M',	'50M',	'72M',	'94M',
																'29M',	'51M',	'73M',	'95M',
																'30M',	'52M',	'74M',	'96M',
																'31M',	'53M',	'75M',	'97M',
																'32M',	'54M',	'76M',	'98M',
																'33M',	'55M',	'77M',	'99M',
																'34M',	'56M',	'78M',	'100M',
																'35M',	'57M',	'79M',	'101M',
																) THEN '>13M'
                                    ELSE curr_Offer_Length_DTV	END 	
									
									
----------------===========================***********************=======================---------------------
									

									
UPDATE propensity_base_1 
SET age = 2017-age WHERE age BETWEEN 1916 AND 1999

UPDATE propensity_base_1 
SET age = 1917-age WHERE age BETWEEN 1816 AND 1899

UPDATE propensity_base_1 
SET age = 1117-age WHERE age BETWEEN 1016 AND 1099

UPDATE propensity_base_1 
SET age = NULL WHERE age BETWEEN 4 AND 17

UPDATE propensity_base_1 
SET age = CASE  WHEN h_age_fine ='18-25' THEN 22
                WHEN h_age_fine ='26-30' THEN 28
                WHEN h_age_fine ='31-35' THEN 33
                WHEN h_age_fine ='36-40' THEN 37
                WHEN h_age_fine ='41-45' THEN 43
                WHEN h_age_fine ='46-50' THEN 48
                WHEN h_age_fine ='51-55' THEN 53
                WHEN h_age_fine ='56-60' THEN 58
                WHEN h_age_fine ='61-65' THEN 63
                WHEN h_age_fine ='66-70' THEN 68
                WHEN h_age_fine ='71-75' THEN 73
                WHEN h_age_fine ='76+' THEN 80
                ELSE NULL END
WHERE age NOT BETWEEN 18 AND 101 



----------------===========================***********************=======================---------------------
---------- Removing BB orders in the last 30D 
UPDATE Propensity_Base_1
SET Up_BB = 0 
    , Up_Fibre = 0
WHERE (  Order_BB_Unlimited_Added_In_Last_30d >= 1
                    OR  Order_BB_Lite_Added_In_Last_30d >= 1
                    OR  Order_BB_Fibre_Cap_Added_In_Last_30d >= 1
                    OR  Order_BB_Fibre_Unlimited_Added_In_Last_30d >= 1 
                    OR  Order_BB_Fibre_Unlimited_Pro_Added_In_Last_30d >=1) 

					
UPDATE Propensity_Base_1
SET Regrade_Fibre = 0 
    
WHERE (Order_BB_Fibre_Cap_Added_In_Last_30d >= 1
                    OR  Order_BB_Fibre_Unlimited_Added_In_Last_30d >= 1 
                    OR  Order_BB_Fibre_Unlimited_Pro_Added_In_Last_30d >=1) 

					
					
					
					

create OR REPLACE  view "pitteloudj"."Propensity_base_BB"
  as 
  select *  
            
            , DATEDIFF(DAY, DTV_Last_CusCan_Churn_Dt, base_dt) AS DTV_Last_cuscan_churn
            , DATEDIFF(DAY, DTV_Last_Activation_Dt, base_dt) AS DTV_Last_Activation
            , DATEDIFF(DAY, DTV_Curr_Contract_Intended_End_Dt, base_dt) AS DTV_Curr_Contract_Intended_End
            , DATEDIFF(DAY, DTV_Curr_Contract_Start_Dt, base_dt) AS DTV_Curr_Contract_Start
            , DATEDIFF(DAY, DTV_Last_SysCan_Churn_Dt, base_dt) AS DTV_Last_SysCan_Churn
            , DATEDIFF(DAY, Curr_Offer_Start_Dt_DTV, base_dt) AS Curr_Offer_Start_DTV
            , DATEDIFF(DAY, Curr_Offer_Actual_End_Dt_DTV, base_dt) AS Curr_Offer_Actual_End_DTV
            , DATEDIFF(DAY, DTV_1st_Activation_Dt, base_dt) AS DTV_1st_Activation
            , DATEDIFF(DAY, BB_Curr_Contract_Intended_End_Dt, base_dt) AS BB_Curr_Contract_Intended_End
            , DATEDIFF(DAY, BB_Curr_Contract_Start_Dt, base_dt) AS BB_Curr_Contract_Start
            , DATEDIFF(DAY, DTV_Last_Active_Block_Dt, base_dt) AS DTV_Last_Active_Block
            , DATEDIFF(DAY, DTV_Last_Pending_Cancel_Dt, base_dt) AS DTV_Last_Pending_Cancel
            , DATEDIFF(DAY, BB_Last_Activation_Dt, base_dt) AS BB_Last_Activation
			, 
from pitteloudj.Propensity_base_1
where Propensity_base_1.random1 = 1 				--- Random sample flag
and Propensity_base_1.BB_eligible = 1			--- Eligible Flag = 1 
AND country = 'UK'	
		
							
							