/* *****************************


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                                                 Adsmart - CR 111 Asia Package ownership 
		
		Description:
			Flag accounts that have or had have the Asia Package 
		Date: 20-01-2015
		Lead: 	Jose Pitteloud		
		Coded by: Jose Pitteloud
	Sections:
			
			
*********************************/

------------------------------------------------------------------------------------------------------
SELECT account_number
	, 'Has Asia Pack' AS Asia
INTO #t1
FROM CUST_SUBS_HIST 
WHERE subscription_type = 'ENHANCED' 
	and subscription_sub_type = 'SKYASIA'
   and status_code in ('AC','AB','PC')                          --Active Status Codes
   and effective_from_dt <= getdate()                       
   and effective_to_dt > getdate()							
   and effective_from_dt<>effective_to_dt  

 COMMIT
CREATE HG INDEX cwd  ON #t1(account_number)
INSERT INTO #t1
SELECT DISTINCT account_number
	, 'Has had Asia Pack in the past but not now' AS Asia
FROM CUST_SUBS_HIST 
WHERE subscription_type = 'ENHANCED' 
	and subscription_sub_type = 'SKYASIA'
	AND status_code NOT in ('AC','AB','PC','PA') 
	AND account_number NOT IN (SELECT account_number FROM #t1)
COMMIT

INSERT INTO #t1
SELECT DISTINCT account_number
	, 'Has never had Asia Pack' AS Asia
FROM CUST_SUBS_HIST 
WHERE subscription_type = 'ENHANCED' 
	and subscription_sub_type = 'SKYASIA'
	AND status_code in ('PA') 
	AND account_number NOT IN (SELECT account_number FROM #t1)
commit
   
UPDATE ###ADsmart###
SET ASIA = CASE WHEN Asia IS NOT NULL  THEN Asia 
				ELSE 'Has never had Asia Pack'
				END
FROM ###ADsmart### as a 
LEFT JOIN #t1  as b ON a.account_number = b.account_number 


----------------------------- QA 
/*

SELECT Asia, count(*) hits
FROM #t1 as a
RIGHT JOIN adsmart as b ON a.account_number = b.account_number
GROUP BY Asia


Asia										|  hits
============================================|===========
Has Asia Pack								|  34510
Has had Asia Pack in the past but not now	|  2850
Has never had Asia Pack						|  9348953
*/