/* *****************************


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ 
             ?$$$,      I$$$ $$$$. $$$$  $$$= 
              $$$$$$$$= I$$$$$$$    $$$$.$$$  
                  :$$$$~I$$$ $$$$    $$$$$$   
               ,.   $$$+I$$$  $$$$    $$$$=  
              $$$$$$$$$ I$$$   $$$$   .$$$   
                                      $$$    
                                     $$$    
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                                                 Adsmart - Quarterly update - New Attributes
                
                Description:
                        Sky GO type of device
                        TABLES:
                        adsmart TABLE                                                                                   pitteloudj.pm_quarterly_release_1_adsmart_region
                        adsmartable flag table                                                                 			PME06.adsmartables
                        
                Date: 28-04-2015
                Lead:   Jose Pitteloud          
                Coded by: Jose Pitteloud / Paolo Menna
                
        Sections:       0.1 - ADSMART DUMMY DATA --- TESTING PURPOSES
                                1 - REGION
                        
                        
*********************************/

--------------------------------------------
-- ADSMARTABLE ACCOUNTS ---- \Git_repository\Vespa\Data Governance\Data Definitions\Adsmart\AdsmartableAccounts v2 0.sql
--------------------------------------------

------------------------------------------------------------------------------------------------------
-- ADSMART DUMMY DATA --- TESTING PURPOSES
------------------------------------------------------------------------------------------------------

/* ********************************************************
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        drop table pm_quarterly_release_1_adsmart_region;

        SELECT top 10000 account_number, cb_key_household
                , cast(NULL AS VARCHAR(15)) AS BARB_TV_REGIONS                 -- type and length not set up in the definition (excel)
		INTO pm_quarterly_release_1_adsmart_region
        FROM adsmart

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
***********************************************************/

SELECT 
		  a.cb_address_postcode_area
		, a.account_number
		, b.barb_desc_itv
INTO #t_region
FROM ADSMART AS a 
LEFT JOIN BARB_TV_REGIONS AS b ON TRIM(a.cb_address_postcode) = TRIM(b.cb_address_postcode)
WHERE account_number IS NOT NULL
	
COMMIT

CREATE HG INDEX hg1 ON #t_region(account_number)
CREATE LF INDEX hg2 ON #t_region(cb_address_postcode_area)
CREATE LF INDEX hg3 ON #t_region(barb_desc_itv)
COMMIT

UPDATE pm_quarterly_release_1_adsmart_region
SET BARB_TV_REGIONS = CASE 	WHEN cb_address_postcode_area IN ('JE','GY') THEN 'Channel Islands'
							WHEN barb_desc_itv LIKE 'Meridian (exc. Channel Islands)' THEN 'Meridian'
							WHEN barb_desc_itv IN ('Central Scotland', 'North Scotland')  THEN 'Scotland'
							ELSE COALESCE (barb_desc_itv, 'Unknown') END 
FROM pm_quarterly_release_1_adsmart_region AS a 							
LEFT JOIN #t_region AS b ON a.account_number = b.account_number 
COMMIT 

/* **************************************************************														
		***************		QA		**********************
SELECT 
	BARB_TV_REGIONS
	, COUNT(*) 
FROM pm_quarterly_release_1_adsmart_region
GROUP BY  
	BARB_TV_REGIONS

	
	----- COUNTS  BASED  ON THE DUMMY TABLE
	BARB_TV_REGIONS	COUNT()
		Border			206
		East Of England	804
		HTV Wales		571
		HTV West		341
		London			1835
		Meridian		984
		Midlands		1288
		North East		394
		North West		915
		Scotland		936
		South West		268
		Ulster			365
		Unknown			124
		Yorkshire		969

******************************************************************/