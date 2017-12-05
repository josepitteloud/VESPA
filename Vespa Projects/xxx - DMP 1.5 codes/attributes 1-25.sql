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
**Project Name:                                                 Adsmart - Drop 3 - External Attributes
		
		Description:
			New Attributes to update the Adsmart table
				
		Lead: 	Jose Pitteloud		
		Coded by: Paolo Menna
	Sections: 	1	Sky ID Type
				2	Account Contract Ended
				3	Reason for customer leaving Sky
				4	Protect
				5	Grow
				6	Support
				7	Maintain
				8	Simple segmentation level 3
				9	Rental Usage over last 12 months
				10	Newspaper Readership
				11	Sky AdSmart Postcode Area
				12	Age Group
				13	Mobile on Contract
				14	Mobile Average Bill
				15	Type of Shopper
				16	Public sector Mosaic 2014
				17	Number of cars in HH
				18	Senior Decision Maker
				19	Pet Ownership
				20	Car Insurance renewal
				21	Mosaic 2014 Groups						
				22	Mosaic 2014 types						
				23	Lifestage Band
				24	Affluence Band
				25	Financial Outlook

			
		
*********************************/


--------------------------------------------------------------------------------------
------- CREATING THE MAIN TABLE WITH SAM_REGISTRANT ACCOUNTS
--------------------------------------------------------------------------------------
IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER('DMP_accounts_attributes_samID')) DROP TABLE  DMP_accounts_attributes_samID
SELECT 
	  account_number
	, samprofileid 					
	, cb_key_household					
	, COALESCE(user_type, 'Unknown') 			AS sky_ID_type
        ,cast('Unknown' as varchar(19)) 	AS Account_Contract_Ended
        ,cast('Unknown' as varchar(255)) 	AS Reason_for_customer_leaving_Sky		-- needs mapping
        ,cast('Unknown' as varchar(30)) 	AS Simple_segmentation_level_2
        ,cast('Unknown' as varchar(30)) 	as Rental_Usage_over_last_12_months
        ,cast('Unknown' as varchar(30)) 	as Newspaper_Readership
        ,cast('Unknown' as varchar(7)) 		as Sky_AdSmart_Postcode_Area
        ,cast('Unknown' as varchar(100)) 	as Age_Group
        ,cast('Unknown' as varchar(100)) 	as Mobile_on_Contract
        ,cast('Unknown' as varchar(100)) 	as Mobile_Average_Bill
        ,cast('Unknown' as varchar(100)) 	as	Type_of_Shopper
        ,cast('Unknown' as varchar(100)) 	as Public_sector_Mosaic_2014
        ,cast('Unknown' as varchar(100)) 	as Number_of_cars_in_HH
        ,cast('Unknown' as varchar(100)) 	as Senior_Decision_Maker
        ,cast('Unknown' as varchar(100)) 	as Pet_Ownership
        ,cast('Unknown' as varchar(100)) 	as Car_Insurance_renewal
        ,cast('Unknown' as varchar(100)) 	as Mosaic_2014_Groups
        ,cast('Unknown' as varchar(100)) 	as Mosaic_2014_types
        ,cast('Unknown' as varchar(100)) 	as Lifestage_Band
        ,cast('Unknown' as varchar(15)) 	as Affluence_Band
        ,cast('Unknown' as varchar(25)) 	as Financial_Outlook
INTO DMP_accounts_attributes_samID
FROM SAM_REGISTRANT 
WHERE x_user_type in ('Primary', 'Secondary','primary','secondary') 
	AND marked_as_deleted = 'N'
	
	
CREATE HG INDEX id1 ON DMP_accounts_attributes_samID(account_number)

COMMIT 


--------------------------------------------------------------------------------------
---- Sky ID Type
--------------------------------------------------------------------------------------


UPDATE DMP_accounts_attributes_samID a
SET a.sky_ID_type = CASE WHEN user_type = 'guest' THEN 'Guest'
						WHEN user_type = 'primary' THEN 'Primary'
						WHEN user_type = 'secondary' THEN 'Secondary'
						WHEN user_type = 'userGuest' THEN 'User Guest'
						WHEN user_type = 'userPrimary' THEN 'User Primary'
						WHEN user_type = 'userSecondary' THEN 'User Secondary'
						end
FROM sam_registrant b
WHERE a.account_number = b.account_number


--------------------------------------------------------------------------------------
---- Account Contract Ended
--------------------------------------------------------------------------------------

SELECT 	  account_number
        , End_dt_calc
        , ROW_NUMBER() over (partition by account_number order by End_dt_calc desc, cb_row_id) as rankk
INTO #dmp_account_contract_ended
FROM Cust_contract_agreements
WHERE account_number IN (SELECT account_number FROM DMP_accounts_attributes_samID)

COMMIT
CREATE HG INDEX fqwe12 ON #dmp_account_contract_ended (account_number)
DELETE FROM #dmp_account_contract_ended WHERE rankk <> 1 
COMMIT 

UPDATE DMP_accounts_attributes_samID a
SET a.Account_Contract_Ended = case when DATEDIFF(dd, End_dt_calc, today())  between 0 and 30 	then '0 - 30 days back'
									when DATEDIFF(dd, End_dt_calc, today())  between 31 and 60 	then '31 - 60 days back'
									when DATEDIFF(dd, End_dt_calc, today())  between 61 and 90 	then '61 - 90 days back'
									when DATEDIFF(dd, End_dt_calc, today())  between 91 and 120 	then '91 - 120 days back'
									when DATEDIFF(dd, End_dt_calc, today())  between 121 and 180 	then '121 - 180 days back'
									when DATEDIFF(dd, End_dt_calc, today())  between 181 and 270 	then '181 - 270 days back'
									when DATEDIFF(dd, End_dt_calc, today())  between 271 and 365 	then '271 - 365 days back'
									when DATEDIFF(dd, End_dt_calc, today())  >= 366 				then '1 year +'
									end
FROM #dmp_account_contract_ended b 
WHERE a.account_number = b.account_number
	AND rankk = 1
	AND End_dt_calc IS NOT NULL 
	AND DATEDIFF(dd, End_dt_calc, today()) >= 31

DROP TABLE #dmp_account_contract_ended

------------------------------------------------------------------------------------
------- ACCT_LATEST_CANCEL_ATTEMPT_REASON
------------------------------------------------------------------------------------ MAPPING NEEDED!!!!!!! -- NO MAP NEEDED
UPDATE DMP_accounts_attributes_samID a
SET a.Reason_for_customer_leaving_Sky = b.ACCT_LATEST_CANCEL_ATTEMPT_REASON
FROM Cust_single_account_view b
WHERE a.account_number = b.account_number
AND b.ACCT_LATEST_CANCEL_ATTEMPT_REASON IS NOT NULL 

------------------------------------------------------------------------------------
-------	SIMPLE_SEGMENTATION Level 2
------------------------------------------------------------------------------------
SELECT a.account_number
	, segment_lev2
	, ROW_NUMBER() over (partition by a.account_number order by OBSERVATION_DATE desc) as rankk  
INTO #simple
FROM zubizaa.SIMPLE_SEGMENTATION_HISTORY AS b
JOIN DMP_accounts_attributes_samID AS a ON a.account_number = b.account_number

COMMIT
DELETE FROM #simple WHERE rankk <> 1
CREATE HG INDEX sef1 ON #simple(account_number)
COMMIT
------------------------------------------------------------------------------------
UPDATE DMP_accounts_attributes_samID a
SET a.Simple_segmentation_level_2 = segment_lev2
FROM #simple as b
WHERE a.account_number = b.account_number
------------------------------------------------------------------------------------
------- VESPA HOUSEHOLD FIELDS
------------------------------------------------------------------------------------
UPDATE DMP_accounts_attributes_samID 
SET   a.Newspaper_Readership 		= b.NEWSPAPER_READERSHIP
	, a.Sky_AdSmart_Postcode_Area 	= b.POSTCODE_AREA
	, a.Mobile_on_Contract 			= b.MOBILE_CONTRACT
	, a.Age_Group 					= b.AGE_GROUP
	, a.Mobile_Average_Bill 		= b.MOBILE_AVG_MONTHLY_BILL
	, a.Type_of_Shopper 			= b.TYPE_OF_SHOPPER
	, a.Public_sector_Mosaic_2014 	= b.PUBLIC_SECTOR_MOSAIC
	, a.Number_of_cars_in_HH 		= b.NUMBER_OF_CARS
	, a.Senior_Decision_Maker 		= b.SENIOR_DECISION_MAKER
	, a.Pet_Ownership 				= b.PET_OWNERSHIP
	, a.Car_Insurance_renewal 		= b.BREAKDOWN_RENWAL_MONTH	-- misspelled!!
	, a.Mosaic_2014_Groups 			= b.mosaic_2014_groups
	, a.Mosaic_2014_types			= b.mosaic_2014_types
	, a.Lifestage_Band 				= b.FAMILY_LIFESTAGE
	, a.Affluence_Band 				= b.AFFLUENCE_BANDS
	, a.Financial_Outlook 			= b.FINANCIAL_OUTLOOK
FROM DMP_accounts_attributes_samID AS a
JOIN vespa_household b ON a.account_number = b.account_number

