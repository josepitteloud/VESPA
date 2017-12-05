------------------------------------------------------------------------------------------------------------
--------------------------------------------- BARB_REGIONS -------------------------------------------------
------------------------------------------------------------------------------------------------------------

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

	
UPDATE 	#t_region
SET barb_desc_itv = CASE WHEN UPPER (barb_desc_itv) LIKE UPPER('Border')				THEN  'Border'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('Channel Islands')	THEN  'Channel Islands'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('East of England')	THEN  'East of England'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('East-of-England')	THEN  'East of England'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('HTV Wales')			THEN  'HTV Wales'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('HTV-Wales')			THEN  'HTV Wales'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('HTV West')			THEN  'HTV West'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('HTV-West')			THEN  'HTV West'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('London')				THEN  'London'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('Meridian')			THEN  'Meridian'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('Midlands')			THEN  'Midlands'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('North East')			THEN  'North East'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('North-East')			THEN  'North East'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('North West')			THEN  'North West'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('North-West')			THEN  'North West'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('Scotland')			THEN  'Scotland'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('South West')			THEN  'South West'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('South-West')			THEN  'South West'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('Ulster')				THEN  'Ulster'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('Unknown')			THEN  'Unknown'
					WHEN UPPER (barb_desc_itv) LIKE UPPER('Yorkshire')			THEN  'Yorkshire'
					ELSE barb_desc_itv
					END 
COMMIT 

UPDATE pm_quarterly_release_1_adsmart_region		-- REPLACE by ADSMART FINAL TABLE 
SET BARB_TV_REGIONS = CASE 	WHEN cb_address_postcode_area IN ('JE','GY') THEN 'Channel Islands'
							WHEN barb_desc_itv LIKE 'Meridian (exc. Channel Islands)' THEN 'Meridian'
							WHEN barb_desc_itv IN ('Central Scotland', 'North Scotland')  THEN 'Scotland'
							ELSE COALESCE (barb_desc_itv, 'Unknown') END 
FROM pm_quarterly_release_1_adsmart_region AS a 	-- REPLACE by ADSMART FINAL TABLE 						
LEFT JOIN #t_region AS b ON a.account_number = b.account_number 
COMMIT 

/* **************************************************************************************
QA
BARB_TV_REGIONS	hits
Border			205
Channel Islands	56
East of England	818
HTV Wales		581
HTV West		336
London			1826
Meridian		994
Midlands		1294
North East		387
North West		925
Scotland		926
South West		269
Ulster			364
Unknown			66
Yorkshire		953
