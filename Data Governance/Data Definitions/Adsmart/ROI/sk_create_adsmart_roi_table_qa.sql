/*  Title       : ADSMART  Table Build Process
    Created by  : Jose PItteloud
    Date        : Jan 2016
    Description : This is a sql to build the ROI ADSMART  Table FROM the CUST_SINGLE_ACCOUNT view AND other tables.

	QA by		: Paolo Menna
    Modified by :
    Changes     : 


*/


/****************************************************************************************
 *                                                                                      *
 *                          SAV sourced attributes                                      *
 *                                                                                      *
 ***************************************************************************************/

--INSERT INTO ADSMART 
 --(
     ----################################## To be added to the main insert section ##########################
     , Residency
     , ROI_COUNTY
	 , ROI_BROADBAND_STATUS
	 , ROI_REGION_LEVEL_4 
 --)
 
	--############################ To be added to the main SELECT in the table population
	--SELECT 	
        , Residency = CASE  WHEN PTY_COUNTRY_CODE LIKE 'GBR' THEN 'UK'
                            WHEN PTY_COUNTRY_CODE LIKE 'IRL' THEN 'ROI'
                            ELSE 'Unknown' END
        , CASE	WHEN cb_address_status = '1' and roi_address_match_source is not null and cb_address_county is not null THEN cb_address_county
                WHEN UPPER(pty_county_raw) like '%DUBLIN%' THEN 'DUBLIN'
                WHEN UPPER(pty_county_raw) like '%WESTMEATH%' THEN 'WESTMEATH'
                WHEN UPPER(pty_county_raw) like '%CARLOW%' THEN 'CARLOW'
                WHEN UPPER(pty_county_raw) like '%CAVAN%' THEN 'CAVAN'
                WHEN UPPER(pty_county_raw) like '%CLARE%' THEN 'CLARE'
                WHEN UPPER(pty_county_raw) like '%CORK%' THEN 'CORK'
                WHEN UPPER(pty_county_raw) like '%DONEGAL%' THEN 'DONEGAL'
                WHEN UPPER(pty_county_raw) like '%GALWAY%' THEN 'GALWAY'
                WHEN UPPER(pty_county_raw) like '%KERRY%' THEN 'KERRY'
                WHEN UPPER(pty_county_raw) like '%KILDARE%' THEN 'KILDARE'
                WHEN UPPER(pty_county_raw) like '%KILKENNY%' THEN 'KILKENNY'
                WHEN UPPER(pty_county_raw) like '%LAOIS%' THEN 'LAOIS'
                WHEN UPPER(pty_county_raw) like '%LEITRIM%' THEN 'LEITRIM'
                WHEN UPPER(pty_county_raw) like '%LIMERICK%' THEN 'LIMERICK'
                WHEN UPPER(pty_county_raw) like '%LONGFORD%' THEN 'LONGFORD'
                WHEN UPPER(pty_county_raw) like '%LOUTH%' THEN 'LOUTH'
                WHEN UPPER(pty_county_raw) like '%MAYO%' THEN 'MAYO'
                WHEN UPPER(pty_county_raw) like '%MEATH%' THEN 'MEATH'
                WHEN UPPER(pty_county_raw) like '%MONAGHAN%' THEN 'MONAGHAN'
                WHEN UPPER(pty_county_raw) like '%OFFALY%' THEN 'OFFALY'
                WHEN UPPER(pty_county_raw) like '%ROSCOMMON%' THEN 'ROSCOMMON'
                WHEN UPPER(pty_county_raw) like '%SLIGO%' THEN 'SLIGO'
                WHEN UPPER(pty_county_raw) like '%TIPPERARY%' THEN 'TIPPERARY'
                WHEN UPPER(pty_county_raw) like '%WATERFORD%' THEN 'WATERFORD'
                WHEN UPPER(pty_county_raw) like '%WEXFORD%' THEN 'WEXFORD'
                WHEN UPPER(pty_county_raw) like '%WICKLOW%' THEN 'WICKLOW'
                WHEN pty_county_raw is null and UPPER(pty_town_raw) like '%DUBLIN%' THEN 'DUBLIN'
                else 'Unknown'
			END as ROI_County
		, CASE  WHEN sav.prod_active_broadband_package_desc IS NULL 		AND PROD_EARLIEST_BROADBAND_ACTIVATION_DT  IS NULL 						THEN 'Never had BB '
				WHEN PROD_LATEST_BROADBAND_STATUS_CODE in ('PO','SC','CN')  AND PROD_LATEST_BROADBAND_ACTIVATION_DT IS NOT NULL AND DATEDIFF(dd,PROD_LATEST_BROADBAND_STATUS_START_DT,TODAY()) <=365  				THEN 'No BB, downgraded in last 0 - 12 months'
				WHEN PROD_LATEST_BROADBAND_STATUS_CODE in ('PO','SC','CN')  AND PROD_LATEST_BROADBAND_ACTIVATION_DT IS NOT NULL AND DATEDIFF(dd,PROD_LATEST_BROADBAND_STATUS_START_DT,TODAY()) BETWEEN 366 AND 730  THEN 'No BB, downgraded in last 12- 24 mths'
				WHEN PROD_LATEST_BROADBAND_STATUS_CODE in ('PO','SC','CN')  AND PROD_LATEST_BROADBAND_ACTIVATION_DT IS NOT NULL AND DATEDIFF(dd,PROD_LATEST_BROADBAND_STATUS_START_DT,TODAY()) >730  				THEN 'No BB, downgraded 24 months+'
				WHEN sav.prod_active_broadband_package_desc = 'Sky Connect Lite (ROI)' 																THEN 'Has BB Connect Lite'
                WHEN sav.prod_active_broadband_package_desc = 'Sky Connect Unlimited (ROI)' 														THEN 'Has BB Connect Unlimited'
				WHEN sav.prod_active_broadband_package_desc IN ('Sky Broadband Unlimited (ROI)', 'Sky Broadband Unlimited')							THEN 'Has BB  Unlimited'
                WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Lite (ROI)'                                                            THEN 'Has BB  Lite'
				WHEN sav.prod_active_broadband_package_desc = 'Sky Fibre Unlimited (ROI)'                                                 			THEN 'Has BB  Fibre Unlimited'
                WHEN sav.prod_active_broadband_package_desc = 'Sky Fibre (ROI)'                                                                     THEN 'Has BB Fibre'
                ELSE 'Never had BB'
              END AS ROI_broadband_status				
	
		, ROI_REGION_LEVEL_4 = CASE WHEN UPPER (ROI_County) IN ('DUBLIN') THEN 'Dublin'
									WHEN UPPER (ROI_County) IN ('KILDARE','LAOIS','LONGFORD','LOUTH','MEATH','OFFALY','WESTMEATH','WICKLOW','CARLOW','KILKENNY','WEXFORD') THEN 'Leinster (Exc. Dublin)'
									WHEN UPPER (ROI_County) IN ('CAVAN','DONEGAL','GALWAY','LEITRIM','MAYO','MONAGHAN','ROSCOMMON','SLIGO') THEN 'Ulster & Connacht'
									WHEN UPPER (ROI_County) IN ('CLARE','KERRY','LIMERICK','TIPPERARY','WATERFORD','CORK') THEN 'Munster'
									ELSE 'Unknown'
								END

				

/****************************************************************************************
 *                                                                                      *
 *                          UPDATE ADSMART TABLE                                    *
 *                                                                                      *
 ***************************************************************************************/


/************************************
 *                                  *
 *        ROI_SIMPLE_SEGMENTS       *
 *                                  *
 ************************************/

MESSAGE 'POPULATE ROI_SIMPLE_SEGMENTS FIELDS - STARTS' type status to client
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR= user_name()
              AND UPPER(TNAME)='TEMP_SIMPLE_SEGMENTATION'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SIMPLE_SEGMENTATION ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE TEMP_SIMPLE_SEGMENTATION
    END

drop table if exists TEMP_SIMPLE_SEGMENTATION

MESSAGE 'CREATE TABLE TEMP_SIMPLE_SEGMENTATION' TYPE STATUS TO CLIENT
GO

SELECT a.account_number
        , SEGMENTATION =  CASE
                                    WHEN LOWER(b.segment) LIKE '%support%'      THEN    'Support'
                                    WHEN LOWER(b.segment) LIKE '%secure%'       THEN    'Secure'
                                    WHEN LOWER(b.segment) LIKE '%stimulate%'    THEN    'Stimulate'
                                    WHEN LOWER(b.segment) LIKE '%stabilise'     THEN    'Stabilise'
                                                    ELSE 'Unknown' END
        , row_number()  OVER (PARTITION BY a.account_number ORDER BY observation_date DESC) AS rank_1
INTO TEMP_SIMPLE_SEGMENTATION
FROM ADSMART  as a
JOIN  SIMPLE_SEGMENTS_ROI as b ON a.account_number = b.account_number

CREATE HG INDEX ISIMSEG ON TEMP_SIMPLE_SEGMENTATION(ACCOUNT_NUMBER)
GO

UPDATE ADSMART 
SET ROI_SIMPLE_SEGMENTS = b.SEGMENTATION
FROM ADSMART  AS a
JOIN TEMP_SIMPLE_SEGMENTATION AS b
ON a.account_number = b.account_number AND b.rank_1 = 1
GO

DROP TABLE TEMP_SIMPLE_SEGMENTATION
GO

MESSAGE 'POPULATE ROI_SIMPLE_SEGMENTS FIELDS - END' type status to client
GO


/*      QA

SELECT account_number
    , Segment
    , rank() OVER (PARTITION BY account_number ORDER BY observation_date DESC) rankk
INTO #roi_segment
FROM  SIMPLE_SEGMENTS_ROI

SELECT
    CASE  WHEN b.Segment LIKE '%Secure%'  THEN 'Secure'
                                WHEN b.Segment LIKE '%Simulate%'  THEN 'Simulate'
                                WHEN b.Segment LIKE '%Support%'  THEN 'Support'
                                WHEN b.Segment LIKE '%Stabilise%'  THEN 'Stabilise'
                                ELSE 'Unknown' END AS ROI_SIMPLE_SEGMENTS
        ,  count(*) hits
from adsmartables_ROI_Nov_2015 As a
LEFT JOIN #roi_segment AS b ON a.account_number = b.account_number
WHERE sky_base_universe LIKE 'Adsmartable with consent%'
GROUP BY ROI_SIMPLE_SEGMENTS


ROI_SIMPLE_SEGMENTS hits
Secure              151098
Stabilise           88069
Support             82023
Unknown             147976

*/






/************************************************
*             ROI   Fibre Available                *
************************************************/
-- Sources: http://sp-sharepoint.bskyb.com/sites/CIKM436/documentcentre/Documents/Analytics%20Data%20Dictionaries/Data%20Dictionaries%203rd%20Party%20Data/Rep%20of%20Ireland/ROI%20Broadband%20Coverage%20Data%20Dictionary%20v1.2.xlsx

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR= user_name()
              AND lower(TNAME)='roi_fibre_accounts'
              AND UPPER(TABLETYPE)='TABLE')
DROP TABLE roi_fibre_accounts


SELECT DISTINCT b.account_number,1
INTO roi_fibre_accounts
FROM ROI_BB_FIBRE_PREQUAL AS a
JOIN CUST_SINGLE_ACCOUNT_VIEW AS b ON a.cb_key_household = b.cb_key_household
WHERE a.cb_address_status = '1'
	AND fibre_type  = 'FTTC'
    AND rfo_date is null

COMMIT

CREATE HG INDEX id1 ON roi_fibre_accounts(account_number)

UPDATE ADSMART 
SET ROI_FIBRE_AVAILABLE = COALESCE(CASE WHEN b.account_number IS NOT NULL THEN 'Yes' ELSE 'No' END , 'Unknown')
FROM ADSMART  AS a
LEFT JOIN roi_fibre_accounts AS b ON a.account_number = b.account_number

COMMIT

/*      QA
SELECT DISTINCT b.account_number
INTO #roi_fibre_accounts
FROM ROI_BB_FIBRE_PREQUAL AS a
JOIN CUST_SINGLE_ACCOUNT_VIEW AS b ON a.cb_key_household = b.cb_key_household
WHERE a.cb_address_status = '1'
        AND rfo_date is null

SELECT CASE WHEN b.account_number is null THEN 'NO' ELSE 'Yes' END fibre_available ,  count(*) hits
from adsmartables_ROI_Nov_2015 As a
LEFT JOIN #roi_fibre_accounts AS b ON a.account_number = b.account_number
WHERE sky_base_universe LIKE 'Adsmartable with consent%'
GROUP BY fibre_available



fibre_available hits
NO      209244
Yes     259922
*/

/************************************************
*               Cable Available                 *
************************************************/

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR= user_name()
              AND lower(TNAME)='roi_cable_accounts'
              AND UPPER(TABLETYPE)='TABLE')
DROP TABLE roi_cable_accounts

SELECT DISTINCT account_number, a.cb_key_household
INTO roi_cable_accounts
FROM SKY_ROI_ADDRESS_MODEL AS a
JOIN CUST_SINGLE_ACCOUNT_VIEW AS b ON a.cb_key_household = b.cb_key_household
WHERE b_invalid = 'N'
	AND a.cb_address_status = '1'
    AND x_sabs is not null
    AND x_sabs in ( SELECT small_area_code
                    FROM SKY_ROI_POINTTOPIC_SAB_BB
                    WHERE cable_available = 'y')
COMMIT
CREATE HG INDEX id1 ON roi_cable_accounts(account_number)
COMMIT
UPDATE ADSMART 
SET CABLE_AVAILABLE = COALESCE(CASE WHEN b.account_number IS NOT NULL THEN 'Yes' ELSE 'No' END , 'Unknown')
FROM ADSMART  AS a
LEFT JOIN roi_cable_accounts AS b ON a.account_number = b.account_number

COMMIT
DROP TABLE roi_cable_accounts
COMMIT

/*      QA
SELECT DISTINCT account_number
INTO #roi_cable_accounts
FROM SKY_ROI_ADDRESS_MODEL AS a
JOIN CUST_SINGLE_ACCOUNT_VIEW AS b ON a.cb_key_household = b.cb_key_household
WHERE b_invalid = 'N'
        AND x_sabs is not null
        AND x_sabs in ( SELECT small_area_code
                                        FROM SKY_ROI_POINTTOPIC_SAB_BB
                                        WHERE cable_available = 'y')
COMMIT
CREATE HG INDEX id1 ON #roi_cable_accounts(account_number)
COMMIT
SELECT CASE WHEN b.account_number is null THEN 'NO' ELSE 'Yes' END cable_available ,  count(*) hits
from adsmartables_ROI_Nov_2015 As a
LEFT JOIN #roi_cable_accounts AS b ON a.account_number = b.account_number
WHERE sky_base_universe LIKE 'Adsmartable with consent%'
GROUP BY cable_available

cable_available hits
NO  366644
Yes 102522
*/

/************************************************
*            On/Off Net Fibre                   *
************************************************/

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR= user_name()
              AND lower(TNAME)='exch'
              AND UPPER(TABLETYPE)='TABLE')
DROP TABLE exch

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR= user_name()
              AND lower(TNAME)='roi_onnet_accounts'
              AND UPPER(TABLETYPE)='TABLE')
DROP TABLE roi_onnet_accounts

SELECT DISTINCT
    MAX(CASE WHEN a.llu_exchange = 'Y' or a.bmb_exchange = 'Y'  THEN 1 ELSE 0 END) AS On_net
    , b.address_reference
INTO exch
FROM ROI_BB_EXCHANGE_LOOKUP     AS a
JOIN ROI_BB_ADDRESS_TO_EXCHANGE AS b ON a.exchange_id_3 = b.exchange_id
GROUP BY b.address_reference
COMMIT
CREATE HG INDEX ed ON  exch(address_reference)
CREATE LF INDEX aed ON  exch(On_net)
COMMIT



SELECT DISTINCT d.account_number
                , onnet  = max( CASE    WHEN On_net = 1 AND e.account_number IS NOT     NULL THEN 'On net, has fibre'
                                    WHEN On_net = 1 AND e.account_number IS         NULL THEN 'On net, no fibre'
                                    WHEN On_net = 0 AND e.account_number IS NOT     NULL THEN 'Off net, has fibre'
                                    WHEN On_net = 0  AND e.account_number IS        NULL THEN 'Off net, no fibre'
                                    ELSE 'Unknown' END)

INTO roi_onnet_accounts
FROM exch AS a
JOIN SKY_ROI_ADDRESS_MODEL      AS c ON a.address_reference = c.ROI_ADDRESS_REFERENCE AND c.CB_ADDRESS_STATUS = '1'
JOIN CUST_SINGLE_ACCOUNT_VIEW   AS d ON c.cb_key_household = d.cb_key_household
LEFT JOIN roi_fibre_accounts   AS e ON e.account_number = d.account_number
GROUP BY d.account_number

COMMIT
CREATE HG INDEX id1 ON roi_onnet_accounts(account_number)

UPDATE ADSMART 
SET ON_OFF_NET_FIBRE    = COALESCE(b.onnet, 'Unknown')
FROM ADSMART  AS a
LEFT JOIN roi_onnet_accounts AS b ON a.account_number = b.account_number

COMMIT
DROP TABLE roi_fibre_accounts
DROP TABLE roi_onnet_accounts

/*      QA

SELECT DISTINCT b.account_number
INTO #roi_fibre_accounts
FROM ROI_BB_FIBRE_PREQUAL AS a
JOIN CUST_SINGLE_ACCOUNT_VIEW AS b ON a.cb_key_household = b.cb_key_household
WHERE a.cb_address_status = '1'
        AND rfo_date is null
commit
CREATE HG INDEX id1 ON #roi_fibre_accounts(account_number)
commit
SELECT DISTINCT
    MAX(CASE WHEN a.llu_exchange = 'Y' or a.bmb_exchange = 'Y'  THEN 1 ELSE 0 END) AS On_net
    , b.address_reference
INTO #exch
FROM ROI_BB_EXCHANGE_LOOKUP     AS a
JOIN ROI_BB_ADDRESS_TO_EXCHANGE AS b ON a.exchange_id_3 = b.exchange_id
GROUP BY b.address_reference
COMMIT
CREATE HG INDEX ed ON  #exch(address_reference)
CREATE LF INDEX aed ON  #exch(On_net)
COMMIT

SELECT DISTINCT d.account_number
                , onnet  = max( CASE    WHEN On_net = 1 AND e.account_number IS NOT     NULL THEN 'On net, has fibre'
                                    WHEN On_net = 1 AND e.account_number IS         NULL THEN 'On net, no fibre'
                                    WHEN On_net = 0 AND e.account_number IS NOT     NULL THEN 'Off net, has fibre'
                                    WHEN On_net = 0  AND e.account_number IS        NULL THEN 'Off net, no fibre'
                                    ELSE 'Unknown' END)

INTO #roi_onnet_accounts
FROM #exch AS a
JOIN SKY_ROI_ADDRESS_MODEL      AS c ON a.address_reference = c.ROI_ADDRESS_REFERENCE AND CB_ADDRESS_STATUS = '1'
JOIN CUST_SINGLE_ACCOUNT_VIEW   AS d ON c.cb_key_household = d.cb_key_household
LEFT JOIN #roi_fibre_accounts   AS e ON e.account_number = d.account_number
GROUP BY d.account_number

COMMIT
CREATE HG INDEX id1 ON #roi_onnet_accounts(account_number)
COMMIT

SELECT COALESCE(Onnet ,'Unknown') Onnet , count(*) hits
from adsmartables_ROI_Nov_2015 As a
LEFT JOIN #roi_onnet_accounts AS b ON a.account_number = b.account_number
WHERE sky_base_universe LIKE 'Adsmartable with consent%'
GROUP BY Onnet




*/

UPDATE ADSMART  a
SET A.ROI_MOSAIC_HE  = CASE WHEN B.MOS_Group_ID IS NULL THEN 'Unknown' ELSE B.MOS_Group_ID END
FROM (  SELECT A.ACCOUNT_NUMBER
            ,B.cb_key_household
            ,MIN(B.MOS_Group_ID) AS MOS_Group_ID
        FROM ADSMART  AS A
        INNER JOIN (SELECT ROI_ADDRESS_MODEL.cb_key_household,
                    CASE MOSAIC.mosaic_group_code
                                WHEN 'A' THEN 'Established Elites'
                                WHEN 'B' THEN 'Upwardly Mobile Enclaves'
                                WHEN 'C' THEN 'City Centre Mix'
                                WHEN 'D' THEN 'Struggling Society'
                                WHEN 'E' THEN 'Poorer Greys'
                                WHEN 'F' THEN 'Industrious Urban Fringe'
                                WHEN 'G' THEN 'Careers & Kids'
                                WHEN 'H' THEN 'Young & Mortgaged'
                                WHEN 'I' THEN 'Better Off Greys'
                                WHEN 'J' THEN 'Commuter Farming Mix'
                                WHEN 'K' THEN 'Regional Identity'
                                WHEN 'L' THEN 'Farming Families'
                                WHEN 'M' THEN 'Arcadian Inheritance'
                                ELSE 'Unknown' END MOS_Group_ID
                    FROM SKY_ROI_ADDRESS_MODEL AS ROI_ADDRESS_MODEL
                    LEFT JOIN SKY_ROI_MOSAIC_2013 AS MOSAIC ON MOSAIC.building_id = ROI_ADDRESS_MODEL.building_id
					WHERE ROI_ADDRESS_MODEL.cb_address_status = '1' 
                    GROUP BY ROI_ADDRESS_MODEL.cb_key_household, MOS_Group_ID
                    ) B ON A.cb_key_household = B.cb_key_household
    GROUP BY A.ACCOUNT_NUMBER
        ,B.cb_key_household
    ) B
WHERE A.account_number = B.account_number

GO





