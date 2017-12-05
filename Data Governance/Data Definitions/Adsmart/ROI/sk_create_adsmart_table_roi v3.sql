/* ***************************************************************************************
 *                                                                                      *
 *                          POPULATE ADSMART TABLE                                      *
 *                                                                                      *
 ***************************************************************************************/
MESSAGE 'Populate Table ${CBAF_DB_DATA_SCHEMA}.ADSMART from the CUST_SINGLE_ACCOUNT_VIEW for ROI Attributes- Start' type status to client
go

INSERT INTO ${CBAF_DB_DATA_SCHEMA}.ADSMART
 (
  record_type   
 , account_number      
 , version_number 
 , cb_key_household    
 , cb_key_db_person    
 , cb_key_individual   
 , src_system_id
 , ROI_COUNTY
 , ROI_BROADBAND_STATUS
 , ROI_REGION_LEVEL_4 
 )
 SELECT
  4 as record_type             
 , sav.account_number          
 , 4 as version_number -- updated version number from 3 to 4           
 , sav.cb_key_household        
 , sav.cb_key_db_person        
 , sav.cb_key_individual       
 , sav.src_system_id
 -- ROI Attributes Start
 , CASE  WHEN cb_address_status = '1' and roi_address_match_source is not null and cb_address_county is not null THEN cb_address_county
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
 , CASE  WHEN sav.prod_active_broadband_package_desc IS NULL AND PROD_EARLIEST_BROADBAND_ACTIVATION_DT  IS NULL THEN 'Never had BB'
         WHEN PROD_LATEST_BROADBAND_STATUS_CODE in ('PO','SC','CN')  AND PROD_LATEST_BROADBAND_ACTIVATION_DT IS NOT NULL AND DATEDIFF(dd,PROD_LATEST_BROADBAND_STATUS_START_DT,TODAY()) <=365 THEN 'No BB, downgraded in last 0 - 12 months'
		 WHEN PROD_LATEST_BROADBAND_STATUS_CODE in ('PO','SC','CN')  AND PROD_LATEST_BROADBAND_ACTIVATION_DT IS NOT NULL AND DATEDIFF(dd,PROD_LATEST_BROADBAND_STATUS_START_DT,TODAY()) BETWEEN 366 AND 730 THEN 'No BB, downgraded in last 12- 24 mths'
         WHEN PROD_LATEST_BROADBAND_STATUS_CODE in ('PO','SC','CN')  AND PROD_LATEST_BROADBAND_ACTIVATION_DT IS NOT NULL AND DATEDIFF(dd,PROD_LATEST_BROADBAND_STATUS_START_DT,TODAY()) >730 THEN 'No BB, downgraded 24 months+'
         WHEN sav.prod_active_broadband_package_desc LIKE 'Sky Connect Lite (ROI%' 			THEN 'Has BB Connect Lite'
         WHEN sav.prod_active_broadband_package_desc LIKE 'Sky Connect Unlimited (ROI%' 	THEN 'Has BB Connect Unlimited'
         WHEN sav.prod_active_broadband_package_desc LIKE 'Sky Broadband Unlimited%'		THEN 'Has BB Unlimited'
		 WHEN sav.prod_active_broadband_package_desc LIKE 'Sky Broadband Unlimited (ROI%'	THEN 'Has BB Unlimited'
         WHEN sav.prod_active_broadband_package_desc LIKE 'Sky Broadband Lite (ROI%' 		THEN 'Has BB Lite'
         WHEN sav.prod_active_broadband_package_desc LIKE 'Sky Fibre Unlimited (ROI%' 		THEN 'Has BB Fibre Unlimited'
         WHEN sav.prod_active_broadband_package_desc LIKE 'Sky Fibre (ROI%' 				THEN 'Has BB Fibre'
		 WHEN sav.prod_active_broadband_package_desc IS NOT NULL  THEN 'Other BB package'
		 ELSE 'Never had BB'
		END AS ROI_broadband_status
 , CASE  WHEN UPPER (ROI_County) IN ('DUBLIN') THEN 'Dublin'
         WHEN UPPER (ROI_County) IN ('KILDARE','LAOIS','LONGFORD','LOUTH','MEATH','OFFALY','WESTMEATH','WICKLOW','CARLOW','KILKENNY','WEXFORD') THEN 'Leinster (Exc. Dublin)'
         WHEN UPPER (ROI_County) IN ('CAVAN','DONEGAL','GALWAY','LEITRIM','MAYO','MONAGHAN','ROSCOMMON','SLIGO') THEN 'Ulster & Connacht'
         WHEN UPPER (ROI_County) IN ('CLARE','KERRY','LIMERICK','TIPPERARY','WATERFORD','CORK') THEN 'Munster'
         ELSE 'Unknown'
   END AS ROI_REGION_LEVEL_4
-- ROI Attributes End
 FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW sav
where sav.account_number <> '99999999999999'
    AND sav.account_number not like '%.%'
    AND sav.cust_active_dtv = 1
    AND sav.cust_primary_service_instance_id is not null
    --AND sav.cb_key_household > 0
    --AND sav.cb_key_household IS NOT NULL
    AND sav.account_number IS NOT NULL
    AND UPPER(sav.PTY_COUNTRY_CODE) LIKE 'IRL'
  GO
	
/* **********************
*		       *
*     ROI_MOSAIC       *
*		       *
***********************/

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART  a
SET A.ROI_MOSAIC = CASE WHEN B.MOS_Group_ID IS NULL THEN 'Unknown' ELSE B.MOS_Group_ID END
FROM (  SELECT A.ACCOUNT_NUMBER
            ,B.cb_key_household
            ,MIN(B.MOS_Group_ID) AS MOS_Group_ID
        FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS A
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
                    FROM ${CBAF_DB_LIVE_SCHEMA}.SKY_ROI_ADDRESS_MODEL AS ROI_ADDRESS_MODEL
                    LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.SKY_ROI_MOSAIC_2013 AS MOSAIC ON MOSAIC.building_id = ROI_ADDRESS_MODEL.building_id
				WHERE cb_key_household IS NOT NULL AND cb_key_household > 0
                    GROUP BY ROI_ADDRESS_MODEL.cb_key_household, MOS_Group_ID
                    ) B ON A.cb_key_household = B.cb_key_household 
    GROUP BY A.ACCOUNT_NUMBER
        ,B.cb_key_household
    ) B
WHERE A.account_number = B.account_number
  GO
  
UPDATE  ${CBAF_DB_DATA_SCHEMA}.ADSMART  a
SET A.ROI_MOSAIC = 'Unknown'
WHERE account_number IN (SELECT account_number FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW 
						WHERE UPPER(PTY_COUNTRY_CODE) LIKE 'IRL' 
							AND (cb_key_household IS NULL OR cb_key_household = 0))

/* ***********************************
 *                                  *
 *        ROI_SIMPLE_SEGMENTS       *
 *                                  *
 ************************************/

MESSAGE 'POPULATE ROI_SIMPLE_SEGMENTS FIELDS - START' type status to client
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR= '${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(TNAME)='TEMP_SIMPLE_SEGMENTATION'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SIMPLE_SEGMENTATION ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_SIMPLE_SEGMENTATION
    END

drop table if exists ${CBAF_DB_DATA_SCHEMA}.TEMP_SIMPLE_SEGMENTATION

MESSAGE 'CREATE TABLE TEMP_SIMPLE_SEGMENTATION' TYPE STATUS TO CLIENT
GO

SELECT a.account_number
        , SEGMENTATION =  CASE
                                    WHEN LOWER(b.segment) LIKE '%start%'      THEN    'Start'
									WHEN LOWER(b.segment) LIKE '%support%'      THEN    'Support'
                                    WHEN LOWER(b.segment) LIKE '%secure%'       THEN    'Secure'
                                    WHEN LOWER(b.segment) LIKE '%stimulate%'    THEN    'Stimulate'
                                    WHEN LOWER(b.segment) LIKE '%stabilise'     THEN    'Stabilise'
                                                    ELSE 'Unknown' END
        , row_number()  OVER (PARTITION BY a.account_number ORDER BY observation_date DESC) AS rank_1
INTO ${CBAF_DB_DATA_SCHEMA}.TEMP_SIMPLE_SEGMENTATION
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART  as a
JOIN ${CBAF_DB_LIVE_SCHEMA}.SIMPLE_SEGMENTS_ROI as b ON a.account_number = b.account_number 
GO

CREATE HG INDEX ISIMSEG ON ${CBAF_DB_DATA_SCHEMA}.TEMP_SIMPLE_SEGMENTATION(ACCOUNT_NUMBER)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART 
SET ROI_SIMPLE_SEGMENTS = b.SEGMENTATION
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART  AS a
JOIN ${CBAF_DB_DATA_SCHEMA}.TEMP_SIMPLE_SEGMENTATION AS b
ON a.account_number = b.account_number AND b.rank_1 = 1 
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_SIMPLE_SEGMENTATION
GO

MESSAGE 'POPULATE ROI_SIMPLE_SEGMENTS FIELDS - END' type status to client
GO

/* ***********************************************
*             ROI Fibre Available                *
************************************************/

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR= '${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='roi_fibre_accounts'
              AND UPPER(TABLETYPE)='TABLE')
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.roi_fibre_accounts
GO

SELECT DISTINCT b.account_number, b.cb_address_status
INTO ${CBAF_DB_DATA_SCHEMA}.roi_fibre_accounts
FROM ${CBAF_DB_LIVE_SCHEMA}.ROI_BB_FIBRE_PREQUAL AS a
JOIN ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW AS b ON a.cb_key_household = b.cb_key_household AND UPPER(b.PTY_COUNTRY_CODE) LIKE 'IRL'
WHERE a.cb_address_status = '1'
	AND  a.cb_key_household IS NOT NULL AND a.cb_key_household > 0
	AND fibre_type  = 'FTTC'
	AND rfo_date is null
GO

CREATE HG INDEX id1 ON ${CBAF_DB_DATA_SCHEMA}.roi_fibre_accounts(account_number)

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART 
SET ROI_FIBRE_AVAILABLE = CASE WHEN b.account_number IS NOT NULL THEN 'Yes' ELSE 'No' END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS a
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.roi_fibre_accounts AS b ON a.account_number = b.account_number  
GO


UPDATE  ${CBAF_DB_DATA_SCHEMA}.ADSMART  a
SET A.ROI_FIBRE_AVAILABLE = 'Unknown'
WHERE account_number IN (SELECT account_number FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW 
						WHERE UPPER(PTY_COUNTRY_CODE) LIKE 'IRL' 
							AND (cb_address_status <> '1' OR cb_key_household IS NULL OR cb_key_household = 0))

/* ***********************************************
*               ROI Cable Available                 *
************************************************/

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR= '${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='roi_cable_accounts'
              AND UPPER(TABLETYPE)='TABLE')
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.roi_cable_accounts
GO

SELECT DISTINCT account_number, a.cb_key_household
INTO ${CBAF_DB_DATA_SCHEMA}.roi_cable_accounts
FROM ${CBAF_DB_LIVE_SCHEMA}.SKY_ROI_ADDRESS_MODEL AS a
JOIN ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW AS b ON a.cb_key_household = b.cb_key_household AND UPPER(b.PTY_COUNTRY_CODE) LIKE 'IRL' 
WHERE b_invalid = 'N'
    AND b.cb_address_status = '1'
    AND x_sabs is not null
    AND x_sabs in ( SELECT small_area_code
                    FROM ${CBAF_DB_LIVE_SCHEMA}.SKY_ROI_POINTTOPIC_SAB_BB
                    WHERE cable_available = 'y')
GO
CREATE HG INDEX id1 ON ${CBAF_DB_DATA_SCHEMA}.roi_cable_accounts(account_number)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART 
SET ROI_CABLE_AVAILABLE = CASE WHEN b.account_number IS NOT NULL THEN 'Yes' ELSE 'No' END 
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART  AS a
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.roi_cable_accounts AS b ON a.account_number = b.account_number  
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.roi_cable_accounts
GO

UPDATE  ${CBAF_DB_DATA_SCHEMA}.ADSMART  a
SET A.ROI_CABLE_AVAILABLE = 'Unknown'
WHERE account_number IN (SELECT account_number FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW 
						WHERE UPPER(PTY_COUNTRY_CODE) LIKE 'IRL' 
							AND (cb_address_status <> '1' OR cb_key_household IS NULL OR cb_key_household = 0))


UPDATE  ${CBAF_DB_DATA_SCHEMA}.ADSMART  a
SET A.ROI_CABLE_AVAILABLE = 'Unknown'
WHERE cb_key_household NOT IN (SELECT cb_key_household FROM SKY_ROI_ADDRESS_MODEL where b_address_status = '1' )

							
/* ***********************************************
*            ROI On/Off Net Fibre                   *
************************************************/

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR= '${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='exch'
              AND UPPER(TABLETYPE)='TABLE')
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.exch
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR= '${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='roi_onnet_accounts'
              AND UPPER(TABLETYPE)='TABLE')
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.roi_onnet_accounts
GO

SELECT DISTINCT
    MAX(CASE WHEN a.llu_exchange = 'Y' or a.bmb_exchange = 'Y'  THEN 1 ELSE 0 END) AS On_net
    , b.address_reference
INTO ${CBAF_DB_DATA_SCHEMA}.exch
FROM ${CBAF_DB_LIVE_SCHEMA}.ROI_BB_EXCHANGE_LOOKUP     AS a
JOIN ${CBAF_DB_LIVE_SCHEMA}.ROI_BB_ADDRESS_TO_EXCHANGE AS b ON a.exchange_id_3 = b.exchange_id
GROUP BY b.address_reference
GO

CREATE HG INDEX ed ON  ${CBAF_DB_DATA_SCHEMA}.exch(address_reference)
CREATE LF INDEX aed ON  ${CBAF_DB_DATA_SCHEMA}.exch(On_net)
GO

SELECT DISTINCT d.account_number
                , onnet  = max( CASE    WHEN On_net = 1 AND e.account_number IS NOT     NULL THEN 'On net, has fibre'
                                    WHEN On_net = 1 AND e.account_number IS         NULL THEN 'On net, no fibre'
                                    WHEN On_net = 0 AND e.account_number IS NOT     NULL THEN 'Off net, has fibre'
                                    WHEN On_net = 0  AND e.account_number IS        NULL THEN 'Off net, no fibre'
                                    ELSE 'Unknown' END)

INTO ${CBAF_DB_DATA_SCHEMA}.roi_onnet_accounts
FROM ${CBAF_DB_DATA_SCHEMA}.exch AS a
JOIN ${CBAF_DB_LIVE_SCHEMA}.SKY_ROI_ADDRESS_MODEL      AS c ON a.address_reference = c.ROI_ADDRESS_REFERENCE AND CB_ADDRESS_STATUS = '1'
JOIN ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW   AS d ON c.cb_key_household = d.cb_key_household and UPPER(d.PTY_COUNTRY_CODE) LIKE 'IRL' AND  d.cb_key_household IS NOT NULL AND d.cb_key_household > 0
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.roi_fibre_accounts   AS e ON e.account_number = d.account_number
GROUP BY d.account_number
GO

CREATE HG INDEX id1 ON ${CBAF_DB_DATA_SCHEMA}.roi_onnet_accounts(account_number)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART 
SET ROI_ON_OFF_NET_FIBRE    = COALESCE(b.onnet, 'Unknown')
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART  AS a
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.roi_onnet_accounts AS b ON a.account_number = b.account_number 
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.roi_fibre_accounts
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.roi_onnet_accounts
GO

UPDATE  ${CBAF_DB_DATA_SCHEMA}.ADSMART  a
SET A.ROI_ON_OFF_NET_FIBRE = 'Unknown'
WHERE account_number IN (SELECT account_number FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW 
						WHERE UPPER(PTY_COUNTRY_CODE) LIKE 'IRL' 
							AND (cb_address_status <> '1' OR cb_key_household IS NULL OR cb_key_household = 0))

