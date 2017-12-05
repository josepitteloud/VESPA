
SELECT DISTINCT  sav.account_number
    , cb_address_county
    , sav.cb_key_household    
    , sav.cb_address_status 
    , cust_active_dtv
    , cust_viewing_data_capture_allowed
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
    , CAST( NULL AS VARCHAR(40)) AS ROI_MOSAIC
    , CASE WHEN b.account_number is null THEN 0 ELSE 1 END adsmartable
    , CASE WHEN c.cb_key_household is null THEN 0 ELSE 1 END adrress_match 
INTO ROI_MOSAIC_COUNT    
FROM CUST_SINGLE_ACCOUNT_VIEW sav
LEFT JOIN (SELECT DISTINCT account_number
            FROM CUST_SET_TOP_BOX
            WHERE x_active_box_flag_new ='Y'
                    AND x_box_type = 'Sky+HD'
                    AND (x_model_number like 'DRX 890%'
                    OR x_model_number like 'DRX 895%')) AS b ON sav.account_number = b.account_number
LEFT JOIN (select DISTINCT cb_key_household from SKY_ROI_ADDRESS_MODEL
            where cb_address_status = '1'
                    and building_id in (select DISTINCT building_id from SKY_ROI_MOSAIC_2013)) 
                            AS c ON sav.cb_key_household  = c.cb_key_household 
                     
WHERE sav.account_number <> '99999999999999'
    AND sav.account_number not like '%.%'
    AND sav.cust_active_dtv = 1
    AND sav.cust_primary_service_instance_id is not null
    AND sav.account_number IS NOT NULL
    AND UPPER(sav.PTY_COUNTRY_CODE) LIKE 'IRL'
tt ROI_MOSAIC_COUNT
 
 
 
UPDATE ROI_MOSAIC_COUNT a
SET A.ROI_MOSAIC = CASE WHEN B.MOS_Group_ID IS NULL THEN 'Unknown' ELSE B.MOS_Group_ID END
FROM (  SELECT A.ACCOUNT_NUMBER
            ,B.cb_key_household
            ,MIN(B.MOS_Group_ID) AS MOS_Group_ID
        FROM ROI_MOSAIC_COUNT AS A
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
				WHERE cb_key_household IS NOT NULL AND cb_key_household > 0
                    GROUP BY ROI_ADDRESS_MODEL.cb_key_household, MOS_Group_ID
                    ) B ON A.cb_key_household = B.cb_key_household 
    GROUP BY A.ACCOUNT_NUMBER
        ,B.cb_key_household
    ) B
WHERE A.account_number = B.account_number
  GO
  
UPDATE  ROI_MOSAIC_COUNT  a
SET A.ROI_MOSAIC = 'Unknown'
WHERE account_number IN (SELECT account_number FROM CUST_SINGLE_ACCOUNT_VIEW 
						WHERE UPPER(PTY_COUNTRY_CODE) LIKE 'IRL' 
							AND (cb_key_household IS NULL OR cb_key_household = 0))
 
 
 
 
 SELECT 
cb_address_county
,cb_address_status
,cust_active_dtv
,cust_viewing_data_capture_allowed
,ROI_County
,ROI_MOSAIC
,adsmartable
,adrress_match
, COUNT(*) hits 
 FROM ROI_MOSAIC_COUNT
 GROUP BY 
 cb_address_county
,cb_address_status
,cust_active_dtv
,cust_viewing_data_capture_allowed
,ROI_County
,ROI_MOSAIC
,adsmartable
,adrress_match

