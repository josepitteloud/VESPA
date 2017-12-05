/* **********************
*		       *
*     ROI_MOSAIC       *
*		       *
***********************/

UPDATE /*####YOUR TABLE ####*/  a
SELECT 
account_number , 
ROI_MOSAIC = CASE WHEN B.MOS_Group_ID IS NULL THEN 'Unknown' ELSE B.MOS_Group_ID END

INTO ROI_MOSAIC 
FROM (  SELECT A.ACCOUNT_NUMBER
            ,B.cb_key_household
            ,MIN(B.MOS_Group_ID) AS MOS_Group_ID
        FROM ADSMART AS A
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
JOIN sav ON A.account_number = B.account_number
  GO
  
  
  
  
UPDATE  /*####YOUR TABLE ####*/  a
SET A.ROI_MOSAIC = 'Unknown'
WHERE account_number IN (SELECT account_number FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW 
						WHERE UPPER(PTY_COUNTRY_CODE) LIKE 'IRL' 
							AND (cb_key_household IS NULL OR cb_key_household = 0))
							
							
 

FROM 							