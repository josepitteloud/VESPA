/*  Title       : Adsmart Attribute build - Q1.5 2016-2017
    Created by  : Jose Pitteloud	
    Date        : 04/08/2016
    Description : This is a sql to build the ADSMART attributes included in the Q1.5 2016-17 release:
						-	Primary Box Type: ammendment due to include new boxex names (Sky Q)
						-	Bundle Type: ammendment to add/fix sky Q bundles
						- 	Sky Q subscription: New attribute for Q customers taking up Q subscription from 16th Aug (Q ph2 go live)
						
/*						





/* ***********************************
 *                                  *
 *           Primary Box Type       *
 *                                  *
 ************************************/

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET    PRIMARY_BOX_TYPE = CSHP_T.PrimaryBoxType
FROM   ${CBAF_DB_DATA_SCHEMA}.ADSMART AS base
   INNER JOIN (SELECT  stb.account_number
					  ,SUBSTR(MIN(CASE
										WHEN x_description  in ('Sky Q Silver','Sky Q Mini','Sky Q 2TB box') 					THEN '1 SkyQ Silver'			---- Adding 'Sky Q 2TB box' description as per requirement
										WHEN x_description IN ('Sky Q','Sky Q 1TB box')								 			THEN '2 SkyQ'					---- Adding 'Sky Q 1TB box' description as per requirement
										WHEN (stb.x_model_number LIKE '%W%' OR UPPER(stb.x_description) LIKE '%WI-FI%') AND  UPPER(stb.x_model_number) NOT LIKE '%UNKNOWN%'		THEN '3 890 or 895 Wifi Enabled'
										WHEN stb.x_model_number IN ('DRX 890','DRX 895') AND stb.x_pvr_type IN ('PVR5','PVR6')  THEN '4 890 or 895 Not Wifi Enabled'
										WHEN stb.x_manufacturer IN ('Samsung','Pace')  AND x_box_type = 'Sky+HD'				THEN '5 Samsung or Pace Not Wifi Enabled'
										  ELSE '9 Unknown' END
												 ),3 ,100) AS PrimaryBoxType
					   FROM  ${CBAF_DB_LIVE_SCHEMA}.cust_set_top_box AS stb
					   WHERE          stb.x_active_box_flag_new = 'Y' 
					   AND account_number IS NOT NULL
					GROUP BY  stb.account_number
						) AS CSHP_T
   ON CSHP_T.account_number = base.account_number
GO

/* ***********************************
 *                                  *
 *           BUNDLE TYPE		    *
 *                                  *
 ************************************/
------ To replace the bundle_type definition in the "POPULATE ADSMART TABLE" section from SAV

CASE 
		WHEN prod_latest_entitlement_genre IN ('Original', 'Original (Legacy 2015)',  'Original (Legacy 2016)') 		 	THEN 'Original'
		WHEN prod_latest_entitlement_genre =  'Variety'  																	THEN 'Variety'
		WHEN prod_latest_entitlement_genre LIKE  'Box Sets%' OR prod_latest_entitlement_genre =  'Family'		 			THEN 'Family or Boxsets'					---- Family or Boxsets new definition according to the requirement
		WHEN prod_latest_entitlement_genre =  'Sky Q Bundle%' OR prod_latest_entitlement_genre LIKE  'SkyQ Legacy%' 		THEN 'SkyQ Legacy'							---- New Sky Q bundles added and renamed
		ELSE 'Others'
   END bundle_type 

   
 /* *********************************
 *                                  *
 *       Sky Q subscription		    *
 *                                  *
 ************************************/
 
 
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET   SKYQ = 'Yes'
FROM  	${CBAF_DB_DATA_SCHEMA}.ADSMART AS a 
JOIN 	${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW AS b ON a.account_number =  b.account_number 
WHERE cust_active_dtv = 1
	AND PROD_LATEST_MS_PLUS_STATUS_CODE 		in ('AC','PC','AB')
 



 