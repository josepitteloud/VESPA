/* *****************************
		Adsmart L3 Drop 1
		Primary Box Type Update		
		
		Description:
		The change is to be able to identify wifi versus non wifi enabled boxes
		
		Coded by James McKane / Jose Pitteloud
		Version: 2014-12-17
		
		Changes:
			- Wifi case statement added
			- Labels renamed
			- Hierarchy based on numbers added to pick wifi enabled first
			- Filters added for null accounts and unknown models
*********************************/
	

	
	
UPDATE ADSMART
SET    new_box_type = CSHP_T.PrimaryBoxType
FROM   ADSMART AS base
                    INNER JOIN (SELECT  stb.account_number
                                       ,SUBSTR(MIN(CASE 
													WHEN (stb.x_model_number LIKE '%W%' OR UPPER(stb.x_description) LIKE '%WI-FI%') 		THEN '1 890 or 895 Wifi Enabled'
													WHEN stb.x_model_number IN ('DRX 890','DRX 895') AND stb.x_pvr_type IN ('PVR5','PVR6') 	THEN '2 890 or 895 Not Wifi Enabled'
													WHEN stb.x_manufacturer IN ('Samsung','Pace') 											THEN '3 Samsung or Pace'
													ELSE '9 Unknown' END
                                           ),3 ,100) AS PrimaryBoxType
                                  FROM  cust_set_top_box AS stb
                                 WHERE  	stb.active_box_flag = 'Y'
										AND account_number IS NOT NULL 
										AND x_model_number <> 'Unknown'
                              GROUP BY  stb.account_number
                                ) AS CSHP_T
                    ON CSHP_T.account_number = base.account_number
                    ;
COMMIT 	
	
	