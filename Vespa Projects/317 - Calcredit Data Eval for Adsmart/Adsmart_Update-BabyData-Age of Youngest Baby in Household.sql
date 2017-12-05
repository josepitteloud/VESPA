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
**Project Name:                                                 Adsmart - Drop 3 - Baby Data - Age of Youngest Baby in Household
		
		Description:
			New Attribute to update the Adsmart table
				
		Lead: 	Jose Pitteloud		
		Coded by: Paolo Menna
	Sections:
			
			
*********************************/

----------------- Baby: 	Age of Youngest Baby in Household

SELECT *
INTO v317_Emmas_view
FROM (
	SELECT cb_key_household
		,baby_dob
		,child1_dob
		,DATEDIFF(MONTH, COALESCE(baby_dob, child1_dob), getdate()) 			AS age_child
		,row_number() OVER (PARTITION BY cb_key_household ORDER BY age_child) 	AS rank_
	FROM pitteloudj.v317_Emmas_view														----- Must be replaced by the productionised table when available
	WHERE baby_dob IS NOT NULL OR child1_dob IS NOT NULL			
	) AS f
WHERE rank_ = 1

UPDATE ####ADSMART####
SET AGE_OF_YOUNGEST_BABY_IN_HOUSEHOLD = CASE WHEN age_child BETWEEN 0	AND 3	THEN '0-3 Months'
											WHEN age_child BETWEEN 4	AND 6	THEN '4-6 Months'
											WHEN age_child BETWEEN 7	AND 12	THEN '7-12 Months'
											WHEN age_child BETWEEN 13	AND 18	THEN '13-18 Months'
											WHEN age_child BETWEEN 19	AND 24	THEN '19-24 Months'
											WHEN age_child > 25	THEN '25+ months'
											ELSE 'Unknown' end
FROM ####ADSMART#### a
LEFT JOIN v317_Emmas_view b ON a.cb_key_household = b.cb_key_household

DROP TABLE v317_Emmas_view
COMMIT