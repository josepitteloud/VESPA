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
**Project Name:                                                 Adsmart - Drop 3 - Baby Data - Pregnant and Number of Children in Household
		
		Description:
			New Attribute to update the Adsmart table
				
		Lead: 	Jose Pitteloud		
		Coded by: Paolo Menna
	Sections:
			
			
*********************************/


------------------- Baby: 	Pregnant and Number of Children in Household

SELECT cb_key_household
        , CASE WHEN cast(number_of_kids as integer) = 0 then 'First Child'
                                    WHEN cast(number_of_kids as integer) = 1 then '1 Child'
                                    WHEN cast(number_of_kids as integer) = 2 then '2 Child'
                                    WHEN cast(number_of_kids as integer) => 3 then '3 +Child'
                                    ELSE 'Unknown' end as pregnant_and_number_of_children_in_household_he
INTO v317_Emmas_view
FROM pitteloudj.v317_Emmas_view			----- Must be replaced by the productionised table when available
WHERE baby_due_date IS NOT NULL

update ####ADSMART####
SET pregnant_and_number_of_children_in_household_he = COALESCE(pregnant_and_number_of_children_in_household_he, 'Unknown')
FROM ####ADSMART#### a
LEFT JOIN pitteloudj.v317_Emmas_view b on a.cb_key_household = b.cb_key_household

DROP TABLE v317_Emmas_view
COMMIT