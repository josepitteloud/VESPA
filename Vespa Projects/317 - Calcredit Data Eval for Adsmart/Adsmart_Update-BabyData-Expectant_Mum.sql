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
**Project Name:                                                 Adsmart - Drop 3 - Baby Data - Expectant Mum
		
		Description:
			New Attribute to update the Adsmart table
				
		Lead: 	Jose Pitteloud		
		Coded by: Paolo Menna
	Sections:
			
			
*********************************/

----------------- Baby:	 	Expectant Mum

update ####ADSMART####
SET EXPECTANT_MUM = CASE WHEN baby_due_date is not null then 'Yes' else 'Unknown' end
from ####ADSMART#### a
join pitteloudj.v317_Emmas_view b on a.cb_key_household = b.cb_key_household
WHERE baby_due_date >= GETDATE() 

