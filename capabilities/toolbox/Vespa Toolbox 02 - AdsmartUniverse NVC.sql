/*


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

**Project Name: 					ADSMARTABLE UNIVERSE
**Analysts:							Angel Donnarumma 	(angel.donnarumma_mirabel@skyiq.co.uk)
									Berwyn Cort			(Berwyn.Cort@SkyIQ.co.uk)
**Lead(s):							Jose Loureda
**Stakeholder:						VESPA / Strategic Insight Team.

									
**Business Brief:

	To breakdown Sky UK base into Adsmartable or not Adsmartable (NO VIEWING CONSENT)

**Sections:

	A: DEFINING SKY UK BASE
		
		A01: flagging from SAV DTH Active customer in UK, Viewing consent (optional))
	
	B: DEFINING ADSMARTABLE UNIVERSE
	
		B01: Ranking STB based on service instance id to dedupe the table
		B02: Extracting Active Boxes per account (one line per box per account)
		B03: Flag Adsmartable boxes based on Adsmart definition (PVR6,5 and 4 from Samsung/Pace)
		
	C: DEFINING VESPA DP BASE
	
		C01: Listing DP active Accounts that have reported at least 1 day amongst last 30 days
		
--------------------------------------------------------------------------------------------------------------------------------------------
USEFUL NOTE:	each building block can be treated as a stand alone unit, hence it is possible to copy/paste the logic and generate a table
				out of any of them if needed/required...
--------------------------------------------------------------------------------------------------------------------------------------------	

*/


create or replace procedure vespa_toolbox_02_AdsmartUniverse_NVC -- execute vespa_toolbox_02_AdsmartUniverse_NVC
as
begin

-----------------------
-- ADSMARTABLE UNIVERSE
-----------------------


select	adsmart.flag
		,count(distinct sav.account_number) as Sky_Base
		,count(distinct	sbv.account_number) as vespa
from	(
            select	distinct account_number
    		from	sk_prod.CUST_SINGLE_ACCOUNT_VIEW
    		where	CUST_ACTIVE_DTV = 1 			-- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
    		and		pty_country_code = 'GBR' 	
    		--and 	cust_viewing_data_capture_allowed = 'Y'	-- [ ENABLE/DISABLE this criteria to consider viewing consent ] 
        )as sav
		left join	( 	
						----------------------------------------------------------
						-- B03: Flag Adsmartable boxes based on Adsmart definition
						----------------------------------------------------------
						select	account_number
								,max(	CASE	WHEN x_pvr_type ='PVR6'									THEN 1
												WHEN x_pvr_type ='PVR5'									THEN 1
												WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung' 	THEN 1
												WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'    	THEN 1
																										ELSE 0
										END) AS flag
						from	(	
									--------------------------------------------------------------------------
									-- B02: Extracting Active Boxes per account (one line per box per account)
									--------------------------------------------------------------------------
									select  *
									from    (	
												--------------------------------------------------------------------
												-- B01: Ranking STB based on service instance id to dedupe the table
												--------------------------------------------------------------------
												Select  account_number
														,x_pvr_type
														,x_personal_storage_capacity
														,currency_code
														,x_manufacturer
														,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
												from    sk_prod.CUST_SET_TOP_BOX
												
											) 	as base
									where   active_flag = 1
									
								) 	as active_boxes
                        where   currency_code = 'GBP'
                        group 	by	account_number
																		
					)	as adsmart
		on	sav.account_number = adsmart.account_number
		left join	(	
						-----------------------------------------------------------------------------------------
						--C01: Listing DP active Accounts that have reported at least 1 day amongst last 30 days
						-----------------------------------------------------------------------------------------
					    select  distinct account_number
						from  	vespa_analysts.vespa_single_box_view
						where	panel = 'VESPA'
						and   	status_vespa = 'Enabled'
						
					)	as sbv
		on	sav.account_number = sbv.account_number
group	by	adsmart.flag


end;



commit;

grant execute on vespa_toolbox_02_AdsmartUniverse to vespa_group_low_security;
commit;


