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

	To provide a valid list of active accounts in UK plus some extra details...

**Sections:

*/

create or replace procedure sig_toolbox_03_ActiveUKCust
as begin

	select  account_number
            ,cust_viewing_data_capture_allowed
            ,cb_key_individual
			,CUST_ACTIVE_DTV
			,pty_country_code
            ,PROD_DTV_ACTIVATION_DT
	from	/*sk_prod.*/CUST_SINGLE_ACCOUNT_VIEW
	where   CUST_ACTIVE_DTV = 1
	and     pty_country_code = 'GBR'

end;

commit;
grant execute on sig_toolbox_03_ActiveUKCust to vespa_group_low_security;
commit;
