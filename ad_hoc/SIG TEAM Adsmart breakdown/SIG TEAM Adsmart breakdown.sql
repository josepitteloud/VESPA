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
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          SIG TEAM
**Due Date:                             11/12/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

**Sections:

	-- [1] Adsmartable Box breakdown by personalisation flag
	-- [2] Adsmartable Breakdown by box model and personalisation flag
--------------------------------------------------------------------------------------------------------------
*/

--------------------------------------------------------
-- [1] Adsmartable Box breakdown by personalisation flag
--------------------------------------------------------

select  case    when acn.viewing_consent_flag = '?' then 'unknown'
                when acn.viewing_consent_flag = 'Y' then 'Personalisation Consent'
                when acn.viewing_consent_flag = 'N' then 'No Personalisation Consent'
                else null
        end     as personalisation
        ,case   when box.adsmart_flag = 0 then 'Non-Adsmartable'
                when box.adsmart_flag = 1 then 'Adsmartable'
                else null
        end     as adsmart_flag
        ,count(distinct box.subscriber_id)  as nboxes
from    VESPA_ANALYSTS.SIG_SINGLE_box_VIEW                  as box
        inner join VESPA_ANALYSTS.SIG_SINGLE_ACCOUNT_VIEW   as acn
        on  box.account_number = acn.account_number
group   by  personalisation
            ,box.adsmart_flag


------------------------------------------------------------------
-- [2] Adsmartable Breakdown by box model and personalisation flag
------------------------------------------------------------------

SELECT  box.box_model
        ,case   when box.adsmart_flag = 0 then 'Non-Adsmartable'
                when box.adsmart_flag = 1 then 'Adsmartable'
                else null
        end     as adsmart_flag
        ,case   when acn.viewing_consent_flag = '?' then 'unknown'
                when acn.viewing_consent_flag = 'Y' then 'Personalisation Consent'
                when acn.viewing_consent_flag = 'N' then 'No Personalisation Consent'
                else null
        end     as personalisation
        ,count(distinct box.subscriber_id)  as nboxes
        ,count(distinct box.account_number) as naccounts
FROM    VESPA_ANALYSTS.SIG_SINGLE_box_VIEW                  as box
        inner join VESPA_ANALYSTS.SIG_SINGLE_ACCOUNT_VIEW   as acn
        on  box.account_number = acn.account_number
group   by  box.box_model
            ,box.adsmart_flag
            ,personalisation