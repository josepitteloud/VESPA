/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining the types of products
#		a Sky Customer has..
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 19/09/2012  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

SELECT csh.account_number
                ,MAX(CASE  WHEN csh.subscription_sub_type ='Broadband DSL Line'
                                AND(status_code in ('AC','AB')
                                OR (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                                OR (status_code='CF' AND prev_status_code='PC'                                  )
                                OR (status_code='AP' AND sale_type='SNS Bulk Migration'                         ))
                           THEN 1 ELSE 0 END)  AS bb
                ,MAX(CASE  WHEN csh.subscription_sub_type = 'SKY TALK SELECT'
                                AND(csh.status_code = 'A'
                                OR (csh.status_code = 'FBP' AND prev_status_code in ('PC','A'))
                                OR (csh.status_code = 'RI'  AND prev_status_code in ('FBP','A'))
                                OR (csh.status_code = 'PC'  AND prev_status_code = 'A')        )
                           THEN 1 ELSE 0 END)   AS talk
           ,MAX(CASE       WHEN csh.subscription_sub_type ='SKY TALK LINE RENTAL'
                                AND csh.status_code in ('A','CRQ','PAX')
                           THEN 1 ELSE 0 END) AS wlr
into #talk_prods
FROM            sk_prod.cust_subs_hist AS csh
        LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel
                on csh.current_short_description = cel.short_description
        inner join VIQ_HH_ACCOUNT_TMP u
                on csh.account_number = u.account_number
WHERE           csh.effective_from_dt < today()
AND             csh.effective_to_dt   >= csh.effective_from_dt
AND             csh.effective_from_dt <> csh.effective_to_dt
GROUP BY        csh.account_number;
commit;

--only the 4 variables required
Select          account_number,
            CASE WHEN ((b.bb = 0) AND (b.talk = 0) AND (b.wlr = 0)) THEN 'DTV Only'
                 --WHEN ((b.bb = 1) AND (b.talk = 0) AND (b.wlr = 0)) THEN 'TV and Broadband'
                 WHEN ((b.bb = 0) AND (b.talk = 1) AND (b.wlr = 0)) THEN 'Talk Only'
                 --WHEN ((b.bb = 0) AND (b.talk = 0) AND (b.wlr = 1)) THEN 'TV and Line Rental'
                 --WHEN ((b.bb = 1) AND (b.talk = 1) AND (b.wlr = 0)) THEN 'TV, SkyTalk and Broadband'
                WHEN ((b.bb = 1) AND (b.talk = 0) AND (b.wlr = 1)) THEN 'BB Only'
                 --WHEN ((b.bb = 0) AND (b.talk = 1) AND (b.wlr = 1)) THEN 'TV, SkyTalk and Line Rental'
                 WHEN ((b.bb = 1) AND (b.talk = 1) AND (b.wlr = 1)) THEN 'Triple Play'
                 ELSE 'Unknown'
            END products
into            #prods
from #talk_prods b;
commit;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


