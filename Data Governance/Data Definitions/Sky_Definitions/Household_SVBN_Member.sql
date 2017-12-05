/*###############################################################################
# Created on:   21/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for those customers 
#		who are on the SVBN (Sky Voice Broadband Network)
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 21/09/2012  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

---------------------------------------------------------------------------------
--  SVBN account  [HH_ON_SVBN]
---------------------------------------------------------------------------------
SELECT distinct csh.account_number
 ,1 as HH_ON_SVBN
INTO #SVBN
FROM sk_prod.cust_subs_hist as CSH
        inner join VIQ_HH_ACCOUNT_TMP as VIQ
           on CSH.account_number = VIQ.account_number
 WHERE technology_code = 'MPF' --SBVN Technology Code
 AND effective_from_dt <= today() --CHANGE THIS!
 AND effective_to_dt > today() --CHANGE THIS!
 AND effective_from_dt != effective_to_dt
 AND ( ( csh.subscription_sub_type = 'SKY TALK SELECT' -- Sky Talk
 AND ( csh.status_code = 'A'
        or (csh.status_code = 'FBP'and prev_status_code in ('PC','A'))
        or (csh.status_code = 'RI' and prev_status_code in ('FBP','A'))
        or (csh.status_code = 'PC' and prev_status_code = 'A')))
        OR ( SUBSCRIPTION_SUB_TYPE ='SKY TALK LINE RENTAL' -- Line Rental
  AND status_code IN ('A') )
        OR ( SUBSCRIPTION_SUB_TYPE ='Broadband DSL Line' -- Broadband
  and ( status_code in ('AC','AB')
        or (status_code='PC' and prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
        or (status_code='CF' and prev_status_code='PC')
        or (status_code='AP' and sale_type='SNS Bulk Migration') )
));
commit;

create unique index viq_idx1 on #SVBN (account_number asc);
commit;


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


