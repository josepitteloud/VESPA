/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining if the account is on 1 months
#		notice to leave Sky
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

---------------------------------------------------------------------------------
--  Platform Notice  [HH_PENDING_CANCEL_1_MNTH]
---------------------------------------------------------------------------------

SELECT distinct base.account_number
,1 AS Pending_cancel
INTO #pending
FROM sk_prod.cust_subs_hist as sky
         inner join VIQ_HH_ACCOUNT_TMP as Base
                    on sky.account_number = base.account_number
WHERE subscription_sub_type = 'DTV Primary Viewing'
  AND base.account_number IS NOT NULL
  AND status_code IN  ('PC')
  AND status_end_dt = '9999-09-09'
  AND FUTURE_SUB_EFFECTIVE_DT >=today()
  AND FUTURE_SUB_EFFECTIVE_DT <=today()+30;

commit;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


