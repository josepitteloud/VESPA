/*###############################################################################
# Created on:   14/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for identifying households that will have 
# 		their accounts cancelled in the next 30 days (from when the query
#		is run)
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 14/01/2013  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

-------------------------------------------------  02 - Platform Notice

SELECT distinct account_number
        ,FUTURE_SUB_EFFECTIVE_DT AS Pend_Can_date
        ,1 AS Pending_cancel
INTO pending
FROM sk_prod.cust_subs_hist
WHERE subscription_sub_type = 'DTV Primary Viewing'
  AND account_number IS NOT NULL
  AND status_code IN  ('PC')
  AND status_end_dt = '9999-09-09'
  AND FUTURE_SUB_EFFECTIVE_DT >=@today
  AND FUTURE_SUB_EFFECTIVE_DT <=@today+30;
commit;

--      update AdSmart file
UPDATE AdSmart
SET   Pend_Can_date = cnx.Pend_Can_date
     ,Pending_cancel = cnx.Pending_cancel
     FROM AdSmart  AS Base
  INNER JOIN pending AS cnx
        ON base.account_number = cnx.account_number;
commit;

-- delete temp file
drop table pending;
commit;


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


