/*###############################################################################
# Created on:   16/01/2013
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
# 16/01/2013  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

SELECT  csh.account_number
           ,1 AS multiroom
           ,SUM(multiroom) AS total_MR
INTO MR_HD_count
      FROM sk_prod.cust_subs_hist AS csh
           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup AS cel
           ON csh.current_short_description = cel.short_description
     WHERE csh.subscription_sub_type  IN ('DTV Extra Subscription')
       AND csh.effective_from_dt <= @today
       AND csh.effective_to_dt    > @today
       AND csh.effective_from_dt <> csh.effective_to_dt
       AND csh.status_code in  ('AC','AB','PC')
       AND account_number in (SELECT account_number
                                FROM AdSmart)
GROUP BY csh.account_number, multiroom;
commit;

--      create index on MR_HD_count
CREATE   HG INDEX idx13 ON MR_HD_count(account_number);
commit;

--      update AdSmart file
UPDATE AdSmart
SET  total_MR = MR.total_MR
FROM AdSmart  AS Base
  INNER JOIN MR_HD_count AS MR
        ON base.account_number = MR.account_number
            ORDER BY base.account_number;
commit;

-- delete temp file
drop table MR_HD_count;
commit;

-- delete temp file
--drop table SetTop;
--commit;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


