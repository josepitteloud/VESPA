/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining if a household has hd,multiroom
#		or skyplus
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

SELECT  csh.account_number
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Extra Subscription'
                      THEN 1 ELSE 0 END) AS multiroom
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV HD'
                      THEN 1 ELSE 0 END) AS hdtv
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Sky+'
                      THEN 1 ELSE 0 END) AS skyplus
INTO #hdpm
      FROM VIQ_HH_ACCOUNT_TMP as ad
           inner join sk_prod.cust_subs_hist AS csh
        on ad.account_number = csh.account_number
     WHERE csh.subscription_sub_type  IN ('DTV Extra Subscription'
                                         ,'DTV HD'
                                         ,'DTV Sky+')
       AND csh.status_code in  ('AC','AB','PC')
       AND csh.effective_from_dt <> csh.effective_to_dt
       AND csh.effective_from_dt <= today()
       AND csh.effective_to_dt    > today()
GROUP BY csh.account_number;
commit;

CREATE UNIQUE hg INDEX idx1 ON #hdpm(account_number);

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################

