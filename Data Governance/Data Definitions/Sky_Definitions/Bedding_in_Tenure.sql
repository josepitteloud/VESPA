/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining the #bedding in period of
#		a first year Sky customer
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

select distinct
       a.account_number,
       acct_first_account_activation_dt,
       datediff(mm, acct_first_account_activation_dt, getdate()) sky_months,
       CASE WHEN datediff(mm, acct_first_account_activation_dt, getdate()) between 0 and 3 THEN '0 - 3 months'
            WHEN datediff(mm, acct_first_account_activation_dt, getdate()) between 4 and 12 THEN '4 - 12 months'
            WHEN datediff(mm, acct_first_account_activation_dt, getdate()) between 13 and 24 THEN '13 - 24 months'
            ELSE 'Other' END bedding_band,
       cust_active_dtv,
       rank() over(PARTITION BY a.account_number ORDER BY acct_first_account_activation_dt desc) AS rank_id
  INTO #tenure_b
  from VIQ_HH_ACCOUNT_TMP a
       LEFT JOIN sk_prod.cust_single_account_view SAV
    ON A.account_number = SAV.Account_number;
commit;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################

