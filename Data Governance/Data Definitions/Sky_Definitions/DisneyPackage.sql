/*###############################################################################
# Created on:   14/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for if a household currently has a Disney package
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

Update ADsmart
set Disney = 1
  from sk_prod.cust_subs_hist as csh
        inner join ADsmart as base on base.account_number = csh.account_number
   and subscription_type ='A-LA-CARTE'                          --A La Carte Stack
   and subscription_sub_type in ('DTV Disney Channel')          --ESPN or disney Subscriptions
   and status_code in ('AC','AB','PC')                          --Active Status Codes
   and effective_from_dt <= @today                              --Start on or before 1st Jan
   and effective_to_dt > @today                                 --ends after 1st Jan
   and effective_from_dt<>effective_to_dt;                      --remove duplicate records
commit;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


