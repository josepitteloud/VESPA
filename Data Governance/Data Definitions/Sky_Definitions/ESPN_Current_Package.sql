/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining the current household that have
#		ESPN
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

-- create temp table with ESPN data in it
SELECT  distinct base.account_number
       ,1 AS ESPN_Subscribers
INTO #ESPN
  FROM sk_prod.cust_subs_hist AS ESPN
        inner join VIQ_HH_ACCOUNT_TMP AS Base
         ON ESPN.account_number = Base.account_number
 WHERE subscription_type ='A-LA-CARTE'               --A La Carte Stack
   AND subscription_sub_type = 'ESPN'                --ESPN Subscriptions
   AND status_code in ('AC','AB','PC')               --Active Status Codes
   AND ESPN.effective_from_dt <= today()
   AND ESPN.effective_to_dt > today()                --9999-09-09 where not set
   AND ESPN.effective_from_dt <> ESPN.effective_to_dt;

commit;

--create unique index on account_number
create unique index viq_espn_acnum_idx on #ESPN (account_number asc);

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################

