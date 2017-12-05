/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining if an account has tried to leave
#		Sky in the last 12 months but been convinced to stay.
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 20/08/2012  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################


---------------------------------------------------------------------------------
--  TA Attempts  [HH_TURNAROUND_LAST_YEAR]
---------------------------------------------------------------------------------
SELECT    cca.account_number
         ,1 AS TA_attempts
INTO #TA_attempts
    FROM sk_prod.cust_change_attempt AS cca
         inner join sk_prod.cust_subscriptions AS subs
            ON cca.subscription_id = subs.subscription_id
   WHERE cca.change_attempt_type                  = 'CANCELLATION ATTEMPT'
     AND subs.ph_subs_subscription_sub_type       = 'DTV Primary Viewing'
     AND cca.attempt_date                        >= DATEADD(day,-365,today())
     AND cca.created_by_id                  NOT IN ('dpsbtprd', 'batchuser')
     AND cca.Wh_Attempt_Outcome_Description_1 in (  'Turnaround Saved'
                                                   ,'Legacy Save'
                                                   ,'Turnaround Not Saved'
                                                   ,'Legacy Fail'
                                                   ,'Home Move Saved'
                                                   ,'Home Move Not Saved'
                                                   ,'Home Move Accept Saved')
   GROUP BY cca.account_number;
--1782256 Row(s) affected
commit;

--      create index on #TA_attempts file
CREATE   HG INDEX idx03 ON #TA_attempts(account_number);
commit;

--      update VIQ_HH_ACCOUNT_TMP file
UPDATE VIQ_HH_ACCOUNT_TMP
SET HH_TURNAROUND_LAST_YEAR  = TA.TA_attempts
FROM VIQ_HH_ACCOUNT_TMP  AS Base
       INNER JOIN #TA_attempts AS TA
        ON base.account_number = TA.account_number;
commit;


drop table #TA_attempts;
commit;


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################

