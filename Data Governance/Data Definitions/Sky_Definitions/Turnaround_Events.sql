 /*###############################################################################
# Created on:   08/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives an indicator as to the number of attempts (and saves) over
#		the last 12 months that an account made to leave Sky
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 08/01/2013  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################


SELECT    cca.account_number
         ,count(*) AS TA_attempts
         ,sum(CASE WHEN cca.Wh_Attempt_Outcome_Description_1 IN ( 'Turnaround Saved'
                                                                 ,'Legacy Save'
                                                                 ,'Home Move Saved'
                                                                 ,'Home Move Accept Saved')
                   THEN 1
                   ELSE 0
          END) AS TA_saves
INTO TA_attempts
    FROM sk_prod.cust_change_attempt AS cca
         inner join sk_prod.cust_subscriptions AS subs
             ON cca.subscription_id = subs.subscription_id
   WHERE cca.change_attempt_type                  = 'CANCELLATION ATTEMPT'
     AND subs.ph_subs_subscription_sub_type       = 'DTV Primary Viewing'
     AND cca.attempt_date                        >= @date_minus__12
     AND cca.created_by_id                  NOT IN ('dpsbtprd', 'batchuser')
     AND cca.Wh_Attempt_Outcome_Description_1 in (  'Turnaround Saved'
                                                   ,'Legacy Save'
                                                   ,'Turnaround Not Saved'
                                                   ,'Legacy Fail'
                                                   ,'Home Move Saved'
                                                   ,'Home Move Not Saved'
                                                   ,'Home Move Accept Saved')
   GROUP BY cca.account_number;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


