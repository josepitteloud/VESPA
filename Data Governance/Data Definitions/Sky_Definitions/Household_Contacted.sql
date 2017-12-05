/*###############################################################################
# Created on:   21/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for those customers 
#		been contacted by Sky in the last 6 months
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
--  calls recieved in the Last 6 months  [HH_CONTACT_LAST_6M]
---------------------------------------------------------------------------------

SELECT  distinct calls.Account_Number
,1 AS CUST_CONTACT
INTO #calls
FROM sk_prod.cust_contact AS calls
        inner join VIQ_HH_ACCOUNT_TMP as VIQ
           on calls.account_number = VIQ.account_number
INNER JOIN VIQ_HH_ACCOUNT_TMP AS base ON calls.Account_Number = base.Account_Number
WHERE calls.created_date BETWEEN today()-180 AND today() -- Limiting to calls in JAN 2012   - change date range (today()- 6)
AND calls.contact_channel = 'I PHONE COMMUNICATION' -- Limiting to Inbound Calls
AND calls.contact_grouping_identity IS NOT NULL; -- Limiting to Inbound Calls

commit;


create unique index viq_idx2 on #calls (account_number asc);
commit;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


