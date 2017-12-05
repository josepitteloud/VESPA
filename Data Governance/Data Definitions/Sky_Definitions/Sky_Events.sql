/*###############################################################################
# Created on:   14/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining if the account has used Sky Events
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

---------------------------------------------------------------------------------
--  Customer has used Sky Events
---------------------------------------------------------------------------------

SELECT    base.account_number
         ,count(*) as Sky_Events_L12
INTO Sky_events
    FROM sk_prod.SKY_REWARDS_EVENTS as sky
         inner join AdSmart as Base
                    on sky.account_number = base.account_number
    WHERE Date_registered >= @date_minus__12
    GROUP BY base.account_number;
commit;


--      create index on Sky_events file
CREATE   HG INDEX idx04 ON Sky_events(account_number);
commit;

--      update AdSmart file
UPDATE AdSmart
SET Sky_Events_L12 = sky.Sky_Events_L12
FROM AdSmart  AS Base
       INNER JOIN Sky_events AS sky
        ON base.account_number = sky.account_number
ORDER BY base.account_number;
commit;

DROP TABLE Sky_events;
commit;


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


