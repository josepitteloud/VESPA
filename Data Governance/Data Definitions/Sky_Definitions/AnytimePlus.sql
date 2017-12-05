/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining the if an account has Anytime Plus
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

---------------------------------------------------------------------------------
--  Anytime+ activated [HH_HAS_ANYTIMEPLUS]
---------------------------------------------------------------------------------
SELECT base.account_number
       ,1 AS Anytime_plus
INTO #Anytime_plus
FROM   sk_prod.CUST_SUBS_HIST AS Aplus
        inner join VIQ_HH_ACCOUNT_TMP as Base
         on Aplus.account_number = Base.account_number
WHERE  subscription_sub_type = 'PDL subscriptions'
AND        status_code='AC'
AND        first_activation_dt<=today()               -- (END)
AND        first_activation_dt>='2010-10-01'        -- (START) Oct 2010 was the soft launch of A+, no one should have it before then
AND        base.account_number is not null
AND        base.account_number <> '?'
GROUP BY   base.account_number;
commit;

----------------------------------------Adsmart query---------------------------------

--code_location_05     code changed in line with changes to Wiki

SELECT base.account_number
       ,1 AS Anytime_plus
INTO Anytime_plus
FROM   sk_prod.CUST_SUBS_HIST AS Aplus
        inner join AdSmart as Base
         on Aplus.account_number = Base.account_number
WHERE  subscription_sub_type = 'PDL subscriptions'
AND        status_code='AC'
AND        first_activation_dt<@today               -- (END)
AND        first_activation_dt>='2010-10-01'        -- (START) Oct 2010 was the soft launch of A+, no one should have it before then
AND        base.account_number is not null
AND        base.account_number <> '?'
GROUP BY   base.account_number;
commit;


--      create index on Anytime_plus file
CREATE   HG INDEX idx05 ON Anytime_plus(account_number);
commit;

--      update AdSmart file
UPDATE AdSmart
SET Anytime_plus = Aplus.Anytime_plus
FROM AdSmart  AS Base
       INNER JOIN Anytime_plus AS Aplus
        ON base.account_number = APlus.account_number
ORDER BY base.account_number;
commit;

DROP TABLE Anytime_plus;
commit;



-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


