/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining the if an account has Skygo 
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
--  Sky Go Usage [HH_HAS_SKYGO]
---------------------------------------------------------------------------------
select base.account_number,
        1 AS SKY_GO_USAGE
into #skygo_usage
from sk_prod.SKY_PLAYER_USAGE_DETAIL AS usage
        inner join VIQ_HH_ACCOUNT_TMP AS Base
         ON usage.account_number = Base.account_number
where cb_data_date >= DATEADD(day,-365,today())
        AND cb_data_date <today()
group by base.account_number;
commit;


--      create index on #Sky_Go file
CREATE   HG INDEX idx06 ON #skygo_usage(account_number);
commit;

--      update VIQ_HH_ACCOUNT_TMP file
UPDATE VIQ_HH_ACCOUNT_TMP
SET HH_HAS_SKYGO = sky_go.SKY_GO_USAGE
FROM VIQ_HH_ACCOUNT_TMP  AS Base
       INNER JOIN #skygo_usage AS sky_go
        ON base.account_number = sky_go.account_number
ORDER BY base.account_number;
commit;

DROP TABLE #skygo_usage;
commit;



-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


