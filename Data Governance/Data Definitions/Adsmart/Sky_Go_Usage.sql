/*###############################################################################
# Created on:   10/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  The number of times a household has used Sky Go in the last 12 
#		12 months
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 10/01/2013  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Original Query								    #####
-- ##############################################################################################################


select base.account_number,
        1 AS SKY_GO_USAGE
--        ,sum(SKY_GO_USAGE)
into skygo_usage
from sk_prod.SKY_PLAYER_USAGE_DETAIL AS usage
        inner join AdSmart AS Base
         ON usage.account_number = Base.account_number
where cb_data_date >= @date_minus__12
        AND cb_data_date <@today
group by base.account_number;
commit;

--      create index on Sky_Go file
CREATE   HG INDEX idx06 ON skygo_usage(account_number);
commit;

--      update AdSmart file
UPDATE AdSmart
SET Sky_Go_Reg = sky_go.SKY_GO_USAGE
FROM AdSmart  AS Base
       INNER JOIN skygo_usage AS sky_go
        ON base.account_number = sky_go.account_number
ORDER BY base.account_number;
commit;

DROP TABLE skygo_usage;
commit;



-- ##############################################################################################################
-- ##### STEP 1.0 Ended									                    #####
-- ##############################################################################################################
