/*###############################################################################
# Created on:   11/01/2013
# Created by:   Tony Kinnaird (TKD)
# 
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 11/01/2013  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

---------------------------------------------------------------------------------
--  Home_Mover
---------------------------------------------------------------------------------

select       nw.account_number
            ,nw.ad_effective_from_dt    as addr_change_dt
            ,nw.ad_effective_to_dt
            ,nw.change_reason           as change_reason
            ,rank() over(partition by nw.account_number order by addr_change_dt DESC, nw.ad_effective_to_dt DESC, nw.cb_row_id DESC) as rank1
into        HM
from        sk_prod.cust_all_address AS nw
where       nw.ad_effective_from_dt <= @today
    and     nw.address_role = 'INSTALL'
    and     nw.account_number <> '?'
    and     nw.account_number IS NOT NULL
    AND     nw.change_reason LIKE ('Move Home%')
    and     nw.account_number in (select account_number from adsmart);
commit;

DELETE FROM HM where rank1 >1;
commit;

Update ADsmart
SET  base.Home_mover = sm.rank1
FROM ADsmart as base INNER JOIN HM AS sm
ON base.account_number = sm.account_number;
commit;

drop table HM;
commit;


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


