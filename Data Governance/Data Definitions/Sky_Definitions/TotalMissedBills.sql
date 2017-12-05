/*###############################################################################
# Created on:   14/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for identifying the number of missed bills
#		that a household has made over a defined period.
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
--  Total number of missed bills that a household has over a defined period
---------------------------------------------------------------------------------

-------------------------------------------------  02 - Unbilled accounts
-- previous missed payments need to fine tune (status = Unbilled)
--code_location_16
SELECT account_number, 1 AS miss, SUM(miss) AS Total_missed
INTO missed
FROM sk_prod.cust_bills
WHERE payment_due_dt between @date_minus__12 AND @today
        AND Status = 'Unbilled'
GROUP BY account_number;
commit;

--select top 100 * from missed;
--select Total_missed, count(*) from missed group by Total_missed;

--      create index on missed
CREATE   HG INDEX idx10 ON missed(account_number);
commit;

--      update AdSmart file
UPDATE AdSmart
SET  Total_miss_pmt = miss.Total_missed
FROM AdSmart  AS Base
  INNER JOIN missed AS miss
        ON base.account_number = miss.account_number
            ORDER BY base.account_number;
commit;

drop table missed;
commit;


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


