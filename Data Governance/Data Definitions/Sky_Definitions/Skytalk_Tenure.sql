/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining the skytalk tenure and length of time
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

SELECT DISTINCT csh.account_number
       ,1 as talk_product
      ,rank() over(PARTITION BY csh.account_number ORDER BY effective_to_dt desc) AS rank_id
      ,effective_from_dt
      ,effective_to_dt
       ,datediff(MM,effective_from_dt,today()) TENURE_TALK_MONTHS
         INTO talk
FROM sk_prod.cust_subs_hist AS CSH
WHERE subscription_sub_type = 'SKY TALK SELECT'
     AND(     status_code = 'A'
          OR (status_code = 'FBP' AND prev_status_code IN ('PC','A'))
          OR (status_code = 'RI'  AND prev_status_code IN ('FBP','A'))
          OR (status_code = 'PC'  AND prev_status_code = 'A'))
     AND effective_to_dt != effective_from_dt
     AND csh.effective_from_dt <= today()
     AND csh.effective_to_dt > today();

commit;

DELETE FROM talk where rank_id >1;
commit;


--      create index on #talk
CREATE   HG INDEX idx09 ON talk(account_number);

commit;

select top 10 * from talk

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


