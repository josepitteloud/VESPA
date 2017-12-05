/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining the tenure 
#			of a customer, active periods only
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
# 13/02/2013  TKD   v02 - Updated version of the code
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

SELECT distinct
       account_number,
       status_code,
       CASE WHEN effective_to_dt > today()
            THEN datediff(dd, effective_from_dt, getdate())
            ELSE datediff(dd, effective_from_dt, effective_to_dt)
        END active_days
  INTO active_t
  FROM sk_prod.cust_subs_hist
 WHERE subscription_sub_type IN ('DTV Primary Viewing')
   AND status_code IN ('AC','AB','PC')
   AND effective_from_dt <= today()
   AND cb_key_household > 0             --UK Only
   AND account_number IS NOT NULL
   AND service_instance_id IS NOT NULL;
commit;

--total number of active months per account

  select account_number,
         datediff(MM,getdate()-sum(active_days), getdate()) TENURE_ACTIVE_ONLY_DTH
    into active_t_agg
    from active_t
group by account_number;
commit;

----add index
CREATE UNIQUE INDEX idx1 ON active_t_agg(account_number);
commit;

select top 10 * from active_t_agg


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


