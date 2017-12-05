/*###############################################################################
# Created on:   21/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for understanding the most recent reason
#		why a customer has churned.
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

--  Last Reason for CHURN  [[HH_LAST_REASON_FOR_CHURN]]
-- at account level
---------------------------------------------------------------------------------

   --Create a list of all the churn Records
select  account_number
       ,effective_from_dt as churn_date --add this into table also
       ,case when status_code = 'PO'
             then 'CUSCAN'
             else 'SYSCAN'
         end as churn_type
       ,RANK() OVER (PARTITION BY  csh.account_number
                     ORDER BY  csh.effective_from_dt desc,csh.cb_row_id) AS 'RANK'  --Rank to get the first event
  into #all_churn_records
  from sk_prod.cust_subs_hist as csh
 where subscription_sub_type ='DTV Primary Viewing'     --DTV stack
   and status_code in ('PO','SC')                       --CUSCAN and SYSCAN status codes
   and prev_status_code in ('AC','AB','PC')             --Previously ACTIVE
   and status_code_changed = 'Y'
   and effective_from_dt != effective_to_dt
   and account_number in (select account_number          --Sub Query to findlist of mailing recipients
                            from VIQ_HH_ACCOUNT_TMP);
commit;


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


