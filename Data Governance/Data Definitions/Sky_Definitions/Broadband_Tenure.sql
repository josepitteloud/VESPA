/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining the broadband tenure and length of time
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
# 13/02/2013  TKD   V02 - Reworked to give a generic table 
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

---------------------------------------------------------------------------------
--  Sky BroadBand
---- need to check the defintion of the tenure

---------------------------------------------------------------------------------


SELECT          csh.Account_number,
                effective_from_dt,
                effective_to_dt,
                CASE WHEN effective_to_dt > today() THEN 1 ELSE 0 END active_today,
                CASE WHEN effective_to_dt > today() THEN datediff(DAY,effective_from_dt,getdate()) ELSE datediff(DAY,effective_from_dt, effective_to_dt) END active_days

INTO            #Activations2
FROM            sk_prod.cust_subs_hist csh
--INNER JOIN      VIQ_HH_ACCOUNT_TMP acc
--                ON  csh.account_number = acc.account_number

                WHERE           status_code = 'AC'
                AND             Prev_status_code NOT IN ('AB','AC','PC')
                AND             subscription_sub_type ='Broadband DSL Line'
                AND             status_code_changed = 'Y'
ORDER BY csh.Account_number;
;
--GROUP BY        csh.Account_number;

COMMIT;

  select account_number,
         max(active_today) has_bb,
         sum(active_days) bb_tenure
    into Activations
    from #Activations2
group by account_number;
commit;

--      create index on #Activations
CREATE   HG INDEX idx100 ON Activations(account_number);
commit;

drop table #Activations2;

select top 10 bb.*,
TENURE_BROADBAND = datediff(MM,getdate()-BB.bb_tenure, getdate())
from
Activations bb

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


