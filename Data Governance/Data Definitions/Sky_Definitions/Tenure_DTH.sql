/*###############################################################################
# Created on:   13/02/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for obtaining Active DTH customers 
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 13/02/2013  TKD   v01 - initial version
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

select distinct
       sav.account_number,
       acct_first_account_activation_dt,
       datediff(mm, acct_first_account_activation_dt, getdate()) sky_months,
       cust_active_dtv,
       rank() over(PARTITION BY sav.account_number ORDER BY acct_first_account_activation_dt desc) AS rank_id
  INTO tenure_b
  from sk_prod.cust_single_account_view SAV;
commit;

DELETE FROM  tenure_b where rank_id >1;
commit;

CREATE INDEX idx2 ON tenure_b(account_number);
commit;

select *
into tenure_b_dup
from tenure_b
where account_number in (select account_number
                           from (select account_number, count(1) sample_count
                                   from tenure_b
                               group by account_number) a
                          where a.sample_Count > 1);
commit;


--delete duplicated rows (from tenure_b)
delete tenure_b
where account_number in (select account_number
                           from tenure_b_dup);
commit;

--insert one row of the duplicates
insert tenure_b
select distinct *
  from tenure_b_dup
 where cust_active_dtv = 1;--restrict
commit;

-- tmp so we can put a new unique index on
select *
into tenure_c
from tenure_b;
commit;

--add index
CREATE UNIQUE INDEX idx2 ON tenure_c(account_number);
commit;

drop table tenure_b;
drop table tenure_b_dup;

select top 10 * from tenure_c



-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


