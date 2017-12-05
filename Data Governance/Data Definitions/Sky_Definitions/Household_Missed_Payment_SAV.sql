select 
account_number,
acct_latest_active_block_dt,
case when ACCT_COUNT_ACTIVE_BLOCKED_IN_12m > 0 then 'Yes'
else 'No' end cust_active_blocked_last_12_months
into #sav_acct_latest_active_block_dt
from sk_prod.cust_single_account_view
where cust_active_dtv = 1
and ACCT_COUNT_ACTIVE_BLOCKED_IN_12m > 0

select count(1) from #sav_acct_latest_active_block_dt
