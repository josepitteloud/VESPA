


     -- Captures all active accounts in cust_subs_hist
select * INTO #weekly_sample from
(     SELECT   account_number
             ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
         FROM sk_prod.cust_subs_hist
      WHERE subscription_sub_type IN ('DTV Primary Viewing')
        AND status_code IN ('AC','AB','PC')
        AND effective_from_dt    <= today()
        AND effective_to_dt      > today ()
        AND effective_from_dt    <> effective_to_dt
        AND EFFECTIVE_FROM_DT    IS NOT NULL
        AND cb_key_household     > 0
        AND cb_key_household     IS NOT NULL
        AND cb_key_individual    IS NOT NULL
        AND account_number       IS NOT NULL
        AND service_instance_id  IS NOT NULL) t
where rank = 1

select * INTO #accounts_AB_Sample
from
        (SELECT  a.account_number
		,'Yes' Missed_payment
             ,rank() over (PARTITION BY a.account_number ORDER BY a.effective_from_dt desc, a.cb_row_id) AS rank,
a.effective_from_dt,
a.effective_to_dt
       FROM sk_prod.cust_subs_hist a,
#weekly_sample b
      WHERE a.account_number = b.account_number
and a.subscription_sub_type IN ('DTV Primary Viewing')
        AND a.status_code = 'AB'
        AND a.effective_from_dt    >= today() - 365
        AND a.effective_to_dt      <= today () 
        AND a.effective_from_dt    <> effective_to_dt
        AND a.EFFECTIVE_FROM_DT    IS NOT NULL
        AND a.cb_key_household     > 0
        AND a.cb_key_household     IS NOT NULL
        AND a.cb_key_individual    IS NOT NULL
        AND a.account_number       IS NOT NULL
        AND a.service_instance_id  IS NOT NULL) p
where rank = 1

select count(1) from #accounts_AB_Sample
