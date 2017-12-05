create variable @profiling_date date;

set @profiling_date = '2012-08-03';

drop table V044_base;

select account_number
        ,convert(bit, 0) as has_sky_talk
        ,convert(bit, 0) as has_SVBN
        ,convert(bit, 0) as is_OnNet
into V044_base
from sk_prod.cust_single_account_view
where CUST_ACTIVE_DTV = 1 and acct_type='Standard' and account_number <>'?' and pty_country_code ='GBR'
group by account_number;

commit;

create unique index fake_pk on V044_base (account_number);

-- how to get SVBN:

SELECT DISTINCT account_number
into #SVBN_Customers
  FROM sk_prod.cust_subs_hist as csh
 WHERE technology_code = 'MPF'                --SBVN Technology Code
   AND effective_from_dt <= @profiling_date
   AND effective_to_dt   >  @profiling_date
   AND effective_from_dt != effective_to_dt
   AND (     (     csh.subscription_sub_type = 'SKY TALK SELECT'   -- Sky Talk
                       and (     csh.status_code = 'A'
                             or (csh.status_code = 'FBP' and prev_status_code in ('PC','A'))
                             or (csh.status_code = 'RI'  and prev_status_code in ('FBP','A'))
                             or (csh.status_code = 'PC'  and prev_status_code = 'A')))
         OR  (     SUBSCRIPTION_SUB_TYPE ='SKY TALK LINE RENTAL'   -- Line Rental
               AND status_code IN  ('A')  )
         OR  (     SUBSCRIPTION_SUB_TYPE ='Broadband DSL Line'     -- Broadband
               and ( status_code in ('AC','AB')
                 or (status_code='PC' and prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                 or (status_code='CF' and prev_status_code='PC')
                 or (status_code='AP' and sale_type='SNS Bulk Migration')  )
             )
       )
;

commit;
create unique index fake_pk on #SVBN_Customers (account_number);
commit;

update V044_base
set has_SVBN = 1
from V044_base
inner join  #SVBN_Customers as c
on V044_base.account_number = c.account_number;

commit;

-- Now for Sky Talk

  SELECT DISTINCT account_number
  into #sky_talk_customers
    FROM sk_prod.cust_subs_hist
   WHERE subscription_sub_type = 'SKY TALK SELECT'
     AND(     status_code = 'A'
          or (status_code = 'FBP' and prev_status_code in ('PC','A'))
          or (status_code = 'RI'  and prev_status_code in ('FBP','A'))
          or (status_code = 'PC'  and prev_status_code = 'A')

        )
     AND effective_from_dt <= @profiling_date
     AND effective_to_dt > @profiling_date
     AND effective_to_dt != effective_from_dt;

commit;
create unique index fake_pk on #sky_talk_customers (account_number);
commit;

update V044_base
set has_sky_talk = 1
from V044_base
inner join  #sky_talk_customers as c
on V044_base.account_number = c.account_number;

-- OK, now, finally, the Onnet / Offnet: because it's hurried, and recent, we'll
-- get the most recent (thoguh not best) hack from SAV. If only we'd know, we'd
-- have got it when we built the population....

SELECT distinct sav.account_number
       ,bpe.exchange_status  --ONNET / OFFNET etc
into #onnnet_customers
  FROM sk_prod.cust_single_account_view as sav
       INNER JOIN sk_prod.Broadband_Postcode_Exchange as bpe ON sav.cb_address_postcode = bpe.cb_address_postcode
 WHERE acct_status_code in ('AC')    --active accounts for this sample
 and CUST_ACTIVE_DTV = 1
 ;
 
 commit;
 create index not_a_pk_because_we_dunno_if_exchange_status_is_good on #onnnet_customers (account_number);
 commit;
 
 -- Are they well defined?
 select account_number, count(1) as hits
 from #onnnet_customers
 group by account_number
 having hits > 1;
 -- Nothing, good!

 select distinct exchange_status from #onnnet_customers;
 -- hah, there's like 5 of them....

 
 alter table V044_base drop is_OnNet, add exchange_status varchar(20);
 
 update V044_base
 set exchange_status = oc.exchange_status
 from V044_base inner join #onnnet_customers as oc
 on V044_base.account_number = oc.account_number;
 
 -- OK, so we're done, the results pull is:
 
 select has_sky_talk, has_SVBN, exchange_status, count(1) as active_standard_accounts
 from V044_base
 group by has_sky_talk, has_SVBN, exchange_status
 order by has_sky_talk, has_SVBN, exchange_status;
 
