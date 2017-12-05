

---Box Return Data by Day----------
--Anything pre 5 am UTC credited to previous day
--drop table vespa_analysts.project060_monthly_log_summary;
create table vespa_analysts.project060_monthly_log_summary

(subscriber_id bigint
,account_number varchar(20)
,log_creation_day date
)
;


insert into vespa_analysts.project060_monthly_log_summary
select
        subscriber_id, account_number
        ,convert(date, dateadd(hh, -5, (stb_log_creation_date))) as log_creation_day
from sk_prod.VESPA_STB_PROG_EVENTS_20120423
where panel_id in  (4,5) and document_creation_date is not null
group by subscriber_id, account_number, log_creation_day
;
commit;

insert into vespa_analysts.project060_monthly_log_summary
select
        subscriber_id, account_number
        ,convert(date, dateadd(hh, -5, (stb_log_creation_date))) as log_creation_day
from sk_prod.VESPA_STB_PROG_EVENTS_20120424
where panel_id in  (4,5) and document_creation_date is not null
group by subscriber_id, account_number, log_creation_day
;
commit;

insert into vespa_analysts.project060_monthly_log_summary
select
        subscriber_id, account_number
        ,convert(date, dateadd(hh, -5, (stb_log_creation_date))) as log_creation_day
from sk_prod.VESPA_STB_PROG_EVENTS_20120425
where panel_id in  (4,5) and document_creation_date is not null
group by subscriber_id, account_number, log_creation_day
;
commit;

insert into vespa_analysts.project060_monthly_log_summary
select
        subscriber_id, account_number
        ,convert(date, dateadd(hh, -5, (stb_log_creation_date))) as log_creation_day
from sk_prod.VESPA_STB_PROG_EVENTS_20120426
where panel_id in  (4,5) and document_creation_date is not null
group by subscriber_id, account_number, log_creation_day
;
commit;

insert into vespa_analysts.project060_monthly_log_summary
select
        subscriber_id, account_number
        ,convert(date, dateadd(hh, -5, (stb_log_creation_date))) as log_creation_day
from sk_prod.VESPA_STB_PROG_EVENTS_20120427
where panel_id in  (4,5) and document_creation_date is not null
group by subscriber_id, account_number, log_creation_day
;
commit;


insert into vespa_analysts.project060_monthly_log_summary
select
        subscriber_id, account_number
        ,convert(date, dateadd(hh, -5, (stb_log_creation_date))) as log_creation_day
from sk_prod.VESPA_STB_PROG_EVENTS_20120428
where panel_id in  (4,5) and document_creation_date is not null
group by subscriber_id, account_number, log_creation_day
;
commit;


insert into vespa_analysts.project060_monthly_log_summary
select
        subscriber_id, account_number
        ,convert(date, dateadd(hh, -5, (stb_log_creation_date))) as log_creation_day
from sk_prod.VESPA_STB_PROG_EVENTS_20120429
where panel_id in  (4,5) and document_creation_date is not null
group by subscriber_id, account_number, log_creation_day
;
commit;

insert into vespa_analysts.project060_monthly_log_summary
select
        subscriber_id, account_number
        ,convert(date, dateadd(hh, -5, (stb_log_creation_date))) as log_creation_day
from sk_prod.VESPA_STB_PROG_EVENTS_20120430
where panel_id in  (4,5) and document_creation_date is not null
group by subscriber_id, account_number, log_creation_day
;
commit;

--Dedupe wher same reporting day in multiple tables
--drop table vespa_analysts.project060_monthly_log_summary_deduped;
select subscriber_id, account_number
        ,max(case when log_creation_day = '2012-04-23' then 1 else 0 end ) as log_20120423
        ,max(case when log_creation_day = '2012-04-24' then 1 else 0 end ) as log_20120424
        ,max(case when log_creation_day = '2012-04-25' then 1 else 0 end ) as log_20120425
        ,max(case when log_creation_day = '2012-04-26' then 1 else 0 end ) as log_20120426
        ,max(case when log_creation_day = '2012-04-27' then 1 else 0 end ) as log_20120427
        ,max(case when log_creation_day = '2012-04-28' then 1 else 0 end ) as log_20120428
        ,max(case when log_creation_day = '2012-04-29' then 1 else 0 end ) as log_20120429
        ,max(case when log_creation_day = '2012-04-30' then 1 else 0 end ) as log_20120430
into  vespa_analysts.project060_monthly_log_summary_deduped
from vespa_analysts.project060_monthly_log_summary
group by subscriber_id, account_number
;

commit;

--select top 500 * from vespa_analysts.project060_monthly_log_summary_deduped;

---Create All Account View----



select account_number
        , cb_key_household
        , csh.current_short_description
        ,service_instance_id
        ,SUBSCRIPTION_SUB_TYPE
        ,status_code
        , rank() over (partition by account_number ,SUBSCRIPTION_SUB_TYPE,service_instance_id order by effective_from_dt, cb_row_id) as rank
into #sky_accounts -- drop table #sky_accounts
from sk_prod.cust_subs_hist as csh
where SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription') --the DTV + Multiroom sub Type
--   and status_code in ('AC','PC','AB')               --Active Status Codes (Including Active Block)
   and effective_from_dt <= '2012-04-23'           --Start on or before date
   and effective_to_dt >'2012-04-23'               --ends after date
   and effective_from_dt<>effective_to_dt            --ignore all but the last thing each customer did in a day

and account_number is not null;
commit;


delete from #sky_accounts where rank>1;
commit;

create hg index idx1 on #sky_accounts(service_instance_id);

create  hg index idx2 on #sky_accounts(cb_key_household);


---Create src_system_id lookup
--drop table  #subs_details;
select src_system_id
,min(cast(si_external_identifier as integer)) as subscriberid
,max(case when si_service_instance_type in ('Primary DTV') then 1 else 0 end) as primary_box
into #subs_details
from
sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
group by src_system_id
;
commit;

commit;
exec sp_create_tmp_table_idx '#subs_details', 'src_system_id';

commit;


---Join Together---
--drop table #account_subscriber_id_and_primary_secondary_type;
select a.*
,b.subscriberid as subscriber_id
into #account_subscriber_id_and_primary_secondary_type
from #sky_accounts as a
left outer join #subs_details as b
on a.service_instance_id=b.src_system_id
;
--select * from #account_subscriber_id_and_primary_secondary_type;

---Create a summary by account

select account_number
,max(case when subscription_sub_type='DTV Primary Viewing' and status_code in ('AC','AB','PC') then 1 else 0 end) as primary_box_active
,max(case when subscription_sub_type='DTV Extra Subscription' and status_code in ('AC','AB','PC') then 1 else 0 end) as secondary_box_active
into #account_summary_active
from #account_subscriber_id_and_primary_secondary_type
group by account_number
having primary_box_active = 1
;

select a.*
,case when subscription_sub_type='DTV Primary Viewing' and status_code in ('AC','AB','PC') then 1 else 0 end as primary_box
,case when subscription_sub_type='DTV Extra Subscription' and status_code in ('AC','AB','PC') then 1 else 0 end as secondary_box
into #add_box_type_for_returned_data
from vespa_analysts.project060_monthly_log_summary_deduped as a
left outer join #account_subscriber_id_and_primary_secondary_type as b
on a.subscriber_id = b.subscriber_id
;

select account_number
,max(case when primary_box=1 then log_20120423 else 0 end) as primary_box_log_20120423
,max(case when secondary_box=1 then log_20120423 else 0 end) as secondary_box_log_20120423

,max(case when primary_box=1 then log_20120424 else 0 end) as primary_box_log_20120424
,max(case when secondary_box=1 then log_20120424 else 0 end) as secondary_box_log_20120424

,max(case when primary_box=1 then log_20120425 else 0 end) as primary_box_log_20120425
,max(case when secondary_box=1 then log_20120425 else 0 end) as secondary_box_log_20120425

,max(case when primary_box=1 then log_20120426 else 0 end) as primary_box_log_20120426
,max(case when secondary_box=1 then log_20120426 else 0 end) as secondary_box_log_20120426

,max(case when primary_box=1 then log_20120427 else 0 end) as primary_box_log_20120427
,max(case when secondary_box=1 then log_20120427 else 0 end) as secondary_box_log_20120427

,max(case when primary_box=1 then log_20120428 else 0 end) as primary_box_log_20120428
,max(case when secondary_box=1 then log_20120428 else 0 end) as secondary_box_log_20120428

,max(case when primary_box=1 then log_20120429 else 0 end) as primary_box_log_20120429
,max(case when secondary_box=1 then log_20120429 else 0 end) as secondary_box_log_20120429
into #account_log_return_by_day_summary
  from #add_box_type_for_returned_data
group by account_number;

select * from #account_log_return_by_day_summary

--drop table #account_summary_return_data;
select a.account_number
,min(case when secondary_box_active =0 and primary_box_log_20120423=1 then '01: Single_box - Data Returned'
when secondary_box_active =0 and primary_box_log_20120423=0 then '02: Single_box - No Data Returned'
when secondary_box_active =1 and primary_box_log_20120423=1 and secondary_box_log_20120423=1 then '03: Multiple Boxes - Both Returned'
when secondary_box_active =1 and primary_box_log_20120423=1 and secondary_box_log_20120423=0 then '04: Multiple Boxes - Primary returned'
when secondary_box_active =1 and primary_box_log_20120423=0 and secondary_box_log_20120423=1 then '05: Multiple Boxes - Secondary returned'
else '99:Other' end) as account_status_20120423

,min(case when secondary_box_active =0 and primary_box_log_20120424=1 then '01: Single_box - Data Returned'
when secondary_box_active =0 and primary_box_log_20120424=0 then '02: Single_box - No Data Returned'
when secondary_box_active =1 and primary_box_log_20120424=1 and secondary_box_log_20120424=1 then '03: Multiple Boxes - Both Returned'
when secondary_box_active =1 and primary_box_log_20120424=1 and secondary_box_log_20120424=0 then '04: Multiple Boxes - Primary returned'
when secondary_box_active =1 and primary_box_log_20120424=0 and secondary_box_log_20120424=1 then '05: Multiple Boxes - Secondary returned'
else '99:Other' end) as account_status_20120424

,min(case when secondary_box_active =0 and primary_box_log_20120425=1 then '01: Single_box - Data Returned'
when secondary_box_active =0 and primary_box_log_20120425=0 then '02: Single_box - No Data Returned'
when secondary_box_active =1 and primary_box_log_20120425=1 and secondary_box_log_20120425=1 then '03: Multiple Boxes - Both Returned'
when secondary_box_active =1 and primary_box_log_20120425=1 and secondary_box_log_20120425=0 then '04: Multiple Boxes - Primary returned'
when secondary_box_active =1 and primary_box_log_20120425=0 and secondary_box_log_20120425=1 then '05: Multiple Boxes - Secondary returned'
else '99:Other' end) as account_status_20120425

,min(case when secondary_box_active =0 and primary_box_log_20120426=1 then '01: Single_box - Data Returned'
when secondary_box_active =0 and primary_box_log_20120426=0 then '02: Single_box - No Data Returned'
when secondary_box_active =1 and primary_box_log_20120426=1 and secondary_box_log_20120426=1 then '03: Multiple Boxes - Both Returned'
when secondary_box_active =1 and primary_box_log_20120426=1 and secondary_box_log_20120426=0 then '04: Multiple Boxes - Primary returned'
when secondary_box_active =1 and primary_box_log_20120426=0 and secondary_box_log_20120426=1 then '05: Multiple Boxes - Secondary returned'
else '99:Other' end) as account_status_20120426

,min(case when secondary_box_active =0 and primary_box_log_20120427=1 then '01: Single_box - Data Returned'
when secondary_box_active =0 and primary_box_log_20120427=0 then '02: Single_box - No Data Returned'
when secondary_box_active =1 and primary_box_log_20120427=1 and secondary_box_log_20120427=1 then '03: Multiple Boxes - Both Returned'
when secondary_box_active =1 and primary_box_log_20120427=1 and secondary_box_log_20120427=0 then '04: Multiple Boxes - Primary returned'
when secondary_box_active =1 and primary_box_log_20120427=0 and secondary_box_log_20120427=1 then '05: Multiple Boxes - Secondary returned'
else '99:Other' end) as account_status_20120427

,min(case when secondary_box_active =0 and primary_box_log_20120428=1 then '01: Single_box - Data Returned'
when secondary_box_active =0 and primary_box_log_20120428=0 then '02: Single_box - No Data Returned'
when secondary_box_active =1 and primary_box_log_20120428=1 and secondary_box_log_20120428=1 then '03: Multiple Boxes - Both Returned'
when secondary_box_active =1 and primary_box_log_20120428=1 and secondary_box_log_20120428=0 then '04: Multiple Boxes - Primary returned'
when secondary_box_active =1 and primary_box_log_20120428=0 and secondary_box_log_20120428=1 then '05: Multiple Boxes - Secondary returned'
else '99:Other' end) as account_status_20120428

,min(case when secondary_box_active =0 and primary_box_log_20120429=1 then '01: Single_box - Data Returned'
when secondary_box_active =0 and primary_box_log_20120429=0 then '02: Single_box - No Data Returned'
when secondary_box_active =1 and primary_box_log_20120429=1 and secondary_box_log_20120429=1 then '03: Multiple Boxes - Both Returned'
when secondary_box_active =1 and primary_box_log_20120429=1 and secondary_box_log_20120429=0 then '04: Multiple Boxes - Primary returned'
when secondary_box_active =1 and primary_box_log_20120429=0 and secondary_box_log_20120429=1 then '05: Multiple Boxes - Secondary returned'
else '99:Other' end) as account_status_20120429


into #account_summary_return_data
from #account_summary_active as a
left outer join #account_log_return_by_day_summary as b
on a.account_number = b.account_number
group by a.account_number
;

select * into vespa_analysts.account_summary_return_data from #account_summary_return_data ; commit;



select account_status_20120423 ,count(*)    from vespa_analysts.account_summary_return_data  group by account_status_20120423 order by account_status_20120423;
select account_status_20120424 ,count(*)    from vespa_analysts.account_summary_return_data  group by account_status_20120424 order by account_status_20120424;
select account_status_20120425 ,count(*)    from vespa_analysts.account_summary_return_data  group by account_status_20120425 order by account_status_20120425;
select account_status_20120426 ,count(*)    from vespa_analysts.account_summary_return_data  group by account_status_20120426 order by account_status_20120426;
select account_status_20120427 ,count(*)    from vespa_analysts.account_summary_return_data  group by account_status_20120427 order by account_status_20120427;
select account_status_20120428 ,count(*)    from vespa_analysts.account_summary_return_data  group by account_status_20120428 order by account_status_20120428;
select account_status_20120429 ,count(*)    from vespa_analysts.account_summary_return_data  group by account_status_20120429 order by account_status_20120429;

select account_status_20120423 ,account_status_20120429,count(*)    from vespa_analysts.account_summary_return_data  
group by account_status_20120423 ,account_status_20120429 order by account_status_20120423,account_status_20120424;




select count(*) from vespa_analysts.account_summary_return_data where account_status_20120423+account_status_20120424+account_status_20120425+account_status_20120426
+account_status_20120427+account_status_20120428+account_status_20120429 >0





--select top 100 *  from sk_prod.VESPA_STB_PROG_EVENTS_20120427;







