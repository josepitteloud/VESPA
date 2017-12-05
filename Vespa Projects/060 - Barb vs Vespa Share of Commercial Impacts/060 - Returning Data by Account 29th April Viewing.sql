---Add Primary/Secondary Box Split on to table

--vespa_analysts.project060_all_viewing



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
   and effective_from_dt <= '2012-04-29'           --Start on or before date
   and effective_to_dt >'2012-04-29'               --ends after date
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

--select count(*) from #Live_viewing_boxes;
select subscriber_id
,account_number
into #Live_viewing_boxes
from vespa_analysts.project060_all_viewing
where live_timeshifted_type='01: Live'
group by subscriber_id
,account_number
;

select  a.subscriber_id
,a.account_number
,b.SUBSCRIPTION_SUB_TYPE
into #add_box_type_detail
from #Live_viewing_boxes as a
left outer join #account_subscriber_id_and_primary_secondary_type as b
on a.subscriber_id=b.subscriber_id
;

select SUBSCRIPTION_SUB_TYPE
,count(*) from #add_box_type_detail
group by SUBSCRIPTION_SUB_TYPE

select account_number
,max(case when SUBSCRIPTION_SUB_TYPE='DTV Primary Viewing' then 1 else 0 end) as primary_box
,max(case when SUBSCRIPTION_SUB_TYPE='DTV Extra Subscription' then 1 else 0 end) as secondary_box
into #account_summary
from #add_box_type_detail
group by account_number
;

select account_number
,max(case when SUBSCRIPTION_SUB_TYPE='DTV Primary Viewing' then 1 else 0 end) as primary_box
,max(case when SUBSCRIPTION_SUB_TYPE='DTV Extra Subscription' then 1 else 0 end) as secondary_box
into #account_summary_all_accounts
from #sky_accounts
group by account_number
;

--drop table  #return_data_summary;
select a.account_number
--,case when a.secondary_box=1 then '02: Multi-Box Household' else '01: Single Box Household' end as household_type
,case when  a.secondary_box=0 then '01: Single Box Household Single Box Return'
when b.primary_box=1 and  b.secondary_box=1 then '02: Multi Box Household Both Boxes Live Viewing'
when b.primary_box=1 and  b.secondary_box=0 then '03: Multi Box Household Primary Box Live Viewing'
when b.primary_box=0 and  b.secondary_box=1 then '04: Multi Box Household Secondary Box Live Viewing'
 else '99: Other' end as box_return_data
into #return_data_summary
from #account_summary_all_accounts as a
left outer join #account_summary as b
on a.account_number=b.account_number
where b.account_number is not null
;

select box_return_data
,count(*) as accounts
from #return_data_summary
group by box_return_data
order by box_return_data
;

---Match To segments----
--drop table  #scaling_segment_id;
select account_number
,scaling_segment_id
,min(weighting) as weight
into #scaling_segment_id
from vespa_analysts.project060_all_viewing
where live_timeshifted_type='01: Live' and scaling_segment_id is not null
group by account_number
,scaling_segment_id
;

select a.scaling_segment_id
,b.box_return_data
,count(*) as account
,sum(weight) as total_weight
into #segment_and_box_type
from #scaling_segment_id as a
left outer join #return_data_summary as b
on a.account_number=b.account_number
group by  a.scaling_segment_id
,b.box_return_data;
commit;
select * from #segment_and_box_type order by scaling_segment_id;


select box_return_data
,sum(total_weight) as weighted_accounts
,sum(cas
from #segment_and_box_type
where box_return_data<>'99: Other'
group by box_return_data
order by box_return_data


