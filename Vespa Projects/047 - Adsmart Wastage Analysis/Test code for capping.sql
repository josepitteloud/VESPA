
select * from jchung.week_caps where live_or_playback = 'Playback'  and max_dur_mins=44

select * from jchung.week_caps where event_start_day = 20 
order by event_start_day,event_start_hour,box_subscription,pack_grp,genre_at_event_start_time;

select * from jchung.channel_lookup

commit;

select event_start_day 
, max_dur_mins 
, count(*)  
from jchung.week_caps 
where live_or_playback = 'Playback' 
group by event_start_day,max_dur_mins order by event_start_day,max_dur_mins;



select max_dur_mins 
, count(*)  
from jchung.week_caps 
where live_or_playback = 'Live' 
group by max_dur_mins order by max_dur_mins;

commit;



select * from sk_prod.vespa_epg_dim where channel_name = 'Sky Sports 1' and tx_date = '20111120' order by tx_time

select top 500 * 
from
sk_prod.CUST_SERVICE_INSTANCE
where effective_to_dt <'9999-09-09'


select top 500 * 
from
sk_prod.CUST_SERVICE_INSTANCE
where effective_to_dt <'9999-09-09'

order by 

commit;
select effective_to_dt ,count(*) 
from
sk_prod.CUST_SERVICE_INSTANCE
group by effective_to_dt
order by effective_to_dt
;

commit;


--create src_system_id lookup
select src_system_id
,cast(si_external_identifier as integer) as subscriberid
,si_service_instance_type
,effective_from_dt
,effective_to_dt
,cb_row_id
,rank() over(partition by src_system_id order by effective_from_dt desc,cb_row_id desc) as xrank
into --drop table
#subs_details
from
sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
;
commit;
--27724595

select top 500 * from #subs_details order by subscriberid;



--create index
create hg index idx1 on subs_details(src_system_id);

--remove dups
delete from subs_details
where xrank>1;


select box_subscription , count(*) from jchung.one_week group by box_subscription order by box_subscription;


select account_number from sk_prod

