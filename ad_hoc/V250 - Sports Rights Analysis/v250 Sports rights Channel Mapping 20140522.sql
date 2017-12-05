/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 01 (Channel mapping Table)
        
        Analyst: Dan Barnett
        SK Prod: 5

        Create a Lookup table of Service Keys to Channel

*/------------------------------------------------------------------------------------------------------------------

select  service_key
,vespa_name as channel_name

into v250_channel_to_service_key_lookup
from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES 
group by service_key
,channel_name
;
commit;

select service_key
,min(channel_name) as min_name
,max(channel_name) as max_name
,count(distinct channel_name) as different_channel_names
into #service_key_details
from v250_channel_to_service_key_lookup
group by service_key
;
commit;
--select * from #service_key_details  order by different_channel_names desc, min_name;
--Some channels such as Sky Sports 2 have had different names such as Sky Sports Ashes
select service_key
,case when min_name='Other TV' then max_name else min_name end as channel_name
into v250_channel_to_service_key_lookup_deduped
from #service_key_details
group by service_key
,channel_name
;

commit;

--Correct BBC2 to BBC HD---

--select * from  v250_channel_to_service_key_lookup_deduped order by channel_name;


update v250_channel_to_service_key_lookup_deduped
set channel_name='BBC HD'
from v250_channel_to_service_key_lookup_deduped
where service_key=2075
;
commit;

update v250_channel_to_service_key_lookup_deduped
set channel_name='More 4'
from v250_channel_to_service_key_lookup_deduped
where service_key=4043
;
commit;

alter table v250_channel_to_service_key_lookup_deduped add pay integer;
alter table v250_channel_to_service_key_lookup_deduped add ent_channel integer;
alter table v250_channel_to_service_key_lookup_deduped add sky_channel integer;

select service_key
,max(case when upper(Pay_free_indicator)='PAY' then 1 else 0 end) as pay
,max(case when new_packaging in ('Entertainment','Entertainment Extra +','Entertainment Extra','Kids') then 1 else 0 end) as ent_channel
,max(case when channel_owner in ('Sky') then 1 else 0 end) as sky_channel
into #pay_flag
from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES
group by service_key
;


update v250_channel_to_service_key_lookup_deduped
set pay=case when b.pay is null then 0 else b.pay end
,ent_channel=case when b.ent_channel is null then 0 else b.ent_channel end
,sky_channel=case when b.sky_channel is null then 0 else b.sky_channel end
from v250_channel_to_service_key_lookup_deduped as a
left outer join #pay_flag as b
on a.service_key=b.service_key
;
commit;

alter table v250_channel_to_service_key_lookup_deduped add grouped_channel varchar(40);

update v250_channel_to_service_key_lookup_deduped
set grouped_channel=case when b.channel_name_inc_hd_staggercast_channel_families is null then a.channel_name else b.channel_name_inc_hd_staggercast_channel_families end
from v250_channel_to_service_key_lookup_deduped as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name=b.channel_name
;
commit;
--select channel_name , max(pay) as pay_flag from v250_channel_to_service_key_lookup_deduped  group by channel_name order by channel_name;

update v250_channel_to_service_key_lookup_deduped
set grouped_channel=case when grouped_channel in ('Sky Disney','Sky Greats','Sky Movies','Sky SciFi/Horror') then 'Sky Movies Channels'  else grouped_channel end
from v250_channel_to_service_key_lookup_deduped as a
;
commit;




grant all on dbarnett.v250_channel_to_service_key_lookup_deduped to public;