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

grant all on dbarnett.v250_channel_to_service_key_lookup_deduped to public;