-- Channel mapping details: we've got some leads from Dan / Suzanne / Martin
-- about how the Channel mapping stuff lines up, so we're going to pull what
-- we need from there and group this stuff as we can within the DB. Well,
-- service key will work for live stuff, but timeshifted events report the
-- service id instead so I don't know what we'll do about that right now,
-- but at least we can line up the live.

-- This script just produces the table V079_techedge_channel_lookup after
-- which we'll just be able to refer to that and everything will be good.

select ska.[service key], ska.full_name,
                ska.techedge_name,      -- We need the Techedge name, so this line is new
                cgroup.primary_sales_house,
                (case when pack.name is null then cgroup.channel_group
                else pack.name end) as channel_category
into V079_techedge_channel_lookup       -- Also want to save the results to use later
from patelj.channel_map_service_key_attributes ska
left join
        (select a.service_key, b.name
         from patelj.channel_map_service_key_landmark a
                join patelj.channel_map_landmark_channel_pack_lookup b
                        on a.sare_no between b.sare_no and b.sare_no + 999
        where a.service_key <> 0
         ) pack
        on ska.[service key] = pack.service_key
left join
        (select distinct a.service_key, b.primary_sales_house, b.channel_group
         from patelj.channel_map_service_key_barb a
                join patelj.channel_map_barb_channel_group b
                        on a.log_station_code = b.log_station_code
                        and a.sti_code = b.sti_code
        where service_key <>0) cgroup
        on ska.[service key] = cgroup.service_key
where cgroup.primary_sales_house is not null
order by cgroup.primary_sales_house, channel_category
;

-- OK, cool, that gives us a TechEdge name. The following is a list of the channels
-- that we've got Techedge pulls for:
/* (Extracted separately via Excel...)
'BBC 1'
'BBC 2'
'CH4'
'Channel 5'
'Comedy Central'
'Dave'
'Discovery'
'ITV Breakfast'
'ITV Breakfast HD'
'ITV1'
'ITV1 HD'
'ITV2'
'Sky 1'
'Sky Atlantic'
'Sky Movies Premiere'
'Sky News'
'Sky Sports 1'
'Sky Sports 2'
'Sky Sports News'
*/

select count(distinct techedge_name), count(distinct [service key])
from V079_techedge_channel_lookup
where techedge_name in ('BBC 1',
'BBC 2',
'CH4',
'Channel 5',
'Comedy Central',
'Dave',
'Discovery',
'ITV Breakfast',
'ITV Breakfast HD',
'ITV1',
'ITV1 HD',
'ITV2',
'Sky 1',
'Sky Atlantic',
'Sky Movies Premiere',
'Sky News',
'Sky Sports 1',
'Sky Sports 2',
'Sky Sports News')
-- 17 and 83 keys... we have 19 of these labels from TechEdge... I bet the
-- ITV breakfast is merged into ITV...

select distinct techedge_name
into #matched_chanels
from V079_techedge_channel_lookup
where  techedge_name in ('BBC 1',
'BBC 2',
'CH4',
'Channel 5',
'Comedy Central',
'Dave',
'Discovery',
'ITV Breakfast',
'ITV Breakfast HD',
'ITV1',
'ITV1 HD',
'ITV2',
'Sky 1',
'Sky Atlantic',
'Sky Movies Premiere',
'Sky News',
'Sky Sports 1',
'Sky Sports 2',
'Sky Sports News');

select * from #matched_chanels
order by  techedge_name;
-- Yeah, there's no items for the ITV breakfast guys, going to have to bump those
-- into place on the TechEdge side.

-- OK, so let's cull everything in that chanel lookup that isn't one of these channels
-- that we're interested in:
delete from V079_techedge_channel_lookup
where techedge_name not in (
'BBC 1',
'BBC 2',
'CH4',
'Channel 5',
'Comedy Central',
'Dave',
'Discovery',
'ITV Breakfast',
'ITV Breakfast HD',
'ITV1',
'ITV1 HD',
'ITV2',
'Sky 1',
'Sky Atlantic',
'Sky Movies Premiere',
'Sky News',
'Sky Sports 1',
'Sky Sports 2',
'Sky Sports News'
);
-- 83 items left. Not even going to bother with indices, too small to need them.

-- OK, we have our channel matching lookup table from Vespa into Techedge! That's good!

-- So maybe for playback events... the service keys are well defined for the channels
-- we're interested in?

select distinct tcl.techedge_name
    ,epg.service_id
into #service_key_lookup
from v079_techedge_channel_lookup as tcl
inner join sk_prod.vespa_epg_dim as epg
on tcl.[service key] = epg.service_key;


select service_id
from #service_key_lookup
group by service_id
having count(1) > 1
-- nothing! So, each service key maps to one Techedge name; this might
-- not work in all channels, but it does work out for the big ones
-- we're mapping right now and that's okay for this exercise.

select count(distinct techedge_name) from #service_key_lookup
-- 17 - that's the number we want to see.

-- Now for the timeshifted stuff: hopefully on the day we're dealing with, we'll get
-- uniquely defined service keys by service ID... they change over time but should be
-- fixed for one day? Then we'd get our channel mapping for both live and timeshifted...
-- Hopefully the particular channels in question don't change for 

select distinct service_id, service_key
into #service_key_deduping
from sk_prod.vespa_epg_dim
where tx_date_utc between '2012-05-14' and '2012-05-27'
and service_key in (select [service key] from V079_techedge_channel_lookup);

commit;
-- It should hopefully be 1-1 over the whole period for the channels we care about.
select count(1)
    ,count(distinct service_id)
    ,count(distinct service_key)
from #service_key_deduping;
-- 80,80,80 - yay, it's consistent and unique!

-- patch those details back intop our lookup:
alter table v079_techedge_channel_lookup
add service_id int default null;

update v079_techedge_channel_lookup
set service_id = skd.service_id
from v079_techedge_channel_lookup
inner join #service_key_deduping as skd
on v079_techedge_channel_lookup.[service key] = skd.service_key;
-- OK, so now we have all the service keys and all the service IDs and all the techedge names
-- for the whole period encapsulated in the one big channel mapping table, awesome. Some of
-- the ITV regions don't have service keys, I guess that timeshifted material is delivered
-- through some other playback mechanism? Sure.

create index for_playback on v079_techedge_channel_lookup (service_id);
commit;

-- Oh hey that [service key] is going to get super annoying real fast, so we'll just tidy it up:
alter table v079_techedge_channel_lookup
add service_key int;

update v079_techedge_channel_lookup
set service_key = [service key];

alter table v079_techedge_channel_lookup
drop [service key];
commit;

create index for_live on v079_techedge_channel_lookup (service_key);
commit;

-- Now our channel maping lookup is all done!
