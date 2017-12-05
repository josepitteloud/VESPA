-- so we're looking into the HD Playback issue of V033 still...

execute V033_assemble_sample_data;
-- That proc is defined in the V33_first_build script.


-- Ok, so now we've got this data set out, we can start to think
-- about other indices and profiling and cube construction and
-- suchlike.

EXECUTE citeam.logger_get_latest_job_events 'AdHocDataPull', 4;
-- Took 2 full hours of processing.

select count(1) from V033_Viewing_Records;
-- 1545865 - that looks like sky view + vespa, yes

drop procedure V033_assemble_sample_data;

commit;

-- More indices:
create index indx01 on V033_Viewing_Records (Account_Number);
create index indx02 on V033_Viewing_Records (Subscriber_Id);
create index indx03 on V033_Viewing_Records (Programme_Trans_Sk);
create index indx04 on V033_Viewing_Records (Adjusted_Event_Start_Time);

commit;

-- We'll have to add profiling variables too... may as well use last week's
-- SBV build even though it's not updated yet... wonder if there are any
-- boxes missing from SBV...
select count(distinct b.subscriber_id)
from V033_Viewing_Records as b
left join vespa_analysts.vespa_single_box_view as a
on b.subscriber_id = a.subscriber_id
where a.subscriber_id is null;
-- 0! all our profiling just got a lot easier. Oh suck... our panel_ID fell
-- off when we did the pull, but we'll be able to stitch it back on via the
-- single box view... probably...

-- No more examples or show sequencing stuff...

-- OK, time since recording is the other thing we don't have that we'll need:

alter table V033_Viewing_Records add recording_delay varchar(20);

update V033_Viewing_Records
set recording_delay = case
    when Play_Back_Speed is null then '1) Live'
    when datediff(hour,  Recorded_Time_UTC, Adjusted_Event_Start_Time) <= 1 then '2) Within 1 hour'
    when convert(date, dateadd(hour, -6, Recorded_Time_UTC)) >= convert(date, dateadd(hour, -6, Adjusted_Event_Start_Time)) then '3) VOSDAL'
    when datediff(day, convert(date, dateadd(hour, -6, Recorded_Time_UTC)), convert(date, dateadd(hour, -6, Adjusted_Event_Start_Time))) < 7 then '3) Within 7 days'
    when datediff(day, convert(date, dateadd(hour, -6, Recorded_Time_UTC)), convert(date, dateadd(hour, -6, Adjusted_Event_Start_Time))) < 28 then '4) Within 28 days'
    else '5) More than 28 days'
end;

commit;

-- First round:
select
    recording_delay, channel_name
    ,sum(X_Programme_Viewed_Duration) / 60.0 / 60 as total_viewing_in_hours
from V033_Viewing_Records
group by recording_delay, channel_name
order by recording_delay, channel_name;
-- So by our first cut... it doesn't look as pronounced as we're led to believe. We can see
-- it a tiny bit in HD playback that happens more than 3 days after recording, but those
-- numbers are small anyway and the trend is reversed for earlier items; the 2nd episode gets
-- watched *more*.

-- ok, so we want x_manufacturer, x_description, x_model_number
-- from sk_prod.cust_set_top_box and that uses service_instance_id...
alter table V033_Viewing_Records add x_manufacturer     varchar(16);
alter table V033_Viewing_Records add x_description      varchar(24);
alter table V033_Viewing_Records add x_model_number     varchar(10);

-- OK, so let's build a list and then go after what we need from the customer database...
select sbv.subscriber_id
    ,sbv.service_instance_id
    ,convert(varchar(16), null) as x_manufacturer
    ,convert(varchar(24), null) as x_description
    ,convert(varchar(20), null) as x_model_number
into V33_box_manuf_lookup
from vespa_analysts.vespa_single_box_view as sbv
inner join (select distinct subscriber_id from V033_Viewing_Records) as t
on sbv.subscriber_id = t.subscriber_id
where sbv.service_instance_id is not null;

commit;

create unique index fake_pk on V33_box_manuf_lookup (subscriber_id);
create unique index other_fake_pk on V33_box_manuf_lookup (service_instance_id);

-- Now pull the things out of customer database...
update V33_box_manuf_lookup
set x_manufacturer   = t.x_manufacturer
    ,x_description   = t.x_description
    ,x_model_number  = t.x_model_number
from V33_box_manuf_lookup
left join sk_prod.cust_set_top_box as t
on V33_box_manuf_lookup.service_instance_id = t.service_instance_id;

commit;

-- Now attach those flags to viewing data:
update V033_Viewing_Records
set x_manufacturer   = coalesce(t.x_manufacturer, 'Unknown')
    ,x_description   = coalesce(t.x_description, 'Unknown')
    ,x_model_number  = coalesce(t.x_model_number, 'Unknown')
from V033_Viewing_Records
inner join V33_box_manuf_lookup as t
on V033_Viewing_Records.subscriber_id = t.subscriber_id

commit;

-- We removed Anytime & pick in a different cycle this time....

-- OK, cool, so box type & PvR are all on single box view... oh wait, premiums aren't...
--drop table V033_Pivot_Pull; -- the old one that's still around
drop table V033_Pivot_Pull_with_box_type;

select 

    panel_id
    ,series_id
    ,episode_number
    ,channel_name
    -- (all the variables)
    ,Tx_Start_Datetime_UTC
    ,recording_delay
    ,box_type_subs
    ,case when lower(channel_name) like '%hd%' then 1 else 0 end as HD_channel
    ,x_manufacturer
    ,x_description
    ,x_model_number
    -- (and the statistics)
    ,count(distinct fv.subscriber_id) as boxes
    ,count(distinct fv.account_number) as households
    ,sum(fv.X_Programme_Viewed_Duration) / 60.0 / 60 as total_viewing_in_hours
into V033_Pivot_Pull_with_box_type
from V033_Viewing_Records as fv
inner join vespa_analysts.vespa_single_box_view as sbv
on fv.subscriber_id = sbv.subscriber_id
group by
    Tx_Start_Datetime_UTC
    ,panel_id
    ,series_id
    ,episode_number
    ,channel_name
    ,recording_delay
    ,HD_channel
    ,box_type_subs
    ,x_manufacturer
    ,x_description
    ,x_model_number;
-- so that pivot gives 14k rows, pretty managable.

select * from V033_Pivot_Pull_with_box_type;

-- Other investigations into the scheduling stuff...

-- Well, we also wanted a raw dump of stuff:

select count(1) from V033_Viewing_Records
where convert(date, Tx_Start_Datetime_UTC) in ('2011-07-22','2011-07-23')
and channel_name like 'Sky%' -- excluding the anytime stuff
and panel_id = 1
-- still 5k items.... that'll do

select t.*
        ,sbv.PS_Flag
        ,sbv.box_type_subs
        ,Box_type_physical
        ,PVR
from V033_Viewing_Records as t
inner join vespa_analysts.vespa_single_box_view as sbv
on t.subscriber_id = sbv.subscriber_id
where convert(date, Tx_Start_Datetime_UTC) in ('2011-07-22','2011-07-23')
and channel_name like 'Sky%' -- excluding the anytime stuff
and panel_id = 1
order by t.account_number, t.subscriber_id, t.adjusted_event_start_time;
