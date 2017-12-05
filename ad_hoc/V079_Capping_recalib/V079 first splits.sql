-- Recalibrating the capping builds. Heh, we need to mix the scaling into this
-- as well, awesomes... It wants the minute allocation too, but that's not ready
-- yet. After we run that batch over the historc cache we've got, then we'll be
-- okay. Oh but we'll have to do that multiple times because we still need to
-- rebuild the entire capped cache with the recalibrated capping rules, awesome.

-- So we need to cut this stuff up by event start hour, consumption hour, channel
-- and timeshifting. And we want average number of subscribers on each channel
-- over the hour and also the average time people watching any of this channel
-- spend on it. So it loks like there are going to be a few intermediate tables
-- before we can line stuff up with the BARB totals, but that's okay.

-- Okay, to split stuff up into consumption hours, we're going to join (causing
-- duplicates) into this kind of index table thing;

create variable @SQL_hack_thing     varchar(2000);
create variable @scanning_day       date;
create variable @looper             tinyint;

-- Because, yeah, all of this is going to have to be looped...

-- Yup, we need a structure to keep all of the aggregates that we get out of the loop
create table V079_aggregate_collection (
    build_date              date
    ,channel                varchar(25)
    ,timeshifting           varchar(12)
--    ,event_start_hour       tinyint     -- This guy is also giving us unpleasant duplication issues, it has to dissappear from the aggregation
    ,consumption_hour       datetime
    ,distinct_households    int
    ,total_minutes          bigint      -- The BARB dat normalises this to the average over the number of poeple viewing anything, but the totals are easier for us to combine to get that
--    ,capping_flag           tinyint     -- because we'd like to know whether it's the bounds or the ntiles work that we need to address - removed, this guy is giving us significant duplication issues.
);

create index joinery_updates on V079_aggregate_collection (consumption_hour, build_date, channel, timeshifting);

create table V079_pull_cache (
    cb_row_id               bigint
    ,subscriber_id          bigint
    ,account_number         varchar(20)
    ,build_date             date
    ,viewing_starts         datetime
    ,viewing_stops          datetime
    ,service_key            int
    ,service_id             int
    ,channel                varchar(25)
    ,timeshifting           varchar(12)
    ,start_hour             tinyint
    ,minutes_viewed         bigint
    ,scaling_segment_ID     int
    ,weightings             float default 0
--    ,capped_flag            tinyint
);

-- Oh and we also need some hourly cache table things, because distinct viewers ends up a bit of a pain to calculate...
create table #V079_deduped_weightings (
    build_date              date
    ,channel                varchar(25)
    ,timeshifting           varchar(12)
    ,account_number         varchar(20)
    ,weightings             float
);


create table #V079_aggregated_viewers (
    build_date              date
    ,channel                varchar(25)
    ,timeshifting           varchar(12)
    ,distinct_households    int
);

create index for_joins on #V079_aggregated_viewers (build_date, channel, timeshifting);

commit;

set @SQL_hack_thing = '
insert into V079_pull_cache
select
    da.cb_row_id
    ,da.subscriber_id
    ,da.account_number
    ,''#£$!^*$#''
    ,da.viewing_starts
    ,da.viewing_stops
    ,epg.service_key
    ,epg.service_id
    ,null               -- getting channel name instead from the Techedge lookup
    ,da.timeshifting
    ,datepart(hour, pe.adjusted_event_start_time)
    ,datediff(minute, da.viewing_starts, da.viewing_stops)
    ,null ,null -- nothing yet for the scaling columns
--    ,capped_flag
from vespa_analysts.vespa_daily_augs_#£$*%$# as da
inner join sk_prod.vespa_stb_prog_events_#£$*%$# as pe
on da.cb_row_id = pe.cb_row_id
inner join sk_prod.vespa_epg_dim as epg
on da.programme_trans_sk = epg.programme_trans_sk
';
-- Now pulling out all the stuff because we need to mash the scaling into it to figure
-- out what the total viewing looks like overall. We'll need both service key and
-- service ID since they get alternately used based on whether the viewing is live
-- or timeshifted.

delete from V079_pull_cache;

-- now these dates are aligned against the control totals we got from Chris:
set @scanning_day = '2012-05-14';

commit;
go

while @scanning_day <= '2012-05-27'
begin
    execute(replace(replace(@sql_hack_thing,
                            '#£$*%$#',
                            dateformat(@scanning_day,'yyyymmdd')
                            )
                    ,'#£$!^*$#'
                    ,dateformat(@scanning_day,'yyyy-mm-dd')
                    )
            )

    commit
    set @scanning_day = dateadd(day, 1, @scanning_day)
    commit
end;

commit;
go

create unique index fake_pk on V079_pull_cache (cb_row_id);

commit;
go

create index get_weightings on V079_pull_cache (account_number, build_date);
create index get_weightings_part_2 on V079_pull_cache (build_date, scaling_segment_ID);

commit;
go

select build_date, count(1) as hits, count(distinct account_number)
from V079_pull_cache
group by build_date
order by build_date;
/* OK, so panel is pretty stable at 250k though there's an interesting hole on the 14th.
'2012-05-14',   5066379,    206551
'2012-05-15',   12800784,   275311
'2012-05-16',   12045558,   273877
'2012-05-17',   12572899,   273264
'2012-05-18',   12739385,   272725
'2012-05-19',   14338552,   269702
'2012-05-20',   14808665,   274326
'2012-05-21',   12112847,   266992
'2012-05-22',   11514745,   268503
'2012-05-23',   11249214,   270370
'2012-05-24',   11390260,   268919
'2012-05-25',   11483044,   265406
'2012-05-26',   12935051,   265115
'2012-05-27',   13507659,   267895
*/

-- Adding all the other important stuff:

update V079_pull_cache
set scaling_segment_ID = l.scaling_segment_ID
from V079_pull_cache as b
inner join vespa_analysts.SC2_intervals as l
on b.account_number = l.account_number
and b.build_date between l.reporting_starts and l.reporting_ends;

commit;

-- Find out the weight for that segment on that day
update V079_pull_cache
set weightings = s.weighting
from V079_pull_cache as b
inner join vespa_analysts.SC2_weightings as s
on b.build_date = s.scaling_day
and b.scaling_segment_ID = s.scaling_segment_ID;

commit;
go

select count(1) from V079_pull_cache
where weightings is null;
-- 1104618 - so that's, yes, a decent chunk of the stuff that's not in the universe
-- Also: from here on out we've only got control totals for the '2012-05-14' build because
-- that's the data set on which we're doing development and testing.

commit;
go

select count(1)
    ,count(distinct account_number)
    ,convert(bigint,sum(weightings * minutes_viewed))
    ,convert(bigint,sum(weightings * datediff(minute, viewing_starts, viewing_stops)))
from V079_pull_cache
where weightings is not null
-- 3961761,157636,2810188104,2810188104
-- This is what our control totals should get to... heh. Oh, but that's before we restrict
-- to channels that we care about, which is the next thing:

-- And also, we need to stitch in the channel mapping bit too: we've got a new
-- construction from Martin to help with the channel mapping, it gets built over
-- in "V079 targeted channel mapping.sql". For live stuff the link is service key...
update V079_pull_cache
set channel = tcl.techedge_name
from V079_pull_cache
inner join v079_techedge_channel_lookup as tcl
on V079_pull_cache.service_key = tcl.service_key
where timeshifting = 'LIVE';
commit;

-- For timeshifted stuff, the link is service id (part of that tripple key but the other
-- flags don't indicate anything about the channel)
update V079_pull_cache
set channel = tcl.techedge_name
from V079_pull_cache
inner join v079_techedge_channel_lookup as tcl
on V079_pull_cache.service_id = tcl.service_id
where timeshifting <> 'LIVE';
commit;

-- OK, and now, we can clip out things that don't belong either on the panel or to the
-- channels of calibratory interest:
delete from V079_pull_cache
where weightings is null
or channel is null;
commit;

-- and in here we should pull out control totals: bear in mind that these guys are no
-- longer total viewing at all, it's specifically restricted to the channels of interest.

select channel
    ,count(1) as records
    ,count(distinct subscriber_id) as panel_viewers
    ,convert(int, sum(datediff(second, viewing_starts, viewing_stops) / 60.0)  / count(distinct subscriber_id)) as raw_viewing_minutes_per_subscriber
    ,convert(int, sum(datediff(second, viewing_starts, viewing_stops) * weightings / 60.0 / 60))                as scaled_total_viewing_hours
from V079_pull_cache
group by channel
order by channel;
-- This doesn't get us the other main metric, the number of distinct eyes on each channel,
-- but we'd need to get distinct subscribers and then sub those weights and all that, which
-- we'll just do later for real in the other consumption hour loop thing.

-- Are the hour starts about where we expect?
select
    convert(date, viewing_starts) as view_start_date
    ,datepart(hour, viewing_starts)  as view_start_hour
    ,count(1) as hits
from V079_pull_cache
group by view_start_date, view_start_hour
order by view_start_date, view_start_hour;
/* Yeaqh, this is pretty mych as we expected, sweet.
'2012-05-14',0,7435
'2012-05-14',1,3616
'2012-05-14',2,2428
'2012-05-14',3,3443
'2012-05-14',4,10007
'2012-05-14',5,32651
'2012-05-14',6,55884
'2012-05-14',7,55535
'2012-05-14',8,46888
'2012-05-14',9,40875
'2012-05-14',10,45770
'2012-05-14',11,49301
'2012-05-14',12,58556
'2012-05-14',13,51187
'2012-05-14',14,58264
'2012-05-14',15,75988
'2012-05-14',16,107236
'2012-05-14',17,161964
'2012-05-14',18,192607
'2012-05-14',19,230352
'2012-05-14',20,202991
'2012-05-14',21,196057
'2012-05-14',22,90502
'2012-05-14',23,46589
'2012-05-15',0,759
'2012-05-15',2,3
'2012-05-15',3,1
*/

-- So we need to build all the columns on the aggregate collection table, which we may
-- as well do in three steps, for beginning, middle and end of these hour intervals we
-- create. Dunno if this will be feasible at all, let's see how long it takes...

create index joinery on V079_pull_cache (viewing_starts, viewing_stops);

create variable @scanning_day date;
create variable @hour_start datetime;
create variable @hour_end   datetime;
create variable @looper tinyint;

delete from V079_aggregate_collection;

set @scanning_day = '2012-05-14';
set @looper = 0;

while @looper <= 30
begin

    set @hour_start  = dateadd(hour, @looper,   convert(datetime,@scanning_day))
    set @hour_end    = dateadd(hour, @looper+1, convert(datetime,@scanning_day))
    
    -- OK so this gets us total viewing minutes within this hour of viewing
    insert into V079_aggregate_collection
    select
        build_date
        ,channel
        ,timeshifting
        ,@hour_start
        ,null
        ,sum(weightings *
            -- And in here lives the calculation for viewing within the hour of interest:
            datediff(
                second
                ,case
                    when pc.viewing_starts >= @hour_start then pc.viewing_starts
                    else @hour_start end
                ,case
                    when pc.viewing_stops <= @hour_end then pc.viewing_stops
                    else @hour_end end
            ) / 60)
    from V079_pull_cache as pc
    where weightings is not null
    and (@hour_start between pc.viewing_starts and pc.viewing_stops
        or pc.viewing_starts between @hour_start and @hour_end)
    and datediff(minute, pc.viewing_starts, @hour_start) <= 120 -- manual hack for weird capping exhibit (1)
    group by build_date, channel, timeshifting
    
    commit

    -- But we still need to do something else to get the total distinct viewers,
    -- because we need to sum up their weights, but in a way that doesn't double
    -- count weights from households that have multiple viewing events. Heh, Or boxes.
    delete from #V079_deduped_weightings
    delete from #V079_aggregated_viewers
    
    commit
    
    insert into #V079_deduped_weightings
    select build_date, channel, timeshifting, account_number, weightings
    from V079_pull_cache as pc -- FROM and WHERE constraints are all the same
    where weightings is not null
    and (@hour_start between pc.viewing_starts and pc.viewing_stops
        or pc.viewing_starts between @hour_start and @hour_end)
    and datediff(minute, pc.viewing_starts, @hour_start) <= 120 -- manual hack for weird capping exhibit (1)
    group by build_date, channel, timeshifting, account_number, weightings
    
    -- OK, and now we've deduplicated accounts we can sum up the number of vieweers watching:
    insert into #V079_aggregated_viewers
    select build_date, channel, timeshifting, sum(weightings) as distinct_households
    from #V079_deduped_weightings
    group by build_date, channel, timeshifting
    
    commit
    
    -- Now tack those things back onto the aggregation table;
    update V079_aggregate_collection
    set V079_aggregate_collection.distinct_households = av.distinct_households
    from V079_aggregate_collection
    inner join #V079_aggregated_viewers as av
    on V079_aggregate_collection.consumption_hour = @hour_start
    and V079_aggregate_collection.build_date      = av.build_date
    and V079_aggregate_collection.channel         = av.channel
    and V079_aggregate_collection.timeshifting    = av.timeshifting
    
    commit
    
    set @looper = @looper + 1

    commit

end;

-- ^^ OK, now we've got proper deduplication in there, and we need a complete rerun.


-- Need the Sky Base numbers to figure out what the average viewing is
select sum(sky_base_accounts)
from vespa_analysts.SC2_weightings
where scaling_day = '2012-05-14';

select 1721315183 / 9422012.0 / 60
-- That says 3.05 hours of viewing, which is... actually on the low side of the BARB
-- estimate, great. Do we repurpose and try to rock out a new build of this capping
-- stuff with the durations fixed? Yeah, I reakon that's the play. Check the new
-- build too I guess.

-- So that's mostly in place, except we need to calculate total viewing and total households
-- impacting separately, otherwise the double counting of accounts accross events messes up
-- the calculations.

