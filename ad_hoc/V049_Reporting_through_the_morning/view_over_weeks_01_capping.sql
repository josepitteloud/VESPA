-- Also: this build is apparently item V050, so whatever.

-- Nothing useful is cached, going to have to rebuild the whole 100 day period from scratch. Oh well.
-- Also: we're running the capping bound over two days, and the query took 8.5 minutes to run. Scale
-- that up by a factor of 50, and I'm looking at... 7 hours of capping limit generation. Awesomes.

-- variable creation - run once only

CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;
CREATE VARIABLE @var_tot                smallint;

 
-- Our period is the 60 days ending on '2012-03-25'
-- but now we're limiting to four weeks of analysis with one week for analysing cohorts. So that's about
-- the same data range as BARB, which we already know isn't enough for Atlantic etc, but hey.
SET @var_prog_period_start  = '2011-07-01';
SET @var_prog_period_end    = '2011-08-31';
-- Now with a new date range to suport the V033/V036 work

-- This one month build should only take two hours or so...
 
/************ STEP ONE : Apply Capping Rules - CODE 1 CAPPING LIMITS  ***********/

-- variable creation - run once only

-- It's gonna take ages, so set up a logger:
create variable @V049_logger_id bigint;
EXECUTE citeam.logger_create_run 'V049', 'Building caps on ' || convert(varchar(10),today(),123), @V049_logger_id output;

-- drop the table if it exists
IF object_id('V049_capping_limits') IS NOT NULL DROP TABLE V049_capping_limits;

--IF object_id('SLQtracker') IS NOT NULL DROP TABLE SLQtracker;
--create table SLQtracker ( SQL_hurg varchar(2000));

-- recreate table
create table V049_capping_limits(    event_start_day  date
                                    ,event_start_hour integer
                                    ,live             bit
                                    ,ntile_100        integer
                                    ,min_dur_mins     integer
);

-- start at 0 and loop until you reach the end of the reporting period
SET @var_cntr = 0;
while dateadd(day, @var_cntr, @var_prog_period_start) <= @var_prog_period_end
begin

    -- drop the temp table if it's there already
    SET @var_sql = 'IF object_id(''V049_ntile_temp'') IS NOT NULL DROP TABLE V049_ntile_temp'
    EXECUTE(@var_sql)
    commit

    -- create a temp table storing the relevant data for the given day
    SET @var_sql =
    'select
        account_number
        , subscriber_id
        , adjusted_event_start_time
        , x_event_duration as event_duration
        , case when play_back_speed is null then 1 else 0 end as live
        , date(adjusted_event_start_time) as event_start_day
        , datepart(hour, adjusted_event_start_time) as event_start_hour
        , cast(x_event_duration/ 60 as int) as dur_mins
    into
        V049_ntile_temp
    from
        sk_prod.VESPA_STB_PROG_EVENTS_' || replace(cast(dateadd(day, @var_cntr, @var_prog_period_start) as varchar(10)), '-', '') ||
    ' where
        video_playing_flag = 1
        and adjusted_event_start_time <> x_adjusted_event_end_time
        and (x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'')
            or (x_type_of_viewing_event = (''Other Service Viewing Event'') and x_si_service_type = ''High Definition TV test service'')

            or (x_type_of_viewing_event = (''HD Viewing Event'')))
        and panel_id in (4,5)
    -- limit view to event_views that are a day or less long
        and cast(x_event_duration/ 86400 as int) = 0
    group by account_number
            ,subscriber_id
            ,adjusted_event_start_time
            ,event_duration
            ,live
    '
    --insert into SLQtracker values (@var_sql)
    --commit
    
    EXECUTE(@var_sql)
    commit

    -- create indexes to speed up the ntile creation
    create hg index idx1 on V049_ntile_temp(event_start_day, event_start_hour)

    commit
    
    -- query ntiles for given date and insert into the persistent table
     insert into V049_capping_limits
     select event_start_day
           ,event_start_hour
           ,live
           ,ntile_100
           ,min(dur_mins) as min_dur_mins
      from (select event_start_day
                  ,event_start_hour
                  ,live
                  ,dur_mins
                  ,ntile(100) over (partition by event_start_day, event_start_hour, live order by dur_mins) as ntile_100
              into ntiles
              from V049_ntile_temp) as sub
     where ntile_100 = 91
  group by event_start_day
          ,event_start_hour
          ,live
          ,ntile_100

  commit

  execute citeam.logger_add_event @V049_logger_id, 3, 'Step One: Caps for day number:', coalesce(@var_cntr, -1)
  
  -- move along to next day
  SET @var_cntr = @var_cntr + 1
  commit
  
END;


-- tidy up
IF object_id('V049_ntile_temp') IS NOT NULL DROP TABLE V049_ntile_temp;

-- add indexes to V049_capping_limits
create hng index idx1 on V049_capping_limits(event_start_day);
create hng index idx2 on V049_capping_limits(event_start_hour);

-- create min caps
IF object_id('V049_vespa_min_cap') IS NOT NULL DROP TABLE V049_vespa_min_cap;
create table V049_vespa_min_cap (
    cap_secs as integer
);
insert into V049_vespa_min_cap (cap_secs) values (6);

grant select on V049_capping_limits to public;
grant select on V049_vespa_min_cap to public;

-- Up to here is done! For the 36 days we wanted at least.
select count(distinct event_start_day) from V049_capping_limits;
-- 62!
select min(event_start_day), max(event_start_day) from V049_capping_limits;
-- 2011-07-01   2011-08-31
-- Yup, that's the range we asked for. Party hard!

-- Okay, from here on out it's slightly desynched since the adjusted period is for the
-- rebuild we're doing for V033. So, yeah. Whatevers.

/************ STEP ONE.FIVE: Filtering daily table pulls for day & channel ***********/

-- This is the current build for a single day; we've now got the capping in play for the
-- 60 day interval we want, we need to roll in the cohorts (grab the example from V049)
-- figure out how we're pulling out the results (probably just the few key numbers, plus
-- ... channel? date? the pivot probably won't be that big, and it's filtered to SD -
-- remember that for the program key pull...)

IF object_id('V049_prog_lookups') IS NOT NULL DROP TABLE V049_prog_lookups;

select
    programme_trans_sk
    ,datediff(day, '2012-02-05', tx_date) as cohort
    ,case
        when lower(channel_name) like 'bbc 1%'                      then 'BBC 1'
        when lower(channel_name) like 'bbc 2%'                      then 'BBC 2'
        when lower(channel_name) in ('channel 4', 'channel 4 +1')   then 'Channel 4'
        when lower(channel_name) in ('channel 5', 'channel 5 +1')   then 'Channel 5'
        when lower(channel_name) in ('dave', 'dave ju vu')          then 'Dave'
        when lower(channel_name) like 'itv1%'                       then 'ITV1'
        when lower(channel_name) like 'itv2%'                       then 'ITV2'
        when lower(channel_name) like 'itv3%'                       then 'ITV3'
        when lower(channel_name) like 'itv4%'                       then 'ITV4'
        when lower(channel_name) like 'sky 1%'                      then 'Sky 1'
        when lower(channel_name) like 'sky1%'                       then 'Sky 1' -- yeah, data quality is balls
        when lower(channel_name) like 'sky atlantic%'               then channel_name -- still split between regular and HD
        when lower(channel_name) like 'sky box office'              then 'Sky Box Office'
        when lower(channel_name) like 'sky news%'                   then channel_name -- also split between regular and HD
        when lower(channel_name) like 'sky sports%'                 then channel_name -- split between 1,2,3,4 and HD
        -- Think that's enough channels for now.
      else null end as channel
into V049_prog_lookups
from sk_prod.vespa_epg_dim as epg
where datediff(day, @var_prog_period_start, tx_date) between 0 and 6
and channel is not null;

commit;
create unique index fake_pk on V049_prog_lookups (programme_trans_sk);

-- OK! that's done. 31534 programmes to pull out. For the first week most of the
-- daily table is going to come out of the pull though... the upside is that we're
-- testing with the first day of our period, so total time will probably only be 10
-- times test duration rather than, say, 40 times (for number of days).

/************ STEP TWO.MINUS.ONE: Prep for mega processing loop ***********/

-- Results structures for all the stuff to get held:
IF object_id('V049_results_summary') IS NOT NULL DROP TABLE V049_results_summary;
create table V049_results_summary (
    channel                                 varchar(40)
    ,cohort                                 tinyint
    ,live                                   bit
    ,consumption_delay                      tinyint
    ,total_viewing_in_thousands_of_hours    decimal(10,2)
);

-- variables tocontrol the loop:
create variable @scanning_day date;
set @scanning_day = @var_prog_period_start;

-- Because logging the progress is good:
create variable @logging_ID bigint;
EXECUTE citeam.logger_create_run 'V049', 'Initial one month build', @logging_ID output;

-- Prep the dynamic SQL lump:
set @var_sql = 'select
    -- Core values we need:
    account_number
    ,subscriber_id
    ,adjusted_event_start_time
    ,document_creation_date
    ,x_programme_viewed_duration
    ,base.programme_trans_sk -- because we need to link it for the channel stuff, and also the broadcasting stuff
    ,recorded_time_utc
    ,x_viewing_start_time
    ,x_viewing_end_time
    ,progs.cohort

    -- Other derived values we want:
    ,case when play_back_speed is null then 1 else 0 end as live
    ,sum(x_programme_viewed_duration) over (partition by subscriber_id, adjusted_event_start_time order by cb_row_id) as cumul_programme_viewed_duration
    ,convert(date, adjusted_event_start_time) as adjusted_event_start_day
    ,datepart(hour, adjusted_event_start_time) as adjusted_event_start_hour

    -- Other things that get ALTERED on later:
    ,convert(datetime, null)    as capped_viewing_start_time
    ,convert(datetime, null)    as capped_viewing_end_time
    ,convert(integer, null)     as capped_programme_viewed_duration
    ,convert(integer, null)     as capped_flag
    
    -- Things we will need to drag in the scaling weights and suchlike    
    ,convert(int, null)         as scaling_segment_id
    ,convert(float, null)       as scaling_weight
    
    -- Other things needed just for the summary into viewing return profiles
    ,convert(tinyint, null)     as consumption_delay
    ,progs.channel              -- rather than deriving it again or using the chunky values in the EPG, just going to grab the filtered version

into
    V049_daily_cache
from
-- test for a day in August where we have capping rules...
    sk_prod.VESPA_STB_PROG_EVENTS_#$£!&^*$%# as base
inner join V049_prog_lookups as progs
on     base.programme_trans_sk = progs.programme_trans_sk

-- With the test for actual viewing data:
where (play_back_speed is null or play_back_speed = 2) -- NULL means live, 2 is timeshifted
and x_programme_viewed_duration > 0
and Panel_id = 4
and x_type_of_viewing_event <> ''Non viewing event''';

commit;

-- And, the big loop itself!
delete from V049_results_summary;
while @scanning_day <= @var_prog_period_end
begin

/************ STEP TWO: Apply Capping Rules - CODE 1 APPLY CAPPING LIMITS  ***********/

-- Okay, so this section is going to get hacked into some pretty different chunks, because
-- we're going to take the "base" table as one daily table at a time, pull out only what
-- we need, scale and aggregate all the viewing inside the loop so we only write the numbers
-- that we want to end up with. Whoever is QA'ing this might like to check the repo history
-- to see where all the code is coming from and going to...

-- First off, build one round worth of stuff to make sure it works...

-- create table including base records you need
IF object_id('V049_daily_cache') IS NOT NULL DROP TABLE V049_daily_cache

execute(replace(@var_sql, '#$£!&^*$%#', dateformat(@scanning_day,'yyyymmdd')))

commit

-- Indexes reshuffled based on what we actually need:
-- create      index idx1 on V049_daily_cache (subscriber_id) -- subscriber ID never gets used?
create      index idx2 on V049_daily_cache (account_number, adjusted_event_start_day)
create      index idx3 on V049_daily_cache (programme_trans_sk)
create      index idx4 on V049_daily_cache (scaling_segment_id, adjusted_event_start_day)
create      index idx5 on V049_daily_cache (adjusted_event_start_day, adjusted_event_start_hour)

commit

-- update the viewing start and end times for playback records
update V049_daily_cache
    set
        x_viewing_end_time = dateadd(second,cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null

commit
update V049_daily_cache
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null

commit

-- update table to create capped start and end times
update V049_daily_cache
    set capped_viewing_start_time =
        case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- else leave start of viewing time unchanged
            else x_viewing_start_time
        end
        , capped_viewing_end_time =
        case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- if start_time+ cap is beyond end time then leave end time unchanged
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) > x_viewing_end_time then x_viewing_end_time
            -- otherwise set end time to start_time + cap
            else dateadd(minute, min_dur_mins, adjusted_event_start_time)
        end
from
        V049_daily_cache base left outer join V049_capping_limits caps
    on (
        base.adjusted_event_start_day = caps.event_start_day
        and base.adjusted_event_start_hour = caps.event_start_hour
        and base.live = caps.live
    )

commit

-- calculate capped_programme_viewed_duration
update V049_daily_cache
    set capped_programme_viewed_duration = datediff(second, capped_viewing_start_time, capped_viewing_end_time)


-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update V049_daily_cache
    set capped_flag =
        case
            when capped_viewing_end_time < x_viewing_end_time then 1
            when capped_viewing_start_time is null then 2
            else 0
        end

commit

-- cap based on min duration of seconds (from min_cap) and set capping flag
-- this nullifies capped_x times as for long duration cap and sets capped_flag = 3
-- note that some capped_flag = 1 records may also be updated if the capping of the end of
-- a long view resulted in a very short view

update V049_daily_cache
    set capped_viewing_start_time = null
        , capped_viewing_end_time = null
        , capped_programme_viewed_duration = null
        , capped_flag = 3
    from
        V049_vespa_min_cap
    where
        capped_programme_viewed_duration < cap_secs

Delete from V049_daily_cache where capped_flag in (2,3)

commit

/************ STEP THREE: Grab channel and scaling details  ***********/

-- For the scaling stuff, we're using "adjusted_event_start_day" as the weighting date, which is already indexed.

update V049_daily_cache
set scaling_segment_ID = l.scaling_segment_ID
from V049_daily_cache as b
inner join vespa_analysts.scaling_dialback_intervals as l
on b.account_number = l.account_number
and b.adjusted_event_start_day between l.reporting_starts and l.reporting_ends

commit

-- Find out the weight for that segment on that day
update V049_daily_cache
set scaling_weight = s.weighting
from V049_daily_cache as b
inner join vespa_analysts.scaling_weightings as s
on b.adjusted_event_start_day = s.scaling_day
and b.scaling_segment_ID = s.scaling_segment_ID

commit

-- Grab the channel and the consumption delay from the EPG table:

update V049_daily_cache
set consumption_delay   = datediff(day, epg.tx_date, dc.document_creation_date)
from V049_daily_cache as dc
inner join sk_prod.vespa_epg_dim as epg
on dc.programme_trans_sk = epg.programme_trans_sk

commit

-- And now, on to summarise!

/************ STEP FOUR: Aggregate into the results we want from this daily table  ***********/

-- And then this pull gives us what we want?

insert into V049_results_summary
select channel, cohort, live, consumption_delay,
    convert(decimal(10,2), sum(scaling_weight * x_programme_viewed_duration) / 60.0 / 60000) as total_viewing_in_thousands_of_hours
from V049_daily_cache
group by channel, cohort, live, consumption_delay

-- Result summary done! Moving on:
set @scanning_day = dateadd(day, 1, @scanning_day)
commit

-- Log each of the first 7 days, because there's a lot of live in each of them, and after that
-- just log each week's worth. That shoudl give us... 11 logger entries in total? Sure.
if datediff(day, @var_prog_period_start, @scanning_day) < 8 or mod(datediff(day, @var_prog_period_start, @scanning_day),7) = 0
    EXECUTE citeam.logger_add_event @logging_ID, 3, 'Chunk completed!', datediff(day, @var_prog_period_start, @scanning_day)

end; -- Of the big loop guy

/************ STEP FIVE: Tidy up and report extract ***********/

-- Build has completed!

drop table V049_daily_cache;

-- Okay, thing is, that pivot still gives us the total on each delayed day,
-- whereas we want to be able to see the total cumulative stuff increase, so:
select channel, cohort, live, consumption_delay
    ,sum(total_viewing_in_thousands_of_hours) as total_viewing_in_thousands_of_hours
into #V049_results_grouped
from V049_results_summary
where consumption_delay <= 35
group by channel, cohort, live, consumption_delay
order by channel, cohort, live, consumption_delay;
-- Getting dupes in table "V049_results_summary", so we just group everything
-- before we attempt the cumulative sum. Kind of odd that the cohort doesn't
-- fully determine the daily table (isn't it set by transmission date? which
-- for us is cohort...) but whatever.

commit;
create index some_key on #V049_results_grouped (channel);
commit;

-- Also: there's no live data return at all past like 16 days, but we still want
-- to have cumulative consumption numbers available for them then, so:
select distinct consumption_delay
into #timesequencing
from #V049_results_grouped;

commit;

-- And so rather than by doing it with a cumulative sum, the LEFT JOIN and the
-- GROUP is going to be what works for us here.

select rg.channel, rg.cohort, rg.live, t.consumption_delay
    ,sum(total_viewing_in_thousands_of_hours) as cumulative_sky_base_viewing_in_thousands_of_hours
    ,convert(decimal(5,4), null) as proportion_of_viewing
into #V049_results_prepared
from #timesequencing as t
left join #V049_results_grouped as rg
on rg.consumption_delay <= t.consumption_delay
group by rg.channel, rg.cohort, rg.live, t.consumption_delay;

commit;
create index some_key on #V049_results_prepared (channel, consumption_delay, cohort);
commit;

-- Because we also want to patch in the percentages so they're comparable:
update #V049_results_prepared
set l.proportion_of_viewing = l.cumulative_sky_base_viewing_in_thousands_of_hours / t.biggest
from #V049_results_prepared as l
inner join
        (select
                channel, cohort, live,
                max(cumulative_sky_base_viewing_in_thousands_of_hours) as biggest
        from #V049_results_prepared
        group by channel, cohort, live) as t
on l.channel = t.channel
and l.cohort = t.cohort
and l.live = t.live;

select * from #V049_results_prepared
order by channel, cohort, live, consumption_delay;