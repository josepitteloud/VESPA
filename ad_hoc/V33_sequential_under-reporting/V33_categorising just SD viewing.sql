/******************************************************************************
**
** Project Vespa: Item V033 (or V036): HD playback investigation
**
** Awesome, so there are dupes on the IC listing:
**
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=33
**
** is actually the same item as...
**
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=36
**
** But whatever. Now we want to outline the SD viewing over some period by Live /
** playback / sequential playback and maybe have the option of splitting it up by
** some other variables too.
**
** The big trick is detecting the transitions from playback of one episode to the
** next, since it's still not necissarily a. easy or b. well defined. First we're
** going to be doing some investigative stuff.
** 
**
** Oh, we'll also need capping and scaling in here too. So we're going to be building
** this somewhere in the period of '2012-01-18' to '2012-03-25' because that's where
** build the capping limits for V049. So, dependency:
**
**  \Vespa\ad_hoc\V049_Reporting_through_the_morning\view_over_weeks_01_capping.sql
**
******************************************************************************/

-- Preperation for looping over daily tables:
create variable @SQL_hurg_daily_table_extract       varchar(2000);

create variable @var_prog_period_start              date;
create variable @var_prog_period_end                date;
create variable @scanning_day                       date;
create variable @cohorts                            tinyint;
create variable @analysis_range                     tinyint;

set @cohorts = 7;                           -- A different group of analysis for each day in a week
set @analysis_range = 28;                   -- Spin it over 28 days to ick up the timeshifting
set @var_prog_period_end = '2011-08-23';    -- The last day in the capping build we have; we want it to cover futurama shown on 22nd and 23rd of July
set @var_prog_period_start = dateadd(day, - @analysis_range - @cohorts, @var_prog_period_end);
-- 2011-07-19, sweet

-- Logging stuff: because this will take a while:
create variable @V033_logger_id bigint;
EXECUTE citeam.logger_create_run 'V033', 'Ad hoc run on ' || convert(varchar(10),today(),123), @V033_logger_id output;

-- First off, build the table of programme keys we're looking for with all their
-- cohort flags. Get the channel name too while we're there. Also need the broadcast
-- day too... this table is going to be huge though :(

IF object_id('V033_programme_lookup') IS NOT NULL DROP TABLE V033_programme_lookup;

select 
    programme_trans_sk
    ,channel_name
    ,tx_date_utc
    ,datediff(day, @var_prog_period_start, tx_date_utc) as cohort
into V033_programme_lookup
from sk_prod.vespa_epg_dim as epg
where cohort >= 0 and cohort < @cohorts;

commit;
create unique index fake_pk on V033_programme_lookup (programme_trans_sk);
commit;

-- Okay, so we actually need *all* viewing data because we want to say what the HD adjacent
-- stuff is doing with respect to the whole viewing base. Yay. 15m records per day, awesome.

IF object_id('V033_multibackers') IS NOT NULL DROP TABLE V033_multibackers;

create table V033_multibackers (
    subscriber_id       bigint  primary key
);

set @SQL_hurg_daily_table_extract = '
select
    -- base data values:
    cb_row_id
    ,account_number
    ,subscriber_id
    ,adjusted_event_start_time
    ,document_creation_date
    ,x_programme_viewed_duration
    ,ev.programme_trans_sk
    ,recorded_time_utc
    ,x_viewing_start_time
    ,x_viewing_end_time
    
    -- Other derived values we want:
    ,case when x_si_service_type = ''High Definition TV test service'' then ''HD'' else ''SD'' end as HD_or_SD
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
    
    -- Groupings based on the broadcasting day
    ,pl.channel_name
    ,pl.cohort
    ,datediff(day, pl.tx_date_utc, convert(date, dateadd(hour, -2, adjusted_event_start_time))) as day_delay -- With the 2AM shuffle to line up like VOSDAL
    
    -- And the thing we need for our categorisation here:
    ,convert(bit, 0) as continuing_playback
    -- we will update it later

into V033_daily_cache
from sk_prod.VESPA_STB_PROG_EVENTS_#$£!&^*$%# as ev
inner join V033_programme_lookup as pl
on ev.programme_trans_sk = pl.programme_trans_sk
where (play_back_speed = 2 or play_back_speed is null)
and x_programme_viewed_duration > 0
and Panel_id in (4,5)
and x_type_of_viewing_event <> ''Non viewing event''
';

-- And then the results storage table:
IF object_id('V033_results_listing') IS NOT NULL DROP TABLE V033_results_listing

create table V033_results_listing (
        HD_or_SD                varchar(2)
        ,live                   bit
        ,cohort                 tinyint
        ,day_delay              tinyint
        ,continuing_playback    bit
        ,households             bigint
        ,boxes                  bigint
        ,total_scaled_viewing_in_hours   decimal(12,2)
        ,scanning_day           date
);        

EXECUTE citeam.logger_add_event @V033_logger_id, 3, 'Prep complete!';

set @scanning_day = @var_prog_period_start;
delete from V033_results_listing;
while @scanning_day <= @var_prog_period_end
begin

-- And here the loop begins. First off, pull out all the relevant daily data then do scaling and capping;

IF object_id('V033_daily_cache') IS NOT NULL DROP TABLE V033_daily_cache

commit

execute(replace(@SQL_hurg_daily_table_extract, '#$£!&^*$%#', dateformat(@scanning_day,'yyyymmdd')))
commit

-- Indexes reshuffled based on what we actually need:
create unique index fake_pk on V033_daily_cache (cb_row_id)
create      index idx1 on V033_daily_cache (subscriber_id)
create      index idx2 on V033_daily_cache (account_number, adjusted_event_start_day)
create      index idx3 on V033_daily_cache (programme_trans_sk)
create      index idx4 on V033_daily_cache (scaling_segment_id, adjusted_event_start_day)
create      index idx5 on V033_daily_cache (adjusted_event_start_day, adjusted_event_start_hour)

commit

-- Do the capping...
update V033_daily_cache
    set
        x_viewing_end_time = dateadd(second,cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null

commit
update V033_daily_cache
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null

commit

-- update table to create capped start and end times
update V033_daily_cache
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
        V033_daily_cache base left outer join V049_capping_limits caps
    on (
        base.adjusted_event_start_day = caps.event_start_day
        and base.adjusted_event_start_hour = caps.event_start_hour
        and base.live = caps.live
    )

commit

-- calculate capped_programme_viewed_duration
update V033_daily_cache
    set capped_programme_viewed_duration = datediff(second, capped_viewing_start_time, capped_viewing_end_time)


-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update V033_daily_cache
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

update V033_daily_cache
    set capped_viewing_start_time = null
        , capped_viewing_end_time = null
        , capped_programme_viewed_duration = null
        , capped_flag = 3
    from
        V049_vespa_min_cap
    where
        capped_programme_viewed_duration < cap_secs

Delete from V033_daily_cache where capped_flag in (2,3)

commit

-- Now, do the scaling ...
update V033_daily_cache
set scaling_segment_ID = l.scaling_segment_ID
from V033_daily_cache as b
inner join vespa_analysts.scaling_dialback_intervals as l
on b.account_number = l.account_number
and @scanning_day between l.reporting_starts and l.reporting_ends

commit

-- Find out the weight for that segment on that day
update V033_daily_cache
set scaling_weight = s.weighting
from V033_daily_cache as b
inner join vespa_analysts.scaling_weightings as s
on @scanning_day = s.scaling_day
and b.scaling_segment_ID = s.scaling_segment_ID

commit

-- Okay, but there's a special case with stuff broadcast before 2AM:
update V033_daily_cache
set day_delay = 0
where day_delay < 0
-- Otherwise it'd be shown as being viewed the day *before* it was actually broadcast.

commit

-- Okay, and this goes on the subset of guys with multiple viewing stuff? so, build the table
-- with just the items we need:
delete from V033_multibackers

insert into V033_multibackers
select subscriber_id
from V033_daily_cache
where live = 0
group by subscriber_id
having count(distinct programme_trans_sk) > 1

commit

IF object_id('V033_view_sequencing') IS NOT NULL DROP TABLE V033_view_sequencing

select
    cb_row_id
    ,programme_trans_sk
    ,ev.subscriber_id
    ,x_viewing_start_time
    ,x_viewing_end_time
    ,channel_name
    ,rank() over (partition by ev.subscriber_id order by adjusted_event_start_time, x_viewing_start_time, cb_row_id) as view_sequence
    ,convert(int, null) as prior_in_sequence
    -- And a flag to record whether the playback event appears to be the second (or third!) in a sequence
    ,convert(bit, 0) as continuing_playback
    -- But we also need the flags to determine the contiguous bits of viewing of the same show...
    ,convert(bit, 0) as new_prog_block
    ,convert(int, null) as prog_block_ID
    -- We're calling a prog block a collection of playback viewing events with the same box and programme with no other playback events "watched" in between
into V033_view_sequencing
from V033_daily_cache as ev
inner join V033_multibackers as mpb
on ev.subscriber_id = mpb.subscriber_id
where live = 0

commit 

-- Yeah, then these bits want to be moved onto the sequential playback-only table. What columns does that guy need?
update V033_view_sequencing set prior_in_sequence = view_sequence - 1

commit
create unique   index fake_pk       on V033_view_sequencing (cb_row_id)     -- need to join it back onto the other dily table thing we have...
create          index join_key_1    on V033_view_sequencing (subscriber_id, view_sequence)
create          index join_key_2    on V033_view_sequencing (subscriber_id, prior_in_sequence)
create          index join_key_3    on V033_view_sequencing (subscriber_id, prog_block_ID)
create          index join_key_4    on V033_view_sequencing (programme_trans_sk)
commit

-- Definition: if playback event A starts within 3 minutes of a playback
-- event B ending and A & B are associated with different programme keys,
-- then playback event A is getting considered an "adjacent playback event".
-- We could futz around trying to track the fast forwards and the pauses and
-- everythingm, but is it worth it? no.

-- Wait... now that we've got these... we still have the contiguity problem.
-- what if one person watches Programme 1 then 2 then 1 again then 3 then 2
-- again? We don't want to flag all cases of program 1 as adjacent viewing...
-- Do we need chains then? something to sort out the contiguity? yeasch!

-- OK so I need to order each subscribers viewing events, and then join it to
-- itself to figure out whether the previous event was the same program or not.
-- Then a cumulative sum of this will create tags that identify each contiguous
-- block of viewing. Then the cb_row_id's in the query above will identify the
-- contiguous blocks that want to be considered adjacent, and pushing the flag
-- for adjacency on blockwise, can categorise all the viewing as adjacent or
-- first-in-sitting. Ugly, but overall not a bad plan...

-- OK, so a bunch of things to build, let's start with the prog block stuff,
-- beginning with a list of all the recorded events which are a different
-- programme to whatever the previously viewed programme was:
select r.cb_row_id
into #prog_changes
from V033_view_sequencing as r
inner join V033_view_sequencing as l
on  r.subscriber_id         = l.subscriber_id
and r.prior_in_sequence     = l.view_sequence
and r.programme_trans_sk   <> l.programme_trans_sk
-- 0 rows here? That's perhaps unexpected. 

commit
create unique index fake_pk on #prog_changes (cb_row_id)
commit

update V033_view_sequencing
set new_prog_block = 1
from V033_view_sequencing
inner join #prog_changes as pc
on V033_view_sequencing.cb_row_id = pc.cb_row_id

commit

-- OK, now if we sum the block changes flag through each subscriber, we'll link up all
-- the programme blocks
select 
    cb_row_id
    ,sum(new_prog_block) over (partition by subscriber_id order by view_sequence) as prog_block_ID
into #prog_blocks
from V033_view_sequencing

commit
create unique index fake_pk on #prog_blocks (cb_row_id)
commit

update V033_view_sequencing
set prog_block_ID = pb.prog_block_ID
from V033_view_sequencing
inner join #prog_blocks as pb
on V033_view_sequencing.cb_row_id = pb.cb_row_id

commit
drop table #prog_changes
drop table #prog_blocks
commit

-- Programme blocks are done. Now to figure out which of those blocks are considered
-- adjacent, ie, happen within 3 minutes of the last one...

select
    r.subscriber_id
    ,r.prog_block_ID
into #adjacent_programme_changes
from V033_view_sequencing as r
inner join V033_view_sequencing as l
on  r.subscriber_id         = l.subscriber_id
and r.prior_in_sequence     = l.view_sequence
and r.channel_name          = l.channel_name    -- Want the programmes to be on the same channel too
and r.prog_block_ID        <> l.prog_block_ID
and datediff(second, l.x_viewing_end_time, r.x_viewing_start_time) between 0 and 180

commit
create unique index fake_pk on #adjacent_programme_changes (subscriber_id, prog_block_ID)
commit
-- OK... so that's a list of all our "adjacent" viewing? let's mark that.

update V033_view_sequencing
set continuing_playback = 1
from V033_view_sequencing
inner join #adjacent_programme_changes as apc
on V033_view_sequencing.subscriber_id = apc.subscriber_id
and V033_view_sequencing.prog_block_ID = apc.prog_block_ID
commit

-- And now push those marks onto our main table:
update V033_daily_cache
set continuing_playback = 1
from  V033_daily_cache as dc
inner join V033_view_sequencing as vs
on dc.cb_row_id = vs.cb_row_id
where vs.continuing_playback = 1

commit
drop table #adjacent_programme_changes
commit

-- And were done! At least for flagging the adjacent stuff. We still need to cap and scale
-- everything to pull out viewing, and loop it over some daily table to get some decent
-- numbers out of it. But it's progress at least. But first, some QA.
/* Old QA stuff:
select subscriber_id, prog_block_ID
from V033_view_sequencing
group by subscriber_id, prog_block_ID
having count(distinct programme_trans_sk) > 1;
-- Prog blocks are well defined! sweet.

select top 30 * from V033_view_sequencing;
-- OK... so that actually looked like it works...

select
        continuing_playback
        ,sum(x_programme_viewed_duration) / 60.0 / 60 as stuff_viewed
from V033_view_sequencing
group by continuing_playback;
-- Fascinating... the majority of it is! hilarious! Even when we're tracking stuff of the same
-- channel. We are looking at VOSDAL though, single daily table, so most of it is going to be
-- the slight delays of stuff we'd expect. Later I guess we'll be able to see more playback
-- themed stuff, and that'll be more informative.

-- OK, so we want to split this out by VOSDAL.... no, that's all VOSDAL. We haven't scanned
-- any other daily tables at all. So that's the next trick then.

-- OK, so what values of stuff are we pulling out that we want to summarise?
-- * Capping & Scaling need to be implemented (V1 only I guess, 10% capping) - DONE
-- * We need HD or SD, as a flag. (how did we cut that last time? Wait, there's a service type for that) - DONE
-- * Adjacency or not - DONE
-- * Day difference (remember to clip by 14 or so...) - DONE
-- * Cohort too, though we can just average in the Excel thingy - DONE
-- * Number of distinct accounts? Is that going to aggregate in the way we want? I think so. Time per day etc, duplication over days is appropriate.
-- Then we also need to summarise live playback into the same thing. So... we still need to cap & scale the *entire* day of data, awesome.

*/

-- Okay, here's our results build through the days:
insert into V033_results_listing
select
        HD_or_SD
        ,live
        ,cohort
        ,day_delay
        ,continuing_playback
        ,count(distinct account_number) as households
        ,count(distinct subscriber_id) as boxes
        ,convert(decimal(10,2), sum(capped_programme_viewed_duration * scaling_weight) / 60.0 / 60) as total_viewing_in_hours
        ,@scanning_day
from V033_daily_cache
group by
        HD_or_SD
        ,live
        ,cohort
        ,day_delay
        ,continuing_playback

commit

-- Yeh, the household counts not so usefull because each day the households have a combination
-- both continuing and non-continuing playback events, but whatever. Clip them out later I guess.

-- And then we move on!

set @scanning_day = dateadd(day, 1, @scanning_day)
commit
if datediff(day, @var_prog_period_start, @scanning_day) < 8 or mod(datediff(day, @var_prog_period_start, @scanning_day),7) = 0
    EXECUTE citeam.logger_add_event @V033_logger_id, 3, 'Chunk completed!', datediff(day, @var_prog_period_start, @scanning_day)

end;

-- clear out transient tables:
drop table V033_multibackers;
drop table V033_view_sequencing;
drop table V033_daily_cache;

-- Make important tables visible to the team:
grant select on V033_results_listing to greenj, dbarnett, jacksons, stafforr, jchung, sarahm, poveys, gillh, rombaoad, louredaj;
grant select on V049_capping_limits  to greenj, dbarnett, jacksons, stafforr, jchung, sarahm, poveys, gillh, rombaoad, louredaj;

-- ^^ up to there is the bit that gets rerun to rebuild all the HD results.

-- OK, so now we need to pull out this big listing and turn it into some sensible graphs which
-- tell us about stuff:
select top 10 * from V033_results_listing;
-- Yeah , that distinct households & days is not useful, we lose that as soon as we try to group
-- here. So... how do we get average viewing? But how would we even aggregate that? Hmrnph. But
-- we can get a kind of average viewing of hours per box by taking viewing / households, even
-- even though there's duplication in the households, it'll still give us viewing per day or some
-- such thing, yes?

select count(1) from V033_results_listing;
-- 1821

select 
    HD_or_SD, live, cohort, day_delay, continuing_playback
    ,sum(total_scaled_viewing_in_hours) as total_scaled_viewing_hours
    ,convert(decimal(10,2), sum(total_scaled_viewing_in_hours) / sum(households)) as scaled_viewing_hours_per_household
from V033_results_listing
group by HD_or_SD, live, cohort, day_delay, continuing_playback
order by HD_or_SD, live, cohort, day_delay, continuing_playback;
-- cool, cohorts all fixed now.