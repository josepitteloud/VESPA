/******************************************************************************
**
**      Project Vespa: Scalability investigations: box to household
**
** So apparently the scaling path for Vespa goes from boxes to households to
** the Sky base to the UK or something like that. This script in a study of
** the various options available when aggregating up to household level. The
** furtherest we've gone in this direction is the daily chains, outlined here:
**
**  http://rtci/vespa1/Household%20aggregation%20Daily%20chained%20viewing.aspx
**
** We refer to a "chain" of viewing, which is the envelope of viewing for a
** given account number and programme, accross any set top box. The chains
** then give all the distinct viewing, whereas the daily tables will give
** the duplicated totals.
**
** Also: for households we're just analysing by account_number. There are HH
** instances with different account numbers, which could be issues with old
** data or terraced flats or flat shares or things like that, so we're going
** with account number for now. Also, account_numbers currently probably hold
** better indices at the moment. Might change if the spec changes.
**
** So this stuff is coming back in again for some control totals on duplicated
** content viewing. We've got some changes that will get merged in from the
** capping stuff which will fix the timeshifted and capping treatment. Also we 
** want analysis of say the 1st. I guess we could do the whole week, but data
** sets are inconveniently large. But hey, that's Vespa.
**
** Outstanding actions:
**  8/ Convert PK from appended id to original cb_row_id
**  9/ Fix the no-timeshifted-viewing bug of ## - DONE!
** 10/ Plug in capping results once capping is established and happy - DONE!
** 11/ Change relevant day to say 1st July - DONE!
** 12/ Clean out the stuff about account number selection because that's in SVoV by box cube.sql - DONE!
**
******************************************************************************/

-- Instead of building our own account lookup, we're just borrowing SVoV_MR_account_lookup
-- from the other script - SVoV by box cube.sql

-- Schema reset!
drop table SVoV_events_vespa_capped;
drop table SVoV_viewing_chains;
drop table SVoV_demo_aggregates;

/*************** Part 0: Naive capping (2 hour bound) ***************/

-- Okay, so we're going to take as actual viewing the stuff 2 hours
-- after and event. That's 7200s, which is why that number tunrs up
-- in a bunch of different places.

-- This capping is crazy bizzaro simplistic, but we're not here to do
-- capping and it should eventually turn up in the x_viewing_start_time
-- and x_viewing_end_time columns once some work has been agreed and
-- Ops have taken it on.
select
    cb_row_id
    ,account_number
    ,subscriber_id
    ,programme_trans_sk
    ,case
        when recorded_time_UTC is null then x_viewing_start_time
        else dateadd(ss, x_duration_since_last_viewing_event, recorded_time_UTC)
     end as x_viewing_start_time
    ,case -- so it turns out that for durations, live and playback can both be treated with the same cases...
                when x_event_duration <= 7200 -- no capping in play; all subsequent cases get capped
                    then x_programme_viewed_duration
                when x_duration_since_last_viewing_event > 7200 -- when the event gets capped before this show starts...
                    then 0
                when x_duration_since_last_viewing_event + x_programme_duration < 7200 -- when the event gets capped and this cap limit is after the end of the show...
                    then x_programme_viewed_duration
                else -- when the cap ends midway through this show
                    7200 - x_duration_since_last_viewing_event
     end as x_programme_viewed_duration -- Makes it more forwards consistent, but will Sybase have parsing issues?
    ,convert(datetime, null) as x_viewing_end_time
    ,play_back_speed
    ,adjusted_event_start_time -- don't need, just for QA
    ,x_adjusted_event_end_time -- don't need, just for QA
into SVoV_events_vespa_capped
from sk_prod.VESPA_STB_PROG_EVENTS_20110701
where panel_id = 5 -- want Vespa viewing events
and (play_back_speed is null or play_back_speed = 2)
and x_programme_viewed_duration > 0
-- and x_viewing_start_time is not null -- should not really be necessary? Or otherwise should replace other conditions?
-- and x_viewing_end_time is not null   -- ditto.
and x_type_of_viewing_event <> 'Non viewing event'
;
-- 8959751

commit;

-- Clear out everythign that doesn't survive the capping
delete from SVoV_events_vespa_capped
where x_programme_viewed_duration is null or x_programme_viewed_duration = 0;
-- nothing, apparently Sybase took the "x_programme_viewed_duration > 0" in the WHERE clause
-- to act on the result fields rather than the source ones. Or something.

-- And now fix up all the durations etc
update SVoV_events_vespa_capped
set x_viewing_end_time = dateadd(ss, x_programme_viewed_duration, x_viewing_start_time);

-- I guess we'll redirect the query to the daily table when the capping
-- rules finally get into play. But as we're here, we can add the most
-- useful indices:
create unique index fake_PK on SVoV_events_vespa_capped (cb_row_id);
create index for_joins on SVoV_events_vespa_capped (account_number, programme_trans_sk);
create index for_other_joins on SVoV_events_vespa_capped (subscriber_id);

-- And, um, yeah, because of the capping, none of the results are
-- comparable at all to the prepared aggregates. Suck. Oh well.

commit;

update SVoV_events_vespa_capped
set evc.x_programme_viewed_duration = 0
from SVoV_events_vespa_capped as evc
left join SVoV_box_lookup as bl -- built instead in "SVoV by box cue.sql"
--on evc.account_number = bl.account_number and
on evc.subscriber_id = bl.subscriber_id
where bl.account_number is null;
-- 2425031 on revised build. Yup, that's a lot of people who have since opted out
-- or just aren't in the subscriber summary or whatever.

delete from SVoV_events_vespa_capped
where x_programme_viewed_duration is null or x_programme_viewed_duration = 0;

commit;

-- QA: all the durations should be positive:
select play_back_speed,
    min(x_programme_viewed_duration),
    max(x_programme_viewed_duration)
from SVoV_events_vespa_capped
group by play_back_speed;
-- Capping stuff now appears to be working okay. At least, some of the controls
-- line up and are good.

-- All the start and end times should be bounded by the event envelope. Update:
-- No, because we've got timeshifted stuff in there too.

-- Wait, QA section 3, are there any cases where the view start time
-- is now somehow after the view end time? (again, there shouldn't)
select * from SVoV_events_vespa_capped
where x_viewing_end_time < x_viewing_start_time;
-- Nothing, also awesome.

/*************** Part 1: Identifying overlapping events ***************/

-- Resolving duplicates by selecting evenything into a bucket, pulling out
-- the distinct cb_row_id keys, then extracting data based on that.

select l.cb_row_id
into #multi_watching
from SVoV_events_vespa_capped as l
inner join SVoV_events_vespa_capped as r
on l.account_number = r.account_number
and l.programme_trans_sk = r.programme_trans_sk
and l.subscriber_id <> r.subscriber_id
-- All viewing filters happened in the capping step; might have to put
-- them back on if we redirect to the daily tables again
-- and l.panel_id = 5 and r.panel_id = 5 -- limit to Vespa only
-- and l.cb_row_id < r.cb_row_id had to remove this constraint as we'd dropped the top event in each chain :( we're deduping in a bit anyway.
-- Limit to live viewing events in l
-- and (l.play_back_speed is null or l.play_back_speed = 2)
-- and l.x_programme_viewed_duration > 0
-- and l.x_type_of_viewing_event <> 'Non viewing event'
-- Limit to live viewing events in r
-- and (r.play_back_speed is null or r.play_back_speed = 2)
-- and r.x_programme_viewed_duration > 0
-- and r.x_type_of_viewing_event <> 'Non viewing event'
-- And now restrict to viewing events that actually overlap:
and (   (l.x_viewing_start_time <= r.x_viewing_start_time and l.x_viewing_end_time > r.x_viewing_start_time)
    or  (r.x_viewing_start_time <= l.x_viewing_start_time and r.x_viewing_end_time > l.x_viewing_start_time)
);

select distinct cb_row_id
into #multibox_viewing_overlap
from #multi_watching;

create unique index fake_PK on #multibox_viewing_overlap (cb_row_id);

-- Now, with all the keys identified, time for data extraction

drop table SVoV_viewing_sample;

select
    t.cb_row_id -- doesn't get used in processing, but audit trails are good
    ,t.account_number
    ,t.subscriber_id
    ,t.programme_trans_sk
    ,t.x_viewing_start_time
    ,t.x_viewing_end_time
    ,t.x_programme_viewed_duration
    ,convert(bit, case when t.play_back_speed is null then 1 else 0 end) as live_viewing
into stafforr.SVoV_viewing_sample
from SVoV_events_vespa_capped as t
inner join #multibox_viewing_overlap as mvo
on t.cb_row_id = mvo.cb_row_id;

-- We've got the sample now, so let's kick out the partial constructions...
drop table #multi_watching;
drop table #multibox_viewing_overlap;

-- Quick QA control totals on things:
select
        count(distinct account_number) as households,
        count(distinct subscriber_id) as total_boxes,
        count(distinct programme_trans_sk) as different_programmes,
        count(1) as hits
from stafforr.SVoV_viewing_sample;
-- 9029     18680   4286    80135
-- Totals are lower now that capping has gone in but that's cool.

-- Indexing is going to be important now we're on the whole population:
create unique index fake_PK on SVoV_viewing_sample (cb_row_id);
create index chain_joining  on SVoV_viewing_sample (account_number, programme_trans_sk);
create index box_joining    on SVoV_viewing_sample (subscriber_id);

-- Yeah so these SVoV capped events are taking up almost 1GB of space. The
-- other tables sitting around are fine, but this one at least should be
-- cleared out once we're finished whatever we're doing with it.

/*************** Part 2: Initialising chains ***************/

-- Trying to join overlapping items would duplicate a bunch of stuff,
-- especially with accounts having 5 or more boxes etc, so we start
-- by pulling te list of all things that don't overlap with anything
-- prior, and these will be exactly the chain starters.

drop table SVoV_viewing_chains;
-- Initialising: assemble all the account number and programme
-- combinations which don't have any prior overlap:
select
        min(r.cb_row_id) as cb_row_id -- this in theory should end up unique across chains?
        ,r.account_number
        ,r.programme_trans_sk
        ,r.x_viewing_start_time
        ,max(r.x_viewing_end_time) as chain_viewing_end_time
        ,r.x_viewing_start_time as previous_loop_viewing_end_time -- for init, we just want something that differs from chain_viewing_end_time
into stafforr.SVoV_viewing_chains
from SVoV_viewing_sample as l
right join SVoV_viewing_sample as r
on l.account_number = r.account_number
and l.programme_trans_sk = r.programme_trans_sk
-- Want things with overlapping viewing intervals with l coming before r
and l.x_viewing_start_time < r.x_viewing_start_time
and l.x_viewing_end_time >= r.x_viewing_start_time
-- Want only vierwing intervals that are first in the chain
where l.account_number is null
-- To catch instances where two boxes in the same house activate at
-- exactly the same time: can't use the rank() - delete trick as we
-- want the event that lasts the longest and rank() just gets the
-- first in the table.
group by r.account_number, r.programme_trans_sk, r.x_viewing_start_time;
-- 27792

-- question: does this catch identical viewing intervals on different boxes?
-- yes, they'll get caught by the group by step.

alter table SVoV_viewing_chains add id bigint not null identity;
alter table SVoV_viewing_chains add primary key (id);
-- And then indices for the joins & groups we're doing:
create index chain_join on SVoV_viewing_chains (account_number, programme_trans_sk);

-- OK, cool, now I can start to build those updates.... because I
-- don't think sybase lets me update based on a join and a group in
-- the same query, so we build the new iteration in a separate table
-- then patch it in using the ID. Update: could have also used the
-- chain start time & programme. Didn't.

-- Ongoing QA: check that the cb_row_id is unique
select cb_row_id, count(1) as hits
from SVoV_viewing_chains
group by cb_row_id
having hits > 1
order by hits desc;
-- Nothing, awesome.

/*************** Part 3: Appending events to chains ***************/

drop table #next_chain_iteration;
-- This process might not pick up *all* of the viewing events;
-- small events contained entirely inside some other event don't
-- count, we get them for free
select
        hhvc.id,
        max(vs.x_viewing_end_time) as next_time
into #next_chain_iteration
from SVoV_viewing_chains as hhvc
inner join SVoV_viewing_sample as vs
-- The households and programmes match...
on hhvc.account_number = vs.account_number
and hhvc.programme_trans_sk = vs.programme_trans_sk
-- Skip processing on chains that are already terminated:
and hhvc.chain_viewing_end_time <> hhvc.previous_loop_viewing_end_time
-- And the intervals overlap with the new table forward in time...
and vs.x_viewing_start_time >= hhvc.x_viewing_start_time
and vs.x_viewing_start_time <= hhvc.chain_viewing_end_time
group by hhvc.id;
-- Everything should get matched to itself the first time around,
-- after which the previous loop viewing gets updated with real
-- values and we start to filter our processing. It's still very
-- inclusive in the sense that we're still considering each thing
-- that happens at a previous stage, but we could filter that out
-- too by including the prior event start time and only considering
-- extensions to the chain that begin strictly after that item.
-- Look into said change if it turns out to be slow - and this will
-- only be a problem with long busy chains - probably not going to
-- be an issue really. Update: Yeah, really not an issue, still
-- goes fast, long chains are rare and by the time you get there,
-- there's barely anything left in your working pool.
create unique index fake_pk on #next_chain_iteration (id);
-- Update the chain ends:
update SVoV_viewing_chains
set
        hhvc.previous_loop_viewing_end_time = hhvc.chain_viewing_end_time,
        hhvc.chain_viewing_end_time = nci.next_time
from SVoV_viewing_chains as hhvc
inner join #next_chain_iteration as nci
on hhvc.id = nci.id;
-- Counting the totals around the loops:
-- 27792
-- 4812
-- 545
-- 144
-- 68
-- 22
-- 15
-- 9
-- 6
-- 2
-- 1
-- 1
-- 1
-- 1
-- 1
-- 0
-- Still with the long tail.

/*************** Part 4: Loop to chain completion ***************/

-- So currently we're still just manually looping section three. The
-- termination condition is this thing:
select
        count(1) as chains,
        sum(case when chain_viewing_end_time = previous_loop_viewing_end_time then 1 else 0 end) as terminated_chains
from SVoV_viewing_chains;
-- When the two numbers match, you're done. (Alternately, the number
-- of row updates coming in decreases to zero).

/*************** Part 5: QA on chain construction ***************/

-- Wait... these should also apply when we throw all the other single
-- box items in too... but yes, it's good to check here as well I guess

-- Alternate termination QA: check for viewing records which are not
-- contained within some chain:
select vs.*
from SVoV_viewing_sample as vs
left join SVoV_viewing_chains as hhvc
-- Join condition for including events in chains:
on vs.account_number = hhvc.account_number
and vs.programme_trans_sk = hhvc.programme_trans_sk
and vs.x_viewing_start_time >= hhvc.x_viewing_start_time
and vs.x_viewing_end_time <= hhvc.chain_viewing_end_time
-- But we only want the non-included ones
where hhvc.id is null;
-- Want this to be empty, so that every viewing event is in some chain.

-- Can also chech that the chains themselves don't overlap. If they do,
-- most likely some of the boundary conditions on the interval overlap
-- checks aren't right, and / or we'll get fencepost errors there.
select l.id, r.id
from SVoV_viewing_chains as l
inner join SVoV_viewing_chains as r
on l.account_number = r.account_number
and l.programme_trans_sk = r.programme_trans_sk
and l.id < r.id -- only care for QA if any exist, don't need full symmetrical view
-- restrict to chains that overlap:
and (   (l.x_viewing_start_time <= r.x_viewing_start_time and l.chain_viewing_end_time > r.x_viewing_start_time)
    or  (r.x_viewing_start_time <= l.x_viewing_start_time and r.chain_viewing_end_time > l.x_viewing_start_time)
);
-- This should be empty; any elements are QA issues.

/*************** Part 6: Building cake report is cake ***************/

-- Now deleted, all the numbers and processes were inconsistent following
-- redeployment with the capping stuff in play. 

/*************** Part 7: Thinking about deployment ***************/

-- For full productionisation: maybe 1 day for requirements, organisation, etc,
-- 1 day for documentation, 1 for implementing the 7 day window for recorded
-- events, 2 for code changes, 2 for testing, 1 for post-dev documentation, 0.5
-- for handoff, so it's like two weeks all up and then we'd have a "by household"
-- capability. On the the next drop implementation at least. Sure.

-- The big chaing is again going to be looping over the daily tables. Also, to
-- figure out what the data eventually looks like; maybe we just end up with the
-- chains in a table somewhere? Well, that's what the day of specification is for.
-- Though, we're also going to have to manage the internal loop tables a bit better
-- too, at the moment it's all manual. Comparing it to some of the aggregate tables
-- for a bit of exra QA would also be good, yes?

/*************** Part 8: Hacking on the aggregated data ***************/

-- First and easiest: length of each chain, the distinct viewing time of the chain
alter table SVoV_viewing_chains add distinct_viewing_seconds unsigned integer;

update SVoV_viewing_chains
set distinct_viewing_seconds = datediff(ss, x_viewing_start_time, chain_viewing_end_time);

-- We've built the chains, but we also want to know about how many total
-- minutes live inside each of the chains, ie rather than do it by the
-- summary tables we want that info individually for each of the chains.

alter table SVoV_viewing_chains add total_live_seconds unsigned integer;
alter table SVoV_viewing_chains add total_playback_seconds unsigned integer;
-- These are only for one household, and even then, unsigned ints store
-- enough seconds for almost 70 years of viewing.
alter table SVoV_viewing_chains add stb_count tinyint;
-- tinyint goes up to 255. No-one has more than like 8 boxes.
alter table SVoV_viewing_chains add view_count smallint;
-- More than 65535 viewing events in a single chain? unlikely. Except that
-- Sybase doesn't like unsigned smallint fields (unsigned int, bigint, and
-- tinyint are fine, tinying is unsigned by default, but unsigned smallint
-- is bad for you apparently).

-- Pull the summary details out of the viewing table:
select
    hhvc.id
    ,sum(case when live_viewing=1 then x_programme_viewed_duration else 0 end) as total_live_seconds
    ,sum(case when live_viewing=0 then x_programme_viewed_duration else 0 end) as total_playback_seconds
    ,count(distinct subscriber_id) as stb_count
    ,count(1) as view_count -- mainly for QA
into #chain_total_details
from SVoV_viewing_sample as vs
inner join SVoV_viewing_chains as hhvc
-- Join condition for including events in chains:
on vs.account_number = hhvc.account_number
and vs.programme_trans_sk = hhvc.programme_trans_sk
and vs.x_viewing_start_time >= hhvc.x_viewing_start_time
and vs.x_viewing_end_time <= hhvc.chain_viewing_end_time
group by hhvc.id;

create unique index fake_pk on #chain_total_details (id);

-- Now poke them into the chains table:
update SVoV_viewing_chains
set
    vc.total_live_seconds       = ctd.total_live_seconds
    ,vc.total_playback_seconds  = ctd.total_playback_seconds
    ,vc.stb_count               = ctd.stb_count
    ,vc.view_count              = ctd.view_count
from SVoV_viewing_chains as vc
inner join #chain_total_details as ctd
on vc.id = ctd.id;

/*************** Part 9: QA on aggregation ***************/

-- From the raw data:
select
    count(1) as hits
    ,floor(sum(x_programme_viewed_duration) / 60.0) as total_viewing
from SVoV_viewing_sample;

select
    sum(view_count) as hits
    ,floor(sum(total_live_seconds) / 60.0) as total_live_viewing
    ,floor(sum(total_playback_seconds) / 60.0) as total_playback_viewing
from SVoV_viewing_chains;
-- Hits should line up, total viewing from sample should match
-- sum of live and playback....

-- QA Note: it's pretty unlikely to be having playback stuff in
-- here, since two boxes would have to have the same thing being
-- played back at the same time, or one is playing it back while
-- the other is still showing live. It's possible I guess, code
-- paths will handle it, it's just uncommon. All the timeshifted
-- stuff comes back in when we convert the non-overlapping views
-- to trivial "chains".

-- What else do we need to QA? Maybe at this point, we're getting
-- into reports...

/*************** Part 10: Mixing in simple viewings ***************/

-- There are also a bunch of viewing events which didn't overlap,
-- starting with all those from homes which aren't multiroom, so we
-- need to roll those into any build of daily viewing.

insert into SVoV_viewing_chains (
    cb_row_id
    ,account_number
    ,programme_trans_sk
    ,x_viewing_start_time
    ,chain_viewing_end_time
    ,distinct_viewing_seconds
    ,total_live_seconds
    ,total_playback_seconds
    ,stb_count
    ,view_count
)
select
    ev.cb_row_id
    ,ev.account_number
    ,ev.programme_trans_sk
    ,ev.x_viewing_start_time
    ,ev.x_viewing_end_time
    ,ev.x_programme_viewed_duration
    ,case when ev.play_back_speed is null then ev.x_programme_viewed_duration else 0 end 
    ,case when ev.play_back_speed = 2     then ev.x_programme_viewed_duration else 0 end 
    ,1
    ,1
from SVoV_events_vespa_capped as ev
left join SVoV_viewing_sample as vs
on ev.cb_row_id = vs.cb_row_id
-- But we want only the things that didn't end up in our original sample:
where vs.cb_row_id is null;
-- but still want to restrict to viewing events:
-- (But the capping is already filtered to viewing events)
--and panel_id = 5
--and (r.play_back_speed is null or l.play_back_speed = 2)
--and r.x_programme_viewed_duration > 0
--and r.x_type_of_viewing_event <> 'Non viewing event';
-- 9441739

select count(1) from SVoV_viewing_sample; -- 76461
select count(1) from SVoV_events_vespa_capped; -- 9518200

-- Yup, so that all works, we got all our viewing organised into chains.

/*************** Part 11: Combined Final QA ***************/

-- Check that there's no overlapping now: (same query as in part 5)
select l.id, r.id
from SVoV_viewing_chains as l
inner join SVoV_viewing_chains as r
on l.account_number = r.account_number
and l.programme_trans_sk = r.programme_trans_sk
and l.id < r.id -- only care for QA if any exist, don't need full symmetrical view
-- restrict to chains that overlap:
and (   (l.x_viewing_start_time <= r.x_viewing_start_time and l.chain_viewing_end_time > r.x_viewing_start_time)
    or  (r.x_viewing_start_time <= l.x_viewing_start_time and r.chain_viewing_end_time > l.x_viewing_start_time)
);
-- Still nothing, awesomes.

-- cb_row_id should still be unique, even after all this processing, right?
select cb_row_id, count(1) as hits
from SVoV_viewing_chains
group by cb_row_id
having hits > 1
order by hits desc;
-- Nothing, score.

-- And we're done!

/*************** Part 12: Daily tables for a household ***************/

-- This will exclude any minute by minute analysis, and even prevent us
-- from knowing whether any particular minute of the show was watched.
-- Maybe we might also look into breaking the programs up by the spots
-- that turn up in between? That'd give us a little better fidelity, at
-- the expense of volume of data. For now, just the basic aggregates:

drop table SVoV_demo_aggregates;

select
    min(cb_row_id) as cb_row_id
    ,account_number
    ,programme_trans_sk
    ,count(distinct subscriber_id) as stb_count
    ,count(1) as view_count
    ,min(x_programme_viewed_duration) as shortest_viewing_event
    ,max(x_programme_viewed_duration) as longest_viewing_event
    -- can't do distinct since that requires us to build the chains first...
    ,sum(case when play_back_speed is null then x_programme_viewed_duration else 0 end) as total_live_seconds
    ,sum(case when play_back_speed = 2     then x_programme_viewed_duration else 0 end) as total_playback_seconds
    -- Guess there might be other things in here too? If we want.
into SVoV_demo_aggregates
from SVoV_events_vespa_capped
group by account_number, programme_trans_sk;

create unique index fake_pk       on SVoV_demo_aggregates (cb_row_id);
create unique index structure_key on SVoV_demo_aggregates (account_number, programme_trans_sk);

-- Okay, and that's that guy done. Tables are huge thouhg, might have to
-- drop them again just for housekeepingness.
