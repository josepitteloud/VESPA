/****************************************************************
**
**      PROJECT VESPA: SPOT ANALYSIS TEST SCRIPT
**
** So we're got the spot import process built, ad now we're
** going to mess around for a bit and see what analysis can
** be built using the spot data.
**
** Not in play any more, this is old and antiquated.
**
****************************************************************/

select top 100 * from vespa_analysts.spots_all;

select min(break_start_time), max(break_start_time) from vespa_analysts.spots_all;
-- 2010-05-05 06:10:39 and 2010-05-06 05:46:20 so yeah, it's the 6AM to 6AM wrapping.

select distinct barb_code from vespa_analysts.spots_all;
/*
4811
4945
4355
4519
4196
*/

-- Oh, heh, we kind of need to increment the spot start and end times too...
update stafforr.fudged_spots
set spot_start_time = dateadd(month, 14, spot_start_time);

select min(break_start_time), max(break_start_time) from stafforr.fudged_spots;
-- Sweet. 2011-07-05 09:17:25 and 2011-07-06 05:43:14

-- Oh, we're going to need the keys and indices and things... even though it's
-- only 700 records or so:
create unique index   fake_PK                         on stafforr.fudged_spots (load_id, infile_id);
create unique index   spot_identifier_using_IDs       on stafforr.fudged_spots (barb_code, broadcasters_break_id, broadcasters_spot_number);
create unique index   spot_identifier_using_time      on stafforr.fudged_spots (barb_code, spot_start_time);
create unique index   spot_identifier_using_sequence  on stafforr.fudged_spots (barb_code, broadcasters_break_id, spot_sequence_id);
-- The preceeding trans key and succeeding trans key aren't being used yet; neither
-- is the clearcast number, so, yeah.

-- Okay, so let's pull all of the viewing data out into our sample:

-- hahahalols, accidently just dropped the one that looked at event viewing. So, no playback stuff for me then :)
-- rebuild from the daily tables...
select distinct programme_trans_sk
into #my_programme_keys
from sk_prod.vespa_epg_dim
where barb_code in (4811, 4945)
and TX_START_DATETIME_UTC between '2011-07-05 09:00:00' and '2011-07-06 00:00:00';

execute get_viewing_sample '#my_programme_keys', 'spot_fudging_view_sample'

-- But yeah, still making indices:
create index break_starttime_index on fudged_spots (break_start_time);

/* Ok, so things we'd like to see:

Hopefully we can identify a break that straddles a change in programme
even given the offset that we've introduced via the month incrementing
and then we can pretend that's how it goes?

Mid-show spots are going to be even more annoying to isolate, not sure
I'll bother with those at all. We want a graph showing:

0. Identify a between-programme spot which is alligned to one of the
breaks we've got defined.

1. Number of boxes watching all of the 30 second chunk 30-60s before the ad break starts
2. Number of boxes watching all of the 30 second chunk 30-60s after the ad break ends
3. Number of boxes watching a complete spot in an internal break (for each spot)

*/

select barb_code, count(1) as hits
from stafforr.spot_fudging_view_sample
group by barb_code;
/* Old build of thing:
4811    2835
4945    7473
*/
/* New build using the proc: they're not quite the same and it might be the timeshifted things.
4811    2746
4945    7754
*/

-- Clear out the timeshifted stuff (otherwise we'd have to mess around with recorded time etc)
delete from spot_fudging_view_sample where recorded_time_utc is not null;

-- Let's also get fast local copies of the relevant EPG cut:
select *
into stafforr.spot_fudging_epgs
from sk_prod.vespa_epg_dim
where barb_code = 4945
and tx_start_datetime_utc between '2011-07-05 09:00:00' and '2011-07-06 00:00:00';
-- 16 rows. Sweet. Nice tight sample to work with.

drop table fudged_break_candidates;
select
        broadcasters_break_id as break_id,
        tx_end_datetime_utc as transition_time_utc,
        programme_trans_sk as preceeding_programme_trans_sk,
        EPG_title as preceeding_title,
        convert(bigint, null) as succeeding_programme_trans_sk,
        convert(varchar(40), null) as succeeding_title,
        t.break_start_time,
        t.break_end_time
into fudged_break_candidates
from spot_fudging_epgs
inner join (
        select distinct
                broadcasters_break_id,
                break_start_time,
                dateadd(second, break_total_duration, break_start_time) as break_end_time
        from fudged_spots
) as t
on tx_end_datetime_utc > dateadd(second, 10, t.break_start_time)
and dateadd(second, 10, tx_end_datetime_utc) < t.break_end_time;
-- Okay, we have 8 candidates, that's good. Not we can see how they line up.

create unique index fake_PK on fudged_break_candidates (break_id);

-- Plug in the following show for each break...

-- If this is empty then the succeding show is well defined...
select break_id, count(1) as fails
from fudged_break_candidates as fbc
inner join spot_fudging_epgs as sfe
on fbc.transition_time_utc = sfe.tx_start_datetime_utc
group by break_id
having fails > 1;
-- Yay! no failures, no ambiguity, no duplication.

update fudged_break_candidates
set
        succeeding_programme_trans_sk = sfe.programme_trans_sk,
        succeeding_title = sfe.EPG_title
from fudged_break_candidates as fbc
inner join spot_fudging_epgs as sfe
on fbc.transition_time_utc = sfe.tx_start_datetime_utc;
-- Ha! We shuold filter on the channel too, but you know what, we're going to pull
-- just those adds out of the other channel so we've got stuff to review.

select * from fudged_break_candidates;
-- Okay, and so, the break start / end times come from the advertising data, awesome.

-- Wait, do I even need the break ID? can I just join on the barb code and the break start?;

select top 10 * from fudged_spots

select break_start_time, broadcasters_break_id
from fudged_spots
where barb_code = 4945
group by break_start_time, broadcasters_break_id
order by broadcasters_break_id;

select * from fudged_spots where broadcasters_break_id = 2;

drop table spot_fudged_watching;
create table spot_fudged_watching (
        barb_code               int not null,
        break_id                int not null,
        spot_sequence_id        int not null,
        load_id                 int not null,
        infile_id               int not null,
        spot_start_time         datetime not null,
        spot_end_time           datetime not null,
        one_sec_watched         int not null default 0,
        five_sec_watched        int not null default 0,
        complete_watched        int not null default 0,
        primary key (break_id, spot_sequence_id)
);
create unique index the_other_consistency_checker on spot_fudged_watching (load_id, infile_id);

insert into spot_fudged_watching (
        barb_code,
        break_id,
        spot_sequence_id,
        load_id,
        infile_id,
        spot_start_time,
        spot_end_time
)
select
        barb_code,
        broadcasters_break_id,
        spot_sequence_id,
        load_id,
        infile_id,
        spot_start_time,
        dateadd(second, spot_duration, spot_start_time)
from fudged_spots as fs
inner join fudged_break_candidates as fbc
on fs.barb_code = 4945
and fs.break_start_time = fbc.break_start_time;
-- Only 12? wtf?
insert into spot_fudged_watching (
        barb_code,
        break_id,
        spot_sequence_id,
        load_id,
        infile_id,
        spot_start_time,
        spot_end_time
)
select
        barb_code,
        broadcasters_break_id,
        spot_sequence_id,
        load_id,
        infile_id,
        spot_start_time,
        dateadd(second, spot_duration, spot_start_time)
from fudged_spots as fs
inner join fudged_break_candidates as fbc
on fs.barb_code = 4811
and fs.break_start_time = fbc.break_start_time
and fs.break_start_time in ('2011-07-05 18:57:14.000000', '2011-07-05 19:57:22.000000', '2011-07-05 21:58:15.000000', '2011-07-05 22:57:56.000000', '2011-07-05 23:58:27.000000');


select * from fudged_break_candidates as fbc
left join spot_fudged_watching as sfw
on fbc.break_id = sfw.break_id;
order by
        sfw.break_id
        sfw.spot_sequence_id;

-- Usually we'd do a loop and update thing, but I think these samples are small enough
-- we'll be able to just join and count distinct and stuff. There's the additional issue
-- of viewing event records being split over the period the ad actually happens... have
-- to go be the event start and end times. Oh wait, did our original sample include those?
-- Hahaha yes, we just pulled everything from the events log.

select count(1) from spot_fudging_view_sample;
-- 10308 and that's completely managable. Update: now 10500, with new build.
select top 10 * from spot_fudging_view_sample;

-- OK, I dunno if the regular update syntax will work, but we can group it into a new
-- table and then update. But it's also a bit of a hack, because we're actually spanning
-- two different channels with these... so we'll have an extra hack of a filter I guess :/
select
        sfw.barb_code,
        sfw.break_id,
        sfw.spot_sequence_id,
        count(distinct subscriber_id) as hits
into #complete_spot_watching
from spot_fudging_view_sample as sfvs
inner join spot_fudged_watching as sfw
on sfvs.barb_code = sfw.barb_code
and sfvs.adjusted_event_start_time < sfw.spot_start_time
and sfvs.x_adjusted_event_end_time > sfw.spot_end_time
group by sfw.barb_code, sfw.break_id, sfw.spot_sequence_id;
-- So we're just looking at the whole event times and then grouping by the set top box to
-- dodge the problem where the viewing event records are split over the spots because they
-- span the boundaries of the shows.

select top 10 * from spot_fudging_view_sample;
select top 10 * from spot_fudged_watching;

select
        sfw.barb_code,
        sfw.break_id,
        sfw.spot_sequence_id,
        count(distinct subscriber_id) as hits
into #five_sec_spot_watching
from spot_fudging_view_sample as sfvs
inner join spot_fudged_watching as sfw
on sfvs.barb_code = sfw.barb_code
and datediff(second,
             case when sfvs.adjusted_event_start_time > sfw.spot_start_time then sfvs.adjusted_event_start_time else sfw.spot_start_time end,
             case when sfvs.x_adjusted_event_end_time < sfw.spot_end_time then sfvs.x_adjusted_event_end_time else sfw.spot_end_time end
             ) >= 5
and sfvs.x_adjusted_event_end_time > sfw.spot_end_time
group by sfw.barb_code, sfw.break_id, sfw.spot_sequence_id;


select
        sfw.barb_code,
        sfw.break_id,
        sfw.spot_sequence_id,
        count(distinct subscriber_id) as hits
into #one_sec_spot_watching
from spot_fudging_view_sample as sfvs
inner join spot_fudged_watching as sfw
on sfvs.barb_code = sfw.barb_code
and datediff(second,
             case when sfvs.adjusted_event_start_time > sfw.spot_start_time then sfvs.adjusted_event_start_time else sfw.spot_start_time end,
             case when sfvs.x_adjusted_event_end_time < sfw.spot_end_time then sfvs.x_adjusted_event_end_time else sfw.spot_end_time end
             ) >= 1
group by sfw.barb_code, sfw.break_id, sfw.spot_sequence_id;

-- Now push those back into the main spot table:

update spot_fudged_watching
set complete_watched = hits
from spot_fudged_watching as sfw
inner join #complete_spot_watching as osw
on sfw.barb_code = osw.barb_code
and sfw.break_id = osw.break_id
and sfw.spot_sequence_id = osw.spot_sequence_id;


update spot_fudged_watching
set five_sec_watched = hits
from spot_fudged_watching as sfw
inner join #five_sec_spot_watching as osw
on sfw.barb_code = osw.barb_code
and sfw.break_id = osw.break_id
and sfw.spot_sequence_id = osw.spot_sequence_id;


update spot_fudged_watching
set one_sec_watched = hits
from spot_fudged_watching as sfw
inner join #one_sec_spot_watching as osw
on sfw.barb_code = osw.barb_code
and sfw.break_id = osw.break_id
and sfw.spot_sequence_id = osw.spot_sequence_id;

-- First build of the report. Now we should also do the minute by minute thing... though that's
-- trickier for the spots that are between shows....
select * from spot_fudged_watching;

-- Okay, still to do are:
-- 1/ the 2-second by 2-second chunks of stuff for each spot being watched; extend to the minute either side of each show?
select * from fudged_break_candidates;
-- Let's use the following for the transition demo:
-- 1: preceeding key:   201107060000001025      Golf
-- succeding key:       201107060000001039      Golfing World


-- Wait.... breaks 47 and 52 have the same shows leading in? as do 18 and 44? ok, nope, the breaks
-- 44, 47 and 51 are straight dupes by programme Id; but they have different break starts and stops.
-- so.. we've crossed over the break info? Yeah, I reakon we've got break info for both channels in
-- linked to the same source. sucks. But probably easy to fix?

delete from fudged_break_candidates where break_id in (44, 47, 51);


create table spot_viewing (
                spot                            int         not null,
                Slice_start                     datetime    not null,
                Viewed                          bigint      not null,
                scaled_Viewed                   double      not null
);


-- Okay, so the dynamic stuff no longer works, instead we're just going to queue
-- them all manueally :/
delete from spot_viewing;

insert into spot_viewing select 10, * from vespa_analysts.make_MBM_graph(201107060000000969, 2, -5);
insert into spot_viewing select 10, * from vespa_analysts.make_MBM_graph(201107060000000983, 2, 5);
insert into spot_viewing select 12, * from vespa_analysts.make_MBM_graph(201107060000000983, 2, -5);
insert into spot_viewing select 12, * from vespa_analysts.make_MBM_graph(201107060000000997, 2, 5);
insert into spot_viewing select 18, * from vespa_analysts.make_MBM_graph(201107060000001011, 2, -5);
insert into spot_viewing select 18, * from vespa_analysts.make_MBM_graph(201107060000001025, 2, 5);
insert into spot_viewing select 21, * from vespa_analysts.make_MBM_graph(201107060000001025, 2, -5);
insert into spot_viewing select 21, * from vespa_analysts.make_MBM_graph(201107060000001039, 2, 5);
--insert into spot_viewing select 44, * from vespa_analysts.make_MBM_graph(201107060000001011, 2, -5);
--insert into spot_viewing select 44, * from vespa_analysts.make_MBM_graph(201107060000001025, 2, 5);
--insert into spot_viewing select 47, * from vespa_analysts.make_MBM_graph(201107060000001025, 2, -5);
--insert into spot_viewing select 47, * from vespa_analysts.make_MBM_graph(201107060000001039, 2, 5);
--insert into spot_viewing select 51, * from vespa_analysts.make_MBM_graph(201107060000001039, 2, -5);
--insert into spot_viewing select 51, * from vespa_analysts.make_MBM_graph(201107060000001053, 2, 5);
insert into spot_viewing select 52, * from vespa_analysts.make_MBM_graph(201107060000001039, 2, -5);
insert into spot_viewing select 52, * from vespa_analysts.make_MBM_graph(201107060000001053, 2, 5);
-- Okay, so that all works, and mysteriously, much much faster than the dynamic exec version. Suck.
-- it's a bit of a pain to manage manually, but sure, not so bad with the procs in place.

select * from spot_viewing;


select count(1) from sk_prod.VESPA_STB_PROG_EVENTS_20110728 where programme_trans_sk is null
