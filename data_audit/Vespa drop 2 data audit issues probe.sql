/******************************************************************************
**
**      PROJECT VESPA DROP 2: DATA AUDIT - EVENTS VIEW ISSUES PROBE
**
** We've got the data audit stuff in a different file, and here we're looking
** into particular issues that have been flagged: 
**      a. Weird duplication of items by account number, event start time &
**              events end time (CONSIDERED PARTIALLY RESOLVED)
**      b. EPG Processing issues where the PROGRAMME_TRANS_SK doesn't link
**              to the EPG lookup properly.
**      c. 
**
** For all of these we're working of a sample population events logged after
** the 1st of July. We're also applying the following filters to them all:
**      i/ panel_id = 5
**      ii/ account_number is not null
**      iii/ ADJUSTED_EVENT_START_TIME <> X_ADJUSTED_EVENT_END_TIME
** These are basic requirements for our events to be valid. Yet remains to be
** seen whether we're also going to restrict our event types to evChangeView
** or similar, but for the moment, the above are valid viewing records.
**
**
******************************************************************************/

/********** VESPA EVENTS VIEW DUPLICATION EXHIBITION **********/

select
        ACCOUNT_NUMBER
        ,SUBSCRIBER_ID
        ,STB_LOG_CREATION_DATE
        ,DOCUMENT_CREATION_DATE
        ,EPG_CHANNEL
        ,EPG_TITLE
        ,EVENT_TYPE
        ,X_EVENT_DURATION
        ,ADJUSTED_EVENT_START_TIME
        ,X_ADJUSTED_EVENT_END_TIME
        ,X_PROGRAMME_VIEWED_DURATION
        ,SI_DETAIL_KEY
        ,TRAFFIC_KEY
        ,PROGRAMME_TRANS_SK
        ,PROGRAMME_SEQUENCE_ID
        ,TX_DATE_TIME_UTC
        ,TX_START_DATETIME_UTC
        ,TX_END_DATETIME_UTC
        ,convert(varchar(1),null) as dupes
into stafforr.vespa_drop2_dupe_testing
from sk_prod.vespa_events_view
where DOCUMENT_CREATION_DATE >= '2011-07-01'
and ADJUSTED_EVENT_START_TIME <> X_ADJUSTED_EVENT_END_TIME -- okay, so we had a lot of dupes, and so now we're filtering out the things of zero duration....
and panel_id = 5
and account_number is not null;
-- That took about an hour, but now we should have everything we need?

alter table stafforr.vespa_drop2_dupe_testing add id bigint not null identity primary key;

-- Not just for the grouping, but because we're joining back later.
create index duping_index on stafforr.vespa_drop2_dupe_testing (SUBSCRIBER_ID, ADJUSTED_EVENT_START_TIME, X_ADJUSTED_EVENT_END_TIME);

-- Figure out where the dupes are
select SUBSCRIBER_ID
        ,ADJUSTED_EVENT_START_TIME
        ,X_ADJUSTED_EVENT_END_TIME
        ,EVENT_TYPE
        ,min(id) as id
into #dupes
from stafforr.vespa_drop2_dupe_testing
group by SUBSCRIBER_ID
        ,ADJUSTED_EVENT_START_TIME
        ,X_ADJUSTED_EVENT_END_TIME
        ,EVENT_TYPE
having count(1) > 1; -- keep only the dupes, don't record unique items

select count(1) from #dupes;
-- omg lots...... like, 13m of them.....

-- For joining;
create unique index id_index on #dupes (SUBSCRIBER_ID, ADJUSTED_EVENT_START_TIME, X_ADJUSTED_EVENT_END_TIME, EVENT_TYPE);

-- Identify dupes in main table:
update stafforr.vespa_drop2_dupe_testing
set dupes = case
        when vddt.id = d.id then 'F' -- First
        else 'S' -- Subsequent duplicates
    end
from stafforr.vespa_drop2_dupe_testing as vddt
inner join #dupes as d
on     vddt.SUBSCRIBER_ID               = d.SUBSCRIBER_ID
   and vddt.ADJUSTED_EVENT_START_TIME   = d.ADJUSTED_EVENT_START_TIME
   and vddt.X_ADJUSTED_EVENT_END_TIME   = d.X_ADJUSTED_EVENT_END_TIME
   and vddt.EVENT_TYPE                  = d.EVENT_TYPE
;

-- Let's get estimates of population:
select dupes, count(1) as hits
from stafforr.vespa_drop2_dupe_testing
group by dupes;
/* Ok, actually quite a few things....
F       13251135
S       30047999
        120642071
*/

-- Let's see what's up:
select top 20 *
from stafforr.vespa_drop2_dupe_testing
where dupes is not null
order by SUBSCRIBER_ID
        ,ADJUSTED_EVENT_START_TIME
        ,X_ADJUSTED_EVENT_END_TIME
        ,EVENT_TYPE
        ,dupes
;
-- Okay, let's pull out everything for account number 200001981956, subscriber id 15396 and adjusted event start time '2011-06-30 18:21:11.000000'

select * from sk_prod.vespa_events_view
where account_number = '200001981956'
--and subscriber_id = '15396'
and ADJUSTED_EVENT_START_TIME = '2011-06-30 18:21:11.000000';
order by X_ADJUSTED_EVENT_END_TIME;

-- Yeah, just those four. Odd. Why?

grant all on stafforr.vespa_drop2_dupe_testing to public;

/******************************************************************************
** So it looks like there are instances where a single viewing record will have
** a duration ("X_EVENT_DURATION") that exceeds the duration of the programme
** ("X_PROGRAMME_DURATION"). There we get the same event start and stop times
** for many records, and then different programs for each record. So there's
** no column which gives the time for which the viewer started watching the
** program indicated by that record, you have to cobble it together from the
** maximum of "ADJUSTED_EVENT_START_TIME" and "TX_START_DATETIME_UTC"... but
** then do you have to correct the UTC for DST and notthe adjusted one? Same
** deal holds for endpoints, we can't actually tell from any one columns the
** point in time where a record stops being relevant.
**
** Fortunately for amount of show watched, there is a single column which has
** in it the number of seconds that the particular show was watched: this one
** is "X_PROGRAMME_VIEWED_DURATION" and we should be using it in all cases. If
** we do ever take "X_EVENT_DURATION" we're going to get duplication of time
** in there, and that's bad.
**
** Could we instead update those adjusted start & end times so that the records
** from the same box don't overlap in their time? We also need to decide how to
** handle the endpoints because we'd rather they didn't overlap, but there's
** probably existing treatment for that, otherwise programs would be defined to
** overlap at all of their endpoints and that'd be annoying.
******************************************************************************/

/********** IDENTIFYING SUBTYPES OF DUPLICATION **********/

-- Do all the duplicates have the structure of different shows which all are on#
-- the same channel and add have viewing time adding up to event time?

-- Now form summary of weird duplicated things:
select
        subscriber_id
        ,ADJUSTED_EVENT_START_TIME
        ,X_ADJUSTED_EVENT_END_TIME
        ,min(account_number) as smallest_account_number
        ,max(account_number) as largest_account_number
        ,count(1) as records
        ,count(distinct epg_title) as distinct_shows
        ,count(distinct TX_START_DATETIME_UTC) as distinct_shows_by_starttime
        ,count(distinct SI_DETAIL_KEY) as distinct_detail_keys
        ,count(distinct TRAFFIC_KEY) as distinct_traffic_keys
        ,count(distinct PROGRAMME_TRANS_SK) as distinct_programme_links
        ,count(distinct EPG_CHANNEL) as distinct_channel_names
        ,count(distinct event_type) as event_type_count
        ,min(X_EVENT_DURATION) as smallest_event_duration
        ,max(X_EVENT_DURATION) as largest_event_duration
        ,sum(X_PROGRAMME_VIEWED_DURATION) as total_view_time
        ,sum(X_PROGRAMME_VIEWED_DURATION) / 60.0 as total_view_time_in_minutes
into stafforr.vespa_drop2_dupe_summary
from stafforr.vespa_drop2_dupe_testing
group by
        subscriber_id
        ,ADJUSTED_EVENT_START_TIME
        ,X_ADJUSTED_EVENT_END_TIME
having records > 1;
-- So that took 2 1/2 hours.

alter table stafforr.vespa_drop2_dupe_summary add dupe_id bigint not null identity primary key;

-- started at 1:30, and donealmost immediately.

select count(1)
        ,sum(case when smallest_account_number <> largest_account_number then 1 else 0 end) as account_number_failures
        ,sum(case when records <> distinct_shows then 1 else 0 end) as show_distinctness_failures
        ,sum(case when records <> distinct_shows_by_starttime then 1 else 0 end) as show_distinctness_fails_by_starttime
        ,sum(case when records <> distinct_traffic_keys then 1 else 0 end) as traffic_key_distinctness_failures
        ,sum(case when records <> distinct_detail_keys then 1 else 0 end) as detail_key_distinctness_failures
        ,sum(case when distinct_channel_names <> 1 then 1 else 0 end) as chanel_name_failures
        ,sum(case when smallest_event_duration <>  largest_event_duration then 1 else 0 end) as event_duration_failures
        ,sum(case when total_view_time <> smallest_event_duration then 1 else 0 end) as view_time_summ_failures
        ,sum(case when abs(datediff(second,ADJUSTED_EVENT_START_TIME,X_ADJUSTED_EVENT_END_TIME ) - total_view_time) > 5 then 1 else 0 end) as majot_viewing_time_fails
        ,sum(case when distinct_programme_links <> 1 then 1 else 0 end) as program_links_exceed_one
        ,max(abs(datediff(minute,ADJUSTED_EVENT_START_TIME,X_ADJUSTED_EVENT_END_TIME ) - total_view_time_in_minutes)) as biggest_minute_view_time_mismatch
        ,max(abs(datediff(second,ADJUSTED_EVENT_START_TIME,X_ADJUSTED_EVENT_END_TIME ) - total_view_time)) as biggest_second_view_time_mismatch
from stafforr.vespa_drop2_dupe_summary;
/* Copied out the Excel and transposed: yeah, there's more in here than the spanning ones, some of it is weird.
count(1)        14392594
account_number_failures 0
show_distinctness_failures      3639510
show_distinctness_fails_by_starttime    0
traffic_key_distinctness_failures       429411
detail_key_distinctness_failures        168871
chanel_name_failures    126019
event_duration_failures 0
view_time_summ_failures 180542
majot_viewing_time_fails     54950
program_links_exceed_one        14392594
biggest_minute_view_time_mismatch       43531.234
biggest_second_view_time_mismatch       2611897
*/

select top 50 *,
        abs(datediff(second,ADJUSTED_EVENT_START_TIME,X_ADJUSTED_EVENT_END_TIME ) - total_view_time) as viewtimefail,
        datediff(second,ADJUSTED_EVENT_START_TIME,X_ADJUSTED_EVENT_END_TIME ) - total_view_time as signed_viewtimefail
from stafforr.vespa_drop2_dupe_summary
where abs(datediff(second,ADJUSTED_EVENT_START_TIME,X_ADJUSTED_EVENT_END_TIME ) - total_view_time) > 20
order by viewtimefail desc;
-- Yeah, there are a lot, a bunch in the millions, all of them with positive viewtimefail
/*
26390173        2011-06-20 02:41:23.000000      2011-07-12 05:45:53.000000
17594864        2011-06-12 11:06:09.000000      2011-07-03 04:07:33.000000
21758684        2011-06-14 20:34:31.000000      2011-07-05 03:44:42.000000
15655095        2011-05-30 22:21:37.000000      2011-06-19 19:05:09.000000
26149895        2011-06-13 00:03:15.000000      2011-07-02 05:02:56.000000
22943566        2011-06-25 01:02:24.000000      2011-07-14 02:26:18.000000
23672856        2011-06-17 13:04:10.000000      2011-07-05 20:47:27.000000
27211848        2011-06-15 17:10:06.000000      2011-07-03 18:48:25.000000
*/
select * from stafforr.vespa_drop2_dupe_testing
where subscriber_id in (26390173,17594864,21758684,15655095,26149895,22943566,23672856,27211848)
        and ADJUSTED_EVENT_START_TIME in ('2011-06-20 02:41:23.000000','2011-06-12 11:06:09.000000','2011-06-14 20:34:31.000000','2011-05-30 22:21:37.000000','2011-06-13 00:03:15.000000','2011-06-25 01:02:24.000000','2011-06-17 13:04:10.000000','2011-06-15 17:10:06.000000')
and X_ADJUSTED_EVENT_END_TIME in ('2011-07-12 05:45:53.000000','2011-07-03 04:07:33.000000','2011-07-05 03:44:42.000000','2011-06-19 19:05:09.000000','2011-07-02 05:02:56.000000','2011-07-14 02:26:18.000000','2011-07-05 20:47:27.000000','2011-07-03 18:48:25.000000')
order by subscriber_id, ADJUSTED_EVENT_START_TIME, X_ADJUSTED_EVENT_END_TIME, TX_START_DATETIME_UTC;
-- IN is not the smartest construction, but hey, likliness of overlap considered small.

-- Hmm... also, all the signed fails are positive, or at least the alarge ones are.
select top 50 *,
        abs(datediff(second,ADJUSTED_EVENT_START_TIME,X_ADJUSTED_EVENT_END_TIME ) - total_view_time) as viewtimefail,
        datediff(second,ADJUSTED_EVENT_START_TIME,X_ADJUSTED_EVENT_END_TIME ) - total_view_time as signed_viewtimefail
from stafforr.vespa_drop2_dupe_summary
where abs(datediff(second,ADJUSTED_EVENT_START_TIME,X_ADJUSTED_EVENT_END_TIME ) - total_view_time) > 20
and signed_viewtimefail < 0
order by viewtimefail desc;
-- Okay, no, there are lots of large elements with


select max(X_EVENT_DURATION) from stafforr.vespa_drop2_dupe_testing;

-- How many big long events are there?
select top 100
        subscriber_id
        ,ADJUSTED_EVENT_START_TIME
        ,X_ADJUSTED_EVENT_END_TIME
        ,(min(X_EVENT_DURATION) + 0.0) / 60 / 60 / 24 as view_time_in_days
from stafforr.vespa_drop2_dupe_testing
where X_EVENT_DURATION >= 864000 -- the number of seconds in 10 days
group by subscriber_id
        ,ADJUSTED_EVENT_START_TIME
        ,X_ADJUSTED_EVENT_END_TIME
order by view_time_in_days desc;
-- Yeah, some 83 people with events 11 or more days in duration :/

select subscriber_id, ADJUSTED_EVENT_START_TIME, TX_START_DATETIME_UTC, count(1) as hits
into #audit_programme_dupes
from stafforr.vespa_drop2_dupe_testing
group by subscriber_id, ADJUSTED_EVENT_START_TIME, TX_START_DATETIME_UTC
having count(1) > 1;

select count(*), max(hits) from  #audit_programme_dupes;
-- 119 rows! All with 2 hits though. Why do those exist? They shouldn't.

select t.*
from  #audit_programme_dupes as p
inner join stafforr.vespa_drop2_dupe_testing as t
on p.subscriber_id = t.subscriber_id
and p.ADJUSTED_EVENT_START_TIME = t.ADJUSTED_EVENT_START_TIME
and p.TX_START_DATETIME_UTC = t.TX_START_DATETIME_UTC
order by t.subscriber_id, t.ADJUSTED_EVENT_START_TIME;
-- Looks like they have different STB Log creation days.

/********** EPG PROCCESSING INCONSISTNCIES **********/

-- What the join forming the events view should be:
/* -- Grabbed from the Vespa create table statement we chopped out of Phase 1b
sk_vespa_data.VESPA_STB_PROG_EVENTS_20110622 as V left outer join
    sk_prod.VESPA_EPG_DIM as P on
    V.PROGRAMME_TRANS_SK = P.PROGRAMME_TRANS_SK
*/

-- So we should be joining on PROGRAMME_TRANS_SK, but that's duplicated in the
-- tests above....

select * from sk_prod.VESPA_EPG_DIM
where PROGRAMME_TRANS_SK = '201106300000008000';
-- It's some show called "A Baby Story". It's not even comparable to the shows
-- we pulled out of that sample. Conclusion: PROGRAMME_TRANS_SK currently not
-- usable?

-- Do programmes do by SI_DETAIL_KEY or TRAFFIC_KEY instead?

select top 10 * from sk_prod.VESPA_SUBSCRIBER_PROG_CURR_MONTH;
-- But do we trust the summary vs the events view with the noted EPG key borkage?
select
        ACCOUNT_NUMBER
        ,SUBSCRIBER_ID
        ,STB_LOG_CREATION_DATE
        ,DOCUMENT_CREATION_DATE
        ,EPG_CHANNEL
        ,EPG_TITLE
        ,EVENT_TYPE
        ,X_EVENT_DURATION
        ,ADJUSTED_EVENT_START_TIME
        ,X_ADJUSTED_EVENT_END_TIME
        ,X_PROGRAMME_VIEWED_DURATION
        ,SI_DETAIL_KEY
        ,TRAFFIC_KEY
        ,PROGRAMME_TRANS_SK
        ,PROGRAMME_SEQUENCE_ID
        ,TX_DATE_TIME_UTC
        ,TX_START_DATETIME_UTC
        ,TX_END_DATETIME_UTC
from sk_prod.vespa_events_view
where subscriber_id = 15396 and  ADJUSTED_EVENT_START_TIME = '30/06/2011  18:21:11'
order by ADJUSTED_EVENT_START_TIME, TX_START_DATETIME_UTC;
-- This example is in June and so isn't in our sample :(
-- Also interesting: it failed to find the sample based on the programme key that we don't trust.
-- Wait... still nothing? That's bizarre :( We'll have to hunt again later.

-- Are there PROGRAMME_TRANS_SK that don't show up in the EPG DIM?
create index programme_lookup on stafforr.vespa_drop2_dupe_testing (PROGRAMME_TRANS_SK);

select count(1), count(distinct t.PROGRAMME_TRANS_SK) as distinct_fail_programmes
from stafforr.vespa_drop2_dupe_testing as t
left join sk_prod.VESPA_EPG_DIM as ed
on t.PROGRAMME_TRANS_SK = ed.PROGRAMME_TRANS_SK
where ed.PROGRAMME_TRANS_SK is null;
-- 19423116        1
-- Okay, so it's all the same null program. Actually, these might not even be associated with any
-- viewing type, or they might be the "sensitive" channels, or something.

select top 10 * from stafforr.vespa_drop2_dupe_testing where PROGRAMME_TRANS_SK = 0;
-- yeah, these examples are not evChangeView...
select distinct EVENT_TYPE from stafforr.vespa_drop2_dupe_testing where PROGRAMME_TRANS_SK = 0;
/* EVENT_TYPE: yeah, evChangeView isn't in here so it's not a problem.
evSurf
evStandbyIn
evStandbyOut
evEmptyLog
evChangeView
evTrackSurf
evPowerUp
*/

/********** LOOKING INTO THE PRE-AGGREGATED DATA **********/

-- So there are tables which provide aggegated information for us,
-- especially the subscriber vs programme channel; that'd be really
-- useful. Do we trust them? Do they link to the EPG like they should?

select top 10 * from sk_prod.VESPA_SUBSCRIBER_PROG_CURR_MONTH;

select distinct programme_trans_sk
into #programme_sk_lookup
 from sk_prod.VESPA_SUBSCRIBER_PROG_CURR_MONTH;

create unique index prog_lookup on #programme_sk_lookup (programme_trans_sk)

select count(1)
from #programme_sk_lookup as pl
left join sk_prod.VESPA_EPG_DIM as ved
on pl.programme_trans_sk = ved.programme_trans_sk
where ved.programme_trans_sk is null;
-- 1. So that's the 0, which is fine.

