/******************************************************************************
**
** Project Vespa: Dialback Report
**                  - Weekly Refresh Script
**
** The Dialback report wants to detail:
**  a. count of boxes by total number of log batches received
**  b. count of boxes by number of distinct days for which logs are received
**  c. count of boxes by largest number of sequential days with returned logs
** Do each of these for both 7 day and 30 day roling windows.
**
** See also:
**  http://rtci/vespa1/Dialback%20report.aspx
**
** The interesting pivot: put the length of largest continuous interval down
** the left hand side, put the total reporting days across the top, and fill
** the square with the number of boxes. You'll get something upper triangular
** with additional zeros in the far top right. This is the table we discussed
** with Sarah, that we'll need some additional modeling etc to figure out what
** it should / could look like under various circumstances, and what it will
** tell us about how boxes dial back.
**
** The other one was a graph of dialback probability vs ranked boxes. See how
** that varies. Then, we'd also want to do something about figuring out if
** dialback chance is constant in time, or if it varies a bunch. That'd be
** interesting too. So, lots to do, and even then it doesn't immediately lead
** to insight because these graphs won't expect even or flat or any particular
** kind of distribution. Analysis! Might actually have to do some algebra!
**
** What we don't have at the moment is anything about the reporting window
** covered by the logs that are returned. We don't care if logs only report
** every three days if each log batch has all three days of data.
**
** This guy will probably end up growing just like the OpDash did into some
** vast behemoth of a monstrosity, but for now we might just tack on some
** details about box type and P/S box because that could be relevant to the
** dialback statistics.
** 
**
** Features to implement:
**
** 10. Need theoretical analysis of what the transition eigenvectors imply
**      for the independence assessment. (It's also just a covariance matrix,
**      so that sort of analysis could help too.) Update: Also need to fix the
**      bug in the eigenvector calculations, it shouldn't be normalising the
**      vectors with the regular Euclidian metric, they should be transition
**      vectors and so sum to 1.
** 17. We've got keys on the big log dump tables. Is it going to be that much
**      faster to drop & recreate all the keys rather than rebuild following
**      30 inserts? Not sure it's worth the effort, especially if it's to be
**      scheduled overnight.
**
** Recently implemented:
**
** 21. Add the two request items: number of events per log and time of day logs
**      are received.
**
** Code sections:
**
**      Part A: A01 -  Initialise Logger
**              A02 -  Temporal Filters
**
**      Part B: B01 -  Create transient tables
**
**      Part C:        Log item processing
**              C01 -  Extract new log items
**              C02 -  Index creation on log items
**              C03 -  Summarise into daily items
**              C04 -  Build continuous intervals (30 day window)
**              C05 -  Build continuous intervals (7 day window)
**
**      Part F:        Box level stuff
**              F01 -  Creation & population
**              F02 -  Tacking on reporting statistics (30 day window)
**              F03 -  Tacking on reporting statistics (7 day window)
**
**      Part H:        Other variables & processing
**              H01 -  Primary / Secondary
**              H02 -  Box Type
**              H03 -  Premiums
**              H04 -  Value segment
**
**      Part J:
**              J01 -  Adjacency marking (30 day window)
**              J02 -  Adjacency marking (7 day window)
**
**      Part Q:        Automated QA section
**              Q01 -  Well formed interval bounds
**              Q02 -  Consistent matrix transition elements
**
**    Part QR: QR01 -  Box Type/Dialback deep dive
**             QR02 -  Dialback by Region
**
**      Part R:        Report building
**              R01 -  30 day analysis reports
**              R02 -  7 day analysis
**              R03 -  Complicated relations: 30 day analysis (not being used?)
**              R04 -  Complicated relations: 7 day analysis (not being used?)
**              R05 -  Transition analysis (placeholder)
**              R06 -  Non-reporting boxes profile
**              R07 -  Events per log
**              R08 -  Time of day logs sent
**
**      Part T: T01 -  Permissions!
**
**
** None of this is being speed profiled seriously yet, but we might later.
** The logger calls are all in place, but nothing seems especially slow. Update:
** This guy takes less than 10 minutes now that the single box view is in play,
** so it's not a concern at all.
**
******************************************************************************/

if object_id('Dialback_make_report') is not null
   drop procedure Dialback_make_report;

go

create procedure Dialback_make_report -- execute Dialback_make_report
as
begin

/****************** A01: SETTING UP THE LOGGER ******************/

declare @DBR_logging_ID         bigint
declare @Refresh_identifier     varchar(40)
declare @run_Identifier         varchar(20)
DECLARE @QA_catcher             integer


-- Now automatically detecting if it's a test build and logging appropriately...
if lower(user) = 'kmandal'
    set @run_Identifier = 'VespaDialback'
else
    set @run_Identifier = 'Dialback test ' || upper(right(user,1)) || upper(left(user,2))

set @Refresh_identifier = convert(varchar(10),today(),123) || ' DBR refresh'
EXECUTE citeam.logger_create_run @run_Identifier, @Refresh_identifier, @DBR_logging_ID output

/****************** A02: TEMPORAL FILTERY STUFF ******************/

declare @latest_full_date date
declare @event_from_date        integer
declare @event_to_date          integer
declare @gmt_start date
declare @bst_start date
-- No longer looking at things relative to days, instead running it on a fixed
-- Sunday -> Saturday data set for the week before. Or whatever the standard is,
-- defined by this common function:
execute vespa_analysts.Regulars_Get_report_end_date @latest_full_date output

-- we want to use the time dimension fields on the Select to speed up the time response (however still there is work to do to achieve a proper
-- state, as removing data manipulation commands from date fields on this Select and manipulating them after extraction... but it's a start)
-- so the shape that the time dimension fields have is YYYYMMDDHH and btw they are integer, so yeah is a numeric representation of a date and hour
-- hence, lets create the date range parting from @latest_full_date...

set @event_from_date    = convert(integer,dateformat(dateadd(day, -30, @latest_full_date),'yyyymmddhh'))        -- YYYYMMDD00
set @event_to_date      = convert(integer,dateformat(dateadd(day,1,@latest_full_date),'yyyymmdd')+'23')         -- YYYYMMDD23

set @bst_start = dateadd(dy, -(datepart(dw, datepart(year, today()) || '-03-31') -1),datepart(year, today()) || '-03-31')  -- to get last Sunday in March
set @gmt_start = dateadd(dy, -(datepart(dw, datepart(year, today()) || '-10-31') -1),datepart(year, today()) || '-10-31')  -- to get last Sunday in October

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'A01&2: Complete! (Prep)'
COMMIT

/****************** B01: CLEAR OUT TRANSIENT TABLES ******************/

-- We could drop them all explicitly, but why duplicate work?
execute Dialback_clear_transients

execute Dialback_clear_transients_BB

/****************** C01: POPULATING NEW LOGS TABLE ******************/

-- Want data going back 30 days...
/*
angeld: 

--30/10
We used to have a fixed set of hours based on the assumption that boxes would transmit the data during such window, this was from 00:00 until
08:59. All LOGS_RECEIVED at these hours of a Day belong to sum( day - 1 )...

Now this concept has changed due the fact that we found a high number of logs being captured at 23 hours of that day that were been dropped
because of the fixed hours on the Select...

The new concept refers to all logs received from 23:00 on day A until 22:59 on next day (A+1) will belong to A... For Example:

Logs for the 18th will be composed of:
+ logs received between 23:00 - 23:59 of the 18th
+ And logs received between 00:00 - 22:59 of the 19th

*/

IF today()  >= @bst_start and today() < @gmt_start
Begin
  insert into vespa_Dialback_log_collection_dump (
         subscriber_id
        ,stb_log_creation_date
        ,doc_creation_date_from_9am
        ,first_event_mark
        ,last_event_mark
        ,log_event_count
        ,hour_received
        ,panel_id
)
 select subscriber_id
        ,dateadd(hour,1, LOG_START_DATE_TIME_UTC)
        ,case
                when convert(integer,dateformat(min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)),'hh')) <23
                then cast(min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)) as date)-1
                else
                cast(min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)) as date)
        end as doc_creation_date_from_9am
        ,min(dateadd(hour,1, EVENT_START_DATE_TIME_UTC))
        ,max(dateadd(hour,1, EVENT_END_DATE_TIME_UTC))
        ,count(1)
        ,datepart(hh, min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)))
        ,min(panel_id)
 from   sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
 where  panel_id in (4,11,12)
    and     dk_event_start_datehour_dim between @event_from_date and @event_to_date
    and     LOG_RECEIVED_START_DATE_TIME_UTC is not null
    and     LOG_START_DATE_TIME_UTC is not null
    and     subscriber_id is not null
	and     EVENT_END_DATE_TIME_UTC is not null
 group by   subscriber_id, LOG_START_DATE_TIME_UTC
 having     doc_creation_date_from_9am is not null
End

ELSE
Begin
  insert into vespa_Dialback_log_collection_dump (
         subscriber_id
        ,stb_log_creation_date
        ,doc_creation_date_from_9am
        ,first_event_mark
        ,last_event_mark
        ,log_event_count
        ,hour_received
        ,panel_id
)
 select subscriber_id
                ,LOG_START_DATE_TIME_UTC
                ,case
                        when convert(integer,dateformat(min(LOG_RECEIVED_START_DATE_TIME_UTC),'hh')) <23
                        then cast(min(LOG_RECEIVED_START_DATE_TIME_UTC) as date)-1
                        else
                        cast(min(LOG_RECEIVED_START_DATE_TIME_UTC) as date)
                end as doc_creation_date_from_9am
                ,min(EVENT_START_DATE_TIME_UTC)
                ,max(EVENT_END_DATE_TIME_UTC)
                ,count(1)
                ,datepart(hh, min(LOG_RECEIVED_START_DATE_TIME_UTC))
                ,min(panel_id)
 from   sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
 where  panel_id in (4,11,12)
    and     dk_event_start_datehour_dim between @event_from_date and @event_to_date
    and     LOG_RECEIVED_START_DATE_TIME_UTC is not null
    and     LOG_START_DATE_TIME_UTC is not null
    and     subscriber_id is not null
	and     EVENT_END_DATE_TIME_UTC is not null
 group by   subscriber_id, LOG_START_DATE_TIME_UTC
 having     doc_creation_date_from_9am is not null
End

execute citeam.logger_add_event @DBR_logging_ID, 3, 'C01: Data loaded'
commit

/*
angeld:

We don't wanna take into count anything that is not related to the week we're producing...

A more context into this is that because logs for the last day of the week under analysis are sitting on next day between 00:00 and 08:59
hence the +1 in the select above for @latest_full_date... but now that we have what we want, lets kill that @latest_full_date+1 date...


Commented on 06-11-2012... I think this is actually not necessary
-- cool, so we have allocated the logs into the right dates... however, still having logs for @latest_full_date + 1 (resulting from saying
-- that logs on this date after 23hh will be alocated to such date which we actually don't need)... so lets get rid of those guys...
delete from vespa_Dialback_log_collection_dump
where doc_creation_date_from_9am > @latest_full_date
*/

set @QA_catcher = -1

select @QA_catcher = count(1)
from vespa_Dialback_log_collection_dump

commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'C01: Complete! (Logs sucked)', coalesce(@QA_catcher, -1)
commit

/****************** C02: INDICES ON A POPULATED TABLE ******************/

-- And now that the tables are populated, throw on the indices.
-- Hahano because we're now building that table elsewhere. Maybe we should disable
-- indices or drop & recreate it, rather than rebuilding it 30 times.

/****************** C03: SUMMARIZE INTO DETAILS BY DAY ******************/

insert into vespa_Dialback_log_daily_summary
select
    subscriber_id
    ,convert(date, doc_creation_date_from_9am) as log_date
    ,count(distinct doc_creation_date_from_9am) -- we never check that this isn't 1?
    ,min(first_event_mark)
    ,max(last_event_mark)
    ,sum(log_event_count) -- we're still not doing anything with these coverage numbers?
    ,min(hour_received)
from vespa_Dialback_log_collection_dump
where panel_id in (4,12) -- split out phone DP from BB panel
group by subscriber_id, log_date

commit

-- OK, well, we may have to do something to truncate out all the suitably old things
-- ... though it shouldn't be possible to timeshift viewing *forwards* in time? But
-- still, just for consistency and roundoff, do we want to say that it's possible to
-- have T+1 continuous days where T is our interval size? No. But it might happen as
-- we're futzing around with the doc_creation_date_from_9am thingy. Might just cap
-- out all the T+1 items later on.
delete from vespa_Dialback_log_daily_summary
where dateadd(day, 30, log_date) <= @latest_full_date
or log_date > @latest_full_date
commit

--Create Separate Table for the Broadband Daily Panel 11

insert into vespa_Dialback_log_daily_summary_BB
select
    subscriber_id
    ,convert(date, doc_creation_date_from_9am) as log_date
    ,count(distinct doc_creation_date_from_9am) -- we never check that this isn't 1?
    ,min(first_event_mark)
    ,max(last_event_mark)
    ,sum(log_event_count) -- we're still not doing anything with these coverage numbers?
    ,min(hour_received)
from vespa_Dialback_log_collection_dump
where panel_id in (11) -- split out phone DP from BB panel
group by subscriber_id, log_date

commit

delete from vespa_Dialback_log_daily_summary_BB
where dateadd(day, 30, log_date) <= @latest_full_date
or log_date > @latest_full_date
commit


-- Oh, and now we have everything we need from the log collection dump, so we can clear
-- that out too just to save on space:
delete from vespa_Dialback_log_collection_dump

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_log_daily_summary

commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'C03.1: Complete! (Summarise days)', coalesce(@QA_catcher, -1)
commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_log_daily_summary_BB

commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'C03.2: Complete! (Summarise days - PANEL 11)', coalesce(@QA_catcher, -1)
commit

/****************** C04: BUILD "CONTINUOUS" REPORTING INTERVALS ******************/

-- Well, Continuous is contestible given we're abusing the discrete day-ness
-- of it. And also because we could still be missing viewing events if it's
-- a busy box. But yeah.

-- So how we do this:
-- 1. The table of the previuos section gives us stuff lined up, one record per day
-- 2. If we were to join on matching subscriber and adjacent days, we'd end up with
--      chains of viewing.
-- 3. We're only interested in endpoints, so we can do a L join to get the first day,
--      a R join to get the last day, and because everything is continuous and we're
--      not interested in reporting across boxes, we don't have to iterate to connect
--      up the fragments.
-- 4. Then we order the endpoints, pair the n'th start point for each box with the
--      n'th endpoint, and those are our intervals.

select
    r.subscriber_id
    ,r.log_date
    ,rank() over (partition by r.subscriber_id order by r.log_date) as interval_sequencer
into #vespa_Dialback_interval_starts
from vespa_Dialback_log_daily_summary as l
right join vespa_Dialback_log_daily_summary as r
on l.subscriber_id = r.subscriber_id
and l.log_date+1 = r.log_date
where l.subscriber_id is null -- want things that have no predecessor - they're the start of the intervals

commit

create unique index fake_pk on #vespa_Dialback_interval_starts (subscriber_id, interval_sequencer)
commit

select
    l.subscriber_id
    ,l.log_date
    ,rank() over (partition by l.subscriber_id order by l.log_date) as interval_sequencer
into #vespa_Dialback_interval_ends    
from vespa_Dialback_log_daily_summary as l
left join vespa_Dialback_log_daily_summary as r
on l.subscriber_id = r.subscriber_id
and l.log_date+1 = r.log_date
where r.subscriber_id is null -- null r means they have no successor - they're the interval endpoints
commit
create unique index fake_pk on #vespa_Dialback_interval_ends (subscriber_id, interval_sequencer)
commit

-- Now with the endpoints established, check that the counts match by subscriber_id...

-- Well, maybe detailed QA later. For now, we saw that the counts were the same on the inserts...
-- is there a Sybase global variable that records the numnber of rows inserted? That could be
-- the usefulls.

-- And form the table of intervals

select
    l.subscriber_id
    ,l.log_date as interval_start
    ,r.log_date as interval_end
    ,datediff(day, l.log_date, r.log_date) +1 as interval_length
into #vespa_Dialback_intervals_30d
from #vespa_Dialback_interval_starts as l
inner join #vespa_Dialback_interval_ends as r
on l.subscriber_id = r.subscriber_id
and l.interval_sequencer = r.interval_sequencer

commit
create unique index fake_pk on #vespa_Dialback_intervals_30d (subscriber_id, interval_start)
-- Yes, (subscriber_id, interval_end) will also be unique, but we don't care that much,
-- we're only going to grab the biggest interval etc.

commit
drop table #vespa_Dialback_interval_starts
drop table #vespa_Dialback_interval_ends
commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from #vespa_Dialback_intervals_30d

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'C04.1: Complete! (30 day intervals)', coalesce(@QA_catcher, -1)
commit

--Create Separate Table for the Broadband Daily Panel 11
select
    r.subscriber_id
    ,r.log_date
    ,rank() over (partition by r.subscriber_id order by r.log_date) as interval_sequencer
into #vespa_Dialback_interval_starts_BB
from vespa_Dialback_log_daily_summary_BB as l
right join vespa_Dialback_log_daily_summary_BB as r
on l.subscriber_id = r.subscriber_id
and l.log_date+1 = r.log_date
where l.subscriber_id is null -- want things that have no predecessor - they're the start of the intervals

commit

create unique index fake_pk on #vespa_Dialback_interval_starts_BB (subscriber_id, interval_sequencer)
commit

select
    l.subscriber_id
    ,l.log_date
    ,rank() over (partition by l.subscriber_id order by l.log_date) as interval_sequencer
into #vespa_Dialback_interval_ends_BB
from vespa_Dialback_log_daily_summary_BB as l
left join vespa_Dialback_log_daily_summary_BB as r
on l.subscriber_id = r.subscriber_id
and l.log_date+1 = r.log_date
where r.subscriber_id is null -- null r means they have no successor - they're the interval endpoints
commit
create unique index fake_pk on #vespa_Dialback_interval_ends_BB (subscriber_id, interval_sequencer)
commit

-- Now with the endpoints established, check that the counts match by subscriber_id...

-- Well, maybe detailed QA later. For now, we saw that the counts were the same on the inserts...
-- is there a Sybase global variable that records the numnber of rows inserted? That could be
-- the usefulls.

-- And form the table of intervals

select
    l.subscriber_id
    ,l.log_date as interval_start
    ,r.log_date as interval_end
    ,datediff(day, l.log_date, r.log_date) +1 as interval_length
into #vespa_Dialback_intervals_30d_BB
from #vespa_Dialback_interval_starts_BB as l
inner join #vespa_Dialback_interval_ends_BB as r
on l.subscriber_id = r.subscriber_id
and l.interval_sequencer = r.interval_sequencer

commit
create unique index fake_pk on #vespa_Dialback_intervals_30d_BB (subscriber_id, interval_start)
-- Yes, (subscriber_id, interval_end) will also be unique, but we don't care that much,
-- we're only going to grab the biggest interval etc.

commit
drop table #vespa_Dialback_interval_starts_BB
drop table #vespa_Dialback_interval_ends_BB
commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from #vespa_Dialback_intervals_30d_BB


EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'C04.2: Complete! (30 day intervals - PANEL 11)', coalesce(@QA_catcher, -1)
commit

/****************** C05: BUILD "CONTINUOUS" REPORTING INTERVALS (7 DAY WINDOW) ******************/

-- Pretty much coppied from 30 day window build. The process is kind of
-- similar except that we also have to exclude the records more than 7
-- days out from sampling date.

select
    r.subscriber_id
    ,r.log_date
    ,rank() over (partition by r.subscriber_id order by r.log_date) as interval_sequencer
into #vespa_Dialback_interval_starts
from vespa_Dialback_log_daily_summary as l
right join vespa_Dialback_log_daily_summary as r
on l.subscriber_id = r.subscriber_id
and l.log_date+1 = r.log_date
and l.log_date+7 > @latest_full_date
where l.subscriber_id is null -- want things that have no predecessor - they're the start of the intervals
and r.log_date+7 > @latest_full_date

commit
create unique index fake_pk on #vespa_Dialback_interval_starts (subscriber_id, interval_sequencer)
commit

select
    l.subscriber_id
    ,l.log_date
    ,rank() over (partition by l.subscriber_id order by l.log_date) as interval_sequencer
into #vespa_Dialback_interval_ends
from vespa_Dialback_log_daily_summary as l
left join vespa_Dialback_log_daily_summary as r
on l.subscriber_id = r.subscriber_id
and l.log_date+1 = r.log_date
and r.log_date+7 > @latest_full_date
where r.subscriber_id is null -- null r means they have no successor - they're the interval endpoints
and l.log_date+7 > @latest_full_date

commit
create unique index fake_pk on #vespa_Dialback_interval_ends (subscriber_id, interval_sequencer)
commit

-- Now with the endpoints established, check that the counts match by subscriber_id...

-- Well, maybe detailed QA later. For now, we saw that the counts were the same on the inserts...
-- is there a Sybase global variable that records the numnber of rows inserted? That could be
-- the usefulls.

-- And form the table of intervals

select
    l.subscriber_id
    ,l.log_date as interval_start
    ,r.log_date as interval_end
    ,datediff(day, l.log_date, r.log_date) +1 as interval_length
into #vespa_Dialback_intervals_7d
from #vespa_Dialback_interval_starts as l
inner join #vespa_Dialback_interval_ends as r
on l.subscriber_id = r.subscriber_id
and l.interval_sequencer = r.interval_sequencer

commit
create unique index fake_pk on #vespa_Dialback_intervals_7d (subscriber_id, interval_start)
-- Yes, (subscriber_id, interval_end) will also be unique, but we don't care that much,
-- we're only going to grab the biggest interval etc.

commit
drop table #vespa_Dialback_interval_starts
drop table #vespa_Dialback_interval_ends
commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from #vespa_Dialback_intervals_7d
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'C05.1: Complete! (7 day intervals)', coalesce(@QA_catcher, -1)
commit

--Create Separate Table for the Broadband Daily Panel 11
select
    r.subscriber_id
    ,r.log_date
    ,rank() over (partition by r.subscriber_id order by r.log_date) as interval_sequencer
into #vespa_Dialback_interval_starts_BB
from vespa_Dialback_log_daily_summary_BB as l
right join vespa_Dialback_log_daily_summary_BB as r
on l.subscriber_id = r.subscriber_id
and l.log_date+1 = r.log_date
and l.log_date+7 > @latest_full_date
where l.subscriber_id is null -- want things that have no predecessor - they're the start of the intervals
and r.log_date+7 > @latest_full_date

commit
create unique index fake_pk on #vespa_Dialback_interval_starts_BB (subscriber_id, interval_sequencer)
commit

select
    l.subscriber_id
    ,l.log_date
    ,rank() over (partition by l.subscriber_id order by l.log_date) as interval_sequencer
into #vespa_Dialback_interval_ends_BB
from vespa_Dialback_log_daily_summary_BB as l
left join vespa_Dialback_log_daily_summary_BB as r
on l.subscriber_id = r.subscriber_id
and l.log_date+1 = r.log_date
and r.log_date+7 > @latest_full_date
where r.subscriber_id is null -- null r means they have no successor - they're the interval endpoints
and l.log_date+7 > @latest_full_date

commit
create unique index fake_pk on #vespa_Dialback_interval_ends_BB (subscriber_id, interval_sequencer)
commit

-- Now with the endpoints established, check that the counts match by subscriber_id...

-- Well, maybe detailed QA later. For now, we saw that the counts were the same on the inserts...
-- is there a Sybase global variable that records the numnber of rows inserted? That could be
-- the usefulls.

-- And form the table of intervals

select
    l.subscriber_id
    ,l.log_date as interval_start
    ,r.log_date as interval_end
    ,datediff(day, l.log_date, r.log_date) +1 as interval_length
into #vespa_Dialback_intervals_7d_BB
from #vespa_Dialback_interval_starts_BB as l
inner join #vespa_Dialback_interval_ends_BB as r
on l.subscriber_id = r.subscriber_id
and l.interval_sequencer = r.interval_sequencer

commit
create unique index fake_pk on #vespa_Dialback_intervals_7d_BB (subscriber_id, interval_start)
-- Yes, (subscriber_id, interval_end) will also be unique, but we don't care that much,
-- we're only going to grab the biggest interval etc.

commit
drop table #vespa_Dialback_interval_starts_BB
drop table #vespa_Dialback_interval_ends_BB
commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from #vespa_Dialback_intervals_7d_BB
commit


EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'C05.2: Complete! (7 day intervals - PANEL 11)', coalesce(@QA_catcher, -1)
commit

/****************** F01: CREATE BOX POPULATION ******************/

-- Now pointing to the single box view, which is where all these lookup things get built.
insert into vespa_Dialback_box_listing (
    account_number
    ,subscriber_id
    ,Vespa_box_state
    ,enabled_date
    ,Selection_date
    ,box_rank
    ,box_type
    ,value_segment  -- Value segment is in a multi-column index, so needs explicit insert here...
    ,premiums       -- ^^ ditto
)
select
    account_number
    ,subscriber_id
    ,status_Vespa
    ,enablement_date
    ,Selection_date
    ,case
        when sbv.PS_Flag = 'P' then 'Primary'
        when sbv.PS_Flag = 'S' then 'Secondary'
        else 'Unknown' end
    ,coalesce(sbv.box_type_subs, 'Unknown')
    ,null, null -- put null values in for premiums and value segment
from vespa_analysts.vespa_single_box_view as sbv
where panel = 'VESPA'
and status_vespa = 'Enabled'
and selection_date is not null

commit
-- Then we also want for the overview to know how many boxes there were enabled at each point.
-- Also, we can do selection request properly here too now as well...
update vespa_Dialback_box_listing
set enabled_30d                 = case when dateadd(day, -30, @latest_full_date) > Selection_date   then 1 else 0 end
    ,confirmed_activation_30d   = case when dateadd(day, -30, @latest_full_date) > enabled_date     then 1 else 0 end  -- Don't need to check Box State because they're all still in Vespa, by the panel = 'VESPA' check above
    ,enabled_7d                 = case when dateadd(day, -7,  @latest_full_date) > Selection_date   then 1 else 0 end
    ,confirmed_activation_7d    = case when dateadd(day, -7,  @latest_full_date) > enabled_date     then 1 else 0 end

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'F01.1: Complete! (Box population)', coalesce(@QA_catcher, -1)
commit

--Create Separate Table for the Broadband Daily Panel 11
insert into vespa_Dialback_box_listing_BB (
    account_number
    ,subscriber_id
    ,Vespa_box_state
    ,enabled_date
    ,Selection_date
    ,box_rank
    ,box_type
    ,value_segment  -- Value segment is in a multi-column index, so needs explicit insert here...
    ,premiums       -- ^^ ditto
)
select
    account_number
    ,subscriber_id
    ,status_Vespa
    ,enablement_date
    ,Selection_date
    ,case
        when sbv.PS_Flag = 'P' then 'Primary'
        when sbv.PS_Flag = 'S' then 'Secondary'
        else 'Unknown' end
    ,coalesce(sbv.box_type_subs, 'Unknown')
    ,null, null -- put null values in for premiums and value segment
from vespa_analysts.vespa_single_box_view as sbv
where panel = 'VESPA11'
and status_vespa = 'Enabled'
and selection_date is not null

commit
-- Then we also want for the overview to know how many boxes there were enabled at each point.
-- Also, we can do selection request properly here too now as well...
update vespa_Dialback_box_listing_BB
set enabled_30d                 = case when dateadd(day, -30, @latest_full_date) > Selection_date   then 1 else 0 end
    ,confirmed_activation_30d   = case when dateadd(day, -30, @latest_full_date) > enabled_date     then 1 else 0 end  -- Don't need to check Box State because they're all still in Vespa, by the panel = 'VESPA' check above
    ,enabled_7d                 = case when dateadd(day, -7,  @latest_full_date) > Selection_date   then 1 else 0 end
    ,confirmed_activation_7d    = case when dateadd(day, -7,  @latest_full_date) > enabled_date     then 1 else 0 end

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'F01.2: Complete! (Box population - PANEL 11)', coalesce(@QA_catcher, -1)
commit

/****************** F02: SUMMARISE INTERVAL DETAILS (30 DAY WINDOW) ******************/

-- First off, total reporting statistics:
select subscriber_id
    ,sum(logs_sent) as total_logs_30d
    ,count(1) as distinct_days_30d
into #dialback_aggregates
from vespa_Dialback_log_daily_summary
group by subscriber_id
commit

create unique index fake_PK on #dialback_aggregates (subscriber_id)
commit

update vespa_Dialback_box_listing
set vdbl.total_logs_30d         = da.total_logs_30d
    ,vdbl.distinct_days_30d     = da.distinct_days_30d
from vespa_Dialback_box_listing as vdbl
inner join #dialback_aggregates as da
on vdbl.subscriber_id = da.subscriber_id

commit

-- OK, now tack on the intervals statistics
select subscriber_id
    ,max(interval_length) as largest_interval_30d
    ,count(1) as interval_count_30d
into #big_intervals
from #vespa_Dialback_intervals_30d
group by subscriber_id
commit

create unique index fake_PK on #big_intervals (subscriber_id)
commit

update vespa_Dialback_box_listing
set vdbl.largest_interval_30d   = bi.largest_interval_30d
    ,vdbl.interval_count_30d    = bi.interval_count_30d
from vespa_Dialback_box_listing as vdbl
inner join #big_intervals as bi
on vdbl.subscriber_id = bi.subscriber_id

-- Cake!

-- If we did distinct days by raw and distinct days by intervals, then
-- we'd get a little inbuilt QA which is always good.

commit
drop table #dialback_aggregates
drop table #big_intervals
-- drop table #vespa_Dialback_intervals_30d -- this guy is used later for the transition analysis
commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where largest_interval_30d <> 0 and interval_count_30d <> 0
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'F02.1: Complete! (30 day summary)', coalesce(@QA_catcher, -1)
commit

--Create Separate Table for the Broadband Daily Panel 11
-- First off, total reporting statistics:
select subscriber_id
    ,sum(logs_sent) as total_logs_30d
    ,count(1) as distinct_days_30d
into #dialback_aggregates_BB
from vespa_Dialback_log_daily_summary_BB
group by subscriber_id
commit

create unique index fake_PK on #dialback_aggregates_BB (subscriber_id)
commit

update vespa_Dialback_box_listing_BB
set vdbl.total_logs_30d         = da.total_logs_30d
    ,vdbl.distinct_days_30d     = da.distinct_days_30d
from vespa_Dialback_box_listing_BB as vdbl
inner join #dialback_aggregates_BB as da
on vdbl.subscriber_id = da.subscriber_id

commit

-- OK, now tack on the intervals statistics
select subscriber_id
    ,max(interval_length) as largest_interval_30d
    ,count(1) as interval_count_30d
into #big_intervals_BB
from #vespa_Dialback_intervals_30d_BB
group by subscriber_id
commit

create unique index fake_PK on #big_intervals_BB (subscriber_id)
commit

update vespa_Dialback_box_listing_BB
set vdbl.largest_interval_30d   = bi.largest_interval_30d
    ,vdbl.interval_count_30d    = bi.interval_count_30d
from vespa_Dialback_box_listing_BB as vdbl
inner join #big_intervals_BB as bi
on vdbl.subscriber_id = bi.subscriber_id

-- If we did distinct days by raw and distinct days by intervals, then
-- we'd get a little inbuilt QA which is always good.

commit
drop table #dialback_aggregates_BB
drop table #big_intervals_BB
-- drop table #vespa_Dialback_intervals_30d -- this guy is used later for the transition analysis
commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where largest_interval_30d <> 0 and interval_count_30d <> 0
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'F02.2: Complete! (30 day summary - PANEL 11)', coalesce(@QA_catcher, -1)
commit

/****************** F03: SUMMARISE INTERVAL DETAILS (7 DAY WINDOW) ******************/

-- Pretty much coppied from 30 day window build.

-- First off, total reporting statistics:
select subscriber_id
    ,sum(logs_sent) as total_logs_7d
    ,count(1) as distinct_days_7d
into #dialback_aggregates
from vespa_Dialback_log_daily_summary
where log_date+7 > @latest_full_date -- Forcing the last 7 days
group by subscriber_id
commit

create unique index fake_PK on #dialback_aggregates (subscriber_id)
commit

update vespa_Dialback_box_listing
set vdbl.total_logs_7d         = da.total_logs_7d
    ,vdbl.distinct_days_7d     = da.distinct_days_7d
from vespa_Dialback_box_listing as vdbl
inner join #dialback_aggregates as da
on vdbl.subscriber_id = da.subscriber_id

commit

-- OK, now tack on the intervals statistics
select subscriber_id
    ,max(interval_length) as largest_interval_7d
    ,count(1) as interval_count_7d
into #big_intervals
from #vespa_Dialback_intervals_7d
group by subscriber_id
commit

create unique index fake_PK on #big_intervals (subscriber_id)
commit

update vespa_Dialback_box_listing
set vdbl.largest_interval_7d   = bi.largest_interval_7d
    ,vdbl.interval_count_7d    = bi.interval_count_7d
from vespa_Dialback_box_listing as vdbl
inner join #big_intervals as bi
on vdbl.subscriber_id = bi.subscriber_id

-- Cake!

-- If we did distinct days by raw and distinct days by intervals, then
-- we'd get a little inbuilt QA which is always good.

commit
drop table #dialback_aggregates
drop table #big_intervals
-- drop table #vespa_Dialback_intervals_7d -- this guy is used later for the transition analysis
commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where largest_interval_7d <> 0 and interval_count_7d <> 0
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'F03.1: Complete! (7 day summary)', coalesce(@QA_catcher, -1)
commit
--Create Separate Table for the Broadband Daily Panel 11
-- First off, total reporting statistics:

select subscriber_id
    ,sum(logs_sent) as total_logs_7d
    ,count(1) as distinct_days_7d
into #dialback_aggregates_BB
from vespa_Dialback_log_daily_summary_BB
where log_date+7 > @latest_full_date -- Forcing the last 7 days
group by subscriber_id
commit

create unique index fake_PK on #dialback_aggregates_BB (subscriber_id)
commit

update vespa_Dialback_box_listing_BB
set vdbl.total_logs_7d         = da.total_logs_7d
    ,vdbl.distinct_days_7d     = da.distinct_days_7d
from vespa_Dialback_box_listing_BB as vdbl
inner join #dialback_aggregates_BB as da
on vdbl.subscriber_id = da.subscriber_id

commit

-- OK, now tack on the intervals statistics
select subscriber_id
    ,max(interval_length) as largest_interval_7d
    ,count(1) as interval_count_7d
into #big_intervals_BB
from #vespa_Dialback_intervals_7d_BB
group by subscriber_id
commit

create unique index fake_PK on #big_intervals_BB (subscriber_id)
commit

update vespa_Dialback_box_listing_BB
set vdbl.largest_interval_7d   = bi.largest_interval_7d
    ,vdbl.interval_count_7d    = bi.interval_count_7d
from vespa_Dialback_box_listing_BB as vdbl
inner join #big_intervals_BB as bi
on vdbl.subscriber_id = bi.subscriber_id

-- Cake!

-- If we did distinct days by raw and distinct days by intervals, then
-- we'd get a little inbuilt QA which is always good.

commit
drop table #dialback_aggregates_BB
drop table #big_intervals_BB

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where largest_interval_7d <> 0 and interval_count_7d <> 0
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'F03.2: Complete! (7 day summary - PANEL 11)', coalesce(@QA_catcher, -1)
commit

/****************** H01: PRIMARY / SECONDARY BOX FLAG ******************/

-- Grab details from the single box view rather than from the customer database (again)

-- Ok, except that pull now runs directly from SBV too, during the table population

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where box_rank <> 'Unknown'
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'H01.1: Complete! (P/S box)', coalesce(@QA_catcher, -1)
COMMIT

--SAME AGAIN FOR BB DP 11
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where box_rank <> 'Unknown'
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'H01.2: Complete! (P/S box - PANEL 11)', coalesce(@QA_catcher, -1)
COMMIT

/****************** H02: BOX TYPE ******************/

-- Oh hey we went to SBV so may as well get box type from there too at the same time

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where box_type <> 'Unknown'
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'H02.1: Complete! (Box type)', coalesce(@QA_catcher, -1)
commit

--SAME AGAIN FOR BB DP 11
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where box_type <> 'Unknown'
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'H02.2: Complete! (Box type - PANEL 11)', coalesce(@QA_catcher, -1)
commit

/****************** H03: PREMIUMS ******************/

-- This section mostly stolen from OpDash section S02 (B02)

UPDATE vespa_Dialback_box_listing
SET   Premiums = CASE   WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN 'Top Tier'
                        WHEN cel.prem_sports = 1 AND cel.prem_movies = 2 THEN 'One sport, two movies'
                        WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN 'No sports, two movies'
                        WHEN cel.prem_sports = 2 AND cel.prem_movies = 1 THEN 'Two sports, one movies'
                        WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN 'Two sports, no movies'
                        WHEN cel.prem_sports = 1 AND cel.prem_movies = 1 THEN 'One sport, one movies'
                        WHEN cel.prem_sports = 0 AND cel.prem_movies = 0 THEN 'Basic' ELSE 'Unknown' END
      FROM vespa_Dialback_box_listing as csb
           inner join sk_prod.cust_subs_hist AS csh on csh.account_number = csb.account_number
           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel on csh.current_short_description = cel.short_description
     WHERE csh.subscription_sub_type ='DTV Primary Viewing'
       AND csh.subscription_type = 'DTV PACKAGE'
       AND csh.effective_from_dt <= @latest_full_date
       AND csh.effective_to_dt    > @latest_full_date
       AND csh.effective_from_dt <> csh.effective_to_dt

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where box_type <> 'Unknown'
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'H03.1: Complete! (Premiums)', coalesce(@QA_catcher, -1)
commit

--SAME AGAIN FOR BB DP 11
UPDATE vespa_Dialback_box_listing_BB
SET   Premiums = CASE   WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN 'Top Tier'
                        WHEN cel.prem_sports = 1 AND cel.prem_movies = 2 THEN 'One sport, two movies'
                        WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN 'No sports, two movies'
                        WHEN cel.prem_sports = 2 AND cel.prem_movies = 1 THEN 'Two sports, one movies'
                        WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN 'Two sports, no movies'
                        WHEN cel.prem_sports = 1 AND cel.prem_movies = 1 THEN 'One sport, one movies'
                        WHEN cel.prem_sports = 0 AND cel.prem_movies = 0 THEN 'Basic' ELSE 'Unknown' END
      FROM vespa_Dialback_box_listing_BB as csb
           inner join sk_prod.cust_subs_hist AS csh on csh.account_number = csb.account_number
           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel on csh.current_short_description = cel.short_description
     WHERE csh.subscription_sub_type ='DTV Primary Viewing'
       AND csh.subscription_type = 'DTV PACKAGE'
       AND csh.effective_from_dt <= @latest_full_date
       AND csh.effective_to_dt    > @latest_full_date
       AND csh.effective_from_dt <> csh.effective_to_dt

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where box_type <> 'Unknown'
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'H03.2: Complete! (Premiums - PANEL 11)', coalesce(@QA_catcher, -1)
commit

/****************** H04: VALUE SEGMENT ******************/

-- This section mostly stolen from OpDash section S02 (B03)

UPDATE vespa_Dialback_box_listing
   SET value_segment = coalesce(tgt.value_seg, 'Bedding In') -- because anything that isn't in the lookup because they're new will be new
  FROM vespa_Dialback_box_listing AS base
       left JOIN sk_prod.VALUE_SEGMENTS_DATA AS tgt ON base.account_number = tgt.account_number

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where value_segment <> 'Bedding In'
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'H04.1: Complete! (Value Segments)', coalesce(@QA_catcher, -1)
commit

--SAME AGAIN FOR BB DP 11

UPDATE vespa_Dialback_box_listing_BB
   SET value_segment = coalesce(tgt.value_seg, 'Bedding In') -- because anything that isn't in the lookup because they're new will be new
  FROM vespa_Dialback_box_listing_BB AS base
       left JOIN sk_prod.VALUE_SEGMENTS_DATA AS tgt ON base.account_number = tgt.account_number

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where value_segment <> 'Bedding In'
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'H04.2: Complete! (Value Segments - PANEL 11)', coalesce(@QA_catcher, -1)
commit

/****************** J01: STATE CHANGE ANALYSIS (30 DAY) ******************/

-- So one of the open questions is how well justified it is to be treating dialbacks
-- as an independent chance of happening per day, even if this chance changes per box?
-- The plan is to calculate a state transition matrix, 2x2 in the basic case, reporting
-- or not yesterday vs reporting or not today. If it's a random variable then these
-- columns should be identical, ie, whether it reported or not yesterday should have no
-- influence on whether or not it reports today.

-- But how? heheheh...

-- Okay, so if we've already reduced it to intervals... for each box... then:
--   0->1 : number of intervals, -1 in cases where the first day of the period was reported
--   1->1 : total lenght of intervals minus number of intervals
--   1->0 : number of intervals, -1 in cases where the last day of sampling is reported
--   0->0 : the difference from the three above? is there a better way?
-- So from the agregates, we need number of intervals, total length of all intervals,
-- whether or not there is reporting on the first day and / or last day.

/* OK, so the transition profiling has been discontinued. Oh well.

select 
    subscriber_id
    ,count(1) as number_of_intervals
    ,case when min(interval_start) = dateadd(day, -29, @latest_full_date) then 1 else 0 end as reporting_first_day
    ,case when max(interval_end) = @latest_full_date then 1 else 0 end as reporting_last_day
    ,sum(interval_length) as total_length_of_intervals
into #vespa_Dialback_transition_analysis_30d
from #vespa_Dialback_intervals_30d
group by subscriber_id

commit
create unique index fake_PK on #vespa_Dialback_transition_analysis_30d (subscriber_id)
commit
-- So now we have the required variables, go and form the transition counts
update vespa_Dialback_box_listing
set
    -- coalesce(.) because if something has no intervals then it's never reporting so it's all 0->0 transitions...
    t_01_counts_30d     = coalesce(number_of_intervals - reporting_first_day,0)
    ,t_11_counts_30d    = coalesce(total_length_of_intervals - number_of_intervals - reporting_first_day,0)
    ,t_10_counts_30d    = coalesce(number_of_intervals - reporting_last_day,0)
from vespa_Dialback_box_listing as vdbl
inner join #vespa_Dialback_transition_analysis_30d as ta
on vdbl.subscriber_id = ta.subscriber_id

commit
update vespa_Dialback_box_listing
set t_00_counts_30d = 29 - t_01_counts_30d - t_11_counts_30d - t_10_counts_30d
commit
-- 29 transitions are available, nothing for first day (we don't know prior state)

-- Then: to quantify how far out we are, what do we calculate? Well, we now in the case
-- of daily independence we end up with a transition matrix with a nullspace - so maybe
-- we report the determinant of that matrix? Not even hard to calculate. Might try that.

update vespa_Dialback_box_listing
set transition_determinant_30d = 
    round(case when (t_00_counts_30d = 0 and t_01_counts_30d = 0) then 0
        when (t_10_counts_30d = 0 and t_11_counts_30d = 0) then 0
        else convert(double, abs(t_00_counts_30d * t_11_counts_30d - t_01_counts_30d * t_10_counts_30d))
                / (t_00_counts_30d * t_00_counts_30d + t_01_counts_30d * t_01_counts_30d) 
                / convert(double, (t_10_counts_30d * t_10_counts_30d + t_11_counts_30d * t_11_counts_30d))
    end,6)
-- Heh, not normalising by the 30x30, but normalising so that the columns of the
-- transition matrix are unit vectors. Except we only want the determinant. From
-- some 1st year maths, the determinant should end up between 0 and 1, and will
-- be the eigenvector that isn't 1.

-- Oh wait, that calculation has the flaw; the columns need to sum to 1, rather
-- than be normalised by the regular Euclidian metric, since they should be state
-- transition vectors. So... yeah, might have to change that around if it comes up.

commit

drop table #vespa_Dialback_transition_analysis_30d
drop table #vespa_Dialback_intervals_30d

-- Now to sample those values and put them into a distribution results table...
declare @box_counter    bigint
declare @sample_diff    bigint

-- Figure out sampling perameters
select @box_counter = count(1) from vespa_Dialback_box_listing where transition_determinant_30d is not null
set @sample_diff = @box_counter / 100 -- bigints take care of the rounding

-- OK, now make the ranks... We could have done this in the report building section (R) but
-- it was already here, so whatever.
select transition_determinant_30d
    ,rank() over (order by transition_determinant_30d desc, subscriber_id) as trans_rank
into vespa_Dialback_11_transition_profiling
from vespa_Dialback_box_listing
where transition_determinant_30d is not null

-- Clip out the things that don't form our sample...
delete from vespa_Dialback_11_transition_profiling
where mod(trans_rank, @sample_diff) <> 1

-- OK, and that's our transitions profile!

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_11_transition_profiling
-- Should always be 101

*/

-- But we should still clear out this guy:
drop table #vespa_Dialback_intervals_30d

commit
--SAME AGAIN FOR BB DP 11
drop table #vespa_Dialback_intervals_30d_BB

commit
--EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'J01: Complete! (30 day adjacency)', coalesce(@QA_catcher, -1)
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'J01: Discontinued! (transition analysis)'
commit

/****************** J02: STATE CHANGE ANALYSIS (7 DAY) ******************/

-- Okay, and now the same for 7 day (though... this one is a strict subset?)

-- Um... really? This is going to give a strictly less good view than the 30
-- day build. Might just leave it out. Though... then we have to explain why
-- we left it out? Oh well. Dupe it then. But yeah, visibility and sample size
-- are going to be terrible here... yeah, leave it out.

-- Clear out that table we didn't think we'd need:
drop table #vespa_Dialback_intervals_7d

commit
--SAME AGAIN FOR BB DP 11
drop table #vespa_Dialback_intervals_7d_BB

commit
-- Done no work, so it don't count for logging.
--EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'J02: Complete! (7 day adjacency)'
--commit

/****************** Q01: WELL FORMED INTERVAL BOUNDS ******************/
-- Stepping through a bunch of different basic tests and throwing warnings
-- at the logger if any of them don't work out

-- This query should return nothing: if there are no logs returned then the
-- derived figures should all be zero too:
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where total_logs_30d = 0
and distinct_days_30d + largest_interval_30d + interval_count_30d > 0

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.01: Warning! 30d magic log summoning!', @QA_catcher
commit

-- This query should also return nothing: if there's only one interval, then
-- the distinct days better all form a single interval:
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where interval_count_30d = 1
and distinct_days_30d <> largest_interval_30d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.02: Warning! 30d single interval coalescing failures!', @QA_catcher
commit

-- Another query that should be zero: largest interval should be bounded
-- by the distinct days of reporting
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where distinct_days_30d < largest_interval_30d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.03: Warning! 30d exceeding interval issues!', @QA_catcher
commit

-- Firther empty QA: distinct days should be bounded by the total logs
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where total_logs_30d < distinct_days_30d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.04: Warning! 30d inverted coverage problems!', @QA_catcher
commit

-- And may as well check the same for the 7's...

-- This query should return nothing: if there are no logs returned then the
-- derived figures should all be zero too:
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where total_logs_7d = 0
and distinct_days_7d + largest_interval_7d + interval_count_7d > 0

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.05: Warning! 7d magic log summoning!', @QA_catcher
commit

-- This query should also return nothing: if there's only one interval, then
-- the distinct days better all form a single interval:
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where interval_count_7d = 1
and distinct_days_7d <> largest_interval_7d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.06: Warning! 7d single interval coalescing failures!', @QA_catcher
commit

-- Another query that should be zero: largest interval should be bounded
-- by the distinct days of reporting
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where distinct_days_7d < largest_interval_7d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.07: Warning! 7d exceeding interval issues!', @QA_catcher
commit

-- Firther empty QA: distinct days should be bounded by the total logs
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where total_logs_7d < distinct_days_7d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.08: Warning! 7d inverted coverage problems!', @QA_catcher
commit

-- And then: there also shouldn't be any cases where the 7 day numbers
-- exceed to 30 day numbers, so this should be empty:
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where total_logs_7d > total_logs_30d
or distinct_days_7d > distinct_days_30d
or interval_count_7d > interval_count_30d
or largest_interval_7d > largest_interval_30d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.09: Warning! Relative log bounds errors!', @QA_catcher
commit

-- Oh and they should be bounded by the size of the interval; should be empty.
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where total_logs_7d > 7
or total_logs_30d > 30
or distinct_days_7d > 7
or distinct_days_30d > 30
or interval_count_7d > 7
or interval_count_30d > 30
or largest_interval_7d > 7
or largest_interval_30d > 30

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.10: Warning! Absolute log bounds errors!', @QA_catcher
commit

--SAME AGAIN FOR BB DP 11
-- This query should return nothing: if there are no logs returned then the
-- derived figures should all be zero too:
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where total_logs_30d = 0
and distinct_days_30d + largest_interval_30d + interval_count_30d > 0

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.11: Warning! 30d magic log summoning (PANEL 11)!', @QA_catcher
commit

-- This query should also return nothing: if there's only one interval, then
-- the distinct days better all form a single interval:
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where interval_count_30d = 1
and distinct_days_30d <> largest_interval_30d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.12: Warning! 30d single interval coalescing failures (PANEL 11)!', @QA_catcher
commit

-- Another query that should be zero: largest interval should be bounded
-- by the distinct days of reporting
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where distinct_days_30d < largest_interval_30d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.13: Warning! 30d exceeding interval issues (PANEL 11)!', @QA_catcher
commit

-- Firther empty QA: distinct days should be bounded by the total logs
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where total_logs_30d < distinct_days_30d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.14: Warning! 30d inverted coverage problems (PANEL 11)!', @QA_catcher
commit

-- And may as well check the same for the 7's...

-- This query should return nothing: if there are no logs returned then the
-- derived figures should all be zero too:
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where total_logs_7d = 0
and distinct_days_7d + largest_interval_7d + interval_count_7d > 0

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.15: Warning! 7d magic log summoning (PANEL 11)!', @QA_catcher
commit

-- This query should also return nothing: if there's only one interval, then
-- the distinct days better all form a single interval:
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where interval_count_7d = 1
and distinct_days_7d <> largest_interval_7d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.16: Warning! 7d single interval coalescing failures (PANEL 11)!', @QA_catcher
commit

-- Another query that should be zero: largest interval should be bounded
-- by the distinct days of reporting
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where distinct_days_7d < largest_interval_7d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.17: Warning! 7d exceeding interval issues (PANEL 11)!', @QA_catcher
commit

-- Firther empty QA: distinct days should be bounded by the total logs
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where total_logs_7d < distinct_days_7d

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.18: Warning! 7d inverted coverage problems (PANEL 11)!', @QA_catcher
commit

-- And then: there also shouldn't be any cases where the 7 day numbers
-- exceed to 30 day numbers, so this should be empty:
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where total_logs_7d > total_logs_30d
or distinct_days_7d > distinct_days_30d
or interval_count_7d > interval_count_30d
or largest_interval_7d > largest_interval_30d

commit
if @QA_catcher is null or @QA_catcher <> 0
     EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.19: Warning! Relative log bounds errors (PANEL 11)!', @QA_catcher
commit

-- Oh and they should be bounded by the size of the interval; should be empty.
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where total_logs_7d > 7
or total_logs_30d > 30
or distinct_days_7d > 7
or distinct_days_30d > 30
or interval_count_7d > 7
or interval_count_30d > 30
or largest_interval_7d > 7
or largest_interval_30d > 30

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q01.20: Warning! Absolute log bounds errors (PANEL 11)!', @QA_catcher
commit


EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'Q01: Complete! (Interval QA)'
commit

/****************** Q02: CONSISTENT MATRIX TRANSITION ELEMENTS ******************/

-- If we've not balsed up counting the endpoints etc during the transition
-- probability calculations, then we shouldn't have any negative numbers
-- here (which are built by just taking the other coeficients away from
-- the expected totals)
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where t_00_counts_30d < 0

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q02.1: Warning! Negative 0->0 transition counts!', @QA_catcher
commit

-- The determinant should have been ABS(.)'d and since it's capped at the
-- value of the "other" eigenvalue, should be capped at 1.
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing
where transition_determinant_30d > 1
or transition_determinant_30d < 0

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q02.2: Warning! Out-of-bounds transition elements!', @QA_catcher
commit

--SAME AGAIN FOR BB DP 11
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where t_00_counts_30d < 0

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q02.3: Warning! Negative 0->0 transition counts (PANEL 11)!', @QA_catcher
commit

-- The determinant should have been ABS(.)'d and since it's capped at the
-- value of the "other" eigenvalue, should be capped at 1.
SET @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_Dialback_box_listing_BB
where transition_determinant_30d > 1
or transition_determinant_30d < 0

commit
if @QA_catcher is null or @QA_catcher <> 0
    EXECUTE citeam.logger_add_event @DBR_logging_ID, 2, 'Q02.4: Warning! Out-of-bounds transition elements (PANEL 11)!', @QA_catcher
commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'Q02: Complete! (Transition QA)'
commit


/****************** QR01: BOX TYPE / DIALBACK DEEP DIVE ******************/

-- Breakdown of dialback quality and Box model

SELECT   NUMBER(*) AS id
        ,RIGHT(('0000000000' || CAST(vdbl.subscriber_id AS VARCHAR(8))),10) AS Subscriber_id_z
        ,src_system_id
        ,total_logs_30d
   --     ,ROW_NUMBER() OVER(ORDER BY Subscriber_id_z) AS id                            -- row number not working! have to do it the long way...
  INTO   --drop table
         #subs_id
  FROM   vespa_analysts.VESPA_DIALBACK_BOX_LISTING AS vdbl
       INNER JOIN sk_prod.CUST_SERVICE_INSTANCE    AS csi
           ON RIGHT(('0000000000' || CAST(vdbl.subscriber_id AS VARCHAR(8))),10) = RIGHT(('0000000000' || CAST(csi.si_external_identifier AS VARCHAR(8))),10)
COMMIT

--dedupe

SELECT  *
       ,RANK() OVER(PARTITION BY subscriber_id_z ORDER BY id) AS dedupe_rnk
       ,CAST('No Desc' AS VARCHAR(30)) AS box_model
  INTO  --drop table
        #subs_id_dist
  FROM  #subs_id
COMMIT

DELETE FROM #subs_id_dist WHERE dedupe_rnk > 1
COMMIT

UPDATE  #subs_id_dist AS base
   SET  box_model = COALESCE(x_model_number,'No Desc')
  FROM  sk_prod.CUST_SET_TOP_BOX AS cstb
 WHERE  cstb.service_instance_id = base.src_system_id
   AND  active_box_flag = 'Y'                                   --- date criteria needed too
COMMIT

if object_id('vespa_analysts.vespa_Dialback_19_box_type') is not null
   drop table vespa_analysts.vespa_Dialback_19_box_type

SELECT  box_model
       ,CASE WHEN total_logs_30d = 0               THEN '1) 0'
             WHEN total_logs_30d BETWEEN 1  AND 5  THEN '2) 1-5'
             WHEN total_logs_30d BETWEEN 6  AND 10 THEN '3) 6-10'
             WHEN total_logs_30d BETWEEN 11 AND 15 THEN '4) 11-15'
             WHEN total_logs_30d BETWEEN 16 AND 20 THEN '5) 16-20'
             WHEN total_logs_30d BETWEEN 21 AND 25 THEN '6) 21-25'
             WHEN total_logs_30d BETWEEN 26 AND 30 THEN '7) 26-30'
             ELSE                                       NULL
             END AS total_logs_30d
       ,COUNT(*) AS customers
  INTO  vespa_Dialback_19_box_type
  FROM  #subs_id_dist
GROUP BY box_model
        ,total_logs_30d
COMMIT


--SAME AGAIN FOR BB DP 11
SELECT   NUMBER(*) AS id
        ,RIGHT(('0000000000' || CAST(vdbl.subscriber_id AS VARCHAR(8))),10) AS Subscriber_id_z
        ,src_system_id
        ,total_logs_30d
   --     ,ROW_NUMBER() OVER(ORDER BY Subscriber_id_z) AS id                            -- row number not working! have to do it the long way...
  INTO   --drop table
         #subs_id_BB
  FROM   vespa_analysts.VESPA_DIALBACK_BOX_LISTING_BB AS vdbl
       INNER JOIN sk_prod.CUST_SERVICE_INSTANCE    AS csi
           ON RIGHT(('0000000000' || CAST(vdbl.subscriber_id AS VARCHAR(8))),10) = RIGHT(('0000000000' || CAST(csi.si_external_identifier AS VARCHAR(8))),10)
COMMIT

--dedupe

SELECT  *
       ,RANK() OVER(PARTITION BY subscriber_id_z ORDER BY id) AS dedupe_rnk
       ,CAST('No Desc' AS VARCHAR(30)) AS box_model
  INTO  --drop table
        #subs_id_dist_BB
  FROM  #subs_id_BB
COMMIT

DELETE FROM #subs_id_dist_BB WHERE dedupe_rnk > 1
COMMIT

UPDATE  #subs_id_dist_BB AS base
   SET  box_model = COALESCE(x_model_number,'No Desc')
  FROM  sk_prod.CUST_SET_TOP_BOX AS cstb
 WHERE  cstb.service_instance_id = base.src_system_id
   AND  active_box_flag = 'Y'                                   --- date criteria needed too
COMMIT

if object_id('vespa_analysts.vespa_Dialback_19_box_type_BB') is not null
   drop table vespa_analysts.vespa_Dialback_19_box_type_BB

SELECT  box_model
       ,CASE WHEN total_logs_30d = 0               THEN '1) 0'
             WHEN total_logs_30d BETWEEN 1  AND 5  THEN '2) 1-5'
             WHEN total_logs_30d BETWEEN 6  AND 10 THEN '3) 6-10'
             WHEN total_logs_30d BETWEEN 11 AND 15 THEN '4) 11-15'
             WHEN total_logs_30d BETWEEN 16 AND 20 THEN '5) 16-20'
             WHEN total_logs_30d BETWEEN 21 AND 25 THEN '6) 21-25'
             WHEN total_logs_30d BETWEEN 26 AND 30 THEN '7) 26-30'
             ELSE                                       NULL
             END AS total_logs_30d
       ,COUNT(*) AS customers
  INTO  vespa_Dialback_19_box_type_BB
  FROM  #subs_id_dist_BB
GROUP BY box_model
        ,total_logs_30d
COMMIT
-- select * from vespa_Dialback_19_box_type order by box_model,total_logs_30d

/****************** QR02: DIALBACK BY REGION ******************/

-- Breakdown of dialback quality and UK Region (account level aggregation)

if object_id('vespa_analysts.vespa_Dialback_20_Region') is not null
   drop table vespa_analysts.vespa_Dialback_20_Region


SELECT  isba_tv_region AS TV_region
       ,CASE WHEN total_logs_30d = 0               THEN '1) 0'
             WHEN total_logs_30d BETWEEN 1  AND 5  THEN '2) 1-5'
             WHEN total_logs_30d BETWEEN 6  AND 10 THEN '3) 6-10'
             WHEN total_logs_30d BETWEEN 11 AND 15 THEN '4) 11-15'
             WHEN total_logs_30d BETWEEN 16 AND 20 THEN '5) 16-20'
             WHEN total_logs_30d BETWEEN 21 AND 25 THEN '6) 21-25'
             WHEN total_logs_30d BETWEEN 26 AND 30 THEN '7) 26-30'
             ELSE                                       NULL
             END AS total_logs_30d
       ,COUNT(*) AS customers
  INTO  vespa_Dialback_20_Region
  FROM  vespa_analysts.VESPA_DIALBACK_BOX_LISTING  AS vdbl
       INNER JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW AS sav   ON sav.account_number = vdbl.account_number
GROUP BY TV_region
        ,total_logs_30d
COMMIT

--SAME AGAIN FOR BB DP 11
if object_id('vespa_analysts.vespa_Dialback_20_Region_BB') is not null
   drop table vespa_analysts.vespa_Dialback_20_Region_BB


SELECT  isba_tv_region AS TV_region
       ,CASE WHEN total_logs_30d = 0               THEN '1) 0'
             WHEN total_logs_30d BETWEEN 1  AND 5  THEN '2) 1-5'
             WHEN total_logs_30d BETWEEN 6  AND 10 THEN '3) 6-10'
             WHEN total_logs_30d BETWEEN 11 AND 15 THEN '4) 11-15'
             WHEN total_logs_30d BETWEEN 16 AND 20 THEN '5) 16-20'
             WHEN total_logs_30d BETWEEN 21 AND 25 THEN '6) 21-25'
             WHEN total_logs_30d BETWEEN 26 AND 30 THEN '7) 26-30'
             ELSE                                       NULL
             END AS total_logs_30d
       ,COUNT(*) AS customers
  INTO  vespa_Dialback_20_Region_BB
  FROM  vespa_analysts.VESPA_DIALBACK_BOX_LISTING_BB  AS vdbl
       INNER JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW AS sav   ON sav.account_number = vdbl.account_number
GROUP BY TV_region
        ,total_logs_30d
COMMIT
/****************** R01: 30 DAY ANALYSIS RESULTS ******************/
-- All of these guys get pivoted so we're not ordering the pulls and
-- as such aren't worying about indices or anything.

-- Satndard 30d control totals....
select box_rank, box_type, premiums, value_segment, total_logs_30d, count(1) as box_count
into vespa_Dialback_01_pivot_for_30d
from vespa_Dialback_box_listing
where total_logs_30d > 0 and confirmed_activation_30d = 1
group by box_rank, box_type, premiums, value_segment, total_logs_30d

-- Distinct day analysis...
select box_rank, box_type, premiums, value_segment, distinct_days_30d, count(1) as box_count
into vespa_Dialback_03_distinct_for_30d
from vespa_Dialback_box_listing
where distinct_days_30d > 0 and confirmed_activation_30d = 1
group by box_rank, box_type, premiums, value_segment, distinct_days_30d

select box_rank, box_type, premiums, value_segment, largest_interval_30d, count(1) as box_count
into vespa_Dialback_05_intervals_for_30d
from vespa_Dialback_box_listing
where largest_interval_30d > 0 and confirmed_activation_30d = 1
group by box_rank, box_type, premiums, value_segment, largest_interval_30d
order by box_rank, box_type, premiums, value_segment, largest_interval_30d

commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R01.1: Complete! (30 day charts)'
commit

--SAME AGAIN FOR BB DP 11
-- Satndard 30d control totals....
select box_rank, box_type, premiums, value_segment, total_logs_30d, count(1) as box_count
into vespa_Dialback_01_pivot_for_30d_BB
from vespa_Dialback_box_listing_BB
where total_logs_30d > 0 and confirmed_activation_30d = 1
group by box_rank, box_type, premiums, value_segment, total_logs_30d

-- Distinct day analysis...
select box_rank, box_type, premiums, value_segment, distinct_days_30d, count(1) as box_count
into vespa_Dialback_03_distinct_for_30d_BB
from vespa_Dialback_box_listing_BB
where distinct_days_30d > 0 and confirmed_activation_30d = 1
group by box_rank, box_type, premiums, value_segment, distinct_days_30d

select box_rank, box_type, premiums, value_segment, largest_interval_30d, count(1) as box_count
into vespa_Dialback_05_intervals_for_30d_BB
from vespa_Dialback_box_listing_BB
where largest_interval_30d > 0 and confirmed_activation_30d = 1
group by box_rank, box_type, premiums, value_segment, largest_interval_30d
order by box_rank, box_type, premiums, value_segment, largest_interval_30d

commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R01.2: Complete! (30 day charts - PANEL 11)'
commit

/****************** R02: 7 DAY ANALYSIS RESULTS ******************/
-- All of these guys get pivoted so we're not ordering the pulls and
-- as such aren't worying about indices or anything.

-- First off, 7d control totals
select box_rank, box_type, premiums, value_segment, total_logs_7d, count(1) as box_count
into vespa_Dialback_02_pivot_for_7d
from vespa_Dialback_box_listing
where total_logs_7d > 0 and confirmed_activation_7d = 1
group by box_rank, box_type, premiums, value_segment, total_logs_7d

select box_rank, box_type, premiums, value_segment, distinct_days_7d, count(1) as box_count
into vespa_Dialback_04_distinct_for_7d
from vespa_Dialback_box_listing
where distinct_days_7d > 0 and confirmed_activation_7d = 1
group by box_rank, box_type, premiums, value_segment, distinct_days_7d

select box_rank, box_type, premiums, value_segment, largest_interval_7d, count(1) as box_count
into vespa_Dialback_06_intervals_for_7d
from vespa_Dialback_box_listing
where largest_interval_7d > 0 and confirmed_activation_7d = 1
group by box_rank, box_type, premiums, value_segment, largest_interval_7d

commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R02.1: Complete! (7 day charts)'
commit

--SAME AGAIN FOR BB DP 11
-- First off, 7d control totals
select box_rank, box_type, premiums, value_segment, total_logs_7d, count(1) as box_count
into vespa_Dialback_02_pivot_for_7d_BB
from vespa_Dialback_box_listing_BB
where total_logs_7d > 0 and confirmed_activation_7d = 1
group by box_rank, box_type, premiums, value_segment, total_logs_7d

select box_rank, box_type, premiums, value_segment, distinct_days_7d, count(1) as box_count
into vespa_Dialback_04_distinct_for_7d_BB
from vespa_Dialback_box_listing_BB
where distinct_days_7d > 0 and confirmed_activation_7d = 1
group by box_rank, box_type, premiums, value_segment, distinct_days_7d

select box_rank, box_type, premiums, value_segment, largest_interval_7d, count(1) as box_count
into vespa_Dialback_06_intervals_for_7d_BB
from vespa_Dialback_box_listing_BB
where largest_interval_7d > 0 and confirmed_activation_7d = 1
group by box_rank, box_type, premiums, value_segment, largest_interval_7d

commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R02.2: Complete! (7 day charts - PANEL 11)'
commit

/****************** R03: SOPHISTICATED RELATIONS - 30 DAYS ******************/

select largest_interval_30d, distinct_days_30d, count(1) as box_count
into vespa_Dialback_07_fancy_pivot_a_30d
from vespa_Dialback_box_listing
where confirmed_activation_30d = 1
group by largest_interval_30d, distinct_days_30d

select interval_count_30d, total_logs_30d, count(1) as box_count
into vespa_Dialback_09_fancy_pivot_b_30d
from vespa_Dialback_box_listing
where confirmed_activation_30d = 1
group by interval_count_30d, total_logs_30d

commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R03.1: Complete! (fancy 30d pivots)'
commit

--SAME AGAIN FOR BB DP 11

select largest_interval_30d, distinct_days_30d, count(1) as box_count
into vespa_Dialback_07_fancy_pivot_a_30d_BB
from vespa_Dialback_box_listing_BB
where confirmed_activation_30d = 1
group by largest_interval_30d, distinct_days_30d

select interval_count_30d, total_logs_30d, count(1) as box_count
into vespa_Dialback_09_fancy_pivot_b_30d_BB
from vespa_Dialback_box_listing_BB
where confirmed_activation_30d = 1
group by interval_count_30d, total_logs_30d

commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R03.2: Complete! (fancy 30d pivots - PANEL 11)'
commit

/****************** R04: SOPHISTICATED RELATIONS - 7 DAYS ******************/

select largest_interval_7d, distinct_days_7d, count(1) as box_count
into vespa_Dialback_08_fancy_pivot_a_7d
from vespa_Dialback_box_listing
where confirmed_activation_7d = 1
group by largest_interval_7d, distinct_days_7d

select interval_count_7d, total_logs_7d, count(1) as box_count
into vespa_Dialback_10_fancy_pivot_b_7d
from vespa_Dialback_box_listing
where confirmed_activation_7d = 1
group by interval_count_7d, total_logs_7d

commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R04.1: Complete! (fancy 7d pivots)'
commit

--SAME AGAIN FOR BB DP 11

select largest_interval_7d, distinct_days_7d, count(1) as box_count
into vespa_Dialback_08_fancy_pivot_a_7d_BB
from vespa_Dialback_box_listing_BB
where confirmed_activation_7d = 1
group by largest_interval_7d, distinct_days_7d

select interval_count_7d, total_logs_7d, count(1) as box_count
into vespa_Dialback_10_fancy_pivot_b_7d_BB
from vespa_Dialback_box_listing_BB
where confirmed_activation_7d = 1
group by interval_count_7d, total_logs_7d

commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R04.2: Complete! (fancy 7d pivots - PANEL 11)'
commit

/****************** R05: TRANSITION / INDEPENDENCE STUFF ******************/

-- Actually, nothing to do here, the report table was built back up in J01, sweet.

-- commit
-- EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R03: Complete! (independence)'
-- commit

/****************** R06: NON-REPORTING BOX POPULATION ******************/
-- There are additional output pulls for 12-16 but they're control totals from
-- core tables, there's no specific table built. That's why we're jumping here
-- to output table 16.

-- Okay, so rather than doing the whole populate and flag thing, the single-box
-- view is kind of well formed enough for us to just jump in an profile and
-- summarise directly... 

select sbv.PS_flag, sbv.box_type_subs, sbv.PVR,
    sbv.Box_has_anytime_plus & sbv.Account_anytime_plus as Anytime_Plus, -- they are both bit columns
    count(1) as box_count
  --  count(distinct sbv.account_number) as account_count -- this might give duplicates, maybe we don't report it 
into vespa_Dialback_16_non_reporting_boxes
from vespa_analysts.vespa_single_box_view as sbv
inner join vespa_Dialback_box_listing as dbl
on sbv.subscriber_id = dbl.subscriber_id
where dbl.total_logs_30d = 0 and confirmed_activation_30d = 1
group by sbv.PS_flag, sbv.box_type_subs, Anytime_Plus, sbv.PVR

-- Only using SBV for the profiling flags here, we could pull them all
-- back onto the dialback listing and then just group from there but w/e.

commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R06.1: Complete! (Non-reporting boxes)'
commit

--SAME AGAIN FOR BB DP 11
select sbv.PS_flag, sbv.box_type_subs, sbv.PVR,
    sbv.Box_has_anytime_plus & sbv.Account_anytime_plus as Anytime_Plus, -- they are both bit columns
    count(1) as box_count
into vespa_Dialback_16_non_reporting_boxes_BB
from vespa_analysts.vespa_single_box_view as sbv
inner join vespa_Dialback_box_listing_BB as dbl
on sbv.subscriber_id = dbl.subscriber_id
where dbl.total_logs_30d = 0 and confirmed_activation_30d = 1
group by sbv.PS_flag, sbv.box_type_subs, Anytime_Plus, sbv.PVR

-- Only using SBV for the profiling flags here, we could pull them all
-- back onto the dialback listing and then just group from there but w/e.

commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R06.2: Complete! (Non-reporting boxes - PANEL 11)'
commit

/****************** R07: HISTOGRAM OF EVENTS PER LOG ******************/

-- Not going to bother with 7 vs 30 days for this either, but stick the date in the
-- pivot.

select box_rank, box_type, premiums                     -- pivot items from box lookup
    ,log_date                                           -- Want this for profiling too I guess
    ,case when (log_event_count / 20) * 20 > 600 then 600
        else (log_event_count / 20) * 20 end as event_count_bracket
    -- It's an integer, so this rounds down to multiples of 20, and caps at 600.
    ,count(1) as log_count
into vespa_Dialback_17_events_per_log
from vespa_Dialback_box_listing as bl
inner join vespa_Dialback_log_daily_summary as lds
on bl.subscriber_id = lds.subscriber_id
group by box_rank, box_type, premiums, log_date, event_count_bracket
-- This pivot moght end up huge though; ~10 box types, 8 premiums, 3 ranks, 6 value
-- segments, 30 days, 20 event buckets... that's about a million records, if it's
-- dense. Maybe we can clip out some of the variables? Premiums and value segments
-- still relevant to people sending back events though... hhmrnnph. Okay, now without
-- value segments it's down to a 66k dump, which is fine by me.

-- Note: The total number of logs described here is a lot bigger than the total number
-- from the 30d pivot. That's because for this pivot we're not restricting to boxes
-- that were enabled 30 days ago; we're just grabbing everything (even boxes enabled
-- less than 7 days ago, which won't feature on the other chart either).

commit
create unique index fake_pk on vespa_Dialback_17_events_per_log
    (box_rank, box_type, premiums, log_date, event_count_bracket)

commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R07.1: Complete! (Events per log)'
commit

--SAME AGAIN FOR BB DP 11
select box_rank, box_type, premiums                     -- pivot items from box lookup
    ,log_date                                           -- Want this for profiling too I guess
    ,case when (log_event_count / 20) * 20 > 600 then 600
        else (log_event_count / 20) * 20 end as event_count_bracket
    -- It's an integer, so this rounds down to multiples of 20, and caps at 600.
    ,count(1) as log_count
into vespa_Dialback_17_events_per_log_BB
from vespa_Dialback_box_listing_BB as bl
inner join vespa_Dialback_log_daily_summary_BB as lds
on bl.subscriber_id = lds.subscriber_id
group by box_rank, box_type, premiums, log_date, event_count_bracket
-- This pivot moght end up huge though; ~10 box types, 8 premiums, 3 ranks, 6 value
-- segments, 30 days, 20 event buckets... that's about a million records, if it's
-- dense. Maybe we can clip out some of the variables? Premiums and value segments
-- still relevant to people sending back events though... hhmrnnph. Okay, now without
-- value segments it's down to a 66k dump, which is fine by me.

-- Note: The total number of logs described here is a lot bigger than the total number
-- from the 30d pivot. That's because for this pivot we're not restricting to boxes
-- that were enabled 30 days ago; we're just grabbing everything (even boxes enabled
-- less than 7 days ago, which won't feature on the other chart either).

commit
create unique index fake_pk on vespa_Dialback_17_events_per_log_BB
    (box_rank, box_type, premiums, log_date, event_count_bracket)

commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R07.1: Complete! (Events per log - PANEL 11)'
commit

/****************** R08: DISTRIBUTION OF TIME OF DAY LOGS SENT ******************/

-- Not sure we care about 7 day or 30 day for this one. But we will put the days on the
-- pivot, so people can futz around with those if we like. Are there other things we'd
-- want to profile by? Hmm...

select box_rank, box_type, premiums                    -- pivot items from box lookup
    ,log_date                                           -- Want this for profiling too I guess
    ,hour_received as time_of_day
    ,count(1) as log_count
into vespa_Dialback_18_time_logs_sent
from vespa_Dialback_box_listing as bl
inner join vespa_Dialback_log_daily_summary as lds
on bl.subscriber_id = lds.subscriber_id
group by box_rank, box_type, premiums, log_date, time_of_day
-- OK, there's the same size-of-resulting-pivot issue here. Hopefully it's sparse...
-- Now without value segments, 33k is a bunch of items but not too many really.

commit
create unique index fake_pk on vespa_Dialback_18_time_logs_sent
    (box_rank, box_type, premiums, log_date, time_of_day)

commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R08.1: Complete! (Time of day distribution)'
commit

--SAME AGAIN FOR BB DP 11
select box_rank, box_type, premiums                    -- pivot items from box lookup
    ,log_date                                           -- Want this for profiling too I guess
    ,hour_received as time_of_day
    ,count(1) as log_count
into vespa_Dialback_18_time_logs_sent_BB
from vespa_Dialback_box_listing_BB as bl
inner join vespa_Dialback_log_daily_summary_BB as lds
on bl.subscriber_id = lds.subscriber_id
group by box_rank, box_type, premiums, log_date, time_of_day
-- OK, there's the same size-of-resulting-pivot issue here. Hopefully it's sparse...
-- Now without value segments, 33k is a bunch of items but not too many really.

commit
create unique index fake_pk on vespa_Dialback_18_time_logs_sent_BB
    (box_rank, box_type, premiums, log_date, time_of_day)

commit

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'R08.2: Complete! (Time of day distribution - PANEL 11)'
commit

/****************** T01: PERMISSIONS! ******************/

grant select on vespa_Dialback_01_pivot_for_30d         to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_02_pivot_for_7d          to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_03_distinct_for_30d      to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_04_distinct_for_7d       to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_05_intervals_for_30d     to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_06_intervals_for_7d      to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_07_fancy_pivot_a_30d     to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_08_fancy_pivot_a_7d      to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_09_fancy_pivot_b_30d     to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_10_fancy_pivot_b_7d      to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
--grant select on vespa_Dialback_11_transition_profiling  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj
-- report pulls 12 through 15 are control totals rather than table extracts, there's no associated tables
grant select on vespa_Dialback_16_non_reporting_boxes   to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_17_events_per_log        to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_18_time_logs_sent        to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_19_box_type              to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_20_Region                to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security

commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'T01.1: Complete! (permissions)'
commit

--SAME AGAIN FOR BB DP 11
grant select on vespa_Dialback_01_pivot_for_30d_BB         to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_02_pivot_for_7d_BB          to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_03_distinct_for_30d_BB      to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_04_distinct_for_7d_BB       to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_05_intervals_for_30d_BB     to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_06_intervals_for_7d_BB      to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_07_fancy_pivot_a_30d_BB     to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_08_fancy_pivot_a_7d_BB      to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_09_fancy_pivot_b_30d_BB     to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_10_fancy_pivot_b_7d_BB      to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
--grant select on vespa_Dialback_11_transition_profiling  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj
-- report pulls 12 through 15 are control totals rather than table extracts, there's no associated tables
grant select on vespa_Dialback_16_non_reporting_boxes_BB   to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_17_events_per_log_BB        to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_18_time_logs_sent_BB        to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_19_box_type_BB              to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_Dialback_20_Region_BB                to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security


commit
EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'T01.2: Complete! (permissions - PANEL 11)'
commit

/****************** ALL DONE! ******************/

EXECUTE citeam.logger_add_event @DBR_logging_ID, 3, 'Dialback: weekly refresh complete!'
commit

end;
grant execute on Dialback_make_report to public;
-- Need the central scheduler thing to be able to call the procs. But it gets
-- run within the vespa_analytics account, so it doesn't mean that any random
-- public person can see what's in the resulting tables.


/****************** Y01: CLEAN OUT TRANSIENT TABLES ******************/
-- This guys needs to be in a different file because all the tables end
-- up in vespa_analysts, which regular users won't have permission to
-- drop afterwards.

go

if object_id('Dialback_clear_transients') is not null
   drop procedure Dialback_clear_transients;

go

create procedure Dialback_clear_transients
as
begin
    delete from vespa_analysts.vespa_Dialback_log_collection_dump
    delete from vespa_analysts.vespa_Dialback_log_daily_summary
    delete from vespa_analysts.vespa_Dialback_box_listing
    commit
    if object_id( 'vespa_analysts.vespa_Dialback_01_pivot_for_30d')        is not null
        drop table vespa_analysts.vespa_Dialback_01_pivot_for_30d
    if object_id( 'vespa_analysts.vespa_Dialback_02_pivot_for_7d')         is not null
        drop table vespa_analysts.vespa_Dialback_02_pivot_for_7d
    if object_id( 'vespa_analysts.vespa_Dialback_03_distinct_for_30d')     is not null
        drop table vespa_analysts.vespa_Dialback_03_distinct_for_30d
    if object_id( 'vespa_analysts.vespa_Dialback_04_distinct_for_7d')      is not null
        drop table vespa_analysts.vespa_Dialback_04_distinct_for_7d
    if object_id( 'vespa_analysts.vespa_Dialback_05_intervals_for_30d')    is not null
        drop table vespa_analysts.vespa_Dialback_05_intervals_for_30d
    if object_id( 'vespa_analysts.vespa_Dialback_06_intervals_for_7d')     is not null
        drop table vespa_analysts.vespa_Dialback_06_intervals_for_7d
    if object_id( 'vespa_analysts.vespa_Dialback_07_fancy_pivot_a_30d')    is not null
        drop table vespa_analysts.vespa_Dialback_07_fancy_pivot_a_30d
    if object_id( 'vespa_analysts.vespa_Dialback_08_fancy_pivot_a_7d')     is not null
        drop table vespa_analysts.vespa_Dialback_08_fancy_pivot_a_7d
    if object_id( 'vespa_analysts.vespa_Dialback_09_fancy_pivot_b_30d')    is not null
        drop table vespa_analysts.vespa_Dialback_09_fancy_pivot_b_30d
    if object_id( 'vespa_analysts.vespa_Dialback_10_fancy_pivot_b_7d')     is not null
        drop table vespa_analysts.vespa_Dialback_10_fancy_pivot_b_7d
    if object_id( 'vespa_analysts.vespa_Dialback_11_transition_profiling') is not null
        drop table vespa_analysts.vespa_Dialback_11_transition_profiling
    -- No tables exist for extracts 12 through 15, they're just control totals that come from other tables
    if object_id( 'vespa_analysts.vespa_Dialback_16_non_reporting_boxes')  is not null
        drop table vespa_analysts.vespa_Dialback_16_non_reporting_boxes
    if object_id( 'vespa_analysts.vespa_Dialback_17_events_per_log')       is not null
        drop table vespa_analysts.vespa_Dialback_17_events_per_log
    if object_id( 'vespa_analysts.vespa_Dialback_18_time_logs_sent')       is not null
        drop table vespa_analysts.vespa_Dialback_18_time_logs_sent        

    commit
end;

go

grant execute on Dialback_clear_transients to public;

--SAME AGAIN FOR BB DP 11
go

if object_id('Dialback_clear_transients_BB') is not null
   drop procedure Dialback_clear_transients_BB;

go

create procedure Dialback_clear_transients_BB
as
begin
    delete from vespa_analysts.vespa_Dialback_log_daily_summary_BB
    delete from vespa_analysts.vespa_Dialback_box_listing_BB
    commit
    if object_id( 'vespa_analysts.vespa_Dialback_01_pivot_for_30d_BB')        is not null
        drop table vespa_analysts.vespa_Dialback_01_pivot_for_30d_BB
    if object_id( 'vespa_analysts.vespa_Dialback_02_pivot_for_7d_BB')         is not null
        drop table vespa_analysts.vespa_Dialback_02_pivot_for_7d_BB
    if object_id( 'vespa_analysts.vespa_Dialback_03_distinct_for_30d_BB')     is not null
        drop table vespa_analysts.vespa_Dialback_03_distinct_for_30d_BB
    if object_id( 'vespa_analysts.vespa_Dialback_04_distinct_for_7d_BB')      is not null
        drop table vespa_analysts.vespa_Dialback_04_distinct_for_7d_BB
    if object_id( 'vespa_analysts.vespa_Dialback_05_intervals_for_30d_BB')    is not null
        drop table vespa_analysts.vespa_Dialback_05_intervals_for_30d_BB
    if object_id( 'vespa_analysts.vespa_Dialback_06_intervals_for_7d_BB')     is not null
        drop table vespa_analysts.vespa_Dialback_06_intervals_for_7d_BB
    if object_id( 'vespa_analysts.vespa_Dialback_07_fancy_pivot_a_30d_BB')    is not null
        drop table vespa_analysts.vespa_Dialback_07_fancy_pivot_a_30d_BB
    if object_id( 'vespa_analysts.vespa_Dialback_08_fancy_pivot_a_7d_BB')     is not null
        drop table vespa_analysts.vespa_Dialback_08_fancy_pivot_a_7d_BB
    if object_id( 'vespa_analysts.vespa_Dialback_09_fancy_pivot_b_30d_BB')    is not null
        drop table vespa_analysts.vespa_Dialback_09_fancy_pivot_b_30d_BB
    if object_id( 'vespa_analysts.vespa_Dialback_10_fancy_pivot_b_7d_BB')     is not null
        drop table vespa_analysts.vespa_Dialback_10_fancy_pivot_b_7d_BB
    if object_id( 'vespa_analysts.vespa_Dialback_11_transition_profiling_BB') is not null
        drop table vespa_analysts.vespa_Dialback_11_transition_profiling_BB
    -- No tables exist for extracts 12 through 15, they're just control totals that come from other tables
    if object_id( 'vespa_analysts.vespa_Dialback_16_non_reporting_boxes_BB')  is not null
        drop table vespa_analysts.vespa_Dialback_16_non_reporting_boxes_BB
    if object_id( 'vespa_analysts.vespa_Dialback_17_events_per_log_BB')       is not null
        drop table vespa_analysts.vespa_Dialback_17_events_per_log_BB
    if object_id( 'vespa_analysts.vespa_Dialback_18_time_logs_sent_BB')       is not null
        drop table vespa_analysts.vespa_Dialback_18_time_logs_sent_BB

    commit
end;

go

grant execute on Dialback_clear_transients_BB to public;

