/******************************************************************************
**
**  VO53 - low power directive
**
** This analysis will look at when people are presently turning their boxes on
** during the night / early mroning in order to assist with the Sky Low Power
** programme.
**
** a/ boxes coming out of standby (there is no interest presently in when they went into standby) and also turning on
** b/ break down into number and proportions that come out of standby for 10 minute slots from 2am to 7am
** c/ need outputs broken down by box type - primary interest in DRX890/895/595, but HD/ non-HD would suffice if former not possible
** d/ breakdown by the days of the week showing total numbers / averages for weekday/ weekend if this makes sense
** Definitions used:
** i boxes coming out of standby are currently being by event StanbyOff.
** ii/ boxes turning on will be those for which the first event in the STB SSP log is a tune to the Sky Welcome Channel (EPG 998) 
**
** Requirements nabbed from IC brief:
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=53
**
** Still heavily in dev.
**      Part A:        Messing about
**              A01 -  Where are the standby events?
**              A02 -  How to figure the PowerOn type events
**              A03 -  Identifying the box model details
**
**      Part B:        Preperation
**              B01 -  Building a table of box details
**              B02 -  Results structures
**
**      Part C:        Looping over daily tables...
**
******************************************************************************/

/****************** A01: STANDBY EVENTS... ARE THEY WHERE THEY SHOULD BE? ******************/

-- So there's nothing of any long term importance in any of the section A areas, other than
-- making sure we get the filter values right.

-- First off, do we see the events we want?
select top 10 *
from sk_prod.VESPA_STB_PROG_EVENTS_20120403
where event_type = 'evStandbyOut'

-- So, yes.

/****************** A02: POWER-ON EVENTS... HOW TO FIND THEM ETC ******************/

-- Okay, so, the standby out is one kind of event and then to detect power-ons we need to
-- connect back to the daily table to find viewing events at the same time on a particular
-- channel... only that the channel is on the EPG table and not the daly? Oh no, we do
-- have x_channel_name on the daily table.

select top 10 * from sk_prod.vespa_epg_dim;
-- does the 998 refer to epg_group?

select distinct epg_channel, epg_group
from sk_prod.vespa_epg_dim
where epg_channel = 'Sky Welcome  (998)'
-- it's identified by epg_channel = 'Sky Welcome  (998)',
-- and the epg_group has nothing to do with it.

select count(1) from sk_prod.VESPA_STB_PROG_EVENTS_20120403
where x_channel_name = 'Sky Welcome  (998)';
-- nothing... so that's a kick in the balls.

select x_channel_name, count(1)
from sk_prod.VESPA_STB_PROG_EVENTS_20120403
where lower(x_channel_name) like '%welcome%'
or lower(x_channel_name) like '%998%'
group by x_channel_name;
-- Nothing... so... awesome?

select top 10 *
from sk_prod.vespa_epg_dim
where epg_channel = 'Sky Welcome  (998)'
and tx_date = '20120403'
-- A bunch of 3 and 4 hour intro things.

select programme_trans_sk
into #welcome_items
from sk_prod.vespa_epg_dim
where epg_channel = 'Sky Welcome  (998)'
and tx_date = '20120403';
commit;
create unique index fake_pk on #welcome_items (programme_trans_sk);
commit;

select x_channel_name, count(1) as hits
from sk_prod.VESPA_STB_PROG_EVENTS_20120403 as ev
inner join #welcome_items as wi
on ev.programme_trans_sk = wi.programme_trans_sk
group by x_channel_name;
-- Okay, so on the daily tables the channel is identified
-- as x_channel_name = 'Sky Intro', but is that all of them?

select count(1) as hits
from sk_prod.VESPA_STB_PROG_EVENTS_20120403
where x_channel_name = 'Sky Intro';
-- But there are other items in here too... timeshifting of
-- the intro channel? weirdly placed events in a different
-- daily table? No idea. Oh well.

select subscriber_id, count(1) as hits
from sk_prod.VESPA_STB_PROG_EVENTS_20120403
where event_type = 'evStandbyOut'
group by subscriber_id
having hits > 1
order by hits desc;
-- Some have upwards of hundreds of events, but we're not going to work around
-- that, just add caveats.

select distinct x_channel_name
from sk_prod.VESPA_STB_PROG_EVENTS_20120403
where event_type = 'evStandbyOut';
-- yeah, all null, gong to have to figure out how that self-join should work.

select max(x_event_duration)
from sk_prod.VESPA_STB_PROG_EVENTS_20120403
where event_type = 'evStandbyOut';
-- 943670... wtf is the meaning of a 10 day coming-out-of-standby event?
-- I'm just going to ignore it and try to find the first viewing event
-- following each out-of-standby thing.

drop table #standbyouts;

-- OK, so processing to pull out all the out-of-standby events:
select subscriber_id
        ,adjusted_event_start_time
--        ,x_event_duration - were going to use this to check that the first view happened sometime within this startup event, but it gives us duplicates so we won't
        ,convert(date,adjusted_event_start_time) as on_date
        ,datepart(hour,adjusted_event_start_time) as on_hour
        ,datepart(minute,adjusted_event_start_time) as on_minute
        ,convert(bit, 0) as powered_on
into #standbyouts
from sk_prod.VESPA_STB_PROG_EVENTS_20120403
where event_type = 'evStandbyOut'
and on_hour between 2 and 7
-- OK, we've had duplicates turn up which makes no sense but we'll
-- group them out... except that it's the durations that are causing
-- us problems, we have some subscriber ids with out-of-standby events
-- starting at the same time and of different durations. So we just
-- won't bother validating the viewing starts against the duration,
-- because there are data issues in the way that make no sense.
group by subscriber_id, adjusted_event_start_time; --, x_event_duration;

commit;
create unique index fake_pk on #standbyouts (subscriber_id, adjusted_event_start_time);
-- dupes....  wtf are we doing with the same box having multiple events starting
-- at the same time? oh well,
commit;

-- Now attempt to find the first playback event after each standby:
select ev.subscriber_id
        ,ev.x_channel_name
        ,sbo.adjusted_event_start_time as start_up_time
        ,ev.adjusted_event_start_time as viewing_start_time
        ,rank() over (partition by ev.subscriber_id order by ev.adjusted_event_start_time) as view_rank
into #initial_channels
from sk_prod.VESPA_STB_PROG_EVENTS_20120403 as ev
inner join #standbyouts as sbo
on ev.subscriber_id = sbo.subscriber_id
and ev.adjusted_event_start_time >= sbo.adjusted_event_start_time;

delete from #initial_channels
where view_rank > 1
or x_channel_name <> 'Sky Intro'
or x_channel_name is null;
commit;
create unique index fake_pk on #initial_channels (subscriber_id,start_up_time); -- fails!
-- wtf.. duplicates here too? that makes no sense at all. You know what, I
-- don't care, as long as the first view is the intro channel they count as
-- a power on rather than an a out-of-standby.
create index fake_pk on #initial_channels (subscriber_id,start_up_time);

-- OK, push those marks back onto the out-of-standby events
update #standbyouts
set powered_on = 1
from #standbyouts as sbo
inner join #initial_channels as ic
on sbo.adjusted_event_start_time = ic.start_up_time;

commit;

/****************** A03: FIGURING OUT WHAT DRX895 IS ABOUT ******************/

-- So now all we need is that box the flag... don't even know what column or
-- which table we're supposed to find "DRX890/895/595"...

select top 10 * from sk_prod.cust_set_top_box
-- Okay, it's probably x_model_number, and there are spaces in it -
select distinct x_model_number from sk_prod.cust_set_top_box
where lower(x_model_number) like 'drx%'
/*
'DRX 890'
'DRX 895'
'DRX 595'
*/

/****************** B01: BOX DETAIL LOOKUP ******************/

-- So we'll profile at the same point that SBV builds from so we can use the shortcuts
-- that it provides...
create variable @profiling_day date;
set @profiling_day = '2012-04-19';

drop table #deduped_list;
-- Okay, copied out of the SBV refresh code, the join into sk_prod.cust_set_top_box
-- goes something like this:
select  b.service_instance_id
        ,b.x_model_number
        ,case when b.active_box_flag = 'Y' then 1 else 0 end as active_box
        ,case when b.box_installed_dt <= @profiling_day and b.box_replaced_dt > @profiling_day then 1 else 0 end as apparently_active
        ,rank() over (partition by b.service_instance_id order by active_box desc, apparently_active desc, b.box_installed_dt desc, b.cb_row_id desc) as rankage
into #deduped_list
from sk_prod.cust_set_top_box as b
inner join vespa_analysts.vespa_single_box_view as vsd
on vsd.service_instance_id=b.service_instance_id
where panel = 'VESPA';

commit;
delete from #deduped_list where rankage > 1;
commit;
create unique index src_index on #deduped_list (service_instance_id);
commit;

drop table V053_box_detail_lookup;
select  sbv.subscriber_id
        ,sbv.HD_box_subs
        ,case
                when dl.x_model_number in ('DRX 890', 'DRX 895', 'DRX 595') then x_model_number
                when lower(dl.x_model_number) like 'drx%' then 'Other DRX'
                when dl.x_model_number is NULL then 'Box model not known'
                else 'Other box model'
            end as box_model
into V053_box_detail_lookup
from vespa_analysts.vespa_single_box_view as sbv
left join #deduped_list as dl
on sbv.service_instance_id = dl.service_instance_id
where panel = 'VESPA';
-- So the specifies DRX models are actually not specifically SD? They're split
-- over whether or not the box has an attached HD subscription. So we'll just
-- report and group on both flags. But as far as establishing marks, it's good.
create unique index fake_pk on V053_box_detail_lookup (subscriber_id);

-- Okay, now with the lookup built we can start processing into some loops!

/****************** B02: RESULTS STRUCTURES ******************/

-- clear out any persistent gunk...
if object_id('V053_leaving_standby_summary') is not null
   drop table V053_leaving_standby_summary;
if object_id('V053_standby_exits') is not null
   drop table V053_standby_exits;
if object_id('V053_initial_channels') is not null
   drop table V053_initial_channels;
if object_id('V053_dynamic_panel') is not null
   drop table V053_dynamic_panel;
   

-- The structure into which all results will end up:
create table V053_leaving_standby_summary (
    scanning_day            date
    ,wake_period            datetime        -- the start of the 10 minute intervals of analysis
    ,grouped_wake_period    time            -- like wake_period but with the date part removed
    ,is_weekend             bit             -- 1 for weekends, 0 for weekdays
    ,box_model              varchar(30)
    ,HD_subscription        bit
    ,is_power_on            bit             -- 1 for power on, 0 for just coming out of standby
    ,waking_boxes           int
);

commit;

-- We also are going to need two tables in which to rest the wakeup events and the initial
-- channel flags:
create table V053_standby_exits (
    subscriber_id                       bigint
    ,adjusted_event_start_time          datetime
    ,wake_period                        datetime
    ,powered_on                         bit default 0
    ,primary key (subscriber_id, adjusted_event_start_time)
);

create table V053_initial_channels (
    subscriber_id                       bigint
    ,start_up_time                      datetime
    ,x_channel_name                     varchar(30)
    ,view_rank                          int
);

-- not unique because of the noted dupes issue
create index for_joins on V053_initial_channels (subscriber_id, start_up_time);
commit;

-- We're also tracking effective panel size and breakdown by day:
create table V053_dynamic_panel (
    scanning_day                        date
    ,box_model                          varchar(30)
    ,HD_box_subs                        bit
    ,panel_boxes                        int
);

commit;

/****************** B03: REQUIRED DYNAMIC SQL ******************/

-- To loop over the daily tables; we'll need two, one for wakeup events and one for the
-- initial channels of the power-on flags:
create variable @wakeup_SQL_hurg    varchar(2000);
create variable @init_chan_SQL_hurg varchar(2000);
create variable @effective_panel_SQL varchar(2000);

set @wakeup_SQL_hurg = 'insert into V053_standby_exits (subscriber_id, adjusted_event_start_time, wake_period)
select subscriber_id
        ,adjusted_event_start_time
        ,dateadd(minute,
            (datepart(hour,adjusted_event_start_time) * 60)                 -- add the hours back on
                + (10 * (datepart(minute,adjusted_event_start_time) / 10))  -- and minutes, rounded down to the nearest 10
            ,convert(datetime,convert(date,adjusted_event_start_time)))     -- add those on to the truncated date
from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*##
where event_type = ''evStandbyOut''
and datepart(hour,adjusted_event_start_time) between 2 and 6
group by subscriber_id, adjusted_event_start_time';

set @init_chan_SQL_hurg = 'insert into V053_initial_channels
select ev.subscriber_id
        ,sbo.adjusted_event_start_time
        ,ev.x_channel_name
        ,rank() over (partition by ev.subscriber_id order by ev.adjusted_event_start_time)
from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as ev
inner join V053_standby_exits as sbo
on ev.subscriber_id = sbo.subscriber_id
and ev.adjusted_event_start_time >= sbo.adjusted_event_start_time';

set @effective_panel_SQL = 'insert into V053_dynamic_panel
select ''##^&$&^##''
    ,bdl.box_model
    ,bdl.HD_box_subs
    ,count(distinct bdl.subscriber_id)
from V053_box_detail_lookup as bdl
inner join sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as ev
on ev.subscriber_id = bdl.subscriber_id
group by bdl.box_model, bdl.HD_box_subs
';

commit;

/****************** C01: LOOPING! OVER DAILY TABLES ******************/

create variable @scanning_day date;

-- just hardcoding the period start and end, we don't care so much
delete from V053_leaving_standby_summary;
delete from V053_dynamic_panel;
set @scanning_day = '2012-04-13';
while @scanning_day <= '2012-04-19'
begin

    -- Reset holding tables:
    delete from V053_standby_exits
    delete from V053_initial_channels

    -- First pull out standby items:
    EXECUTE(replace(@wakeup_SQL_hurg,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit
    
    -- Then sort out the first channel watched after each wake:
    EXECUTE(replace(@init_chan_SQL_hurg,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit

    -- Completing the power on flagging:
    delete from V053_initial_channels
    where view_rank > 1
    or x_channel_name <> 'Sky Intro'
    or x_channel_name is null

    -- OK, push those marks back onto the out-of-standby events
    update V053_standby_exits
    set powered_on = 1
    from V053_standby_exits as sbo
    inner join V053_initial_channels as ic
    on sbo.subscriber_id = ic.subscriber_id
    and sbo.adjusted_event_start_time = ic.start_up_time
    
    commit
    
    -- Summarising into result sets:
    insert into V053_leaving_standby_summary
    select
        @scanning_day
        ,se.wake_period
        ,convert(time, se.wake_period)
        ,0                  -- we'll patch this in later
        ,bl.box_model
        ,bl.HD_box_subs
        ,se.powered_on
        ,count(1)
    from V053_standby_exits as se
    inner join V053_box_detail_lookup as bl
    on se.subscriber_id = bl.subscriber_id
    group by se.wake_period, bl.box_model, bl.HD_box_subs, se.powered_on

    -- Then getting effective panel size for each division of boxes we care about
    EXECUTE(replace(
            replace(@effective_panel_SQL,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd'))
            ,'##^&$&^##'
            ,dateformat(@scanning_day, 'yyyy-mm-dd')
            ))
    commit
    
    -- moving on!
    set @scanning_day = dateadd(day, 1, @scanning_day)
    commit
end;

-- ^^ - huh, this guy had some kind of type casting bug with regards to the time field.
-- Goin to have to run through it slowly tommorrow.

drop table V053_standby_exits;
drop table V053_initial_channels;

-- Oh, hey, we need to add that weekends flag too:
update V053_leaving_standby_summary
set is_weekend = case
    when datepart(weekday, wake_period) in (1,7) then 1 -- these datepart components indicate weekends
    else 0
end;

commit;

/****************** D01: QA ON WHAT HAS BEEN BUILT ******************/

select count(1) as records
    ,count(distinct scanning_day) as scanned_days -- we expect 7
    ,count(distinct wake_period) as boundary_starts -- we expect... 7 (for days) * 5 (for hours) * 6 (for bands per hour) -> at most 210 distinct interval start ponts
    ,count(distinct grouped_wake_period) as grouped_starts -- we expect... 30 of these guys
from V053_leaving_standby_summary;
/* So these numbers generally look good...
1720	7	210 30
*/

-- Hey, we could also validate effective panel size against the scaling build of the time...
-- the total only though, because only the scaling 1 build is available right now. But the
-- totals of reporting boxes should still end up close.

select top 30 * from V053_leaving_standby_summary;
-- Seems to be what we need, sure.

select count(1) as hits
    ,count(distinct box_model) as box_models
    ,count(distinct scanning_day) as source_days
from V053_dynamic_panel
;
/* yeah, that's what we expected
hits	box_models	source_days
84	6	7
*/

/****************** E01: RESULTS EXTRACTION ******************/

select * from V053_leaving_standby_summary;
select * from V053_dynamic_panel;
-- Then messing around with pivots in excel
