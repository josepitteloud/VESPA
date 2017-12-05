/******************************************************************************
**
** Project Vespa: Operational Dashboard Report
**                  - Historical rebuild of P/S & Anytime+ data
**
** need to get:
** 1.) Anytime+ at a point in time
** 2.) P/S box at a point in time
** 3.) Logs reporting over that period
** 4.) Cumulative enablements over that period
**
**
** Okay, so a process we might be able to do:
**  A/ Each week, pull the panel 4 activations out of campaign cells (open)
**  B/ 
**  C/ Form intersection
** But there's still a lot of panel 5 cases that haven't been disabled yet.
** Call blight wil probably show no decrease in enablement, because that was
** not logged into the DB. Overall, dunno, cutting and pasting the historical
** data and leaving it in the template might be the easiest way to go.
**
******************************************************************************/

-- ok, so assuming that we're looking at on day:
create variable @end_of_cycle date;
set @end_of_cycle = '2012-01-14';
-- which represents the end of a 7 day cycle. Then for consistency we want the
-- last Thursday before that date to do all the profiling with:
create variable @profiling_thursday date;
select @profiling_thursday = min(calendar_date)
from sk_prod.sky_calendar
where subs_last_day_of_week = 'Y'
and calendar_date > @end_of_cycle;

/****************** COLLECT RAW LOGS ******************/

-- Next: assemble the log listing. The Operational Dashboard uses the silly annoying
-- document dates from 9AM so we have to go a bit either side, but that's okay, we
-- can still just copy the code.

create variable @SQL_daily_kludge       varchar(2000);
create variable @scanning_day           date;


create table Vespa_HackDash_log_collection_dump (
        subscriber_id                   decimal(8)      not null
        ,account_number                 varchar(20)     not null
        ,stb_log_creation_date          datetime        not null
        ,doc_creation_date_from_9am     date            not null
);

-- This guy couldn't be a parameterised query anyway, since we're changing the
-- source table on each loop iteration
set @SQL_daily_kludge = 'insert into Vespa_HackDash_log_collection_dump (
        subscriber_id
        ,account_number
        ,stb_log_creation_date
        ,doc_creation_date_from_9am
)
select
        subscriber_id
        ,min(account_number) as account_number
        ,stb_log_creation_date
        ,case when dateformat(min(document_creation_date),''hh'') in (''00'',''01'',''02'',''03'',''04'',''05'',''06'',''07'',''08'')
                then cast(min(document_creation_date) as date)-1 end as doc_creation_date_from_9am
from sk_prod.VESPA_STB_PROG_EVENTS_#*££*# -- will get replaced by the daily stamp of each table
where panel_id in (4,5)
group by subscriber_id, stb_log_creation_date
having doc_creation_date_from_9am is not null
';

set @scanning_day = dateadd(day, -7, @end_of_cycle);
commit;

while @scanning_day <= @end_of_cycle + 1
begin

    execute(replace(@SQL_daily_kludge, '#*££*#', dateformat(@scanning_day,'yyyymmdd')))
    -- Move on to the next daily table
    set @scanning_day = dateadd(day, 1, @scanning_day)
    
    commit
end;

/****************** PROCESS LOGS INTO ACOUNT LISTINGS ******************/

-- Wait, in this case we're only interested in counts of boxes, so we don't
-- eed to care about account ID! awesome!

delete from Vespa_HackDash_log_collection_dump
where not datediff(day, doc_creation_date_from_9am, @end_of_cycle) between 0 and 6;
commit;

select
    subscriber_id as subscriber_id
    ,count(distinct stb_log_creation_date) as logs_returned
    ,min(account_number) as account_number
    ,doc_creation_date_from_9am as doc_creation_date_from_9am
    ,convert(varchar(1), 'U') as PS_flag
    ,convert(bit,0) as account_has_anytime
    ,convert(bit,0) as box_has_anytime
into vespa_HackDash_box_returns
from Vespa_HackDash_log_collection_dump
group by subscriber_id, doc_creation_date_from_9am;
-- Oh hey so this incidentally is why we're counting log fragment duplicates, we're
-- counting each log fragment for the number of days it puts things over, but not
-- really super interested in fixing that all up now. Entails major rebuild.

commit;
create unique   index fake_pk               on vespa_HackDash_box_returns (subscriber_id, doc_creation_date_from_9am);
create          index account_number_index  on vespa_HackDash_box_returns (account_number);
commit;

-- ok, so now we need to assemble the Anytime+ and P/S flags for these boxes,
-- and being historical, we can't just use the SBV :-/ but we can copy the code!

/****************** OKAY, WAIT, THE BOX LOOKUP ******************/

-- We need P/S flags on the whole base, not just on those returning data. So now we need
-- to get a view of boxes that were enabled... on each day we're processing... and all we
-- have are records of enablementor disablement...

-- OK, so process:
-- 1/ get all boxes enabled before the start of our profiling period; keep the most recent enablement
-- 2/ remove from the list those with disablements before our reporting period but after the enablement of 1
-- So now we've got our initial population.
-- 3/ Get the enablements and disablements that happen during our week, and track the counts.
-- Thing is, we don't care about which boxes were enabled / disabled on any day, we just want the counts.
-- 4/ Build a seperate list of boxes enabled before the end of the reporting period and use this for the profiling.

-- ahahaha except there's no panel_ID on the enablements table. Okay, so we also have to identify and
-- exclude sky view panel people, which we do via... [sk_prod].[VESPA_SKY_VIEW_PANEL] ... which only has
-- account numbers, so we'll need those from the enablement history too. *sigh* stil a bit of a mission
-- though.

-- wait, we also need to know whether the subscriber table overlaps with or augments the subscriber
-- history table.... oh hey it doesn't overlap, it augments. Which means we have to scan both at all
-- times. The most recent record is in the status table, yes, but we want the historical thing too...

-- Wait, the most recent activation is the 24th of november. This means that we'll be able to just
-- pull out the disablements, and not worry about the historical reconstruction so much. Are we going
-- to mark the disables too? Well, we'd still need to pull stuff out of the history for people that
-- have since disabled... not sure it makes it to much easier to be honest. Actually, given the current
-- sizes (combined row count < 2m), it's not going to be hard to throw the two tables together and
-- proceed as if there were only one.

-- Okay, so step 1, identify all enablements before the profiling week starts.
select
    account_number
    ,convert(int, card_subscriber_id) as subscriber_id
    ,request_dt
    ,result
    ,convert(bit,0) as is_sky_view
into #all_enables_disables
from sk_prod.vESPA_SUBSCRIBER_STATUS
where result in ('Enabled', 'Disabled');
-- Only enabled and disabled lead to changes of status? There's pending and failed, but only Trumped
-- might actually have any kind of deal, 

insert into #all_enables_disables
select
    account_number
    ,convert(int, card_subscriber_id) as subscriber_id
    ,request_dt
    ,result
    ,convert(bit,0) as is_sky_view
from sk_prod.vESPA_SUBSCRIBER_STATUS_HIST
where result in ('Enabled', 'Disabled');

commit;
create index account_number_index on #all_enables_disables (account_number);
create index subscriber_id_index  on #all_enables_disables (subscriber_id);
commit;

-- OK, now with them all together, it's easiest to exclude the Sky View panel here:
update #all_enables_disables
set is_sky_view = 1
from #all_enables_disables as aed
inner join sk_prod.vespa_sky_view_panel as svp
on aed.account_number = svp.account_number;

delete from #all_enables_disables where is_sky_view = 1;
commit;
-- 0 rows affected! so, yeah, there's no Sky Panel in it then *sheepish*

-- New plan: 1/ get the most recent enablement of all boxes before the reporting period ends
-- 2/ Attach to each box the first disablement flag after the enablement
-- We'll run into issues of people disabling and then reenabling within the same week, but
-- we're okay with that, that population won't be large.

-- Okay, so, step 1, get most recent enables:
select subscriber_id
    ,min(account_number) as account_number
    ,max(request_dt) as enabled_date
    ,convert(date, null) as disable_date
into vespa_HackDash_box_lookup
from #all_enables_disables
where request_dt <= @end_of_cycle
and result = 'Enabled'
group by subscriber_id;

commit;
create unique index fake_pk on vespa_HackDash_box_lookup (subscriber_id);
commit;

-- Step 2 - get first disable after the above enablement
select
    bei.subscriber_id
    ,min(request_dt) as disable_date
into #relevant_disables
from vespa_HackDash_box_lookup as bei
inner join #all_enables_disables as aed
on bei.subscriber_id = aed.subscriber_id
and aed.result = 'Disabled'
and bei.enabled_date < aed.request_dt
group by bei.subscriber_id;

commit;
create unique index fake_pk on #relevant_disables (subscriber_id);
commit;

update vespa_HackDash_box_lookup
set disable_date = rd.disable_date
from vespa_HackDash_box_lookup as bei
inner join #relevant_disables as rd
on bei.subscriber_id = rd.subscriber_id;

-- okay, now kick out the things that were disabled before our reporting period
-- started, and that's our master list of accounts and boxes too :D

delete from vespa_HackDash_box_lookup
where datediff(day, disable_date, @end_of_cycle) > 6;

-- Sweet!

commit;

-- okay, so in preperation for the profiling, add the columns we'll need and indices and things:
create index account_number_index on vespa_HackDash_box_lookup (account_number);
alter table vespa_HackDash_box_lookup add
    (PS_flag                varchar(1) default 'U'
    ,account_has_anytime    bit default 0
    ,box_has_anytime        bit default 0);
commit;

/****************** THE ANYTIME+ FLAGS ******************/

-- First account capability
update vespa_HackDash_box_lookup a
set account_has_anytime = 1
from sk_prod.cust_subs_hist    b
where a.account_number = b.account_number
and subscription_sub_type = 'PDL subscriptions'
AND    status_code = 'AC'
AND   @profiling_thursday between effective_from_dt and effective_to_dt;

-- And now the same for the Anytime+ box version...
select convert(bigint, card_subscriber_id) as subscriber_id
into #vespa_card_anytime_plus
from sk_prod.cust_card_subscriber_link    c
inner join sk_prod.cust_set_top_box d
on c.service_instance_id = d.service_instance_id
where active_box_flag = 'Y'
and d.x_anytime_plus_enabled = 'Y';

commit;
create index subscriber_id_index on #vespa_card_anytime_plus (subscriber_id);
commit;

update vespa_HackDash_box_lookup
set Box_has_anytime = case when cap.subscriber_id is null then 0 else 1 end
from vespa_HackDash_box_lookup
left join #vespa_card_anytime_plus as cap
on vespa_HackDash_box_lookup.subscriber_id = cap.subscriber_id;

commit;

/****************** THE PRIMARY / SECONDARY FLAGS ******************/

-- First from Olive..... actually, there is only Olive, because Vespa doesn't have the
-- historical view on tables that we need to make this work.

/*
select
    service_instance_id
    ,convert(integer,min(si_external_identifier)) as subscriber_id -- should be unique per service instance ID?
    ,convert(bit, max(case when si_service_instance_type = 'Primary DTV' then 1 else 0 end)) as primary_box
    ,convert(bit, max(case when si_service_instance_type = 'Secondary DTV (extra digiboxes)' then 1 else 0 end)) as secondary_box
into #subscriber_details
from sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
and @profiling_thursday between effective_from_dt and effective_to_dt
group by service_instance_id; */
-- Okay, so that other guy died a slow death... rebuilding...

select
    convert(integer, si_external_identifier) as subscriber_id
into #primary_box_marks
from sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type = 'Primary DTV'
and @profiling_thursday between effective_from_dt and effective_to_dt;
commit;

create index for_joining on #primary_box_marks (subscriber_id);

select
    convert(integer, si_external_identifier) as subscriber_id
into #secondary_box_marks
from sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type = 'Secondary DTV (extra digiboxes)'
and @profiling_thursday between effective_from_dt and effective_to_dt;
commit;

create index for_joining on #secondary_box_marks (subscriber_id);

commit;

update vespa_HackDash_box_lookup set PS_flag = 'U';

update vespa_HackDash_box_lookup
set PS_flag = 'P'
from vespa_HackDash_box_lookup as bl
inner join #primary_box_marks as pbm
on bl.subscriber_id = pbm.subscriber_id;

commit;

update vespa_HackDash_box_lookup
set PS_flag = case when PS_flag = 'P' then '?' else 'S' end
from vespa_HackDash_box_lookup as bl
inner join #secondary_box_marks as pbm
on bl.subscriber_id = pbm.subscriber_id;

commit;
drop table #primary_box_marks;
drop table #secondary_box_marks;
commit;

/****************** OK, SO NOW PUSH THE MARKS FROM THE LOOKUP ONTO THE COLLECTED LOGS ******************/

update vespa_HackDash_box_returns
set 
    PS_flag                 = bl.PS_flag
    ,account_has_anytime    = bl.account_has_anytime
    ,box_has_anytime        = bl.box_has_anytime
from vespa_HackDash_box_returns as br
inner join vespa_HackDash_box_lookup as bl
on br.subscriber_id = bl.subscriber_id;

commit;

-- Well that was easy

/****************** MAKING THE SUMMARIES ******************/

-- Okay, and now stitch these guys back onto the histrical table we have...
-- Wait, if we're doing a historical reset, then we should trim it back to panel
-- 4 and just show what's gone on since the call blight? We're not reporting on
-- 5 now, but we'd have to do the historical stuff... Maybe. Dunno. That'd invovle
-- resetting a lot of the Operational Dashboard, which we're kind of going to do
-- anyway for the Sky Panel rebuild....

-- Okay, because we've already taken it down to distinct boxes, we don't even need
-- to loop, just group!

drop table Vespa_Hackdash_reporting_totals;
commit;

select doc_creation_date_from_9am
    ,sum(logs_returned) as Logs
    ,count(distinct account_number) as distinct_accounts
    ,count(1) as distinct_boxes
    ,sum(case when PS_flag = 'P' then 1 else 0 end) as reporting_primary_boxes
    ,sum(case when PS_flag = 'S' then 1 else 0 end) as reporting_secondary_boxes
    ,sum(case when PS_flag = 'P' and box_has_anytime = 1 and account_has_anytime = 1 then 1 else 0 end) as reporting_primary_anytimes
    ,sum(case when PS_flag = 'S' and box_has_anytime = 1 and account_has_anytime = 1 then 1 else 0 end) as reporting_secondary_anytimes
into Vespa_Hackdash_reporting_totals
from vespa_HackDash_box_returns
group by doc_creation_date_from_9am;
commit;

select calendar_day
into #vespa_hackdash_reporting_days
from sk_prod.sky_calendar
where datediff(day, calendar_day, @end_of_cycle) between 0 and 6;
-- This version now also robust against days with zero log returns

select calendar_date
    ,count(1) as enabled_boxes
    ,sum(case when PS_flag = 'P' then 1 else 0 end) as enabled_primary_boxes
    ,sum(case when PS_flag = 'S' then 1 else 0 end) as enabled_secondary_boxes
from #vespa_hackdash_reporting_days as rd
inner join vespa_HackDash_box_lookup as bl
on (rd.calendar_date between bl.enabled_date and bl.disable_date)
or (rd.calendar_date > bl.enabled_date and bl.disable_date is null)
group by rd.calendar_date
order by rd.calendar_date;

commit;
-- Okay, this is still giving us too many hits. We're going to narrow it to just the panel 4 things...

select count(1) from stafforr.vespa_HackDash_box_lookup as bl
inner join vespa_single_box_view as sbv
on bl.subscriber_id = sbv.subscriber_id
where Panel_ID_4_cells_confirm = 1


select count(distinct subscriber_id) from vespa_HackDash_box_returns;

-- oh wait... these numbers are no good... the above construction gives us closed loop,
-- but the Operational Dashboard goes as closed loop. Mostly. Kind of. So, a restart is called for.
