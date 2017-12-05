/******************************************************************************
**
** Project Vespa: One-time hack for boxes not dialing back - Part 2
**
** Now we want to look at the links between the households with secondary
** boxes not dialing back; for given P or S box that is / isn't reporting,
* are the other boxes in the house also not reporting?
**
******************************************************************************/

-- First things first: get a list of all those boxes that have reported sometime
-- since the end of the call blight. This time, keep their account numbers too.

create variable @sql_hurg varchar(2000);
create variable @loop_day date;

drop table reporting_boxes_store;
create table reporting_boxes_store (
    subscriber_id decimal(8)
    ,account_number varchar(20)
    ,reporting_day varchar(8)
);

SET @sql_hurg = '
    insert into reporting_boxes_store
    select distinct subscriber_id, account_number, ''#£^^*^*£#''
    from sk_prod.VESPA_STB_PROG_EVENTS_#£^^*^*£#
';

-- Testing that it works before loop:
-- select replace(@sql_hurg, '#£^^*^*£#', dateformat(@loop_day,'yyyymmdd'))
-- yeah, cool, we good.

delete from reporting_boxes_store;
set @loop_day = '2011-11-22'; -- the end of the call blight
commit;

while @loop_day < '2012-01-20' -- four days prior to report construction
begin
        execute(replace(@sql_hurg, '#£^^*^*£#', dateformat(@loop_day,'yyyymmdd')))
        commit

        set @loop_day = dateadd(day, 1, @loop_day)
        commit
end;

-- <- to here

select count(1) from reporting_boxes_store;
-- 17715614, acceptable

drop table boxes_reporting_since_call_blight;
select subscriber_id
    ,min(account_number) as account_number
    ,min(reporting_day) as first_reporting, count(1) as tot_returned
    ,convert(varchar(1), 'N') as PS_flag -- defaults 'N' for No
into stafforr.boxes_reporting_since_call_blight
from reporting_boxes_store
group by subscriber_id;

create unique index fake_PK on boxes_reporting_since_call_blight (subscriber_id);
create index account_numebr on boxes_reporting_since_call_blight (account_number);

commit;

-- OK, so now we just compare that to the confirmed panel 4 enabledments in the single
-- box view, and then we can pull out our numbers? We need to know if each of those
-- are P or S (or U!) but generally we'd just need to update the following table:
select 
    account_number
    ,sum(case when PS_flag = 'P' then 1 else 0 end) as enabled_P_boxes
    ,sum(case when PS_flag = 'S' then 1 else 0 end) as enabled_S_boxes
    ,sum(case when PS_flag = 'U' then 1 else 0 end) as enabled_U_boxes
    ,convert(tinyint, 0) as reporting_P_boxes
    ,convert(tinyint, 0) as reporting_S_boxes
    ,convert(tinyint, 0) as reporting_U_boxes
into reporting_PS_breakdown
from vespa_analysts.vespa_single_box_view
where Panel_ID_4_cells_confirm = 1 and Status_Vespa in ('Enabled', 'DisablePending')
group by account_number;

commit;

create index fake_pk on reporting_PS_breakdown (account_number);

-- So let's put that P/S on the reporting table:
update boxes_reporting_since_call_blight
set PS_flag = sbv.PS_flag
from boxes_reporting_since_call_blight as rscb
inner join vespa_analysts.vespa_single_box_view as sbv
on rscb.subscriber_id = sbv.subscriber_id;

commit;

-- How many broken links?
select count(1) from boxes_reporting_since_call_blight where PS_flag = 'N';
-- 31470
-- These broken links will include Sky Panel too, but w/e, we exclude those when
-- we connect to the PS breakdown table because that has the filter for panel 4.
-- And yeah, this is just slightly larger than Sky View, so cool.

-- Ok, so tag the box counts on...
update reporting_PS_breakdown
set  reporting_P_boxes = t.reporting_P_boxes
    ,reporting_S_boxes = t.reporting_S_boxes
    ,reporting_U_boxes = t.reporting_U_boxes
from reporting_PS_breakdown as rpsb
inner join (
    select
        account_number
        ,sum(case when PS_flag = 'P' then 1 else 0 end) as reporting_P_boxes
        ,sum(case when PS_flag = 'S' then 1 else 0 end) as reporting_S_boxes
        ,sum(case when PS_flag = 'U' then 1 else 0 end) as reporting_U_boxes
    from boxes_reporting_since_call_blight
    group by account_number
) as t
on rpsb.account_number = t.account_number;

commit;
-- and now most of the work on collecting all the data is done, and it's just
-- presentation and pivoting now.

-- We'd also like to have a view of how many of these boxes overlapped with the
-- boxes on the non-reporting list we received, but hey, that was at box level
-- and this is at account level. We will, however, be able to identify the cells
-- in the original control totals breakdown that these numbers relate to.

-- So... we have data?
select
        enabled_P_boxes
        ,enabled_S_boxes
        ,enabled_U_boxes
        ,count(1) as hits
from reporting_PS_breakdown
group by enabled_P_boxes, enabled_S_boxes, enabled_U_boxes
order by enabled_P_boxes, enabled_S_boxes, enabled_U_boxes;
-- Yeah, messy, like what we expected realy...

-- Oh, first off, we don't care about the population that only has their primary
-- box enabled; we want to know about secondary boxes.
delete from reporting_PS_breakdown
where enabled_S_boxes = 0 and enabled_U_boxes = 0;

commit;

-- It's not going to clear anything up, but the numbes will be smaller now...
select count(1) from reporting_PS_breakdown;
-- 84328 is entirely managable for analysis. Just not pivoting. Now let's see
-- about the reporting numbers...

-- Report build 1: control totals for boxes
select
        count(1) as hits,
        sum(enabled_P_boxes) as enabled_P_boxes,
        sum(enabled_S_boxes) as enabled_S_boxes,
        sum(enabled_U_boxes) as enabled_U_boxes,
        sum(reporting_P_boxes) as reporting_P_boxes,
        sum(reporting_S_boxes) as reporting_S_boxes,
        sum(reporting_U_boxes) as reporting_U_boxes
from reporting_PS_breakdown;
-- (Note that these are for Vespa-MR only. IE, if a MR household only has their
-- primary box enabled for Vespa, they don't show up here.)

-- Report fragments 2:
select
        enabled_P_boxes
        ,reporting_P_boxes
        ,enabled_S_boxes
        ,reporting_S_boxes
        ,count(1) as count_of_households
from reporting_PS_breakdown
where enabled_P_boxes = 1
and enabled_S_boxes > 0
group by enabled_P_boxes, reporting_P_boxes, enabled_S_boxes, reporting_S_boxes
order by enabled_P_boxes, reporting_P_boxes, enabled_S_boxes, reporting_S_boxes;
-- Okay, and then we can filter / group / pivot or whatever in Excel...



