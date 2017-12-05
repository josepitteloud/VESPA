/******************************************************************************
**
** Project Vespa: One-time hack for boxes not dialing back
**
** So we've got this list of boxes that haven't dialed back ever. That's been
** loaded into the table [stafforr].[boxes_not_dialing_back_raw] and now we
** want to 1) verify that these guys haven't dialed back in the last month
** (they should be not-having-dialed-back-EVAR), and that they're active (could
** be an open loop vs closed loop thing) and then do some profiling.
**
** Eventually we'll work this into the Dialback report; at that point, are we
** profiling boxes not reprting ever, or not reporting in the last months?
**
******************************************************************************/

select count(1) from boxes_not_dialing_back_raw
-- 169473

-- So all the subscriber IDs were loaded as VARCHARs, so now we want to structure
-- and index all that stuff;

create table boxes_not_dialing_back (
        subscriber_id                   decimal(8,0)   not null primary key
        ,open_loop_enablement           bit            default 0
        ,closed_loop_enablement         bit            default 0
        ,returned_data_ever             bit            default 0
        ,first_reporting_day            date           default null
        ,logs_returned_thus_far         int            default null
);

-- Then later we'll build separate profiling details.

-- So the Vespa schema has subscriber_id being a decimal(8,0) so the length better
-- not exceed 8:
select max(len(subscriber_id)) from boxes_not_dialing_back_raw;
-- 8, sweet.

insert into boxes_not_dialing_back (subscriber_id)
select convert(decimal(8,0), subscriber_id)
from boxes_not_dialing_back_raw;
-- 169473. Nice.

-- We could rebuild the single box view for this week, or we could just use last
-- week's (since this is an old source)

select count(1)
from vespa_analysts.vespa_single_box_view as sbv
inner join boxes_not_dialing_back as bndb
on sbv.subscriber_id = bndb.subscriber_id;
-- 169075 -- hilarious; so we're allready dropping boxes from our population

-- OK, next, how many are in the open loop?
select count(1)
from vespa_analysts.vespa_single_box_view as sbv
inner join boxes_not_dialing_back as bndb
on sbv.subscriber_id = bndb.subscriber_id
where Panel_ID_4_cells_confirm = 1
and Status_Vespa not in ('Enabled','DisablePending');
-- 85771

-- OK, so the rest are active panel 4 guys, correct?
select count(1)
from vespa_analysts.vespa_single_box_view as sbv
inner join boxes_not_dialing_back as bndb
on sbv.subscriber_id = bndb.subscriber_id
where Status_Vespa in ('Enabled','DisablePending');
-- 83304

select 169075 - 83304 - 85771;
-- zero! partitioned!

-- Oh, and we've got space for marks on the table, push them in:
update boxes_not_dialing_back
set open_loop_enablement = 1
from boxes_not_dialing_back as bndb
inner join vespa_analysts.vespa_single_box_view as sbv
on sbv.subscriber_id = bndb.subscriber_id
where Panel_ID_4_cells_confirm = 1;
-- 168397 updated

update boxes_not_dialing_back
set closed_loop_enablement = 1
from boxes_not_dialing_back as bndb
inner join vespa_analysts.vespa_single_box_view as sbv
on sbv.subscriber_id = bndb.subscriber_id
where Status_Vespa in ('Enabled','DisablePending');
-- 83304 updated

-- ok, so let's build a list of boxes tht have returned data ever post call blight
create variable @sql_hurg varchar(2000);
create variable @loop_day date;

drop table reporting_boxes_store;
create table reporting_boxes_store (
    subscriber_id decimal(8)
    ,reporting_day varchar(8)
);

SET @sql_hurg = '
    insert into reporting_boxes_store
    select distinct subscriber_id, ''#£^^*^*£#''
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

select count(1) from reporting_boxes_store;
-- 17711648, which is about the right order of magnitude.

drop table boxes_reporting_since_call_blight;
select subscriber_id, min(reporting_day) as first_reporting, count(1) as tot_returned
into stafforr.boxes_reporting_since_call_blight
from reporting_boxes_store
group by subscriber_id;
-- 408631

create unique index fake_pk on boxes_reporting_since_call_blight (subscriber_id);

select count(1) from boxes_reporting_since_call_blight as rscb
inner join boxes_not_dialing_back as bndb
on rscb.subscriber_id = bndb.subscriber_id;
-- 1812 - a few, kind of what we expected

update boxes_not_dialing_back
set returned_data_ever = 0
    ,first_reporting_day = null
    ,logs_returned_thus_far = 0
;

update boxes_not_dialing_back
set returned_data_ever = 1,
    first_reporting_day = convert(date, first_reporting, 112),
    logs_returned_thus_far = tot_returned
from boxes_not_dialing_back as bndb
inner join boxes_reporting_since_call_blight as rscb
on rscb.subscriber_id = bndb.subscriber_id;
-- 1812 rows updated

-- Resutls output 1: Summary totals: Okay, so I reakon we want to build a little cross chart...
select open_loop_enablement, closed_loop_enablement, returned_data_ever, count(1) as box_count
from boxes_not_dialing_back
group by open_loop_enablement, closed_loop_enablement, returned_data_ever
order by open_loop_enablement, closed_loop_enablement, returned_data_ever;
-- Okay, awesome, all 8 combinations are in here :/

-- Results output 2: we then also want a report of the boxes that *have* returned
-- data and the first day on which they returned data:
select subscriber_id, first_reporting_day, logs_returned_thus_far
from boxes_not_dialing_back
where returned_data_ever = 1
order by subscriber_id;

-- Results 3: the last output is going to be the profiling thing with
select top 10 * from vespa_analysts.vespa_single_box_view

-- OK, so we also want region, and we need to strap that on
-- from... SAV.. which means I need account numbers. Oh well.
select distinct sav.account_number, sav.isba_tv_region
into #isba_tv_regions
from sk_prod.cust_single_account_view as sav
inner join vespa_analysts.vespa_single_box_view as vsbv
    on vsbv.account_number = sav.account_number;

commit;
create unique index fake_pk on #isba_tv_regions (account_number);
-- ahahah, that didn't work...
select account_number from #isba_tv_regions
group by account_number
having count(1) > 1;
/* account_number
210174636147
200001078407
*/

select * from #isba_tv_regions
where account_number in ('210174636147','200001078407');
-- So one of these is 'Not Defined' and the other claims to be both
-- South and scotland. We're going to catch non-matched accounts
-- here anyway, so let's clip them all out and default them to the
-- Unkown.
select isba_tv_region, count(1) as hits
from #isba_tv_regions
group by isba_tv_region;
-- Already some 85k not defineds, we won't mind a few more Unknowns.

delete from #isba_tv_regions
where account_number in ('210174636147','200001078407');
-- 4 rows gone.
create unique index fake_pk on #isba_tv_regions (account_number);
-- Works now.

drop table stafforr.non_reporting_pivot_summary;
select
        case when(bndb.open_loop_enablement = 1 and bndb.closed_loop_enablement = 1) then 1 else 0 end as current_panel
        ,bndb.returned_data_ever
        ,coalesce(vsbv.PS_flag, 'U')
        ,coalesce(vsbv.Box_Type, 'Unknown')
        ,vsbv.Account_anytime_plus
        ,vsbv.Box_has_anytime_plus
        ,vsbv.PVR
        ,coalesce(isba.isba_tv_region, 'Unknown')
        ,count(1) as box_count
into stafforr.non_reporting_pivot_summary
from boxes_not_dialing_back as bndb
left join vespa_analysts.vespa_single_box_view as vsbv
        on bndb.subscriber_id = vsbv.subscriber_id
left join #isba_tv_regions as isba
        on vsbv.account_number = isba.account_number
group by current_panel
        ,bndb.returned_data_ever
        ,vsbv.PS_flag
        ,vsbv.Box_Type
        ,vsbv.Account_anytime_plus
        ,vsbv.Box_has_anytime_plus
        ,vsbv.PVR
        ,isba.isba_tv_region
;

-- And now output 4:
select * from non_reporting_pivot_summary;

-- So everythign else is Excel based.


create variable @username varchar(20);
set @username = 'stafforr'

  SELECT table_name
        ,cast((select kbytes / 1024 from sp_iqtablesize(@username ||'.' ||table_name)) as decimal(16,5)) as mbytes
    FROM sys.systable
   WHERE user_name(creator)  = @username;

-- result output 5: boxes which are on the list but are not in any kind of Vespa data set:
select subscriber_id from boxes_not_dialing_back
where open_loop_enablement = 0 and closed_loop_enablement = 0
order by subscriber_id;

/* tables to eventually drop:
drop table boxes_not_dialing_back
drop table boxes_reporting_since_call_blight
drop table non_reporting_pivot_summary
drop table reporting_boxes_store
drop table boxes_not_dialing_back_raw
*/


