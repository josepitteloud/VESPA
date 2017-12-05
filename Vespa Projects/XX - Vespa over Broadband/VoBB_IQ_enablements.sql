/*  Collate IQ enablements

Script to add broadband migration group tags to IQ enablements according their enablement batch.
This is needed to provide a closer tracking of enabled boxes against expectations, since boxes/accounts
that were originally flagged for migration may have been disabled as part of the weekly disablements
(Waterfall conditions).

This script now references the table netezza_p5, which is generated/updated by the script
\Vespa\Vespa Projects\XX - Vespa over Broadband\create_Netezza_panel5_log.sql


Source overview:
select
    created_dt
    , request_dt
    , request_filename
    , action
    , result
    , panel_no
    , count()
    , count(distinct account_number)        as accounts
    , count(distinct card_subscriber_id)    as boxes
from sk_prod.vespa_subscriber_status
where
    panel_no in (5,6,7,11,12)
    and created_dt > '2013-11-01'
group by
    created_dt
    , request_dt
    , request_filename
    , action
    , result
    , panel_no
order by
    created_dt desc
    , request_dt desc
    , request_filename
    , action
    , result
    , panel_no
;

~~~
2014/02/11  Author  :   Hoi Yu Tang, hoiyu.tang@skyiq.co.uk

*/


/*
drop table VoBB_VSS_migration_batches;

create table VoBB_VSS_migration_batches(
    account_number          varchar(20) default null
    , card_subscriber_id    varchar(8)  default null
    , created_dt            datetime    default null
    , request_dt            datetime    default null
    , request_filename      varchar(50) default null
    , panel_no              int         default null
    , migration_group       varchar(20) default null    -- migration group label eg. M1, M2, M3 ....etc
    , cbk_day_batch         varchar(20) default null
    , expected              bit         default 0       -- flag from cross-referencing against the planned migration groups
    , multiroom             bit         default 0       -- flag for single-room (0) and multi-room (1) subscription accounts
    )
;

create hg index idx1 on VoBB_VSS_migration_batches(card_subscriber_id);

grant select on VoBB_VSS_migration_batches to greenj;

*/


truncate table VoBB_VSS_migration_batches;

-------------------------------------------
-- Early 6k migration (5k to P5, 1k to P11)
-------------------------------------------
insert into VoBB_VSS_migration_batches(
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , migration_group
    , cbk_day_batch
    )
select
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , 'M1M2_early_6k'
    , 'OTA'
from sk_prod.vespa_subscriber_status
where
    created_dt = '2014-01-17 13:29:16.924941'
    and request_dt = '2013-12-24 13:00:48.000000'
    and request_filename is null
    and panel_no in (5,11)
    and result = 'Enabled'
;


-- Cross reference against expected migratees
update VoBB_VSS_migration_batches
set a.expected = 1
from
    VoBB_VSS_migration_batches  as a
    left join tanghoi.VoBB_early_6K     as b    on b.card_subscriber_id = a.card_subscriber_id
where
    a.migration_group = 'M1M2_early_6k'
    and b.card_subscriber_id is not null
;




---------------------------------------------------
-- Early 6k migration part 2 (6k from M3, M4 to P5)
---------------------------------------------------
insert into VoBB_VSS_migration_batches(
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , migration_group
    , cbk_day_batch
    )
select
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , 'M3M4_early_6k'
    , 'OTA'
from sk_prod.vespa_subscriber_status
where
    created_dt = '2014-01-17 13:29:16.924941'
    and request_dt = '2014-01-10 15:02:08.000000'
    and request_filename is null
    and panel_no = 5
    and result = 'Enabled'
;

-- Cross reference against expected migratees
update VoBB_VSS_migration_batches
set a.expected = 1
from
    VoBB_VSS_migration_batches          as a
    left join tanghoi.VoBB_early_M3M4_rq_fix    as b    on b.card_subscriber_id = a.card_subscriber_id
where
    a.migration_group = 'M3M4_early_6k'
    and b.card_subscriber_id is not null
;

------------------------------------------------------
-- M1, M3, and M4 migration group. Callback days 3 - 9
------------------------------------------------------
insert into VoBB_VSS_migration_batches(
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , migration_group
    , cbk_day_batch
    )
select
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , 'M1M3M4_full'
    , '3-9'
into VoBB_VSS_migration_batches
from sk_prod.vespa_subscriber_status
where request_filename in (
    '2014-01-31-SKY-SKY-SPMSQ-P005-0020.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0019.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0018.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0017.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0016.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0015.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0014.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0013.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0012.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0011.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0010.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0009.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0008.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0007.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0006.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0005.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0004.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0003.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0002.xml'
    ,'2014-01-31-SKY-SKY-SPMSQ-P005-0001.xml'
    )
;




--------------------------------------------------------
-- M1, M3, and M4 migration group. Callback days 10 - 16
--------------------------------------------------------
insert into VoBB_VSS_migration_batches(
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , migration_group
    , cbk_day_batch
    )
select
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , 'M1M3M4_full'
    , '10-16'
from sk_prod.vespa_subscriber_status
where request_filename in (
    '2014-02-06-SKY-SKY-SPMSQ-P005-0003.xml'
    ,'2014-02-06-SKY-SKY-SPMSQ-P005-0002.xml'
    ,'2014-02-06-SKY-SKY-SPMSQ-P005-0001.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0018.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0017.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0016.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0015.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0014.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0013.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0012.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0011.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0010.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0009.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0008.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0007.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0006.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0005.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0004.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0003.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0002.xml'
    ,'2014-02-05-SKY-SKY-SPMSQ-P005-0001.xml'
    )
;





--------------------------------------------------------
-- M1, M3, and M4 migration group. Callback days 17 - 23
--------------------------------------------------------
insert into VoBB_VSS_migration_batches(
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , migration_group
    , cbk_day_batch
    )
select
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , 'M1M3M4_full'
    , '17-23'
from sk_prod.vespa_subscriber_status
where request_filename in (
    '2014-02-13-SKY-SKY-SPMSQ-P005-0017.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0016.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0015.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0014.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0013.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0012.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0011.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0010.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0009.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0008.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0007.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0006.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0005.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0004.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0003.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0002.xml'
    ,'2014-02-13-SKY-SKY-SPMSQ-P005-0001.xml'
    )
;





-----------------------------------------------------------------
-- M1, M3, and M4 migration group. Callback days 24 - 28, 1 and 2
-----------------------------------------------------------------
insert into VoBB_VSS_migration_batches(
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , migration_group
    , cbk_day_batch
    )
select
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , 'M1M3M4_full'
    , '24-28,1-2'
from sk_prod.vespa_subscriber_status
where request_filename in (
    '2014-02-26-SKY-SKY-SPMSQ-P005-0003.xml'
    ,'2014-02-26-SKY-SKY-SPMSQ-P005-0004.xml'
    ,'2014-02-26-SKY-SKY-SPMSQ-P005-0001.xml'
    ,'2014-02-26-SKY-SKY-SPMSQ-P005-0002.xml'
    ,'2014-02-25-SKY-SKY-SPMSQ-P005-0009.xml'
    ,'2014-02-25-SKY-SKY-SPMSQ-P005-0010.xml'
    ,'2014-02-25-SKY-SKY-SPMSQ-P005-0007.xml'
    ,'2014-02-25-SKY-SKY-SPMSQ-P005-0008.xml'
    ,'2014-02-25-SKY-SKY-SPMSQ-P005-0005.xml'
    ,'2014-02-25-SKY-SKY-SPMSQ-P005-0006.xml'
    ,'2014-02-25-SKY-SKY-SPMSQ-P005-0003.xml'
    ,'2014-02-25-SKY-SKY-SPMSQ-P005-0004.xml'
    ,'2014-02-25-SKY-SKY-SPMSQ-P005-0001.xml'
    ,'2014-02-25-SKY-SKY-SPMSQ-P005-0002.xml'
    )
;

---------------------------------------------------------
-- Cross reference M1M3M4_full against expected migratees
---------------------------------------------------------
update VoBB_VSS_migration_batches
set a.expected = 1
from
    VoBB_VSS_migration_batches                          as a
    left join VoBB_migration_groups_20140126_preM1M3M4  as b    on  b.card_subscriber_id = a.card_subscriber_id
where
    a.migration_group = 'M1M3M4_full'
    and b.card_subscriber_id is not null
    and b.migration_group in ('M1','M3','M4')
;


-----------------------------------------------------------------
-- Migration groups 1a, 1d, 2a, 2d, 3a and 3d
-----------------------------------------------------------------
insert into tanghoi.VoBB_VSS_migration_batches(
    account_number
    , card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , migration_group
    , cbk_day_batch
    )
select
    vss.account_number
    , vss.card_subscriber_id
    , created_dt
    , request_dt
    , request_filename
    , panel_no
    , requested_movement_type
    , cast(ca_callback_day as varchar)
from sk_prod.vespa_subscriber_status as vss
     inner join vespa_analysts.panel_movements_log as log on vss.card_subscriber_id = log.card_subscriber_id
   where (request_filename like '2014-04-%-SKY-SKY-SPMSQ-P005%' and cast(substr(request_filename,9,2) as int) >= 15)
      or (request_filename like '2014-05-%-SKY-SKY-SPMSQ-P005%' and cast(substr(request_filename,9,2) as int) <  15)
      or (request_filename like '2014-04-%-SKY-SKY-SPMSQ-P011%' and cast(substr(request_filename,9,2) as int) >= 15)
      or (request_filename like '2014-05-%-SKY-SKY-SPMSQ-P011%' and cast(substr(request_filename,9,2) as int) <  15)
;










/* Checks...

select top 20 * from VoBB_VSS_migration_batches;

select
    migration_group
    , created_dt
    , request_dt
    , panel_no
    , cbk_day_batch
    , expected
    , count(distinct account_number)    as accounts
    , count()                       as boxes
from VoBB_VSS_migration_batches
group by
    migration_group
    , created_dt
    , request_dt
    , panel_no
    , cbk_day_batch
    , expected
order by
    migration_group
    , created_dt
    , request_dt
    , panel_no
    , cbk_day_batch
    , expected
;

*/






---------------------------------------
-- Add single/multiroom accounts detail
---------------------------------------

-- First, identify on an account-level which contain CURRENT multiroom subscriptions
drop table #mr_accounts;
select  VMB.account_number
into #mr_accounts
from
    sk_prod.cust_subs_hist                  as CSH
    right join VoBB_VSS_migration_batches   as VMB  on VMB.account_number = CSH.account_number
where
    CSH.effective_to_dt = '9999-09-09'
    and CSH.status_code in ('AC','AB','PC')
    and CSH.subscription_sub_type = 'DTV Extra Subscription'
group by VMB.account_number
;




-- Update multiroom field
update VoBB_VSS_migration_batches
set multiroom = 1
from
    VoBB_VSS_migration_batches      as VMB
    left join #mr_accounts          as MRA  on MRA.account_number = VMB.account_number
where MRA.account_number is not null
;





/* Summary for pivot table...
select
    migration_group
    , 'panel ' + convert(varchar,panel_no) as panel_no
    , cbk_day_batch
    , case expected
        when 1 then 'expected'
        else 'unexpected'
      end case as expected
    , case multiroom
        when 1 then 'multiroom'
        else 'singleroom'
      end as multiroom
    , count()
from VoBB_VSS_migration_batches
group by
    migration_group
    , panel_no
    , cbk_day_batch
    , expected
    , multiroom
order by
    migration_group
    , panel_no
    , cbk_day_batch
    , expected
    , multiroom
;
*/





-------------------------------------
-- Now join onto Netezza panel 5 logs
-------------------------------------
drop table #tmp;
select
    VMB.*
    , NET.dt
into #tmp
from
    VoBB_VSS_migration_batches          as VMB
    left join netezza_p5                as NET  on  NET.card_subscriber_id = VMB.card_subscriber_id
where NET.dt >= '2014-02-03'
;


/* Summary...
--Note: this will be a day-by-day breakdown across the various groupings

select
    migration_group
    , 'panel ' + convert(varchar,panel_no) as panel_no
    , cbk_day_batch
    , case expected
        when 1 then 'expected'
        else 'unexpected'
      end case as expected
    , case multiroom
        when 1 then 'multiroom'
        else 'singleroom'
      end as multiroom
    , dt
    , count()
from #tmp
group by
    migration_group
    , panel_no
    , cbk_day_batch
    , expected
    , multiroom
    , dt
order by
    migration_group
    , panel_no
    , cbk_day_batch
    , expected
    , multiroom
    , dt
;

*/


select top 10 * from tanghoi.BB_panels_daily_sub_log
select top 10 * from #results
select max(days),min(days) from #results
select max(log_received_date) from tanghoi.BB_panels_daily_sub_log



         -------------------------------
         --Pre v Post migration analysis
         -------------------------------
  create table #results(migration_group    varchar(30)
                       ,subscriber_id      int
                       ,panel_id_reported  tinyint
                       ,days               tinyint
                       ,old_panel          tinyint
                       ,old_panel_max_dt   date
                       ,old_panel_days     tinyint
                       ,new_panel_start_dt date
);

  insert into #results(migration_group
        ,subscriber_id
        ,panel_id_reported
        ,days
)
  select migration_group
        ,cast(subscriber_id as int)
        ,panel_id_reported
        ,count(1)
    from tanghoi.BB_panels_daily_sub_log as log
         inner join tanghoi.VoBB_VSS_migration_batches as bat on log.subscriber_id = bat.card_subscriber_id
   where log_received_date between '2014-04-27' and '2014-05-11'
group by migration_group
        ,subscriber_id
        ,panel_id_reported
  having min(log_received_date) <= '2014-04-27'
;

      -- find max date for old panel
  select bas.subscriber_id
        ,max(dt) as max_dt
    into #max_dt
    from #results as bas
         inner join vespa_analysts.panel_data as pan on bas.subscriber_id = pan.subscriber_id
                                                    and panel <> panel_id_reported
group by bas.subscriber_id
;

      -- put max date for old panel into results table
  update #results as bas
     set old_panel_max_dt = max_dt
    from #max_dt as mxd
   where bas.subscriber_id = mxd.subscriber_id
;

     -- put old panel number into table
  update #results as bas
     set old_panel = panel
    from vespa_analysts.panel_data as pan
   where bas.subscriber_id = pan.subscriber_id
     and bas.old_panel_max_dt = pan.dt
;

      -- count days of data return on old panel for those on daily panel
  select bas.subscriber_id
        ,sum(data_received) as days
    into #old_days
    from #results as bas
         inner join vespa_analysts.panel_data as pan on bas.subscriber_id = pan.subscriber_id
   where dt between old_panel_max_dt - 14 and old_panel_max_dt
     and old_panel = 12
group by bas.subscriber_id
;

      -- count days of data return on old panel for those on alt. panels
  insert into #old_days(subscriber_id
        ,days)
  select bas.subscriber_id
        ,sum(data_received) as days
    into #old_days
    from #results as bas
         inner join vespa_analysts.panel_data as pan on bas.subscriber_id = pan.subscriber_id
   where dt between old_panel_max_dt - 29 and old_panel_max_dt
     and old_panel in (6, 7)
group by bas.subscriber_id
;

      -- put days of data return into results table
  update #results as bas
     set old_panel_days = old.days
    from #old_days as old
   where bas.subscriber_id = old.subscriber_id
;

      -- calculate summary results
  select migration_group
        ,count(1) as boxes
        ,avg(old_panel_days)/15 as prev_ARQ
        ,avg(days)/15 as new_ARQ
    into #summary
    from #results
group by migration_group
;

      -- count boxes that migrated
  select migration_group
        ,count(1) as total
    into #full_count
    from tanghoi.VoBB_VSS_migration_batches
group by migration_group
;

     -- output
  select sum.migration_group
        ,total
        ,boxes
        ,prev_ARQ
        ,new_ARQ
    from #summary as sum
         inner join #full_count as ful on sum.migration_group = ful.migration_group
;

      -- total
  select count(1) as boxes
        ,avg(old_panel_days) as prev_ARQ
        ,avg(days) as new_ARQ
    from #results
;



