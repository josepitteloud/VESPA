/******************************************************************************
**
** PROJECT VESPA: Ad hoc patch to get some guess at enablement dates for the
**      Vespa panel ramp up...
**
** So we have no idea what's actually enabled at this point right now. In
** addition to getting the panel_status stuff into play, we're also going to
** try to build some planned enablement date using the golden box list:
**
**  1. Get the call back number assigned to each account
**  2. Figure out when the enablements started
**  3. Get the call back number for each box
**  4. Calculate the expected enablement date of each box by getting the day
**      of the month of (3) that's after the account batch date (1) that's 
**      after then enablement start.
**
** The complication is that this fix also straddles the migration from P10
** into P4, so we're managing bringing over the structures we need too...
** How does the selected date in the panel status table help? is this going
** to give us the enablements we need? It's not specific to each date, but
** it does give us the date of first activation or something? That was the
** 25th of May, so the first activations are on the 28th of May...
**
******************************************************************************/

/****************** DATA IMPORT ******************/

-- These things live on P10, and need t obe put into an appropriate historical record
-- so we can continue to refer to them...

-- Also, there are no DROP TABLE calls in here because if P10 disappears, we won't
-- be able to get the tables back again...

CREATE TABLE "golden_boxes" (
id int default 0,
 "subscriber_ID"      varchar(10) DEFAULT NULL,
 "STB_Make_Model"     varchar(50) DEFAULT NULL,
 "Cbk_Day"            varchar(2) DEFAULT NULL,
 "Missing_Cbcks"      varchar(3) DEFAULT 0,
 "gt12_Hours_Late"    varchar(3) DEFAULT 0,
 "gt5_Minutes_Late"   varchar(3) DEFAULT 0,
 "x8_Hour_Attempt"    varchar(3) DEFAULT 0,
 "x4_Hour_Attempt"    varchar(3) DEFAULT 0,
 "On_Time"            varchar(3) DEFAULT 0,
 "Expected_Cbcks"     varchar(3) DEFAULT 0
    ,account_number varchar(50)
    ,anytimeplus bit default 0
    ,nds_stb_no varchar(50)
    ,src_system_id varchar(50)
    ,prefix varchar(10)
)
;

commit;
go

INSERT INTO vespa_analysts.golden_boxes
   LOCATION 'DCSLOPSKPRD10_olive_prod.greenj' 'SELECT * FROM greenj.golden_boxes';

commit;
go

-- The other table we need is the listing of how the chosen accounts ended up
-- in the batches they were assigned... so:

create table V059_enablement_listing_all_panels (
    account_number                  varchar(20) primary key
    ,panel_id                       tinyint                 -- NULL for disablements
    ,batch                          tinyint
);

go

INSERT INTO vespa_analysts.V059_enablement_listing_all_panels
   LOCATION 'DCSLOPSKPRD10_olive_prod.stafforr' 'SELECT * FROM stafforr.V059_enablement_listing_all_panels';

commit;
go

-- Permissions and indices and other things that aren't easily moved:
grant select on vespa_analysts.golden_boxes                         to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
grant select on vespa_analysts.V059_enablement_listing_all_panels   to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;

create unique index fake_pk     on vespa_analysts.golden_boxes(subscriber_id);
create        index for_joins   on vespa_analysts.golden_boxes(account_number);

commit;
go

/****************** TURN IT INTO THE LIST OF EXPECTED ENABLEMENT DATES ******************/

drop table vespa_analysts.vespa_SBV_enablement_plan;
create table vespa_analysts.vespa_SBV_enablement_plan (
    subscriber_id                   bigint primary key
    ,account_number                 varchar(20)
    ,account_enablement_batch       tinyint
    ,box_call_back_day              tinyint
    ,planned_account_enablement     date
    ,expected_box_enablement        date
);

-- OK so this table is pretty lean and basically has on it only the things we'll need...
insert into vespa_analysts.vespa_SBV_enablement_plan
    (subscriber_id, account_number, account_enablement_batch, box_call_back_day)
select
    gb.subscriber_id,
    elap.account_number,
    elap.batch,
    gb.cbk_day
from vespa_analysts.V059_enablement_listing_all_panels as elap
inner join vespa_analysts.golden_boxes as gb
on elap.account_number = gb.account_number
where elap.panel_id is not null; -- Don't want to include the disablement request guys

commit;
go

-- Okay, so we should check that this stuff lines up with the requests that we have

select count(1) from vespa_analysts.vespa_SBV_enablement_plan;
-- 1,933,670

select count(distinct account_number) from vespa_analysts.vespa_SBV_enablement_plan;
-- 1,729,028; yup, that matches the total of 979028 + 375000 + 375000 from our enablement requests, good.

select count(1) from  vespa_analysts.vespa_SBV_enablement_plan
where account_enablement_batch is null;
-- 0, good

select count(1) from  vespa_analysts.vespa_SBV_enablement_plan
where box_call_back_day is null;
-- 0, also good

select count(1) from  vespa_analysts.vespa_SBV_enablement_plan
where box_call_back_day < account_enablement_batch;
-- 0, that's good...

go

-- Hmmm, so do we do the enablement date construction with a bunch of case statements, or
-- a bunch of joins into the calendar? Let's start with the cases and stuff...

-- So batch 28 was the first to be enabled, that happened on 28th of May, and after that
-- everything other account gets enabled on the June call back....
update vespa_analysts.vespa_SBV_enablement_plan
set planned_account_enablement = dateadd(day, account_enablement_batch-1, '2012-06-01');

-- Wind the 28th of May activated ones back to May...
update vespa_analysts.vespa_SBV_enablement_plan
set planned_account_enablement = dateadd(month, -1, planned_account_enablement)
where account_enablement_batch = 28;

commit;
go

-- Wait, so because the box call back date is always later than the account activation day,
-- the expected activation date is just going to be the activation date in June, except for
-- the guys in batch 28 (which should have call back date = account enablement date = 28)
update vespa_analysts.vespa_SBV_enablement_plan
set expected_box_enablement = dateadd(day, box_call_back_day-1, '2012-06-01');

update vespa_analysts.vespa_SBV_enablement_plan
set expected_box_enablement = dateadd(month, -1, expected_box_enablement)
where account_enablement_batch = 28;

commit;
go

-- Ok, so lets check that accounts aren't enabled later than boxes are expected to be activated...
select count(1) from vespa_analysts.vespa_SBV_enablement_plan
where expected_box_enablement < planned_account_enablement;
-- 0, awesome

-- And miscelaneous QA:
select expected_box_enablement, box_call_back_day
from vespa_analysts.vespa_SBV_enablement_plan
group by expected_box_enablement, box_call_back_day
order by expected_box_enablement, box_call_back_day;
-- 1 to 28, lined up on the days we expect, with 28 at the beginning
-- There are still 28/06/2012 in here to, those are the multiroom boxes
-- of accounts in lower numbered batches, those accounts that got enabled
-- in June rather than late May and so these boxes are catching up.

select planned_account_enablement, account_enablement_batch
from vespa_analysts.vespa_SBV_enablement_plan
group by planned_account_enablement, account_enablement_batch
order by planned_account_enablement, account_enablement_batch;
-- 1 to 28, lined up on the days we expect, with 28 at the beginning

-- Oh, do we have coverage we expect?
select count(1) from vespa_analysts.vespa_SBV_enablement_plan
where planned_account_enablement is null or expected_box_enablement is null;
-- 0, also awesome.

-- Looks like we're good!

/****************** TIDY STUFF UP ******************/

-- Are we dropping these things or keeping them around?
--drop table vespa_analysts.golden_boxes;
--drop table vespa_analysts.V059_Prioritised_household_enablements;

-- For the moment we're keeping them around. Oh, but we want to be able to share
-- those expected enablement dates, and make sure the table is usable...
grant select on vespa_analysts.vespa_SBV_enablement_plan to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
create index for_joins on vespa_analysts.vespa_SBV_enablement_plan(account_number);
