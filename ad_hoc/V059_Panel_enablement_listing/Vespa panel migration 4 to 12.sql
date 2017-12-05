/****************** EXTRACTION OF VESP PANEL BY CALL BACK DATE ******************/

-- So the first thing we're going to do is batch the Vespa panel 4 out as determined
-- by the call back date, whatever that is. Panel members are on SBV, call back date
-- is on the golden box tables, so we need to patvh those together. Also we want to
-- take the first of the batches into several parts, though we're in the middle of a
-- month so we need to agree a cuttoff day after which we append everything from the
-- beginning of the month...

select count(1) from greenj.golden_boxes
-- 6556291 - good!

select count(1) from vespa_analysts.vespa_single_box_view where panel = 'VESPA'
-- 539958

select count(1) as counted, count(distinct sbv.card_subscriber_id) as distincted
from vespa_analysts.vespa_single_box_view as sbv
inner join greenj.golden_boxes as gb
on sbv.card_subscriber_id = gb.subscriber_id
where sbv.panel = 'VESPA';
-- 442934  442934 - okay, so a bunch of existing vespa boxes are not golden...

-- first some control totals for expectations:
select
    coalesce(cbk_day, 'Not Golden!') as call_back_day
    ,count(1) as Vespa_boxes
from vespa_analysts.vespa_single_box_view as sbv
left join greenj.golden_boxes as gb
on sbv.card_subscriber_id = gb.subscriber_id
where sbv.panel = 'VESPA'
group by call_back_day
order by call_back_day;
/* OK, so this is pretty well organised, just have to organise the batching and rollover andf suchlike...
01	15074
02	14376
03	14142
04	15569
05	14456
06	15244
07	14981
08	15423
09	16085
10	15884
11	16103
12	16030
13	16172
14	15913
15	15893
16	16013
17	16231
18	16065
19	16145
20	16645
21	16360
22	16997
23	18354
24	16456
25	16399
26	15807
27	16285
28	13832
Not Golden!	97024
*/

-- also: do all the accounts have the same call back date?
select account_number, count( distinct cbk_day) as daysthings
from greenj.golden_boxes
group by account_number
having daysthings > 1
order by daysthings desc;
-- um... yeah, they're all different. Guess we're not doing it household per day or anything
-- haha, no, just take the earliest callback date...

-- oh hey slight complication, everything in the golden boxes is by box, including
-- the call back date, though we really want to organise everything by account,
-- though the call back dates are not aligned by the account in any way at all...
-- Plan is:
--  1) Group accounts by earliest day in the month for a box call back
--  2) Split the 97k with no call back date into 6 batches
--  3) Make batches for every day for now (we’ll split into smaller ones once we know the first date)

-- So, Part 1, orgasine the Vespa panel into call back dates at account level.
select
    sbv.account_number
    ,coalesce(min(cbk_day), 'Not Golden!') as call_back_day
    ,count(1) as Vespa_boxes
    ,convert(tinyint, min(cbk_day)) as batch -- we'll update it in a bit for the non-golden boxes
into PanMan_Golden_account_batches
from vespa_analysts.vespa_single_box_view as sbv
left join greenj.golden_boxes as gb
on sbv.card_subscriber_id = gb.subscriber_id
where sbv.panel = 'VESPA'
group by sbv.account_number;
-- 361570 rows, good.

select count(distinct account_number)
from vespa_analysts.vespa_single_box_view
where panel = 'VESPA';
-- 361570, awesome

commit;
create unique index fake_pk on PanMan_Golden_account_batches (account_number);
commit;

-- OK, lets see how this balance goes...
select call_back_day, count(1) as accunts
from PanMan_Golden_account_batches
group by call_back_day
order by call_back_day;
/* Oh, it's fine, don't even have to worry about splitting out the non-golden boxes!
01              13898
02              13114
03              12907
04              14052
05              12945
06              13550
07              13249
08              13442
09              13918
10              13560
11              13605
12              13450
13              13515
14              13188
15              13094
16              13102
17              13024
18              12948
19              12792
20              13107
21              12777
22              12915
23              12367
24              11801
25              11477
26              11257
27              11340
28              8982
Not Golden!     2194
*/

update PanMan_Golden_account_batches
set batch = 29
where batch is null;
commit;

-- OK... so, how do we extract things into CSV files with Sybase?

-- So we're going back to a single big file with a second column for the batch number. Awesome.
select convert(bigint, account_number)  -- shows up with quotes around it if you eave it a varchar
    ,batch
from PanMan_Golden_account_batches
order by batch, account_number;
output to 'D:\\Vespa\\Golden box migration batches\\Vespa_Golden_Batch_all.csv';
-- It only works on Sybase interactive on P5X1:

-- OK, so for the control totals:
select batch, count(1)
from PanMan_Golden_account_batches
group by batch
order by batch;
output to 'D:\\Vespa\\Golden box migration batches\\Vespa_Golden_Batch_Control_Totals.csv';

-- Cool, so all those are built and added to a zip file. We've spot shecked some of the files
-- against the totals quoted in the control totals, and also checked that the control totals
-- add up to 361570 so we're pretty happy with it.

-- Now one other thing: we need to pull out the subscriber IDs for everything in batch 29
-- which doesn't have a call-back date:
select sbv.subscriber_id
from PanMan_Golden_account_batches as gab
inner join vespa_analysts.vespa_single_box_view as sbv
on gab.account_number = sbv.account_number
where gab.batch = 29
order by sbv.subscriber_id;
output to 'D:\\Vespa\\Golden box migration batches\\Vespa_Golden_Batch_29_Subscriber_IDs.csv';
-- Again, P5X1 only.
