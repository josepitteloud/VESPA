/******************************************************************************
**
**      Project Vespa: Scalability investigations: box to household
**
** So apparently the scaling path for Vespa goes from boxes to households to
** the Sky base to the UK or something like that. (Why are we scaling viewing
** to people that aren't on Sky?) It's not super well defined at the moment,
** but check here:
**
**  http://rtci/vespa1/Vespa%20scalability%20rules.aspx
**
** That might not work yet, but that's where the scaling documentation will go
** when we have it. There's a flow chart being built, apparently.
**
** This script is about messing about, seeing what we can infer about what a
** household is watching, or coming up with some statistical estimates of the
** stability of the measurements or something.
**
** Update: turns out we're not nearly that ambitious at this point, we just
** want to answer some basic questions about how multi-box reporting works
** out for us. Oh well.
**
******************************************************************************/

-- Okay, because we're scaling up to households, we're really only interested
-- Multi-room, right? Specifically we're *most* interested in multiroom setups
-- where some boxes return data, some do not. Wait, do we have that log lookup
-- yet? Might have to do the drop 4 data audit stuff again.

-- What about multiroom households which have only one box enabled? I'm not sure
-- they count at all. Let's just go entirely off Vespa enablement then?

select top 10 * from sk_prod.VESPA_STB_LOG_SNAPSHOT;

select min(x_days_since_last_log_sent), max(x_days_since_last_log_sent), count(1) from sk_prod.VESPA_STB_LOG_SNAPSHOT;
-- 453,953 boxes have returned anything ever?
select count(1) from sk_prod.VESPA_subscriber_status where result='Enabled'
-- 717,273 - lols so there's a lot of non-reporting.

-- Now to build a summary of reporting and not reporting by account number to
-- see how many things are good or not... going to have to chain the table
-- definitions together since we don't really get updates based on subqueries
-- supported.

select account_number, count(1) as enabled_boxes
into #enabled_boxes_summary
from  sk_prod.VESPA_subscriber_status
where result='Enabled'
group by account_number;

select account_number, count(1) as reporting_boxes
into #reporting_boxes_summary
from  sk_prod.VESPA_STB_LOG_SNAPSHOT
where panel_id = '5'
group by account_number;

commit;

create unique index fake_pk on #enabled_boxes_summary (account_number);
create unique index fake_pk on #reporting_boxes_summary (account_number);

commit;

select ebs.account_number, enabled_boxes, coalesce(reporting_boxes,0) as reporting_boxes
into multiroom_enablement_reporting_overview
from #enabled_boxes_summary as ebs
left join #reporting_boxes_summary as rbs
on ebs.account_number = rbs.account_number;

create unique index fake_pk on multiroom_enablement_reporting_overview (account_number);

-- This is going to be okay as a high level view, but we're going to need to
-- know about days since last report too... lols.

select top 10 * from multiroom_enablement_reporting_overview;

-- OK, that's good, now we can build a profile of things... good start.

select enabled_boxes, reporting_boxes, count(1) as accounts
from multiroom_enablement_reporting_overview
where enabled_boxes <= 6 and reporting_boxes <= enabled_boxes -- added because some are borked
group by enabled_boxes, reporting_boxes
order by enabled_boxes, reporting_boxes;

alter table multiroom_enablement_reporting_overview add borked bit default 0;

update multiroom_enablement_reporting_overview
set borked = case when enabled_boxes > 6 or reporting_boxes > enabled_boxes then 1 else 0 end;

select enabled_boxes, reporting_boxes, count(1) as accounts
from multiroom_enablement_reporting_overview
where borked = 0
group by enabled_boxes, reporting_boxes
order by enabled_boxes, reporting_boxes;

select borked, count(1), sum(enabled_boxes), sum(reporting_boxes)
from multiroom_enablement_reporting_overview
group by borked;
-- 0       525289  716756  415225
-- 1       83      517     194
-- That's a fairly small proportion to isolate, so sure.; less than 0.1% of boxes

-- Okay, first tag to add is value segments:
alter table multiroom_enablement_reporting_overview add value_segment varchar(12);

update multiroom_enablement_reporting_overview
set value_segment = t.value_seg
from multiroom_enablement_reporting_overview as mero
inner join sk_prod.VALUE_SEGMENTS_DATA as t
on mero.account_number = t.account_number;

select count(1) from multiroom_enablement_reporting_overview
where value_segment is null or len(value_segment) < 1
and borked = 0;
-- 133. Over a hundred people don't have value segments. WTF? Oh well.

update multiroom_enablement_reporting_overview
set borked = 1
where value_segment is null or len(value_segment) < 1;

-- Okay, and now we're going to pull in the lifestage stuff from SAV:

select * from sys.syscolumns where lower(cname) like '%life%'
and lower(cname) like '%stage%'
and lower(tname) like '%single_account%'
and creator = 'sk_prod';

select ilu_lifestage_desc, count(1) as hits
from sk_prod.cust_single_account_view
group by ilu_lifestage_desc
order by ilu_lifestage_desc;
-- Okay, so 15% of our accounts have no lifestage on them. Oh well.

alter table multiroom_enablement_reporting_overview add lifestage varchar(40);

update multiroom_enablement_reporting_overview
set lifestage = ilu_lifestage_desc
from multiroom_enablement_reporting_overview as mero
inner join sk_prod.cust_single_account_view as sav
on mero.account_number = sav.account_number;

-- OK, now let's see how big these spreads are:
select
        value_segment,
        lifestage,
        enabled_boxes,
        reporting_boxes,
        count(1) as accounts
from multiroom_enablement_reporting_overview
where borked = 0
group by value_segment, lifestage, enabled_boxes, reporting_boxes
order by value_segment, lifestage, enabled_boxes, reporting_boxes;

update multiroom_enablement_reporting_overview set borked = 1 where lifestage is null or len(lifestage) < 3;

-- Oh hey now the viewing sampling procedures are back online, so we can
-- make that stuff a bit easier, no?

-- okay, so now the viewing sampling stuff works, we can ideitify an
-- acocunt with a bunch of boxes, and then pull some decent samples of
-- those together for detailed stuffs:

-- Update: ahahahahano, because the too-many-concurrent-threads silent
-- fail of Sybase keeps turning up and we need to do something silly like
-- limit the date range in order to get back data at all. Should add those
-- logger calls to the thing to see if they're all failing, or at least,
-- find out if the failure is causing it to bail (and I'm guesing not as
-- it still says 0 rows updated).

-- Start with an account that has 3 boxes, 3 reporting:
select top 10 *
from  multiroom_enablement_reporting_overview as mero
inner join sk_prod.vespa_stb_log_snapshot as sls
on mero.account_number = sls.account_number
where enabled_boxes = 3 and reporting_boxes = 3
and x_no_of_logs_in_curr_month > 10
and x_days_since_last_log_sent < 3;
-- hmmmm... but are these all going to be panel = 1 people>
-- 200001376348 and 'F) Unstable'  and   '(18-24) Living with Parents'
-- 200001108220 and 'A) Platinum'  and   '(25-34) Couple (no kids)'
-- 200001544390 and 'C) Silver'    and   '(35-54) Child 5-10'
-- So that's a good spread.

select * from sk_prod.VESPA_STB_PROG_EVENTS_20110823 v
where subscriber_id = 1090144;
-- but it's an emptylog :( but it is panel 5 :D


drop table multiroom_threes_sample;
create table multiroom_threes_sample (
        subscriber_id decimal(8,0) not null primary key,
        account_number varchar(20),
        value_segment varchar(40),
        lifestage varchar(40)
);

insert into multiroom_threes_sample (subscriber_id, account_number)
select
        ss.card_subscriber_id,
        ss.account_number
from sk_prod.vespa_subscriber_status as ss
where account_number in ('200001376348', '200001108220', '200001544390');

grant select on multiroom_threes_sample to vespa_analysts;

drop table multiroom_threes_sample_viewing;

select
        *
into multiroom_threes_sample_viewing
from vespa_analysts.get_viewing_by_stb(
        'stafforr.multiroom_threes_sample',
        '2011-07-01',
        '2011-09-01'
);
-- takes only a minute or so! but... not getting anything back.
-- but we've run some tests and the items are indeed Panel=5,
-- and the EPG entries do exist as required, it's just that...
-- Sybase isn't giving us anythign back. We've checked the loop
-- conditions, the argument passing order... nothing sane :(

-- Yeah, okay, I think Sybase is silently failing on the too many
-- threads errors we were getting earlier. Yeah, if you split it
-- up month by month then it doesn't fail quite so badly. Which
-- is a shame, means Sybase figured out a way to optimise stuff
-- so that it doesn't work :(

-- Okay, so, item added, and now, we've got the sample stuff for
-- the last two months. more than that we don't have... wait,
-- we should also check that we've got useful stuff...

select * from multiroom_threes_sample_viewing;

-- But I'm still only pulling back stuff from the first two days....
-- Still think Sybase is failing silently here somewhere. It's
-- just crazy :(

