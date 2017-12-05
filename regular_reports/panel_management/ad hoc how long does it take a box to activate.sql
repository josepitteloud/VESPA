-- Okay, so at the end of this guy we actually derive a metric which we want to build
-- to help determine which boxes are returning data all good. We'll build it into SBV,
-- and provide some docs etc too.

create variable @SQL_daily_kludge       varchar(2000);
create variable @scanning_day           date;

drop table reporting_reconciliation;
create table reporting_reconciliation (
    subscriber_id       bigint          not null
    ,dateoflogging      date            not null
);
-- Don't care about differences relating to 6AM shuffle or playback issues
-- or whatever at this point, we just want to se a bunch of new subscribers
-- turn up. Not everyone will be timeshifting stuff into weeks past? Hopefully.

-- This guy couldn't be a parameterised query anyway, since we're changing the
-- source table on each loop iteration
set @SQL_daily_kludge = 'insert into reporting_reconciliation (
        subscriber_id
        ,dateoflogging
)
select distinct
        subscriber_id
        ,''#*£%%£*#''
from sk_prod.VESPA_STB_PROG_EVENTS_#*££*# -- will get replaced by the daily stamp of each table
where panel_id = 4
';

-- We have panel 4 enablements on 08/11/2011, 15/11/2011, 18/11/2011, 24/11/2011 and 26/01/2012
-- so we want to trap daily events to figure out exactly when those boxes start reporting:
set @scanning_day = '2011-11-01';
while @scanning_day < '2012-03-01'
begin

        -- poke that date into the SQL chunk and get the daily data
        execute(replace(replace(@SQL_daily_kludge, '#*££*#', dateformat(@scanning_day,'yyyymmdd'))
            ,'#*£%%£*#', dateformat(@scanning_day,'yyyy-mm-dd')))

        -- Move on to the next daily table
        set @scanning_day = dateadd(day, 1, @scanning_day)
        
        commit
end
;
-- so that table tops out at about 30m records, taking a bit under 10 minutes to process
alter table reporting_reconciliation add primary key (subscriber_id, dateoflogging);

-- Now with all the daily scan items added, let's form the listing of the first time
-- each box returns data...

select subscriber_id
    ,min(dateoflogging) as first_report
into first_box_reporting
from reporting_reconciliation
group by subscriber_id;
-- 465,597 boxes logged here

commit;
create unique index fake_pk on first_box_reporting (subscriber_id);
create index for_search on first_box_reporting (first_report);
commit;

-- Getting profile of how many boxes started reporting over time ...
--select first_report, count(1) as new_boxes
--from first_box_reporting
--group by first_report
--order by first_report desc;

-- OK, we want the left join starting with SBV so we can see boxes
-- that have never reported...
drop table reporting_uptakes;

select
    sbv.subscriber_id
    ,sbv.account_number
    ,fbr.first_report
    ,sbv.enablement_date
    ,datediff(day, sbv.enablement_date, fbr.first_report) as reporting_delay
    ,convert(tinyint, null) as logs_returned_in_30d -- this guy isn't actually in the Vespa_analysts SBV, it's still in the RS isolated build...
into reporting_uptakes
from vespa_analysts.vespa_single_box_view as sbv
left join first_box_reporting as fbr
on sbv.subscriber_id = fbr.subscriber_id
where sbv.closed_loop_enabled = 1;
-- 615,958 - which is what we expected

commit;
create unique index fake_pk on reporting_uptakes (subscriber_id);

-- Rememeber also to discard the first couple of days or reporting behaviour
-- since at the start of the period, everything will show up new.
select reporting_delay, count(1) as hits
from reporting_uptakes
where enablement_date = '2011-11-24'
group by reporting_delay
order by reporting_delay
;

-- erm... so... looks like half the time it takes 20 days or so for the majority
-- of the boxes to start returning data? need to go out to 36 days to get 95% of
-- boxes who will ever return data...

update reporting_uptakes
set logs_returned_in_30d = sbv.logs_returned_in_30d
from reporting_uptakes
inner join stafforr.vespa_single_box_view as sbv
on reporting_uptakes.subscriber_id = sbv.subscriber_id;

-- OK, now, what are those numbers like for boxes that are good at returning data?

select reporting_delay, count(1) as hits
from reporting_uptakes
where first_report >= '2011-11-06'
and logs_returned_in_30d >= 25
group by reporting_delay
order by reporting_delay
;

-- And the other part: how many boxes on Vespa right now currently "Somewhat Regularly" report data?
select count(1) as total,
    sum(case when logs_returned_in_30d >= 25 then 1 else 0 end) as good_reporters
from reporting_uptakes;
-- 615958	213738

-- notice this goes by boxes and not accounts... need to do it by accounts for scaling...
select count(distinct account_number)
from reporting_uptakes;
-- 411,212 is the current total Vespa account panel

select count(1) from
(
select
    account_number
    ,min(logs_returned_in_30d) as worst_reporting
from reporting_uptakes
group by account_number
having worst_reporting >= 25
) as t
-- 118755 - these are the households with somewhat reliable reporting. So then... doesn't
-- match what we're seeing from the PanMan report current build, this number is a lot better...

-- Okay, so the other thing we need here, is to ask, out of those boxes which are currently
-- reporting data well, what proportion of them returned data for the first 7 days after they
-- first report? IE, we wait 15 days, we then expect 95% to be enabled, and then we wait...
-- how long to gather the reporting metrics?

-- OK, so we'll patch on days-since-activation only for the boxes that are good... though
-- juggling the MR is still going to be a pain...

-- First, identify the good account numbers:
select
    account_number
    ,min(logs_returned_in_30d) as worst_reporting
    ,max(reporting_delay) as largest_reporting_delay
into responsive_accounts
from reporting_uptakes
where enablement_date = '2011-11-24'
group by account_number
having worst_reporting >= 25
and largest_reporting_delay < 16
-- 41079 accounts... not a whole lot really, but kind of in line with the 10% we calculated,
-- given that there were a bit over 300k boxes enabled that day.

commit;
create unique index fake_pk on responsive_accounts (account_number);

-- Putting this in a new table so as to not have many joins on these updates
select
    ru.subscriber_id
    ,ru.account_number
    ,rr.dateoflogging
    ,ru.first_report
    ,datediff(day, ru.first_report, rr.dateoflogging) as days_since_first_report
into Reporting_from_inits
from reporting_uptakes as ru
inner join responsive_accounts as racs
on ru.account_number = racs.account_number
inner join reporting_reconciliation as rr
on ru.subscriber_id = rr.subscriber_id;
-- Also, we're just filtering it to the accounts we've decided are responsive enough

select count(distinct subscriber_id) as boxes, count(distinct account_number) as accounts from Reporting_from_inits;
-- 48848   41079

select days_since_first_report
        ,count(1) as boxes
        ,count(distinct account_number) as accounts
from Reporting_from_inits
group by days_since_first_report
order by days_since_first_report;
/* So it kind of waxes a little then picks up again, interesting. Drops by maybe 4% to recover later...
0       48848   41079
1       48320   40803
2       48119   40694
3       48042   40658
4       47956   40587
5       47937   40559
6       47896   40529
7       47889   40521
8       47876   40527
9       47834   40493
10      47750   40423
11      47759   40414
12      47755   40413
13      47724   40389
14      47677   40354
15      47616   40328
16      47597   40302
17      47546   40266
18      47527   40250
19      47485   40216
20      47500   40224
21      47432   40190
22      47438   40180
23      47395   40137
24      47443   40177
25      47470   40199
26      47386   40160
27      47414   40192
28      47387   40152
29      47457   40231
30      47505   40221
.
.
.
*/

-- but the real question is how many boxes that aren't reliable report
-- for how many days before revealing their unreliability...

alter table reporting_reconciliation
add days_since_first_report int,
add account_number varchar(20),
add good_account bit default 0,
add first_reporting date;

update reporting_reconciliation
set account_number = ru.account_number
        ,first_reporting = ru.first_report
from reporting_reconciliation
inner join reporting_uptakes as ru
on reporting_reconciliation.subscriber_id = ru.subscriber_id;

commit;
create index for_joins on reporting_reconciliation (account_number);
commit;

update reporting_reconciliation
set good_account = 1
from reporting_reconciliation
inner join responsive_accounts as ra
on reporting_reconciliation.account_number = ra.account_number;

update reporting_reconciliation
set days_since_first_report = datediff(day, first_reporting, dateoflogging)
;

-- So, do we have everything we need?
select top 10 * from reporting_reconciliation
-- some don't have account number? Right, account number only for active Vespa boxes.
-- And we only care about Vespa oxes right now, correct?
select count(distinct account_number) as households, count(distinct subscriber_id) as boxes
from reporting_reconciliation
where account_number is not null;
-- 366538 and 434298. Those are close enough to Vespa Panel, we happy...

delete from reporting_reconciliation where account_number is null;

select days_since_first_report
    ,good_account
    ,count(1) as boxes
    ,count(distinct account_number) as accounts
from reporting_reconciliation
where days_since_first_report < 60
group by days_since_first_report, good_account
order by days_since_first_report, good_account;


select days_since_first_report
    ,good_account
    ,count(1) as boxes
    ,count(distinct account_number) as accounts
from reporting_reconciliation
where days_since_first_report < 60
group by days_since_first_report, good_account
order by days_since_first_report, good_account;

-- So by day 15, some of the boxes have fallen off their reporting by about 1/4...
-- so if we take the number of distinct accounts reporting somewhere between 13 and
-- 17 days from initial... we'd (hope) to see the number of distinct items of good
-- accounts be about 1/5th of the total, whereas for not good accounts we'd expect
-- significantly more than that (duplication etc...)

-- Totals in pool: everyone has a first log...
select good_account, count(1), count(distinct account_number) from reporting_reconciliation
where days_since_first_report = 0
group by good_account
order by good_account;
/*
0       385450  325459
1       48848   41079
*/

select good_account,
        count(1) as total_reports,
        count(1) / 5.0 as normed_reports,
        count(distinct subscriber_id) as distinct_boxes
from reporting_reconciliation
where days_since_first_report between 13 and 17
group by good_account
order by good_account;
/* So... not as pronounced as I'd hoped?
0       1371124         274224.80       296261
1       238160          47632.00        48429
*/

-- OK, so how many boxes are reliably reporting for the first 30 days that don't end up in our cut?
select good_account, count(1) as boxes
from (
    select good_account,
            subscriber_id
    from reporting_reconciliation
    where days_since_first_report <= 30
    group by good_account, subscriber_id
    having count(1) > 25
) as t
group by good_account
order by good_account;
/*
0       247420
1       47140
*/
-- erm, heh, a lot; ok, so let's try to limit this to the guys that were activated on the 24th...

alter table reporting_reconciliation
add enablement_date     date;

update reporting_reconciliation
set enablement_date = ru.enablement_date
from reporting_reconciliation
inner join reporting_uptakes as ru
on reporting_reconciliation.subscriber_id = ru.subscriber_id;

-- Should be NUL because we've already limited it to Vespa?
select count(1) from reporting_reconciliation where enablement_date is null;
-- 0, sweet

-- So now let's try it again with that particular sample...
select good_account, count(1) as boxes
from (
    select good_account,
            subscriber_id
    from reporting_reconciliation
    where days_since_first_report <= 30
    and enablement_date = '2011-11-24'
    group by good_account, subscriber_id
    having count(1) > 25
) as t
group by good_account
order by good_account;
/*
0       107823
1       47002
*/

-- Okay, and what are those proportions out of?
select good_account, count(1), count(distinct account_number) from reporting_reconciliation
where days_since_first_report = 0 and enablement_date = '2011-11-24'
group by good_account
order by good_account;
/* OK, so for the accounts we ended up wanting, we have 96% of them returning sufficient
** data in their first 30 days, and for boxes we don't target, it's only 66%
0       152035  115654
1       48666   41079
*/

select good_account, count(1) as boxes
from (
select good_account,
        subscriber_id
from reporting_reconciliation
where days_since_first_report <= 15
and enablement_date = '2011-11-24'
group by good_account, subscriber_id
having count(1) > 12
) as t
group by good_account
order by good_account;
/* So 13 or more in first 15 days permits significant culling...
0       115993
1       47567
*/

select good_account, count(1) as boxes
from (
select good_account,
        subscriber_id
from reporting_reconciliation
where days_since_first_report <= 10
and enablement_date = '2011-11-24'
group by good_account, subscriber_id
having count(1) > 8
) as t
group by good_account
order by good_account;
/* Reporting on 9 or 10 of first 10 days still kind of works...
0       119818
1       47736
*/

select good_account, count(1) as boxes
from (
select good_account,
        subscriber_id
from reporting_reconciliation
where days_since_first_report <= 7
and enablement_date = '2011-11-24'
group by good_account, subscriber_id
having count(1) > 5
) as t
group by good_account
order by good_account;
/* Yeah, 6 or 7 replies in the first week doesn't really sort it out so much...
0       126331
1       48101
*/

-- Looks like we're going to need a reporting quality coeficient, for proportion of
-- days reporting has occured in last 30 days (for boxes that first reported 30 days
-- ago), or proportion of days reporting has occured since box first reported (for
-- boxes that first reported between 15 and 29 days ago inclusive), otherwise
-- defaulting to zero for boxes that were enabled more than 30 days ago (expect
-- boxes to report after 15 days after enablement, after which it should show up on
-- the 15-29 scan), finally defaulting to null (either box hasn't been enabled for
-- long, or box has only just started reporting).

-- Then it's this metric that we want to drive the selection of boxes. And by the
-- time we get 30 days from box enablement, we'll have a rating for almost all boxes.
-- Except then we need to convert that into a rating for households... going to have
-- to figure out the null treatment there too, though just min(.) would do it...

