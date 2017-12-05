-- Okay, so we need to build a day-by-day panel completeness mertic variant, and see how that changes
-- when the data return window is truncated at various times between 2AM and 6AM. There's a bunch of
-- reporting from Midnight onwards, so we're going to start with at least a decent chunk of things.

-- We can still to the panel indexing and use SBV for a sufficiently close day, that'll help a bit by
-- already having boxes and households in segments. The downside wil be tracking when households get
-- the report from their last box, and then rebuilding all the metrics and indices for each point in
-- time.

-- We can kind of fudge towards the ralistic numbers by restricting the population to boxes which are
-- already regularly reporting; we already have those things on SBV so it'd be easy to build...

-- Update: The next trick is to redo all of this by the scaling variables only. Fortunately that
-- just turns out to be doing the same thing but grouping by one less variable, right? Except that
-- we've also got to summarise the sky base etc differently because there it goes by both keys.

-- OK, so we're going to do analyst the Wednesday before the most recent SBV build etc:
create variable @analysis_day date;
select @analysis_day = max(cb_data_date) from sk_prod.cust_single_account_view;
-- ??? now it's 2012-03-15

-- Pull out all the logs for this day:
select subscriber_id, account_number, document_creation_date
into V049_log_collection_dump
from sk_prod.VESPA_STB_PROG_EVENTS_20120305
where panel_id = 4 and document_creation_date is not null and event_type <> 'evEmptyLog'
group by subscriber_id, account_number, document_creation_date;

commit;

-- We also need empty logs that get submitted the next day:
insert into V049_log_collection_dump
select subscriber_id, account_number, document_creation_date
from sk_prod.VESPA_STB_PROG_EVENTS_20120306
where panel_id = 4 and document_creation_date is not null and event_type = 'evEmptyLog'
group by subscriber_id, account_number, document_creation_date;

commit;

-- OK, cull those that are outside the range we care about:
delete from V049_log_collection_dump
where document_creation_date > '2012-03-06 6:00:00'
or document_creation_date < '2012-03-05 7:00:00';

commit;

-- What is our distribution looking like then?
select
        convert(date,document_creation_date) as daysection
        ,datepart(hour,document_creation_date) as hourbit
        ,count(distinct subscriber_id) as hits
from V049_log_collection_dump
group by daysection, hourbit
order by daysection, hourbit;

-- OK, so now somehow we have to find the time when each household returns it's last box
-- worth of data. Also need to check SBV to validate that these are houses returning *all*
-- their data, not just like 4 / 6 boxes or something.

select account_number
    ,count(distinct subscriber_id) as boxes_returned
    ,max(document_creation_date) as last_logs_received
    ,convert(tinyint, null) as expected_boxes
    ,convert(int, null) as scaling_segment_ID
    ,convert(int, null) as non_scaling_segment_ID
into V049_household_reporting
from V049_log_collection_dump
group by account_number;

commit;
create unique index fake_pk         on V049_household_reporting (account_number);
create        index for_filtering   on V049_household_reporting (last_logs_received, scaling_segment_ID, non_scaling_segment_ID);

-- wait, nope, we're not going to single box view, we're going to go to the PanMan intermediate
-- tables (just make sure this is run after PanMan and before PanMan table reset!) since that
-- already has the box count and the segmentation IDs etc already on it:
update V049_household_reporting
set expected_boxes          = pm.hh_box_count
    ,scaling_segment_ID     = pm.scaling_segment_ID
    ,non_scaling_segment_ID = pm.non_scaling_segment_ID
from V049_household_reporting
inner join vespa_analysts.Vespa_PanMan_all_households as pm
on V049_household_reporting.account_number = pm.account_number;

commit;

-- Are these numbers sensible?
select boxes_returned, expected_boxes, count(1) as accounts
from V049_household_reporting
group by boxes_returned, expected_boxes
order by boxes_returned, expected_boxes;
-- Yeah, mostly looks sensible. Still a bunch of things returning data we don't expect
-- and cases where more boxes return data than are enabled, but whatever.

delete from V049_household_reporting
where expected_boxes is null
or boxes_returned < expected_boxes
or expected_boxes is null;
-- Hey, if we need to get them back at all, we've still got everything in the log dump.

-- Okay, so now the plan: we can get all the segmentation targets this week from the PanMan
-- build, off the segmentation summary table. Then we can build a (huge!?) table of the
-- various clicks through the evening, and the sky base populations at each point, then
-- calculate the indices for them (stealing code from the PanMan) and it hopefully won't be
-- toooo messy? Still going to have to clip the illegals out of the log dump before we report
-- with them tough...

-- Okay, up to here is the same for non-scaling-variable inclusion or not. Now to patch in
-- the not-inclusion-of-non-scaling... OK, that's done, so now we just reset these tables and
-- continue to run from here:
drop table V049_segmentation_base_targets;
drop table V049_reporting_at_cutoffs;
drop table V049_panel_size_at_cutoffs;
drop table V049_one_day_completeness_results_scaling_only;

commit;

-- First step is to archive the segmentation and segment base targets relevant to this week
-- so that we can continue this reporting later when the report is refreshed... Oh hey, we've
-- already got the segmentations, now we just need the targets:
select
    scaling_segment_ID
    ,sum(Sky_Base_Households) as Sky_Base_Households
into V049_segmentation_base_targets
from vespa_analysts.Vespa_PanMan_Scaling_Segment_Profiling
group by scaling_segment_ID;
-- 28105

create unique index fake_pk on V049_segmentation_base_targets (scaling_segment_ID);

commit;

-- Build the thing that'll contain all the partial results structures we build...
create table V049_reporting_at_cutoffs (
    scaling_segment_ID          int         not null
    ,reporting_cutoff           datetime    not null
    ,reporting_count            int
    ,reporting_index            float       default null
    ,primary key (scaling_segment_ID, reporting_cutoff)
);

-- We're also going to need a separate table of how many boxes we've got reporting after
-- each point, but that'll be easy to do after this guy is built. Also, the PK facilitates
-- both the uniqueness condition and also the join into the segmentation base targets, so,
-- sweet.

create variable @reporting_cutoff   datetime;

-- Start at midnight then we'll click through till 7AM in 10 minute clicks, for 56 data points.

delete from V049_reporting_at_cutoffs;
set @reporting_cutoff = '2012-03-06 0:00:00';

while @reporting_cutoff <= '2012-03-06 7:00:00'
begin
    insert into V049_reporting_at_cutoffs (
        scaling_segment_ID
        ,reporting_cutoff
        ,reporting_count
    )
    select 
        scaling_segment_ID
        ,@reporting_cutoff
        ,count(1)
    from V049_household_reporting
    where last_logs_received <= @reporting_cutoff
    group by scaling_segment_ID
    
    set @reporting_cutoff = dateadd(mi,10, @reporting_cutoff)
    
    commit
end;

create index for_joining on V049_reporting_at_cutoffs (reporting_cutoff);

-- Ok, how much data did we build?
select reporting_cutoff
    ,count(1) as segments
    ,sum(reporting_count) as reporting_households
into V049_panel_size_at_cutoffs
from V049_reporting_at_cutoffs
group by reporting_cutoff
order by reporting_cutoff;
/* reporting_households agrees with full build, so that's good
2012-03-06 00:00:00.000000	155	164
2012-03-06 00:10:00.000000	161	170
2012-03-06 00:20:00.000000	170	179
2012-03-06 00:30:00.000000	181	191
2012-03-06 00:40:00.000000	3162	5922
2012-03-06 00:50:00.000000	4626	12046
2012-03-06 01:00:00.000000	5548	18373
2012-03-06 01:10:00.000000	6330	25127
...snip...
2012-03-06 05:00:00.000000	12072	188714
2012-03-06 05:10:00.000000	12094	189615
2012-03-06 05:20:00.000000	12110	190343
2012-03-06 05:30:00.000000	12124	190770
2012-03-06 05:40:00.000000	12140	191238
2012-03-06 05:50:00.000000	12160	191759
2012-03-06 06:00:00.000000	12181	192548
2012-03-06 06:10:00.000000	12181	192548
2012-03-06 06:20:00.000000	12181	192548
2012-03-06 06:30:00.000000	12181	192548
2012-03-06 06:40:00.000000	12181	192548
2012-03-06 06:50:00.000000	12181	192548
2012-03-06 07:00:00.000000	12181	192548
*/

create unique index fake_pk on V049_panel_size_at_cutoffs (reporting_cutoff);

select count(1) from V049_household_reporting;
-- 192548

select count(1) from (select distinct scaling_segment_ID from V049_household_reporting) as t;
-- 12181

-- Okay, sweet, so those totals all add up. But with the number of segments with just one box in it,
-- we might want to rethink doing this by only the scaling variables because almost everyone is in
-- a segment by themselves...
select reporting_count
        ,count(1) as hits
from V049_reporting_at_cutoffs
where reporting_cutoff = '2012-03-06 07:00:00.000000'
group by reporting_count
order by reporting_count;
-- Heh, yeah, for scaling only it's a lot more stable, though not hugely; vast majority of segments
-- still have 20 or fewer boxes in them.

-- And also, that whole reporting cutoffs table is still only 70MB, a bit under 4m rows, so it
-- will all be awesome and fast to process?

-- Now make all of the indices, based on code stolen from the PanMan report...

create variable @total_sky_base int;
select @total_sky_base = sum(Sky_Base_Households) from V049_segmentation_base_targets;

update V049_reporting_at_cutoffs
set reporting_index           = -- *sigh* there's no GREATEST / LEAST operator in this DB...
        case when 200 < 100.0 * V049_reporting_at_cutoffs.reporting_count * @total_sky_base / convert(float, sbt.Sky_Base_Households) / psac.reporting_households
                then 200
        else            100.0 * V049_reporting_at_cutoffs.reporting_count * @total_sky_base / convert(float, sbt.Sky_Base_Households) / psac.reporting_households
      end
from V049_reporting_at_cutoffs
inner join V049_panel_size_at_cutoffs as psac
on      V049_reporting_at_cutoffs.reporting_cutoff          = psac.reporting_cutoff
inner join V049_segmentation_base_targets as sbt
on      V049_reporting_at_cutoffs.scaling_segment_ID        = sbt.scaling_segment_ID
;
-- ok, so, done. Are the indices any good at all?
select reporting_cutoff,
    sum(case when reporting_index <= 80 then 1 else 0 end) as low
    ,sum(case when reporting_index < 120 then 1 else 0 end) as good
    ,sum(case when reporting_index >= 120 then 1 else 0 end) as high
from V049_reporting_at_cutoffs
group by reporting_cutoff
order by reporting_cutoff;
-- Fascinating, so a bunch of stuff gets massively overindexed, probably just because
-- of how many segments we're trying to work with.

-- Next step: putt out the completeness metrics and panel size for each point in time:
-- can't quite do this with the left join as we need to duplicate all the sky base
-- targets by the reporting time, and that number is only on the reporting at cutoffs,
-- but we can still loop a thing.

create table V049_one_day_completeness_results_scaling_only (
    reporting_cutoff                datetime    primary key
    ,effective_panel_size           int
    ,represented_households         int
    ,one_day_completeness_metric    float
);

delete from V049_one_day_completeness_results_scaling_only;
set @reporting_cutoff = '2012-03-06 0:00:00';

while @reporting_cutoff <= '2012-03-06 7:00:00'
begin

    insert into V049_one_day_completeness_results_scaling_only
    select
        @reporting_cutoff
        ,sum(rac.reporting_count)
        ,sum(case when rac.reporting_index > 80 then sbt.Sky_Base_Households else 0 end)
        ,sum(case when rac.reporting_index > 80 then sbt.Sky_Base_Households else 0 end) / convert(float, @total_sky_base)
    from V049_reporting_at_cutoffs as rac
    left join V049_segmentation_base_targets as sbt
    on rac.scaling_segment_ID = sbt.scaling_segment_ID
    where reporting_cutoff = @reporting_cutoff

    
    set @reporting_cutoff = dateadd(mi,10, @reporting_cutoff)
    
    commit
end;

-- So let's grab the resuls into Excel!

select * from V049_one_day_completeness_results_scaling_only
order by reporting_cutoff;
-- Yup, that peaks at about 68.2%, significantly better than the non-scaling stuff in
-- there too, but still not awesome. And we're done! Just need to put that into Excel.

-- Oh hey, in case someone else also needs to review this stuff:

grant select on V049_log_collection_dump            to greenj, dbarnett, jacksons, stafforr, jchung, sarahm, poveys, gillh;
grant select on V049_household_reporting            to greenj, dbarnett, jacksons, stafforr, jchung, sarahm, poveys, gillh;
grant select on V049_segmentation_base_targets      to greenj, dbarnett, jacksons, stafforr, jchung, sarahm, poveys, gillh;
grant select on V049_reporting_at_cutoffs           to greenj, dbarnett, jacksons, stafforr, jchung, sarahm, poveys, gillh;
grant select on V049_panel_size_at_cutoffs          to greenj, dbarnett, jacksons, stafforr, jchung, sarahm, poveys, gillh;
grant select on V049_one_day_completeness_results   to greenj, dbarnett, jacksons, stafforr, jchung, sarahm, poveys, gillh;
-- Oh hey we'll put the scaling only in a different table so that we've still got the old results around should we need it:
grant select on V049_one_day_completeness_results_scaling_only to greenj, dbarnett, jacksons, stafforr, jchung, sarahm, poveys, gillh;
