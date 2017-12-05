/******************************************************************************
**
** PROJECT VESPA: PANEL BALANCE
**
** Refer to 
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=68
**
** This script makes allthe decisions about panel balance, though we're cloning
** a bit of code both from panel management report and maybe als othe other
** enablement thing, though we never got as far as doing any actual balance work
** there.
**
** The data isn't coming from daily tables, but rather an external source that
** gets loaded and managed in "box dialback data import.sql" and also, the
** segmentation we grab from the scaling 2 build of 2012-07-19, and that happens
** in the script "Panel balance stuff.sql" so consider both of those dependencies.
**
** Oh, so we want to have 650-750K *boxes* dialing back nightly, and at least
** 500k households dialing back each day. We're also maximising with respect to
** two different metrics (data return, segment coverage) while minimising actual
** boxes transferred and balancing variable profiles against the Sky base. So,
** four opposing constraints, that's not all that bad.
**
** Oh, and we probably want to keep all the Anytime+ accounts on the panel too.
** Do we have a separate flag for that? Given that the SBV version is not good,
** maybe we talk to Tony to get flags from his version? There's probably a pull
** from the customer DB we can use... wait, no, because we're going by account
** and the account level flags are still good.
**
** Thus far normalising /balancing against non-scaling segments is out of scope.
**
** We're also grabbing a bunch of code and structures and flow from
**      \V059_Panel_enablement_listing\ad hoc initial anytime+ enablement targeting.sql
** since we're kind of following a similar plan to what we did there. I think.
**
******************************************************************************/

/************ INITIAL QA AND STUFF ***********/

-- We want to know how many boxes we've got reporting, what proportion of the
-- Sky base has any representation at all, what the overall balance of decent
-- boxes is. Basically, where are we now and how far can we expect to be able
-- to go?

-- This is the table delivered by "box dialback data import.sql"
select panel, reporting_categorisation, count(1) as accounts
from V068_data_return_by_account
group by panel, reporting_categorisation
order by panel desc, reporting_categorisation;
/* Okay, so there's a bit to work with, and immediately a bunch of badly
** reporting stuff to kick out of the mix.
12,'Acceptable    ',433952
12,'Unreliable    ',258908
12,'Zero reporting',168813
7,'Acceptable    ',230970
7,'Unreliable    ',51018
7,'Zero reporting',87795
6,'Acceptable    ',237254
6,'Unreliable    ',43972
6,'Zero reporting',88717
*/


-- Anytime+ <- we actually really need this, so let's grab the Anytime+ flags
-- at account level... not from SBV... but we probably want this for the whole
-- Sky base. OK then.
alter table V068_sky_base_profiling
add anytime_household       bit     default 0;

SELECT     account_number
          ,min(first_activation_dt)                   as anytimeplus_activation_dt
          ,count(distinct first_activation_dt)        as num_anytimeplus_activations 
into    #anytime_accounts
FROM       sk_prod.CUST_SUBS_HIST
WHERE      subscription_sub_type='PDL subscriptions'  --anytime plus subscription
AND        status_code='AC'
AND        first_activation_dt<'9999-09-09'         -- (END)
AND        first_activation_dt>='2010-10-01'        -- (START) Oct 2010 was the soft launch of A+, no one should have it before then
and        effective_from_dt <= '2012-07-19'        -- 2012-07-19 is our profiliing date
and        effective_to_dt <= '2012-07-19'
AND        account_number is not null
AND        account_number <> '?'
GROUP BY   account_number;
COMMIT;
create unique index fake_pk on #anytime_accounts (account_number);
commit;

update V068_sky_base_profiling
set anytime_household = 1
from V068_sky_base_profiling
inner join #anytime_accounts as aa
on V068_sky_base_profiling.account_number = aa.account_number;
commit;

-- Ok, now we can profile on anytime+ accounts

/************ INITIAL PROFILING ***********/

-- Okay, so here we're treating the initial balance questions. To do that,
-- we should throw all stuff onto a base table or something...

select reporting_categorisation
    ,count(1)           -- Anytime+ accounts
    ,sum(hh_box_count)  -- boxes in anytime+ homes (not all will be Anytime+)
from V068_sky_base_profiling as sbp
inner join V068_data_return_by_account as drba
on sbp.account_number = drba.account_number
where anytime_household = 1
group by reporting_categorisation
order by reporting_categorisation
/* So a chunk are acceptable, but we'll also need some unreliable ones too.
** Grab all of them, that gives us 340k Anytime+ accounts though not all of
** them are reliably returning data, but it's probably enough. Hopefully?
'Acceptable    ',212195,241627
'Unreliable    ',128709,242297
'Zero reporting',75194,103674
*/

-- We also need a segment level view of things: for which we need to basically
-- rebuild all of the panel management report... Are we going to try to map it
-- into exactly the structures that panel management used? Why not?

-- Well, first, some quick overall counts of how the stuff fits together: get
-- us a table of segments and counts and suchlike:
select sbp.scaling_segment_id
    ,min(universe) as universe
    ,count(1) as sky_base_accounts
    ,sum(case when drba.reporting_categorisation = 'Acceptable' then 1 else 0 end) as Vespa_acceptables
    ,sum(case when drba.reporting_categorisation = 'Unreliable' then 1 else 0 end) as Vespa_unreliables
into #unavailability_checks
from V068_sky_base_profiling as sbp
left join V068_data_return_by_account as drba
on sbp.account_number = drba.account_number
group by sbp.scaling_segment_id;

commit;
create unique index fake_pk on #unavailability_checks (scaling_segment_id);
commit;

select sum(sky_base_accounts)
    ,sum(Vespa_acceptables)
    ,sum(Vespa_unreliables)
from #unavailability_checks;

-- OK, so what are the basic unavailability numbers like?
select
    universe
    ,sum(case when Vespa_acceptables = 0 then 1 else 0 end)                      / convert(float,count(1))                   as Inaccessible_proportion_of_segments
    ,sum(case when Vespa_acceptables = 0 then sky_base_accounts else 0 end)                                                 as Inaccessible_count_of_base
    ,sum(case when Vespa_acceptables = 0 then sky_base_accounts else 0 end)     / convert(float, sum(sky_base_accounts))    as Inaccessible_proportion_of_base
    -- Our normalistion factor is 17.15 because there are 9434629 on the sky base and we're aiming
    -- for a panel of 550k acounts acceptably reporting so if there are cases where the Sky Base
    -- is more than 30x the number of Vespa panel accounts, these won't be well represented by
    -- any selection.
    ,sum(case when Vespa_acceptables * 30 < sky_base_accounts then 1 else 0 end) / convert(float,count(1))                   as Underrepresented_proportion_of_segments
    ,sum(case when Vespa_acceptables * 30 < sky_base_accounts then sky_base_accounts else 0 end)                             as Underrepresented_count_of_base
    ,sum(case when Vespa_acceptables * 30 < sky_base_accounts then sky_base_accounts else 0 end) / convert(float, sum(sky_base_accounts)) as Underrepresented_proportion_of_base
from #unavailability_checks
group by universe
order by universe;
-- Overall: numbers are large, but hopefully not too bad? The underrepresented is inclusive of the
-- inaccessibles, so there's some 5% of the base on which we have no vilisbility, and another 12%
-- which we won't be able to get well scaled results. But by universe... yeah, dual and multiroom
-- hoseholds get a bit of a shafting. Dunno what we'll do re: the stratified sampling. Let's first
-- exile off the Vespa panel anything which goes past the thresholds we want for stratified sampling.

/************ BASIC STRATIFIED SAMPLING: APPLYING METADATA ***********/

-- So the first trick is to cull boxes from the Vespa panel that are overrepresenting particular
-- segments. Doing it this way round to minimise the panel flux. Then afterwards we'll run through
-- the PanMan stats to see what it gives us. But first, that segment level view is acutally a
-- thing we need to keep aorund long term, but just for the Vespa panel
select sbp.scaling_segment_id
    ,min(universe) as universe
    ,count(1) as sky_base_accounts
    ,sum(case when drba.reporting_categorisation = 'Acceptable' then 1 else 0 end) as Vespa_acceptables
    ,sum(case when drba.reporting_categorisation = 'Unreliable' then 1 else 0 end) as Vespa_unreliables
    ,convert(int, null) as target_pop
    ,convert(int, null) as diff         -- Number of Vespa accounts to cull
into V068_segment_level_view
from V068_sky_base_profiling as sbp
left join V068_data_return_by_account as drba
on sbp.account_number = drba.account_number and drba.panel = 12
group by sbp.scaling_segment_id;

commit;
create unique index fake_pk on V068_segment_level_view (scaling_segment_id);
commit;

update V068_segment_level_view
set target_pop = ceil(sky_base_accounts / 18.5) -- making these slightly smaller since ceil() always increases the panel size a bit
    ,diff = Vespa_acceptables - ceil(sky_base_accounts / 18.5)
;

commit;

-- OK, are there things to cull?
select count(1), sum(diff) 
from v068_segment_level_view
where diff > 0;
-- Yeah, about 81k worth of box space made. Oh, plus all those unreliable accounts. Mainly:
-- there are this many accounts which we'd cull for panel balance even though they report
-- good.

select sum(target_pop) from v068_segment_level_view;
-- 557k target accounts... that's a bit more than we want, but hey, we're not going to find
-- them all, and the hard limit is 750k boxes returning data, so it'll play.

-- OK, then we clip the other unacceptably and zero reporting items out of Vespa, and see what
-- we can find in the alternate panels...

-- OK, let's grab everything which meets these segmentation needs... wait... we first need the
-- sequencing order on all of these boxes...
alter table V068_data_return_by_account
    add scaling_segment_id      bigint
    ,add accno_SHA1             varchar(40)
    ,add ranking_within_segment int
    ,add ranking_within_panel   int
    ,add selection_round        int         -- kind of more a deselection round, but they're lists of stuff we're focusing on at each point
;

commit;

update V068_data_return_by_account
set scaling_segment_id = sbp.scaling_segment_id
from V068_data_return_by_account
inner join V068_sky_base_profiling as sbp
on V068_data_return_by_account.account_number = sbp.account_number;

update V068_data_return_by_account
set accno_SHA1 = sha1.accno_SHA1
from V068_data_return_by_account
inner join vespa_analysts.Vespa_PanMan_SHA1_archive as sha1
on V068_data_return_by_account.account_number = sha1.account_number;

commit;

select count(1) from V068_data_return_by_account
where scaling_segment_id is null;
-- 13k guys that aren't in our profiling, awesome. What to do about those? Just leave them
-- wherever I guess.

select count(1) from V068_data_return_by_account
where accno_SHA1 is null;
-- just 1. So, update him... Oh wait, it's account number NULL, so yeah, I guess we don't
-- care about him so much. Never actually checked that the account numbers weren't NULL,
-- so whatever. They're getting cleansed out anyway.

delete from V068_data_return_by_account
where accno_SHA1 is null;

commit;

-- OK, now with things in play, let's rank the accounts within each segmentation:
-- oh but yeah Sybase can't rank() in an update, have to do it separately...
create index for_ranking on V068_data_return_by_account (scaling_segment_id, min_reporting_quality);
commit;

-- This ranking is for deciding to throw out of panels...
select 
    account_number,
    rank() over (partition by scaling_segment_id
                order by min_reporting_quality desc,
                         max_reporting_quality desc,
                         recent_enablement,                     -- older boxes better
                         accno_sha1                             -- tiebreaker
                ) as ranking_within_segment
into #ranking_update
from V068_data_return_by_account;

-- This ranking is overall to help us figure out what to grab from other panels..
select 
    account_number,
    rank() over (partition by case when panel = 12 then 1 else 0 end, scaling_segment_id -- because we don't care where accounts come from, as long as they're the best ones not already in Vespa
                order by min_reporting_quality desc,
                         max_reporting_quality desc,
                         recent_enablement,                     -- older boxes better
                         accno_sha1                             -- tiebreaker
                ) as ranking_within_panel
into #ranking_update_by_panel
from V068_data_return_by_account;

commit;
create unique index fake_pk on #ranking_update (account_number);
create unique index fake_pk on #ranking_update_by_panel (account_number);
commit;

update V068_data_return_by_account
set ranking_within_segment = t.ranking_within_segment
from V068_data_return_by_account
inner join #ranking_update as t
on V068_data_return_by_account.account_number = t.account_number;

update V068_data_return_by_account
set ranking_within_panel = t.ranking_within_panel
from V068_data_return_by_account
inner join #ranking_update_by_panel as t
on V068_data_return_by_account.account_number = t.account_number;

commit;

/************ BASIC STRATIFIED SAMPLING: FIRST ROUND OF DESICIONS ***********/

-- Mark off round 0 selections: accoutns that have weak data return statistics
update V068_data_return_by_account
set selection_round = 0
where reporting_categorisation <> 'Acceptable'
and panel = 12;

commit;

-- Mark off round 1 selections: items to remove from VESPA because they over-represent
-- a segment and we don't need that.
update V068_data_return_by_account
set selection_round = 1
from V068_data_return_by_account
inner join V068_segment_level_view as slv
on V068_data_return_by_account.scaling_segment_id = slv.scaling_segment_id
where V068_data_return_by_account.ranking_within_panel > slv.target_pop
and selection_round is null
and panel = 12;

commit;

-- I mean I guess we could track all of those selections in different tables but
-- does that do anything else over what we're getting from trackign here? No.
select panel, selection_round, count(1) as hits
from V068_data_return_by_account
group by panel, selection_round;
/* OK so we have to scrounge another 150k accounts from somewhere...
7,  ,   369783
6,  ,   369942
12, ,   352579      -> semi-balanced panel at this stage in the process
12, 0,  427721      -> Disablements or hybernation or whatever
12, 1,   81373      -> Move to alternate panels
*/

-- Selection round 2: let's pull out things from the alternate panels which feature
-- in the segments we want... thing is, we've already got a bunch of accounts on the
-- panel which aren't at the top of their rankings in the Vespa panel, and we'll leave
-- them there to minimise transfers because they're good enough, we need to figure
-- out how many boxes that is and just source the rest from alternate panels:
select scaling_segment_id, count(1) as current_hits
into #subtots
from V068_data_return_by_account
where panel = 12 and selection_round is null
group by scaling_segment_id;

commit;
create unique index fake_pk on #subtots (scaling_segment_id);
commit;

select slv.scaling_segment_id, slv.target_pop, coalesce(st.current_hits, 0) as current_hits
into #current_spread
from V068_segment_level_view as slv
left join #subtots as st
on slv.scaling_segment_id = st.scaling_segment_id;

commit;

-- This should be empty....
select * from #current_spread where current_hits > target_pop;
-- It is! great!

select sum(target_pop - current_hits) from #current_spread
-- 205911

-- Yeah, that's about what we want at this step, that'd bring us past 500k (though this
-- includes what we'd *like* to get from segments that just aren't represented by acocunts.

update V068_data_return_by_account
set selection_round = 2
from V068_data_return_by_account
inner join #current_spread as cs
on V068_data_return_by_account.scaling_segment_id = cs.scaling_segment_id
where V068_data_return_by_account.ranking_within_segment <= (cs.target_pop - cs.current_hits)
and selection_round is null
and reporting_categorisation = 'Acceptable'
and panel in (6,7);

commit;

select panel, selection_round, count(1) as hits
from V068_data_return_by_account
group by panel, selection_round
order by panel, selection_round;
/* This gives us about 438k which means we shouldn't need all that much more really...
6,  ,   316017
6,  2,   53925      -> Transfer 6->12
7,  ,   338352      
7,  2,   31431      -> Transfer 7->12
12, ,   352579      -> left in Vespa
12, 0,  427721      -> Disablements - poor data return
12, 1,   81373      -> Move to alternate panels (over-representation)
*/

-- Okay, so let's grab... see if there are some non-junk accounts in segments we
-- still need that we kicked out due to weak data return quality?

select scaling_segment_id, count(1) as current_hits
into #subtots2
from V068_data_return_by_account
where (panel = 12 and selection_round is null)
or (selection_round = 2)
group by scaling_segment_id;

commit;
create unique index fake_pk on #subtots2 (scaling_segment_id);
commit;

select slv.scaling_segment_id, slv.target_pop, coalesce(st.current_hits, 0) as current_hits
into #current_spread_w_basic_trans
from V068_segment_level_view as slv
left join #subtots2 as st
on slv.scaling_segment_id = st.scaling_segment_id;

commit;

select sum(target_pop) as goal
    ,sum(current_hits) as so_far
    ,sum(target_pop - current_hits) as required
from #current_spread_w_basic_trans;
-- 557942 and 437387 and 120555; we're not going to get all of that 120k but we'll see
-- what we can do about it... Should also at some point check how many boxes are on this
-- list to make sure it doesn't exceed the dialback cap.

select count(1) from V068_ad_hoc_box_reporting_raw as brr
inner join V068_data_return_by_account as drba
on brr.account_number = drba.account_number
where (drba.panel = 12 and drba.selection_round is null)
or (drba.selection_round = 2)
-- 514864 boxes so far... I've got another 200k boxes to play with before I worry about
-- the 750k box cap on the daily panel. There are some others that didn't turn up in our
-- segmentations that we're not playing with, plus a few for subscribers we couldn't link
-- to an acocunt, and we're just leaving them there, we should figure out how much that
-- will do to stuff...

select count(1) from V068_ad_hoc_box_reporting_raw where account_number is null;
-- less than 1k.

-- Oh, and the others, they're still included in the (panel=12 and selection_round is NULL)
-- constraint, so our numbers seem pretty good.

-- Oh, a final thing; taking truly terrible boxes off the alternate panels. We're not
-- kicking unrelioable boxes off, since they might get better?
update V068_data_return_by_account
set selection_round = 3
where panel in (6,7)
and reporting_categorisation = 'Zero reporting';
commit;
-- Wait, shouldn't we then be putting semi-acceptable Vespa into the Alt panels? well,
-- that's a lot of transfers and we'd rather not have transfers eh bro.

select panel, selection_round, count(1) as hits
from V068_data_return_by_account
group by panel, selection_round
order by panel, selection_round;
/*
6,  ,   227300      -> Gets to stay in ALT6
6,  2,   53925      -> Transfer 6->12
6,  3,   88717      -> Disable from 6 - zero reporting
7,  ,   250557      -> Gets to stay in ALT7
7,  2,   31431      -> Transfer 7->12
7,  3,   87795      -> Disable from 7 - zero reporting
12, ,   352579      -> Remains in Vespa
12, 0,  427721      -> Disable / hybernate: unreliable reporting
12, 1,   81373      -> Transfer 12->6&7 (overrepresentation)
*/

/************ BASIC STRATIFIED SAMPLING: FURTHER DECISIONS ***********/

-- OK, but hey, this is actually where the "basic" stratified sampling ends; there's no
-- scope for weighting a relaxing of various conditions, there's just apply-the-filter-
-- take-the-balanced-accounts. I mean, I guess we could throw in more accounts just so
-- that we get over 500k, but we'd only have to remove those accounts again if we were
-- to do anything more sophisticated. Let's juist push out these control totals and
-- see what the panel ends up looking like.... well, okay, the reporting statistics
-- are going to be generous due to the 80% vs 90% reporting quality shift, maybe we
-- recalculate those for this PanMan thing?

/************ PREPARING STRUCTURES TO MATCH PANEL MANAGEMENT PROCESS ***********/

-- Oh, hey, we're not building the PanMan structures, but the raw segmentation and
-- single box view items.

-- Okay, first: faking the SBV:
select
    brr.subscriber_id
    ,drba.account_number
    ,brr.enablement_date
    ,case
        when drba.panel = 12 and drba.selection_round is null then 'VESPA'
        when drba.panel = 6  and drba.selection_round is null then 'ALT6'
        when drba.panel = 7  and drba.selection_round is null then 'ALT7'
        when drba.selection_round = 2 then 'VESPA'
        when drba.selection_round = 1 then 'VESPA'              -- okay, now we're trying not kicking the overrepresented accounts out of Vespa at all...
      else ''
      end as panel
    ,reporting_quality                                          -- heh everything is now going to get judged at 90% rather than 80% we currently use
    ,convert(int, reporting_quality*30) as logs_returned_in_30d -- horribly faked, but w/e, results will turn out the same
    ,convert(bit, floor(reporting_quality)) as logs_every_day_30d -- 1 only for reporting_quality=1
into stafforr.vespa_single_box_view
from V068_data_return_by_account as drba
inner join V068_ad_hoc_box_reporting_raw as brr
on brr.account_number = drba.account_number;
-- So the 30d fields are a hack around only having 21 days of data, but the
-- main thing is to get the PanMan report to compile, it'll patch together
-- alright enough *I guess*.

-- Also, we're explicitly calling it stafforr.vespa_single_box_view so it's
-- obvious that we're not futzing about with the live vespa_analyusts version.

commit;
create unique index fake_pk                 on stafforr.vespa_single_box_view (subscriber_id);
create        index account_number_index    on stafforr.vespa_single_box_view (account_number);
commit;

-- What do we end up with?
select panel, count(1) as boxes, count(distinct account_number) as accounts
from stafforr.vespa_single_box_view
group by panel
order by panel;
/* Again, that's what we needed, so good.
'     ',    870983, 604233
'ALT6 ',    253718, 227300
'ALT7 ',    282679, 250557
'VESPA',    596940, 519308
*/

-- Next to fake: the scaling tables. I'll do that later, it needs the scaling
-- build, right now I want to get out the rest of those coverage metrics...

select account_number, scaling_segment_id
into stafforr.Scaling_weekly_sample
from V068_sky_base_profiling;

-- Oh hey and we need all the keys on both of those tables so that PanMan
-- might have a chance of finishing today...
commit;
create unique index fake_pk on stafforr.Scaling_weekly_sample (account_number);
create        index account_number_index        on stafforr.vespa_single_box_view (account_number);
commit;

/************ PREPARING STRUCTURES TO GO INTO THE SCALING BUILDS... ***********/

/* So we need the empty structures for the following tables:
SC2_category_subtotals
SC2_metrics
SC2_category_working_table

** We need a full reconstruction of: SC2_weighting_working_table but that shouldn't be hard

** The last this is to redirect the lookup tables in the loop to the vespa_analysts version, and then we can scale!

*/

-- This guy is just a summary by segment of the Sky Base, so he goes like this:
insert into SC2_weighting_working_table (scaling_segment_id, universe, sky_base_accounts)
select scaling_segment_id, min(universe), count(1)
from V068_sky_base_profiling
group by scaling_segment_id;

commit;

-- Sweet, now assemble the Vespa population over the scaling segments

select scaling_segment_id, count(1) as vespa_hits
into #vespa_guys
from  V068_data_return_by_account as drba
where (drba.panel = 12 and drba.selection_round is null)
or (drba.selection_round in (1,2)) -- not excuding those overrepresenting guys any more
group by scaling_segment_id;

commit;
create unique index fake_pk on #vespa_guys (scaling_segment_id);
commit;

select count(1), sum(vespa_hits) from #vespa_guys;
-- 27166 and 519308; 27k represented segments, that's okay really I guess.

update SC2_weighting_working_table
set vespa_panel = vg.vespa_hits
from SC2_weighting_working_table
inner join #vespa_guys as vg
on SC2_weighting_working_table.scaling_segment_id = vg.scaling_segment_id;

commit;

select count(1), sum(sky_base_accounts) as bassus, sum(vespa_panel) as vespas
from SC2_weighting_working_table;
/* These totals are still good, proceed with the RIM weighting group!
72784,  9434629.0,  518760.0
*/

-- Sweet, now put vespa_analysts in front of the lookup tables and we can run
-- the rim weighting loop... 

-- ^^ To here: we're currently running on the rim weighting loop at the moment.

-- And after that Rim weighting loop:
select case when abs(sky_base_accounts - sum_of_weights) > 3 then 0 else 1 end as convergence
    ,count(1) as segments
from SC2_category_working_table
group by convergence;
-- 145! all categories still converge, that's cool.

select sum(sum_of_weights), sum(sky_base_accounts)
from SC2_category_working_table;
-- Heh, we actually have exact convergence here too down to 6dp. again. Oh hey that's because
-- we've not yet removed the weights that trickled in from the unrepresented segments, heh.

-- Apparently over-representation doesn't affect panel convergence that much. (though this is
-- full panel scaling, whereas actual daily builds will fare worse off, but ctually setting
-- that up would take time we don't have.)

select @iteration;
-- 38 this time.

-- OK, so, category by category, how do all of these things line up vs their targets?
select round(sky_base_accounts - sum_of_weights,1) as int_diff
    ,count(1) as segments
from SC2_category_working_table
group by int_diff
order by int_diff;
/* Same outliers, same general structure.
-2.1,1
-1.6,1
-1.3,1
-0.9,1
-0.4,3
-0.3,1
-0.2,8
-0.1,14
0.0,92
0.1,9
0.2,4
0.3,3
0.4,2
0.5,2
0.9,1
2.3,1
2.7,1
*/

-- Sweet! A balanced panel it is then.

-- Asking about convergence within a segment: RIM weighting doesn't do this, but apparently
-- we wanted visibility of it?
select round(sky_base_accounts - sum_of_weights,-1) as diff
    ,count(1) as segments
from SC2_weighting_working_table
where vespa_panel > 0.5
group by diff
order by diff;
-- Yeah, these generally don't line up at all. Not that they were expected to. Still, for
-- "visibility" or something.

/************ PUSHING THOSE ENABLEMENT REQUESTS OUT AS LISTS ***********/

-- Do we want them as account number or subscriber ID? I guess subscriber ID because
-- it came in from the external file like that and we can guarantee those are in place?
-- or at least, we can hand data quality issues on to someone else.

-- For outwards trasnfers due to non-reporting:
-- How are we splitting the accounts between ALT6 and ALT7? let's split ranking_within_segment
-- and put evens in one and odds in the other. The split might help with panel balance, though
-- the non-reporting kind of laughs at that idea.

select brr.subscriber_id, 
    brr.panel as old_panel,
    case
        when selection_round = 2 then 12
        when selection_round = 0 and mod(ranking_within_segment,2) = 1 then 6
        when selection_round = 0 and mod(ranking_within_segment,2) = 0 then 7
        else -1
      end as new_panel
into V068_transfer_requests
from V068_data_return_by_account as drba
inner join V068_ad_hoc_box_reporting_raw as brr
on drba.account_number = brr.account_number
where selection_round in (0, 2);

-- [Update: there's a new column for tracking the date the list was raised that gets added in a
-- later section because the spec changed and we needed to raise an additional batch.]

commit;
create unique index fake_pk on V068_transfer_requests (subscriber_id);
commit;

-- Did that work out?
select old_panel, new_panel
    ,count(1) as transfer_population
from V068_transfer_requests
group by old_panel, new_panel
order by old_panel, new_panel;
/* OK, sure, looks good.
6,      12,     73361
7,      12,     45526
12,     6,      342102
12,     7,      328057
*/

-- So the current populations are:
select panel, count(1) as hits
from V068_ad_hoc_box_reporting_raw
group by panel;
/*
12,     1148999
7,      428084
6,      428031
*/

-- So there are our control totals. We can ship those out. This only works on P5X1:
select * from V068_transfer_requests;
output to 'D:\\Vespa\\Panel balance migrations\\Vespa_balance_migraion_requests.csv';
-- 789046 rows written.

-- [Update: Note that this is still all pre-appending-of-stuff, if you run this again
-- after the spec change you'll get a bunch of build dates too, as well as different
-- control totals. The section immediately below has all the details.]

-- Oh, and so it works as a reference:
grant select on V068_transfer_requests to greenj, dbarnett, jacksons, sarahm, gillh, rombaoad, louredaj, patelj, kinnairt, angeld, bednaszs, vespa_analysts, vespa_group_low_security, sk_prodreg;
commit;

-- Final QA: alignment of box and panel details against original source file:
select count(1) from V068_transfer_requests as tr
inner join V068_ad_hoc_box_reporting_raw as brr
on tr.subscriber_id = brr.subscriber_id
-- no WHERE clause: 789046 as expected
where tr.old_panel = brr.panel
-- with WHERE clause: 789046, all boxes and panel designations line up.

-- And we're done!

/************ SELECTING EVEN MORE BOXES ***********/

-- We've got a new task to add ~80k more boxes to panel 12 from 6 and 7
-- in order to get the daily reporting numbers past 500k. So what we'll
-- do is go for further flat samples across everything that's not already
-- oversampled, in an effort to contain the oversampling we're not going
-- to be able to avoid.

-- First step: figure out what the actual panel looks like now, after the
-- initial transfers and stuff.
select distinct account_number
into #balanced_guy
from stafforr.V068_ad_hoc_box_reporting_raw as a
left join stafforr.V068_transfer_requests as b
on a.subscriber_id = b.subscriber_id
where coalesce(b.new_panel, a.panel) = 12;

commit;
create unique index fake_pk on #balanced_guy (account_number);
commit;

-- OK, now we need the distribution between segments....
select drba.scaling_segment_id
    ,count(1) as panel_members
into #balanced_segments
from #balanced_guy as bg
inner join V068_data_return_by_account as drba
on bg.account_number = drba.account_number
group by drba.scaling_segment_id;

commit;
create unique index fake_pk on #balanced_segments (scaling_segment_id);
commit;

-- So connect that to the targets table to update that diff column
update V068_segment_level_view
set diff = null;

update V068_segment_level_view
set V068_segment_level_view.diff = bs.panel_members - V068_segment_level_view.target_pop
from V068_segment_level_view
inner join #balanced_segments as bs
on V068_segment_level_view.scaling_segment_id = bs.scaling_segment_id;

commit;

-- So we've got 27,166 segments, so if we grab another 4 or 5 accounts
-- for each segment. Are we going to worry about correcting for segments
-- we already have oversampled? Not sure we will really eh.

select count(1)
from V068_segment_level_view
where diff <= 16
-- 26k, so yeah, it might help spread it out a bit. (We upped the limit from 6
-- to 16 to get the account volume we wanted, though the number of segments here
-- doesn't increase much and in any case balance is only going to get worse.)

-- OK, we're going to check that there are no available panel 6 or 7
-- accounts on this breakdown we've just built;
select distinct account_number
    ,convert(bigint, null)  as scaling_segment_id
    ,convert(int, null)     as ranking_within_segment   -- could really use either here
    ,convert(tinyint, null) as selection_round          -- we don't need this, it's just for QA
into #leftovers
from stafforr.V068_ad_hoc_box_reporting_raw as a
left join stafforr.V068_transfer_requests as b
on a.subscriber_id = b.subscriber_id
where coalesce(b.new_panel, a.panel) in (6,7);

commit;
create unique index fake_pk on #leftovers (account_number);
commit;

update #leftovers
set #leftovers.scaling_segment_id      = drba.scaling_segment_id
    ,#leftovers.ranking_within_segment = drba.ranking_within_segment
    ,#leftovers.selection_round        = drba.selection_round
from #leftovers
inner join V068_data_return_by_account as drba
on #leftovers.account_number = drba.account_number
where drba.reporting_categorisation = 'Acceptable';

delete from #leftovers where scaling_segment_id is null;

select selection_round, count(1) from #leftovers
group by selection_round;
-- Yeah, sweet, just a whole load of NULL things that have never been chosen.

-- This methodology is a little messy, but yeah. Isolate those segments
-- that aren't too oversampled and also get the diffs too:
select scaling_segment_id, diff
into #next_to_choose
from V068_segment_level_view
where diff <= 16;

commit;
create unique index fake_pk on #next_to_choose (scaling_segment_id);
commit;

-- Ok, so now we can go and figure out which new accounts we want to move:
select
    account_number
    ,rank() over (partition by l.scaling_segment_id order by ranking_within_segment) as sequencer
    ,diff
into #next_selection_round
from #leftovers as l
inner join #next_to_choose as ntc
on l.scaling_segment_id = ntc.scaling_segment_id;
-- 198k, so, like only twice what we wanted to use. It might work out? Now to
-- clip out the ones that would be too far oversampling; diff is the panel members
-- less the target, so we'll accept onto the panel accounts for which the sequencer
-- is less than 6 minus the diff...

commit;

select count(1)
from #next_selection_round
where sequencer < 16 - diff;
-- 77k - that's take us up to some 597k accounts, which is bang on what we want. (heh,
-- there's still no focus on balancing any individual universes, but hey...)

-- Isolate the accounts we want to raise in the new selection round:
delete from #next_selection_round
where sequencer >= 16 - diff;
-- done!
commit;

-- oh and we're joining a thing:
create unique index fake_pk on #next_selection_round (account_number);
commit;

-- And then push those lists back out to the holding table, and also make an external
-- list of them to hand off to the Tech Ops guys. Can't have a new Transfer Requests
-- table because that'd change the current panel query which we said wasn't moving, so
-- we'll tack on an extra column to track when we raised these things.
alter table V068_transfer_requests
add raised_date         date;

update V068_transfer_requests
set raised_date = '2012-08-10';
-- That's when the first batch was raised.

-- And poke the new transfer requests in: Ok, first we're going to isolate them and
-- check that there's nothing dumb going on with them
select
    brr.subscriber_id
    ,brr.panel
    ,12             as new_panel
    ,today()        as build_date
into #selection_confirmation
from #next_selection_round as nsr
inner join stafforr.V068_ad_hoc_box_reporting_raw as brr
on nsr.account_number = brr.account_number;
-- 87548 new box migration requests

commit;
create unique index fake_pk on #selection_confirmation (subscriber_id);
commit;

-- Okay, have we requested their transfer anywhere else yet?
select count(1) from #selection_confirmation as dt
inner join V068_transfer_requests as  tr
on dt.subscriber_id = tr.subscriber_id;
-- 0! good!

-- And are any of them currently on panel 12?
select count(1) from #selection_confirmation as dt
inner join stafforr.V068_ad_hoc_box_reporting_raw as  brr
on dt.subscriber_id = brr.subscriber_id
and brr.panel = 12;
-- 0! also good!

-- they're cool! poke them in!
insert into V068_transfer_requests
(
    subscriber_id
    ,old_panel
    ,new_panel
    ,raised_date
)
select * from #selection_confirmation;

-- The other thing to do is to push a list of the new transfer requests out to
-- disk: again ,this is a p5x1-only kind of deal. Actually, wait, that was the
-- case when we were running WinSQL over the laptop, but now we have Sybase
-- Interactive for the laptop, that restraint is probably no longer in play...
-- Still, we'll use p5X1 so that the files end up where the last ones were.
select subscriber_id, old_panel, new_panel
from V068_transfer_requests
where raised_date = '2012-08-22';
output to 'D:\\Vespa\\Panel balance migrations\\Vespa_balance_migraion_additional_requests.csv';

-- The thing we need to do is sum the reporting qualities to get an estimate of
-- how many boxes will be returning data on any given day, because that wants to
-- stay below 700k or so.
select count(distinct account_number)     as accounts
    ,count(1)                             as boxes
    ,convert(int, sum(reporting_quality)) as anticipated_nightly_callbacks
from stafforr.V068_ad_hoc_box_reporting_raw as a
left join stafforr.V068_transfer_requests as b
on a.subscriber_id = b.subscriber_id
where coalesce(b.new_panel, a.panel) = 12;
-- 596412 and 685275 and 630720 - those numbers are all okay, within spec bounds.

-- So, how does that do for expected daily panel size?
select distinct account_number as account_number
into #final_panel
from stafforr.V068_ad_hoc_box_reporting_raw as a
left join stafforr.V068_transfer_requests as b
on a.subscriber_id = b.subscriber_id
where coalesce(b.new_panel, a.panel) = 12;

commit;
create unique index fake_pk on #final_panel (account_number);
commit;

select convert(int, sum(min_reporting_quality))   -- okay really we should be using a product of box reporting quality within a household as data return is probably independent, but this is kind of an estimate of quality
from #final_panel as fp
inner join V068_data_return_by_account as drba
on fp.account_number = drba.account_number;
-- 596412 and 546079 - so we're expecting >500k daily panel, so that's good, though
-- we'll see how the box migration stuff goes, eh?

-- Okay, so they're all the stats we need, we're done! We're not going to invest
-- in more panel balance stats, since it's going to be worse than it was before
-- it was oversampled (and this is the second round of oversampling). Hopefully
-- it's not too bad? We'll see what comes through the various reports and profile
-- work.

-- Oh wait, one more thing, how is the >90% vs >80% reporting split going in this
-- newly rebuilt panel? Oh hey that's actually on drba too...
select
    count(1) as total
    ,sum(case when drba.min_reporting_quality >= 0.9 then 1 else 0 end) as exceeding_ninety
    ,sum(case when drba.min_reporting_quality >= 0.8 then 1 else 0 end) as exceeding_eighty
from #final_panel as fp
inner join V068_data_return_by_account as drba
on fp.account_number = drba.account_number;
-- 403652 accounts >90% quality, the rest are >80% as expected.

-- Now we're done!

-- Oh, we should also but a thing in the drba table about the selection round, that
-- stuff should be kept up to date as well: the next selection round is 4.
update V068_data_return_by_account
set selection_round = 4
from V068_data_return_by_account
inner join stafforr.V068_ad_hoc_box_reporting_raw as brr
on V068_data_return_by_account.account_number = brr.account_number
inner join stafforr.V068_transfer_requests as tr
on brr.subscriber_id = tr.subscriber_id
where tr.raised_date = '2012-08-22';
-- 77102 records updated, nice.

commit;

-- Are things looking consistent:
select panel, selection_round, count(1) as hits
from V068_data_return_by_account
group by panel, selection_round
order by panel, selection_round;
/* Sweet, we're good, the old selection rounds are unchanged and in total everything
** adds up to the final panel size - the ones marked ! end up in the post-ramp-up panel
** and they sum to 596410 like we expect.
6,  ,   187922
6,  2,   53925      !
6,  3,   88717
6,  4,   39378      !
7,  ,   212833
7,  2,   31431      !
7,  3,   87795
7,  4,   37724      !
12, ,   352579      !
12, 0,  427721
12, 1,   81373      !
*/

-- Now finally we're done!

-- Oh, actually, turns out we want to have all of them in one big list, so here we go:
-- (P5X1 only again, for consistency)
select subscriber_id, old_panel, new_panel
from V068_transfer_requests;
output to 'D:\\Vespa\\Panel balance migrations\\Vespa_balance_migraion_complete_requests.csv';
