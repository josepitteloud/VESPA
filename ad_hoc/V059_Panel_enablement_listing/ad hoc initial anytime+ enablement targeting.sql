/******************************************************************************
**
**  PROJECT VESPA: PANEL ENABLEMENT LISTING
**
** OK, so we want to select households that will tend to balance our panel,
** limiting the selections to some particular constant number, and then
** prioritising them over how useful they'd be to the panel in terms of
** how many extra boxes they'd represent. The extra constraints are now:
**
**  1. 500k balanced households on Vespa and
**  2. 150k (balanced) on each Alternate day panel
**  3. 300k anytime + boxes across all panels
**  4. Balance panels wrt scaling 2 variables
**  5. Prioritise boxes using the golden box deliverables from Jonathan (NYIP)
**  6. Double the box counts for each criteroin, plus a little more? since
**      we only really get 40% worth of good reporting from accounts anyways.
**  7. Are we purging from Vespa boxes which are not reliably reporting?
**      Answer: yes, to 50% reporting quality.
**  8. Put all the enablements into one file, with a column for batch and
**      another column for Panel. Disablements will get NULL for panel I
**      guess. Also, expecting another batch 29 deal, because we might not
**      get all the Anytime+ we need from the golden boxes.
**
** Note that 1 and 4 are potentially mutually exclusive, and 2 & 3 might
** also act against each other. We'll see what we can do about these, and
** just document the workarounds we have to use.
**
** Note that the golden box analysis stuff comes from a different file in
** the ad_hoc area, and we reference it here, and in fact, following a
** script refactoring, there are a number of dependent scripts for this
** code:
**  A) Scaling 2 build, though we just inheiret the table (and the code
**      that builds Scaling 2 isn't version tracked at the moment)
**  B) Jonathan's Golden Box table construction (code currently in golden
**      box analysis ad hoc folder, we just use the table though)
**  C*) "Vespa panel migration 4 to 12.sql" also in the V059 folder
**  D*) "golden waterfall breakdown.sql" also in the V059 folder
**  E**) May also be untracked dependencies on the panel manageemnt report,
**      which isn't yet demonstrably stable with Scaling 2, and would at
**      point be out of sync with our other sources, but I think we only
**      need the Anytime+ flag and we've got that on the golden boxes?
**  F) "external - box loss waterfall.sql" - builds the waterfall for the
**      whole Sky UK base, and we plug into it here to see which golden
**      boxes surive the filtering
**
** *: Potential dependency; the code used to be an earlier section in this
** script, but did a different thing, so now it's outsourced.
** **: removed dependency, but in a comic way: it would go on the PanMan
** report, PanMan is ideally suited for it, however, Scaling 2 integration
** into PanMan was never completed and so the build isn't useful to us, we
** have to replicate the functionality from scratch.
**
** In any case, as we go we'll sort out the dependencies and tidy it all up,
** because we thought we were going to get a flag saying "box is good" to
** choose from, but instead we've got other prioritisations to follow and
** other caps to meet. We'll see.
**
** We're also going to start by identifying everything in the Vespa panel
** which has less than 50% reporting quality, and kick all of those accounts
** out of the panel too. We also need to have the structure and table names
** finalised by the end of play Thursday (which is delivery time anyway) as
** we want these guys on the Sybase 15 migration list so we can continue to
** track them. We're also going to pull off some control totals to demonstrate
** that the totals we have are consistent and well mapped to other things.
**
** Code sections: (will probably grow as different stuff happens)
**
**      Part A: A01 -  Preserving builds of required items
**
**      Part B: B01 -  Output structures
**
**      Part C: C01 -  Preparing the Box selection listing
**              C02 -  Cleansing Vespa of unreliably reporting accounts
**
**      Part D:     -  Golden box filtering
**              D01 -   Summarising boxes into accounts
**              D02 -   Pushing accounts through the waterfall filter
**
**      Part E:     -  Initial selections
**              E01 -   Initial marginal utility calculations
**              E02 -   Selection of initial Anytime+ boxes (heh, all of them)
**              E03 -   Selection of (up to) 1.4m by stratified sampling (heh, it'll be much less)
**              E04 -   Consolidation & recording existing picks (heh, this is going to be everything survivng the waterfall)
**              E05 -  Splitting selections across panels
**
**      Part H:*    -  Marginal utility calculations for remainder of panel; looped in chunks until panel size is reached*
**              H01 -   Assembly, anytime+ flags, disablement exclusion
**              H02 -   Current welbeing of Vespa (post account clippings & existing selections)    
**              H03 -   A round of marginal weighting utility
**              H04 -   (Do we need other flags?)
**              H05 -   Box selection
**
**      Part K:     -  Production of results
**              K01 -   Assignment of batches from call back dates
**              K02 -   Estimates on balance statistics & panel size / health
**              K03 -   Generation of batch control totals to demonstrate consistency
**              K04 -   Export to .csv (requires P5X1)
**
**      Part Q:     -  Quality Assurance on process & build
**              Q01 -   QA on selection order calculations
**              Q02 -   [Whereas this QA is to demonstrate process correctness]
**              Q03 -   [Hope it works out because there's no fallback time?]
**              Q04 -   [Probably testing more than one thing?]
**
** *: for part H, the marginal reweightings consider not just segments, but
** single variable weights as well, so that we can try to balance the panel in
** a way that assists Scaling 2 rim weighting. But this means as we select, a
** lot of the marginal weights change (based on changing profiles of single
** scaling variables) so we'll re-weight the remaining accounts after each
** batch of account picks. H01 assembles the structure, but then H02-H05 get
** looped so we (hope to) get a good spread over each individual scaling
** variable.
**
** We've also left a little lexiographic space between code parts, in case we
** have to add a bunch of new stuff.
**
******************************************************************************/

-- Plan:
--      Get the golden box & prioritisation from Jonathan
--      Get the segmentation for the Sky Base using the Scaling 2 build (of last week? As long as some build is static at the time)
--      Grab the reporting quality from last week's SBV to isolate bad accounts already on the panel
--      Rebuild the waterfall profiling, active for the same period as the scaling rebuild
--      Assume we're going to clip everything out of Vespa except the reliables
--      Order every account surviving the golden filter by marginal utility
--      Start by picking the first 600k anytime boxes through the golden filter by marginal utility (raise on fail)
--      Pick up to the first 1.4m from the golden survivors by stratified sampling (expect to fall short though)
--      Take the intersection of the above two
--      Continue by marginal weights from the calculated intersection up to the 1.4m (or however many we need to recover the weak excluded ones)
--          (though, going to have to reweight this every couple of boxes to make sure we've got variable balance and not just segment cover)
--
-- Question: what to do if we don't get enough Anytime+ from the golden boxes waterfallens?
--  Answer: escalate, warn, chill. Heh, iniditial indications show that there are only 120k or so Anytime+ boxes surviving the golden box waterfall.
-- Question 2: when we have fewer than the target number of boxes availbale, are we going to prefer balance or panel size?
--  Answer: we only currently have only 1.3. boxes available for enablement, barely enough for targets. Again, escalate, chill.
--
-- To be honest, the plan mostly involves selecting 1.6m good boxes that together
-- all provide good cover of the Sky Base, randomly assigning them to panels, and
-- then doing careful balance work once they start returning data and we can see
-- which boxes are good.

-- So accounts that don't report at least this good are kicked out of Vespa.
create variable @required_reporting_quality float;
set @required_reporting_quality = 0.5;
-- PanMan considers things > 0.9 but not sure we're being specific. Maybe
-- we get to tweak it later to improve panel quality?

/****************** PART A01: PREREQUISITE TAGGING ******************/

-- So we're currenntly working without the dependencies we need in place. But we've
-- got some old builds, and so we're reserving those in our own schema while people
-- might rebuild them:

/* Thing is, these only need to be done once, we don't need to rebuild them each run
select *
into rs_golden_box_clone_demo
from greenj.golden_boxes;
-- 6.5 mil or so
-- Update: the new build of this source is available, so we've done the search/replace
-- trick to point directly to that table...

select *
into rs_scaling_segmentation
from vespa_Analysts.Scaling_weekly_sample;
-- 9415508 - that's sky base, good.

select account_number, subscriber_id, panel, reporting_quality
    ,account_anytime_plus & box_has_anytime_plus as has_anytime_plus
into rs_last_weeks_sbv_build
from vespa_analysts.vespa_single_box_view;
-- 785243, that's my dogs

commit;

-- Oh wait, we need keys and indices on those too...
create unique index fake_pk on rs_last_weeks_sbv_build (subscriber_id);
create unique index fake_pk on rs_scaling_segmentation (account_number);
create unique index fake_pk on rs_golden_box_clone_demo (subscriber_id);
create unique index fake_fake_pk on rs_golden_box_clone_demo (id); -- this one's been culled out? but, good for consistency

commit;

-- Okay, so we're adding indices to support our actions, but we have no idea
-- whether the actual live replacement tables will have this indexing on them,
-- so the live build tomorrow could end up orders of magnitude slower than
-- this demo one, even if the size of data doesn't move much...

create index account_number_index on rs_last_weeks_sbv_build (account_number);
create index account_number_index on rs_golden_box_clone_demo (account_number);
create index for_joining on rs_scaling_segmentation (scaling_segment_id);

commit;

-- These names should be unique, so if we do update them or switch them to
-- live current versions, Find/Replace should be able to do that all for us.
*/

-- Others will be added as processing reveals their need

/****************** PART B01: OUTPUT STRUCTURE ******************/

-- So this guy is the format that we need to populate for Phil, all one big table,
-- and we'll dump it to disk with all it's columns in one go:
if object_id('V059_enablement_listing_all_panels') is not null
    drop table V059_enablement_listing_all_panels;
create table V059_enablement_listing_all_panels (
    account_number          varchar(20) not null primary key    -- Accounts is easier than subscriber IDs...
    ,panel_id               tinyint                             -- 12 for VESPA, 6 for ALT6, 7 for ALT7, NULL for VESPA disablements
    ,batch                  tinyint                             -- 1-28 by call back day, NULL for disablements
);
-- Are we going to keep NULL batches for disablements? if those are being
-- communicated during the same conditional access process, then disablements
-- might want to get batched too.... we'll see. Just mention it in the outline
-- of deliverables and see if it gets negotiated. Ad this point we just want
-- to build a thing though, haggle about .csv formats etc later.

-- We're not publishing a prioritisation within each batch, we just want *all
-- of them* and they can just do whatever until they're all on.

-- Maybe we want to end up putting disablements in a separate file? who knows?
grant select on V059_enablement_listing_all_panels to greenj, dbarnett, jacksons, sarahm, poveys, gillh, rombaoad, louredaj;

commit;
-- OK, now we just have to fill the things....

-- We also need that main box lookup table where we store which panel each account
-- is on, what important flags it has, the marginal weightings and selection round
-- details:
if object_id('V059_Prioritised_household_enablements') is not null
    drop table V059_Prioritised_household_enablements;
create table V059_Prioritised_household_enablements (
    account_number                  varchar(20) primary key
    ,inpanel                        int         default -1  -- -1 means not on any panel yet
    ,has_anytime_plus               bit
    ,scaling_segment_id             int         not null
    ,accno_SHA1                     varchar(40)
    ,marginal_selection_benefit     float
    ,selection_order                int
);
-- regarding inpanel: we're going to set that to 0 for everything that we want to keep
-- in Vespa Panel, -1 for everything we still get to choose, and then set inpanel to 1,
-- 2, 3, etc for subsequent selection rounds. That way, when we order by inpanel desc
-- we will consider the weightings provided by every box that already on the panel (or
-- already selected) and then consider the marginal weightings of the new boxes we have
-- as options to select.

grant select on V059_Prioritised_household_enablements to greenj, dbarnett, jacksons, sarahm, poveys, gillh, rombaoad, louredaj;
-- okay, so we're also going to need an index that supports the selection order deal
-- and splits ties by the SHA1 and stuff but we'll figure out exactly what we need
-- when we write the update, because it might end up needing almost all the items
create index for_prioritisation on V059_Prioritised_household_enablements (scaling_segment_id, accno_SHA1);
-- We'd like this to be unique, but the SHA1 doesn't get set immediately, so that
-- won't work out.
create index for_selection on V059_Prioritised_household_enablements (selection_order);
-- Selection order doesn't get set for ages, but we'll still eventually need it indexed
create index for_choosing on V059_Prioritised_household_enablements (accno_SHA1);


-- Also: by the time these needs are actually communicated to boxes, there will be
-- churn and new opt outs and a bunch of stuff like that. There'll probably also be
-- nontrivial intersection of the disablements with other disablement listings we've
-- already produced, because we've not seen all those boxes drop off yet, but we'll
-- see how that goes.


/****************** PART C01: PREPARING THE BOX SELECTION LISTING ******************/

-- Let's start by summarising what we've already got on the panels (currently only
-- supporting Vespa though):
select account_number
    ,max(has_anytime_plus) as has_anytime_plus
    ,min(reporting_quality) as reporting_quality -- NULLs are not goin to feature here, but we'll them the benefit of the doubt later in the processing
into #V059_Vespa_household_summary
from rs_last_weeks_sbv_build
where panel = 'VESPA'
group by account_number;

commit;
create unique index fake_pk on #V059_Vespa_household_summary (account_number);
commit;

-- OK, so we can prepare the listing based on accounts that are good at returning data:
insert into V059_Prioritised_household_enablements (
    account_number
    ,inpanel
    ,has_anytime_plus
    ,scaling_segment_id
    ,accno_SHA1
)
select
    vhs.account_number
    ,0
    ,vhs.has_anytime_plus
    ,ss.scaling_segment_id
    ,null                   -- Sybase can't handle figuring out putting default values into multi column indices, so we have to be explicit about accno_SHA1
from #V059_Vespa_household_summary as vhs
inner join rs_scaling_segmentation as ss
on vhs.account_number = ss.account_number
where vhs.reporting_quality > @required_reporting_quality or vhs.reporting_quality is null;
-- reporting quality is null for boxes that haven't been around very long, and we give them
-- the benefit of the doubt.

commit;

-- The SHA1s are still not set, but we'll update those later when the activation options
-- are also added into the table.

/****************** PART C02: EXCLUDING BAD BOXES ALREADY ON VESPA ******************/

-- So all the guys that we don't want to keep in C01 are getting chucked into the table
-- of guys to discard:
insert into V059_enablement_listing_all_panels
select distinct account_number, null, null
from #V059_Vespa_household_summary
where reporting_quality <= @required_reporting_quality;

commit;

-- Note that there wil probably be overlap with other account disablements we're missing.
-- But then, like before, this data will be weeks / months out of date before it's actioned,
-- so guys can do what they can with it.

/****************** PART D01: GOLDEN BOXES INTO GOLDEN ACCOUNTS ******************/

-- Okay, all of Part D has some overlap with "golden waterfall breakdown.sql", in fact,
-- some identical table constructions are in place. But the processing diverges, and we
-- have changed the table names, so they shouldn't really intersect...
if object_id('V059_Golden_account_waterfalling') is not null
    drop table V059_Golden_account_waterfalling;
select account_number
    ,min(convert(tinyint,left(on_time, 1)))                         as worst_calling_back
    ,max(convert(tinyint,anytimeplus))                              as has_anytime_plus
    ,max(case when prefix is null then 1 else 0 end)                as has_null_prefix_boxes
    ,max(case when prefix = '' then 1 else 0 end)                   as has_emptystring_prefix_boxes
    ,max(case when prefix <> '' then 1 else 0 end)                  as has_bad_boxes
    ,cast(0 as int)                                                 as waterfall_exit   -- there are no zeros, should help track non-updates
    ,min(cbk_day)                                                   as callback_day
into V059_Golden_account_waterfalling
from greenj.golden_boxes
group by account_number;

commit;
create unique index fake_pk on V059_Golden_account_waterfalling (account_number);
-- OK, and now attatch to the waterfall thing we built for all accounts:

update V059_Golden_account_waterfalling
   set waterfall_exit = case when awb.knockout_level is null then -1
                             else awb.knockout_level end
  from V059_Golden_account_waterfalling
       inner join PanMan_adhoc_waterfall_base as awb on V059_Golden_account_waterfalling.account_number = awb.account_number
;

-- Now we're ignoring accounts that drop out at the null prefix test
-- because Jonathan's supercedes this. It's the last test in the waterfall
-- priority, so everything failing here has already passed every other
-- condition, so we needn't worry about having to rerun the watefall build
-- to establish where things drop out. (If there were any further tests
-- after the one being removed, that would require a waterfall rerun.)


-- Okay, so, yeah, that's all the processing we need there, which we borrowed from
-- the "golden waterfall breakdown.sql" script, though the table name is now prefixed
-- with V059.

select waterfall_exit, count(1) as hits
from V059_Golden_account_waterfalling
group by waterfall_exit
order by waterfall_exit;
-- wait... so we've nulls and zeroes? The NULLs are the good ones we can use, but
-- we're also seeing about 5% of things not in the waterfall base at all. I guess
-- those are the inactive accounts or suchlike.

--JG amend: Null was acting weird, so I changed it to -1

/****************** PART D02: FILTERING THE GOLDEN WATERFALL ******************/

-- So here we're taking the flags we made above and building a list of boxes which might still be viable:
select gaw.account_number, gaw.has_anytime_plus,gaw.worst_calling_back
into #account_selection_options
from V059_Golden_account_waterfalling as gaw
left join vespa_analysts.V059_enablement_listing_all_panels as elap
on gaw.account_number = elap.account_number
where gaw.has_bad_boxes = 0
and has_anytime_plus = 1
and has_null_prefix_boxes = 1
and gaw.worst_calling_back >= 4
and elap.account_number is null
and waterfall_exit =-1;

--6125 accounts with less than 4 callbacks sent to Paul Hicks in file 0_3.csv

-- elap currently only contains the Vespa disablements; they shouldn't overlap with whatever
-- survives the waterfall because we're excluding Vespa panel, but we'll check anyway. Um,
-- this might laos collide with the PKs of V059_Prioritised_household_enablements when we
-- insert, but we'll deal with that if we get to it.
commit;
create unique index fake_pk on #account_selection_options (account_number);
commit;

select has_anytime_plus, count(1) as hits
from #account_selection_options
group by has_anytime_plus
order by has_anytime_plus;
/* ahahaha so even with the new build, still way below the 600k need, no appreciable difference
** in the new build of prefix numbers
0 1474733
1 254384
*/

-- Cool, so now we'll stick these guys into the selection prioritisation table; only, hah, yeah,
-- we do have the duplicates to worry about, so first:
delete from #account_selection_options
where account_number in (select account_number from V059_Prioritised_household_enablements);
-- This is the slow clunky way to clear these out, but we don't care right now.

insert into V059_Prioritised_household_enablements (
    account_number
    ,inpanel
    ,has_anytime_plus
    ,scaling_segment_id
    ,accno_SHA1
)
select
    aso.account_number
    ,-1 as inpanel
    ,aso.has_anytime_plus
    ,ss.scaling_segment_id
    ,null                   -- Sybase can't handle figuring out putting default values into multi column indices, so we have to be explicit about accno_SHA1
from #account_selection_options as aso
inner join rs_scaling_segmentation as ss
on aso.account_number = ss.account_number;
-- Not worrying about the non-scaling variables at this point, the non-scaling variables
-- won't get considered at this point.

-- control total time:
select inpanel, has_anytime_plus, count(1) as hits
from V059_Prioritised_household_enablements
group by inpanel, has_anytime_plus
order by inpanel, has_anytime_plus;
/*
-1 0 1474660
-1 1 254368
0 0 160867
0 1 99313
*/
-- As expected, we're *way* off the mark for the AQnytime+ enablements, looking
-- at only 50% of requirement. We're slightly over on total panel sizes, but we're
-- not going to have any time to look into selecting for balance.

commit;
drop table #account_selection_options;
drop table #V059_Vespa_household_summary;
commit;

/****************** PART E01: INITIAL MARGINAL UTILITY OF AVAILABLE ACCOUNTS ******************/

-- Yeah, we need to calculate the utility first off because we want it for the various
-- initial selections, we still want to rank them to get the first choices. We need the
-- SHA1 keys to break selection ties:
update V059_Prioritised_household_enablements
set accno_SHA1 = hc.accno_SHA1
from V059_Prioritised_household_enablements
inner join vespa_analysts.Vespa_PanMan_SHA1_archive as hc
on V059_Prioritised_household_enablements.account_number = hc.account_number;

commit;

-- QA - do we need to build any more hashes? This should retunr zero...
select count(1) from V059_Prioritised_household_enablements where accno_SHA1 is null;
-- 0! Brilliant.

-- Okay, now we're in a position to calculate the initial marginal utilities: we could do
-- something on the weights calculated for us in the scaling build, maybe we'll use those
-- in the further iterations where we're worrying about single variable balance, but for
-- not we're just doing stratified sampling over the scaling segmentation IDs.
-- But first, we need some totals by scaling segment ID of the number of Vespa households
-- we're going to keep in each segment:
select scaling_segment_id,
    count(1) as current_vespa_accounts
into #Vepsa_scaling_segment_totals
from V059_Prioritised_household_enablements
where inpanel >= 0
group by scaling_segment_id;
-- Don't need to filter on quality because we already segregated the weak ones into the
-- disablement listing we were playing with.

commit;
create unique index fake_pk on #Vepsa_scaling_segment_totals (scaling_segment_id);
commit;

-- oh hey but we also need the number of sky base accounts in each of those segments too,
-- good thing we have the scaling build nearby....
select scaling_segment_id
    ,count(1) as Sky_Base_Households
    ,convert(int, 0) as current_vespa_accounts
    ,convert(int, 0) as selectible_accounts
into #sky_base_segment_totals
from rs_scaling_segmentation
group by scaling_segment_id;

commit;
create unique index fake_pk on #sky_base_segment_totals (scaling_segment_id);
commit;

update #sky_base_segment_totals
set current_vespa_accounts = vsst.current_vespa_accounts
from #sky_base_segment_totals
inner join #Vepsa_scaling_segment_totals as vsst
on #sky_base_segment_totals.scaling_segment_id = vsst.scaling_segment_id;

commit;

-- So we'd also like to know how many accounts we have to chose from. It's not
-- currently essential at this bit of the processing, but we'l definately need
-- it in a while and this is the sensible place to build it.
select scaling_segment_id,
    count(1) as selectible_Accounts
into #Selectible_scaling_segment_totals
from V059_Prioritised_household_enablements
where inpanel < 0
group by scaling_segment_id;

commit;
create unique index fake_pk on #Selectible_scaling_segment_totals (scaling_segment_id);
commit;

update #sky_base_segment_totals
set selectible_Accounts = ssst.selectible_Accounts
from #sky_base_segment_totals
inner join #Selectible_scaling_segment_totals as ssst
on #sky_base_segment_totals.scaling_segment_id = ssst.scaling_segment_id;

-- So now we've assembled those totals, don't need the partch tables:
commit;
drop table #Selectible_scaling_segment_totals;
drop table #Vepsa_scaling_segment_totals
commit;

-- And this was the bit which used to be dependent on PanMan, but that wasn't ready for
-- Scaling 2, so we cut the dependency. That said, the dependency wasn't complicated...

select
    account_number,
    accno_SHA1,
    convert(float, sbst.Sky_Base_Households) -- converting to float so that integer division doesn't kill the fractions
        / (sbst.current_vespa_accounts -- this is the number of households that are good already in the segment
            + (rank() over (partition by phe.scaling_segment_id order by accno_SHA1) / 3.0))
                as marginal_selection_benefit
            -- and that is a term which adjusts each item to consider how many other boxes would be
            -- enabled in the segment beore this one - ranking by the account number hash - which
            -- will also help evenly spread the priorities across the boxes in a segment.
into #marginal_weightings
from V059_Prioritised_household_enablements as phe
inner join #sky_base_segment_totals as sbst
on phe.scaling_segment_id = sbst.scaling_segment_id
where inpanel = -1; -- only calculate marginal weightings for boxes available for selection

-- Account number is the unique key, but as we've got another ranking to do, we'll
-- instead index what we're processing on:
commit;
create unique index processing_key on #marginal_weightings (marginal_selection_benefit, accno_SHA1);
commit;

-- remember that disablement requests, if we're going to have to do them, should order by SHA1 descending...
select account_number
    ,marginal_selection_benefit
    ,rank() over (order by marginal_selection_benefit desc, accno_SHA1) as selection_order
into #marginal_prioritised_weightings
from #marginal_weightings;

-- Now we're about to patch the stuff back into the main table, so we do need the structural key:
commit;
create unique index fake_pk on #marginal_prioritised_weightings (account_number);
commit;

update V059_Prioritised_household_enablements
set marginal_selection_benefit = t.marginal_selection_benefit
    ,selection_order = t.selection_order
from V059_Prioritised_household_enablements
inner join #marginal_prioritised_weightings as t
on V059_Prioritised_household_enablements.account_number = t.account_number;

-- Okay, so now we have all the marginal weights? We can go and select stuff?

commit;
drop table #marginal_prioritised_weightings;
drop table #marginal_weightings;
drop table #sky_base_segment_totals;
drop table #Vepsa_scaling_segment_totals;
commit;

/****************** PART E02: INITIAL SELECTION OF ANYTIME+ ACCOUNTS ******************/

-- hehehe, this bit doesn't actually need to be this hard or specific, with the way the
-- flags are currently arranged.

/****************** PART E03: INITIAL SELECTION VIA STRATIFIED SAMPLING ******************/

-- ditto for this bit, but, well, only because we don't have nearly enough Anytime+. 

/****************** PART E04: CONSOLIDATION & MARKING OF INITIAL SELECTIONS ******************/

-- Can do this guy in one step, lols:
update V059_Prioritised_household_enablements
set inpanel = 1     -- 1 for first round of selection
where inpanel = -1;
-- and has_anytime_plus or selection_order < 1400000 -- heh, if only there were time to write
-- panel balance code that respected the single variable profiles...

commit;
-- Ha! This structure also kind of doesn't really tolerate mixing between panels, but whatever.
-- We also don't have balance prioritisaiton recommendations either.

/****************** PART E05: SPLITTING SELECTIONS BETWEEN THE PANELS ******************/

-- OK, so now that we have those initial selections, how do we split them between the panels?
-- Well, we kind of have the hashes specifically to order boxes by some random variable, so
-- I guess we'll use those :)

-- And we'll mark it by changing the inpanel to 12 or 6 or 7, which is a bit of a hack, but
-- that's what we're doing at the moment...

-- We want to split total panel size up on a rough ratio of 10:3:3 to get more or less the
-- 500k - 150k - 150k split. So the control totals in section D02 show that there are 260k
-- boxes we're keeping in Vespa and another 1.7m to chose from. Splitting 1.99m in the ratio
-- gives 1.25m for Vespa and .375m for each Alt panel. Playing cheap tricks with the fact
-- that we're only splitting between three panels:

select top 375000 accno_sha1
into #Alt6_selection
from V059_Prioritised_household_enablements
where inpanel = 1
order by accno_sha1;

select top 375000 accno_sha1
into #Alt7_selection
from V059_Prioritised_household_enablements
where inpanel = 1
order by accno_sha1 desc;

commit;
create unique index fake_pk on #Alt6_selection (accno_sha1);
create unique index fake_pk on #Alt7_selection (accno_sha1);
commit;

update V059_Prioritised_household_enablements
set inpanel = 6
from V059_Prioritised_household_enablements
inner join #Alt6_selection as a6s
on V059_Prioritised_household_enablements.accno_sha1 = a6s.accno_sha1;

update V059_Prioritised_household_enablements
set inpanel = 7
from V059_Prioritised_household_enablements
inner join #Alt7_selection as a7s
on V059_Prioritised_household_enablements.accno_sha1 = a7s.accno_sha1;

commit;

-- and at this point with the 6 & 7 marks pushed, everything else should end up Vespa:
update V059_Prioritised_household_enablements
set inpanel = 12 where inpanel = 1;

commit

-- Okay, so does this end up giving us about the split we want?
select inpanel, count(1) as hits
from V059_Prioritised_household_enablements
group by inpanel
order by inpanel;
/* So the new golden box build added 406 accounts...
0 260180  <- already Vespa
6 375000
7 375000
12 979028  <- requested Vespa: total 12.5m so that's in line with requested numbers really.
*/

commit;
drop table #Alt7_selection;
drop table #Alt6_selection;
commit;

-- And we also have to put those into the big enablement listing:
insert into V059_enablement_listing_all_panels (
    account_number
    ,panel_id
)
select account_number, inpanel
from V059_Prioritised_household_enablements
where inpanel > 0;

commit;
-- that was easy.

/****************** PART H: PRIORITISATION (REQUIREES SCALING 2) ******************/

-- Part H needs a bit of refactoring into the iterative structure to support single
-- variable balance to support the rim weightings of Scaling 2. It also hasn't been
-- reviewing in a while, so a major reqork is probably on the cards.

-- This section has still not been updated for Scaling 2. Some of the table names
-- need to be updated, though the structure of the tables should b similar enough
-- to not need so many other changes? Though, it's still using the scaling 1 method
-- for calculating the prioritising weights, thich is kind of okay, have to think
-- about how to place a slightly lower emphasis on the non-scaling variables though.

-- Okay, this bit we can now inhieret from a lot of the structure that we built in
-- section E01, though we'll need to rebuild the temporary tables and the marginal
-- weightings, and do it in a way that pays some atention to single variable
-- profiles in this case.

-- Heh, not that we really have to worry about this yet, since we're not going to
-- have enough boxes to actually make any selection and in any case we don't have
-- time to deliver the balancing code, that's going to have to wait until some 
-- other time. Oh well!

-- ## Needs rebuild pending processing done in E01 ##

/****************** PART K01: ASSIGNMENT OF BATCH DATES ******************/

-- We've got the call back dates on the golden box summary, so we just patch them into the enablement table:
update V059_enablement_listing_all_panels
set batch = callback_day
from V059_enablement_listing_all_panels
inner join V059_Golden_account_waterfalling as gaw
on V059_enablement_listing_all_panels.account_number = gaw.account_number;

commit;

-- This should be empty, we should have a date for everything:
select panel_id, count(1) as hits
from V059_enablement_listing_all_panels
where batch is null
group by panel_id;
-- yeah, sweet. Oh wait... We ended up getting batch dates for most of the disablements
-- we wanted too, great, that's a nice unexpected accident. But there are still some holes
-- there. Oh well.

/****************** PART K02: EXPECTED PANEL WELLBEING METRICS ******************/

-- Edit: these totals below are just the current Vespa panel. Which will actually be good
-- for comparison? But this needs a rebuild with options for full selection enablement...

-- Panel size is a bit difficult to guess at because that's all about how well boxes report.
-- In terms of raw coverage, how big are the bits of the Sky UK base that have no change for
-- representation at all?
select sum(Sky_Base_Households)
from #sky_base_segment_totals
where current_vespa_accounts = 0;
-- 9415508 for full sky base...
-- 663017 with zero panel cover...

select 663017 / 9415508.0;
-- 7% - so that's not too much... but this isn't weak indexing, this
-- is no coverage at all from the panel. Oh well. And this isn't about
-- balance either, the balance metric wil be much lower.

select sum(Sky_Base_Households)
from #sky_base_segment_totals
where current_vespa_accounts = 0 and selectible_accounts = 0;
-- 202754 -> 2%, so that's okay, but what about the indexing?

-- Okay, so how well is our sample spread across these segments? Grabbing
-- the indexed coverage and traffic light stuff from the panel management
-- dashboard... we need a panel totals table first, with the number of
-- of accounts in each panel:
select
    case when inpanel in (0, 12) then 'VESPA'
        when inpanel = 6 then 'ALT6'
        when inpanel = 7 then 'ALT7'
      end as panel
    ,count(1) as accounts
into #panel_totals
from V059_Prioritised_household_enablements
group by panel;

-- And we also need a table of how many guys are in each segment:
select
    scaling_segment_id
    ,case when inpanel in (0, 12) then 'VESPA'
        when inpanel = 6 then 'ALT6'
        when inpanel = 7 then 'ALT7'
      end as panel
    ,count(1) as panel_selections
    ,convert(int, 0) as sky_base_accounts
    ,convert(decimal(6,2), null) as selectoin_index_coverage
into #panel_selections
from V059_Prioritised_household_enablements
group by scaling_segment_id, panel;

commit;
create unique index fake_pk on #panel_selections (scaling_segment_id, panel);
commit;

-- Now we poke in the Sky total for each segment:
update #panel_selections
set sky_base_accounts = sbst.Sky_Base_Households
from #panel_selections
inner join #sky_base_segment_totals as sbst
on #panel_selections.scaling_segment_id = sbst.scaling_segment_id;

commit;

-- And calculate the indices for each segment:
create variable @total_sky_base int;
select @total_sky_base = sum(Sky_Base_Households)
from #sky_base_segment_totals;
commit;
update #panel_selections
set selectoin_index_coverage =
        case when 200 < 100 * (panel_selections)   * @total_sky_base / convert(float, Sky_Base_accounts) / pt.accounts
                then 200
        else            100 * (panel_selections)   * @total_sky_base / convert(float, Sky_Base_accounts) / pt.accounts
      end
from #panel_selections
inner join #panel_totals as pt
on #panel_selections.panel = pt.panel;

commit;

-- And now build the coverage metrics:
select
    panel,
    convert(decimal(6,3), sum(Sky_Base_accounts) / convert(float, @total_sky_base)) as coverage_metric
from #panel_selections
where selectoin_index_coverage > 80
group by panel;
/* So this isn't bad, but bear in mind it's coverage only over the scaling variables.
VESPA .664
ALT6  .644
ALT7  .648
*/

-- This doesn't compare to the coverage metrics on the PanMan report because they
-- use both scaling and non-scaling variables. What is the current coverage only
-- by scaling varibles then? Of the curent panel? for comparison...

create variable @retained_vespa_size int;
select @retained_vespa_size = sum(current_vespa_accounts)
from #sky_base_segment_totals;

select
    scaling_segment_id
    ,sky_base_households
    ,current_vespa_accounts
    ,case when 200 < 100 * (current_vespa_accounts)   * @total_sky_base / convert(float, Sky_Base_households) / @retained_vespa_size
                then 200
        else            100 * (current_vespa_accounts)   * @total_sky_base / convert(float, Sky_Base_households) / @retained_vespa_size
    end as coverage_index
into #retained_segment_totals
from #sky_base_segment_totals;

-- it's also not comparable because the minimum box reporting quality differs
-- from what gets used in PanMan, but if we were to set that perameter to 0.9
-- it would... only have the previously mentioned structural difference, rather
-- than two major structural differences.
select
    convert(decimal(6,3), sum(Sky_Base_households) / convert(float, @total_sky_base)) as coverage_metric
from #retained_segment_totals
where coverage_index > 80;
-- 0.609, which means the benefits we're immediately getting are tiny. Which
-- is what we expected.

-- Though these metrics are on the full selection list, they'll differ a lot
-- from what we'll get when we see what reports back decent data...

-- Maybe we need some stochastic simulations of how good the metrics are
-- under a few different runs of randomly assigning 45% of boxes good data
-- return flags and seeing how the metrics come out. Not exactly time to
-- do that now though, eh?

-- Heh, we've also got no space to do any theoretical assesment of how high that
-- number could go were the panel balanced and ideal. No idea at all. That'd be
-- an informative graph to build, if we actually wanted to make some informed
-- decisions of how big the panel should be, but that'd require a lot more time...

/****************** PART K03: BATCH CONTROL TOTALS FOR DEMONSTRATION OF CONSISTENT UNLOAD/LOAD ******************/

select panel_id, batch, count(1) as hits, mod(sum(mod(convert(bigint, account_number),2111)),2111)
into #enablement_control_totals
from V059_enablement_listing_all_panels
group by panel_id, batch
order by panel_id, batch;

-- So we've got the number of accounts in each batch, and the modulus of the sum of
-- all account numbers by 2111 (though you have to convert them to integers etc)

/****************** PART K04: EXPORT TO .csv FILES ******************/

-- This section only works on P5X1 and only through Sybase interactive. So, whatever.

select *
from V059_enablement_listing_all_panels
order by panel_id, batch, account_number;
output to 'D:\\Vespa\\Golden box migration batches\\Vespa_all_enablement_requests.csv';

-- Ok, done. Well, requests at least. Let's also get the control totals:

select *
from #enablement_control_totals
order by panel_id, batch;
output to 'D:\\Vespa\\Golden box migration batches\\Vespa_all_enablement_controls.csv';

/****************** PART Q01: QA ON THE SELECTION ORDER ******************/

-- Here's a little QA on the selection order; is it really an ordering? Are
-- the higly ranked items really from segments that are massively under
-- represented?

-- What's the spread of selection orders like?
select max(selection_order), min(selection_order)
from V059_Prioritised_household_enablements;
-- 1729422 and 1...

-- This should only give us the NULLs that are on the Vespa panel
-- or things already through a prior round of selection
select selection_order, count(1) as hits
from V059_Prioritised_household_enablements
group by selection_order
having hits > 1
order by hits;

-- OK, so logically it is a selection order thing. Now to check the business sense:


/****************** PART Q: BUILD & PROCESS QA ******************/

-- Don't actually know what we're checking on this yet... we'll see what turns up...
-- Box inclusions in listings? dunno really.























--additional accounts required:
--6125 accounts with Callbacks <4 were sent to Paul Hicks in file 0_3.csv
--This section will find accounts excluding waterfall exclusion 17, with 3 or more callbacks. It's a copy from above, just without filter 17.

update V059_Golden_account_waterfalling
   set waterfall_exit = 0
;
alter table V059_Golden_account_waterfalling add waterfall_exit_exc17 int default 0;

update V059_Golden_account_waterfalling
   set waterfall_exit_exc17 = case when awb.knockout_level_exc17 is null then -1
                             else awb.knockout_level_exc17 end
  from V059_Golden_account_waterfalling
       inner join PanMan_adhoc_waterfall_base as awb on V059_Golden_account_waterfalling.account_number = awb.account_number
;

-- Now we're ignoring accounts that drop out at the null prefix test
-- because Jonathan's supercedes this. It's the last test in the waterfall
-- priority, so everything failing here has already passed every other
-- condition, so we needn't worry about having to rerun the watefall build
-- to establish where things drop out. (If there were any further tests
-- after the one being removed, that would require a waterfall rerun.)


-- Okay, so, yeah, that's all the processing we need there, which we borrowed from
-- the "golden waterfall breakdown.sql" script, though the table name is now prefixed
-- with V059.

select waterfall_exit_exc17, count(1) as hits
from V059_Golden_account_waterfalling
group by waterfall_exit_exc17
order by waterfall_exit_exc17;
-- wait... so we've nulls and zeroes? The NULLs are the good ones we can use, but
-- we're also seeing about 5% of things not in the waterfall base at all. I guess
-- those are the inactive accounts or suchlike.

--JG amend: Null was acting weird, so I changed it to -1

/****************** PART D02: FILTERING THE GOLDEN WATERFALL ******************/

-- So here we're taking the flags we made above and building a list of boxes which might still be viable:
drop table #account_selection_options

  select gaw.account_number, gaw.has_anytime_plus,gaw.worst_calling_back
    into #account_selection_options
    from V059_Golden_account_waterfalling as gaw
         left join vespa_analysts.V059_enablement_listing_all_panels as elap on gaw.account_number = elap.account_number
   where gaw.has_bad_boxes = 0
     and has_anytime_plus = 1
     and has_null_prefix_boxes = 1
     and gaw.worst_calling_back >= 3
     and elap.account_number is null
     and waterfall_exit > -1 --we are only looking at the set that fell at hurdle 17 last time
     and waterfall_exit_exc17 =-1
;

--6125 accounts with less than 4 callbacks sent to Paul Hicks in file 0_3.csv
--only 9996 more found this time.




