/****************************************************************
**
**      PROJECT VESPA: PROMO DATA LOADING
**
** SCRIPT ORIGINALLY WRITTEN FOR SPOT DATA AND AMENDED FOR PROMO DATA
**
** (So we've got advertisment data in a bunch of plain text files.
** (Adverts are broadcast in particular "spots", hence the name.)
** These arrive with fixed width delimitations from some unknown
** source, and while that's being turned into a live feed, we're
** hacking together a Python script (p5x1 has 2.6.6!!) to convert
** all the files into the appropriate format for Sybase loading.
**
** This script picks up from there, looking at the header file
** built by Python and spinning through the spot files listed.
** Each spot file gets loaded into a temporary table with all the
** fields being VARCHAR(X)'s, and from there are processed into
** more appropriate data types like DATETIME and INT. Some fields
** are combined, others dropped, and we're also linking to EPG
** and Clearcast data to try to get the table usable. Ultimately
** all the adds of the batch are combined into a single holding
** pen and dumped at once into the general persistent spots_all
** table.
**
** There's also a .pdf which has a bunch of spec stuff in it. In
** one place it says the filenames of the spot files have years
** as YYYY and in others it says years as YY. Unfortunate. Looking
** at the filenames themselves, it appears they use YYYY for dates,
** but it doesn't make us trust it any more in this case.
**
** Also: the spots are as-run logs. The EPG: is scheduled stuff.
** There may well be conflicts between these, but we'll manage that
** later. There might be a bunch of post-load processing that goes
** on, depending on how we want to try to line stuff up.
**
** This script can be run as your own user. The common tables are
** created in a different script.)
**
** Code sections:
**
**      Part 0:
**              001: Logger initiation
**              002: Remove antiquated promos
**
**      Part A: Loading data!
**              A01: Load promo headers
**              A02: Load promo data
**
**      Part B:
**              B01: Processing headers
**              B02: Removing duplicate files
**              B03: Segregating conflicting logs
**
**      Part C: Promo processing
**              C01: Data cleansing & formatting
**              C02: Linkage to Clearcast
**              C03: Linkage to EPG
**
**      Part D: Fancy detailed processing? (not yet in scope)
**              D01: Placeholder for Promo-level deduplication?
**              D02: Placeholder for other stuff?
**
**      Part E: Data migration!
**              E01: Putting linking statistics on the lookup
**              E02: Moving new promo data into the main table
**              E03: Tidying up various stuff
**
**      Part F: Failures.
**              F01: Looking into failures (currently manual)
**
** To do:
**     15/ Linking to Clearcast data, when that's available, or the Promo quivalent thereof?
**     16/ catch and trap the quarantined promos at various important points.
**     17/ do we want to quarentine promos that have missing / invalid values for fields we consider important? yes. Well, they're never going to be null, because they come in fixed width, but essential field might end up empty or they might fail to be numeric or something like that.
**     18/ Check that the duplicate log import functionality works like we want it to
**     20/ Pull the counts of quarentined items back onto the loading lookup table
**     21/ There are still a bunch of items on the EPG table that don't have barb codes. We need to get those added too I think.
**     22/ In the case where there are load files collisions with matching hashes, we want to take the first of them rather than mark them all as colissions. Meaning we have to rank on ID based on file hash etc. This guy really wants to be implemented before the full build is rolled out.
**
** OK, so not sure how we're taking the Promos loading going
** forwards. Are we doing the full duplication treatment? Do
** we need to check the spec and rebuild all the things we need?
** Who is well placed to organise all that stuff about how
** promos are processed once the raw load is done?
**
****************************************************************/

-- Are we doing anything else to record which channels do and do
-- not have advertisment data supplied? Could be useful to see
-- the kind of promo coverage that we have ##!Feature!Request!##

-- OK, so the whole process works, things are going into the full
-- promos table. Now I should check that the duplicate handling does
-- what it should before rolling in the whole set of all that data.
-- Heh, still need to talk to Ops about having them upload it :-/


/**************** 001: PREPARE THE LOGGER! ****************/

-- Because this kind of automated thing, it's good to be able to fire
-- it off and then check later what it's up to.

CREATE VARIABLE @Promo_logging_ID bigint;
CREATE VARIABLE @Refresh_identifier varchar(40);

set @Refresh_identifier = dateformat(getdate(),'yyyy-mm-dd hh:mm') || ' promo load batch'
EXECUTE citeam.logger_create_run 'VespaAdPromoLoading', @Refresh_identifier, @Promo_logging_ID output;

-- Push something in to note that the whole process has started
EXECUTE citeam.logger_add_event @Promo_logging_ID, 3, '001: Starting the promo load batch.';

commit;

/**************** 002: REMOVE ANY PROMOS STILL BEING KEPT AROUND ****************/

-- Yeah, not especially threadsafe, but will we ever be running this
-- more than once at a time? I hope not. Maybe?
delete from vespa_analysts.promos_holding_pen;
delete from vespa_analysts.promos_derived_variables;

commit;
-- Mark any outstanding things that didn't get flushed last time the process finished
update vespa_analysts.promos_load_lookup
set load_status = 'Aborted!'
where load_status in ('Pending','Pre-link','CC-linker','EPG-linker','Holding');
-- We're not marking off the various failure codes, only the ones that represent
-- valid internal states and thus aborted procedures.
commit;
-- There may yet be things in the Quarantine too if the last run
-- wasn't properly resolved. But those are expected to be treated
-- manually, so we're not culling them.

EXECUTE citeam.logger_add_event @Promo_logging_ID, 3, '002: Complete! (Purge ancients)';

commit;

/**************** A01: ADD ITEMS TO THE LOADING LOOKUP ****************/

-- Okay, with the current build, you have to run it from inside p5x1,
-- and you have to use the (face-palmingly bad) Sybase Interactive
-- SQL Java interface.
input into vespa_analysts.promos_load_lookup (
        file_MD5_hash
        ,count_from_preprocessing
        ,original_full_path
        )
from 'D:\\Vespa promo loading\\Promo_parse_headers.csv'
format ascii;

commit;
EXECUTE citeam.logger_add_event @Promo_logging_ID, 3, 'A01: Complete! (Import headers)';
commit;

/**************** A02: IMPORT ALL THE AD PROMOS ****************/

-- And the actual promo data:
input into vespa_analysts.promos_holding_pen (
        file_MD5_hash
        ,r_infile_id
        ,promo_date
        ,channel
        ,Barb_code
        ,promo_start_time
        ,promo_duration
        ,promo_id
        ,cart_no
        ,promo_product_description
)
from 'D:\\Vespa promo loading\\Promo_parse_dump.csv'
format ascii;
--112909 rows read

-- This is mildly faster than the format with all the derived values in
-- there too, though not as fast as we'd remembered / hoped. The ID might
-- be slowing it down a bit? But that'd be truly tragic :/

commit;
EXECUTE citeam.logger_add_event @Promo_logging_ID, 3, 'A02: Complete! (Import logs)';
commit;


/**************** B01: POST-IMPORT HEADER PROCESSING ****************/

-- Deriving the other values: the transmission date and station ID:
update vespa_analysts.promos_load_lookup
set
         log_station_code = substring(original_full_path, length(original_full_path) - 7, 4)
        ,date_of_transmission = convert(date,
                substring(original_full_path, length(original_full_path) - 16, 4) || '-' ||
                substring(original_full_path, length(original_full_path) - 12, 2) || '-' ||
                substring(original_full_path, length(original_full_path) - 10, 2))
where load_status = 'Pending';

-- Pull out the QA totals for the number of records in each file:
update vespa_analysts.promos_load_lookup
set count_loaded_raw = t.tehcounts
from vespa_analysts.promos_load_lookup
inner join (
        select
                file_MD5_hash
                ,count(1) as tehcounts
        from vespa_analysts.promos_holding_pen
        group by file_MD5_hash
) as t
on t.file_MD5_hash = vespa_analysts.promos_load_lookup.file_MD5_hash
-- This guy is not stable with respect to possible MD5 hash collision
-- within a single load set, but hey, they'll all get isolated anyway.

commit;
EXECUTE citeam.logger_add_event @Promo_logging_ID, 3, 'B01: Complete! (Header processing)';
commit;

/**************** B02: REMOVAL OF DUPLICATED LOG FILES ****************/

-- ## Oh, wait; the current build won't help if the file is duplicated
-- inside the same loading batch. Are we going to check that? We really
-- do need to, this has been flagged as request 22 and needs to be done
-- before the whole thing is rolled out. Not a big change though.##

-- Check that the file isn't a duplicate of some other file we've already processed
select sll.load_id, sll.file_MD5_hash,
        max(case when sll2.load_status = 'Pending' then 1 else 0 end) as current_load_dupe
into #Duplicate_load_hashes
from vespa_analysts.promos_load_lookup as sll
inner join vespa_analysts.promos_load_lookup as sll2
on sll.file_MD5_hash = sll2.file_MD5_hash
and sll.load_id <> sll2.load_id
and sll2.load_status in ('Complete', 'Pending') -- only check against promo sets that are fine, or that we're currently loading
and sll.load_status = 'Pending'
group by sll.load_id, sll.file_MD5_hash;

commit;

update vespa_analysts.promos_load_lookup
set sll.load_status = case
        when dlh.current_load_dupe = 0 then 'Duplicate'
        else 'Collision!'
    end
from vespa_analysts.promos_load_lookup as sll
inner join #Duplicate_load_hashes as dlh
on sll.load_id = dlh.load_id;

-- Also: segregate off all the items which are duplicated files within
-- the same load, because we should really look into any of this junk
-- manually:

select
insert into vespa_analysts.promos_quarantine (
        failed_because
        ,failed_key
        ,shp.file_MD5_hash
        ,r_infile_id
        ,promo_date
        ,channel
        ,Barb_code
        ,promo_start_time
        ,promo_duration
        ,promo_id
        ,cart_no
        ,promo_product_description)
select
        'Colliding MD5 hashes within same load cycle.'
        ,rank() over (order by shp.file_MD5_hash)
        ,shp.file_MD5_hash
        ,shp.r_infile_id
        ,shp.promo_date
        ,shp.channel
        ,shp.Barb_code
        ,shp.promo_start_time
        ,shp.promo_duration
        ,shp.promo_id
        ,shp.cart_no
        ,shp.promo_product_description
from vespa_analysts.promos_holding_pen as shp
inner join #Duplicate_load_hashes as dlh
on dlh.file_MD5_hash = shp.file_MD5_hash
where dlh.current_load_dupe = 1;

-- Now delete those guys from the holding pen, because they're either
-- already previously been processed or are now quarentined:
delete from vespa_analysts.promos_holding_pen where file_MD5_hash in (
        select file_MD5_hash from #Duplicate_load_hashes
);

commit;
EXECUTE citeam.logger_add_event @Promo_logging_ID, 3, 'B02: Complete! (Remove load dupes)';
commit;

/**************** B03: SEGREGATION OF CONFLICTING LOGS ****************/

-- Check each promo file doesn't overlap time and station with some other *different* file:
select distinct sll.load_id, sll.file_MD5_hash
into #Colliding_load_files
from vespa_analysts.promos_load_lookup as sll
inner join vespa_analysts.promos_load_lookup as sll2
on sll.log_station_code = sll2.log_station_code
and sll.date_of_transmission = sll2.date_of_transmission
and sll.file_MD5_hash <> sll2.file_MD5_hash
and sll.load_id <> sll2.load_id
and sll2.load_status = 'Complete' -- only check against promo sets that are fine
and sll.load_status = 'Pending';

commit;

update vespa_analysts.promos_load_lookup
set load_status = 'Overlap'
from vespa_analysts.promos_load_lookup as sll
inner join #Colliding_load_files as clf
on sll.load_id = clf.load_id;

-- Move all the items into the quarantine, out of the active processing area
insert into vespa_analysts.promos_quarantine (
        failed_because
        ,failed_key
        ,shp.file_MD5_hash
        ,r_infile_id
        ,promo_date
        ,channel
        ,Barb_code
        ,promo_start_time
        ,promo_duration
        ,promo_id
        ,cart_no
        ,promo_product_description
)
select
        'Distinct MD5 hashes but time / station overlaps with existing log file.'
        ,null -- even with the default, Sybase insists on columns featuring in a multi-column index to be explicitly INSERT'ed :-/
        ,shp.file_MD5_hash
        ,shp.r_infile_id
        ,shp.promo_date
        ,shp.channel
        ,shp.Barb_code
        ,shp.promo_start_time
        ,shp.promo_duration
        ,shp.promo_id
        ,shp.cart_no
        ,shp.promo_product_description
from vespa_analysts.promos_holding_pen as shp
inner join #Colliding_load_files as clf
on shp.file_MD5_hash = clf.file_MD5_hash;

commit;
EXECUTE citeam.logger_add_event @Promo_logging_ID, 3, 'B03: Complete! (Segregate conflicts)';
commit;

/**************** C01: ROW-BY-ROW DERIVATIONS FOR PROMOS ****************/

-- First, forge all the text columns into some other data format, with
-- any date processing etc as required.
insert into vespa_analysts.promos_derived_variables (
        id
        ,file_MD5_hash
        ,infile_id
        ,promo_date
        ,channel
        ,Barb_code
        ,promo_start_time
        ,promo_duration
        ,promo_id
        ,cart_no
        ,promo_product_description
)
select
        id
        ,file_MD5_hash
        ,convert(int, r_infile_id)
        ,convert(datetime, substring(promo_date,1,4) || '-' ||
                        substring(promo_date,5,2) || '-' ||
                        substring(promo_date,7,2) || ' 00:00:00') -- promo_date
        ,channel
        ,convert(decimal(10,0), Barb_code) -- barb_code
        ,dateadd(second,
                convert(int, promo_start_time),
                convert(datetime, substring(promo_date,1,4) || '-' ||
                        substring(promo_date,5,2) || '-' ||
                        substring(promo_date,7,2) || ' 00:00:00')) -- promo_start_time
        ,convert(int, promo_duration) -- promo_duration
        ,convert(bigint, promo_id) -- promo_id
        ,cart_no -- cart_no
        ,promo_product_description
from vespa_analysts.promos_holding_pen;

-- ##Automated QA option##: compare the total duration of promos (based on this partitioning)
-- to the total stated length of the break.

-- Okay, we have to deal with the BST->UTC conversion; everything between
-- the last Sundays in March and October are bumped forwards an hour. Our
-- processing so far has caused overlap at the October boundary, but that
-- is okay, we still have the date of transmission so we can see the days
-- that need the hour clipped off them.

create variable @earliest_log_year   int;
create variable @latest_log_year     int;

select
    @earliest_log_year  = datepart(year, min(promo_start_time))
    ,@latest_log_year   = datepart(year, max(promo_start_time))
from vespa_analysts.promos_derived_variables;

commit;

-- Then pull a list of boundary days from the calendar: all Sundays
-- in March and October for the years in which we have logs.
select
    sky_date
    ,rank() over (partition by datepart(year, sky_date), datepart(month, sky_date) order by sky_date desc) as last_sunday_rank
into #BST_boundary_days
from CITeam.sky_calendar
where datepart(year,sky_date) >= @earliest_log_year
and datepart(year,sky_date) <= @latest_log_year
and datepart(month, sky_date) in (3,10) -- Only want the March & October instances
and datediff(day, sky_week_start, sky_date) = 2; -- Sky weeks start on Friday, so Sundays are always 2 days past week start
--Not bothering to index, there might be like 4 or 6 records at most
commit;
-- Now clip out all but the last sunday for each month
delete from #BST_boundary_days where last_sunday_rank > 1;
commit;

-- And then: go from a listing of days into a BST-or-not lookup for each ID
-- Actually: we only need a list of those IDs needing a BST correction
select
    id
into #BST_correction_lookup
from vespa_analysts.promos_derived_variables as shp
inner join #BST_boundary_days as bd
    on shp.promo_start_time <= bd.sky_date
group by shp.id
having mod(count(1),2) = 1;
-- How this works: an odd number of Sundays less than or equal to
-- date_of_transmission means it's inside the BST adjustment. An
-- odd number of Sundays less than or equal to date_of_transmission
-- means that it's outside the BST interval.
commit;


create index forjoining on #BST_correction_lookup (id);
commit;

-- Push relevant BST corrections onto the derived variables table:
update vespa_analysts.promos_derived_variables
set
    promo_start_time    = dateadd(hour, -1, promo_start_time)
from vespa_analysts.promos_derived_variables as sdv
inner join #BST_correction_lookup as bcl
on sdv.id = bcl.id;

commit;

-- More post-load processing stuff:

-- Get the autonumbered load IDs from the loading header onto the promos:
update vespa_analysts.promos_derived_variables
set load_id = sll.load_id
from vespa_analysts.promos_derived_variables
inner join vespa_analysts.promos_load_lookup as sll
on vespa_analysts.promos_derived_variables.file_MD5_hash = sll.file_MD5_hash
and sll.load_status = 'Pending';
-- Okay, so that's not quite header processing, but its the best place for it
-- because those states might change in a bit.

-- Need something to test that the promos are all good with their barb codes?

EXECUTE citeam.logger_add_event @Promo_logging_ID, 3, 'C01: Complete! (Promo processing)';
-- Oh, and mark that we've done all the pre-linking stuff on the headers table
update vespa_analysts.promos_load_lookup
set load_status = 'Pre-link'
where load_status = 'Pending';
commit;

/**************** C03: LINKING PROGRAMME KEYS FROM THE EPG ****************/

update vespa_analysts.promos_load_lookup
set load_status = 'EPG-linker'
where load_status = 'Pre-link';
commit;

-- First build a lookup for the station, break start time and break end times for each
-- and then populate this smaller table, and then stitch those details back into the
-- promo stuff.
select
        barb_code
        ,promo_start_time
        ,dateadd(second, promo_duration, promo_start_time) as promo_end_time
        ,rank() over (partition by barb_code, promo_start_time order by id) as rankage
        ,convert(bigint, null) as prior_epg_key
        ,convert(bigint, null) as following_epg_key
into #break_instances
from vespa_analysts.promos_derived_variables;
-- Better than doing GROUP BY apparently.
delete from #break_instances where rankage <> 1;

commit;

-- Because we have a couple of joins to do on these things:
create unique index break_start_joining on #break_instances (barb_code, promo_start_time);

commit;

-- Pull in the EPGs for the prior show based on when the add break starts (should be in
-- the middle of the show that just finished?)
update #break_instances
set bi.prior_epg_key = ved.programme_trans_sk
from #break_instances as bi
inner join sk_prod.vespa_epg_dim as ved
on ved.barb_code = bi.barb_code
and ved.tx_start_datetime_utc < bi.promo_start_time
and ved.tx_end_datetime_utc > bi.promo_start_time;

-- EPG for following show based on what's playing when the ad break ends:
update #break_instances
set bi.following_epg_key = ved.programme_trans_sk
from #break_instances as bi
inner join sk_prod.vespa_epg_dim as ved
on ved.barb_code = bi.barb_code
and ved.tx_start_datetime_utc < bi.promo_end_time
and ved.tx_end_datetime_utc > bi.promo_end_time;

commit;

-- Now push those marks back onto the promo detail table:
update vespa_analysts.promos_derived_variables
set
        preceeding_programme_trans_sk = bi.prior_epg_key
        ,succeeding_programme_trans_sk = bi.following_epg_key
from vespa_analysts.promos_derived_variables as svd
inner join #break_instances as bi
on bi.barb_code = svd.barb_code
and bi.promo_start_time = svd.promo_start_time;

commit;

-- Afterwards: check if there are any cases of an internal ad break having different
-- programmes before and after, or a transitional ad break having the same programme
-- on either side. We'll probably get this eventually since we're comparing planned
-- stuff to actual broadcasts, but it's a good start.


-- Maybe move some stuff to Quarentine, if it don't link no good.
-- ##

-- Pull out some control totals on volumes of good EPG linkage.
update vespa_analysts.promos_load_lookup
set preceeding_programme_linked = t.tehcounts
from vespa_analysts.promos_load_lookup
inner join (
        select
                load_id
                ,count(1) as tehcounts
        from vespa_analysts.promos_derived_variables
        where preceeding_programme_trans_sk is not null
        group by load_id
) as t
on t.load_id = vespa_analysts.promos_load_lookup.load_id;

update vespa_analysts.promos_load_lookup
set succeeding_programme_linked = t.tehcounts
from vespa_analysts.promos_load_lookup
inner join (
        select
                load_id
                ,count(1) as tehcounts
        from vespa_analysts.promos_derived_variables
        where succeeding_programme_trans_sk is not null
        group by load_id
) as t
on t.load_id = vespa_analysts.promos_load_lookup.load_id;

commit;

-- Mark all the things that are still good (hahaha not processing anything into the
-- quarantine based on linkage fails yet) as sitting around doing their thing

update vespa_analysts.promos_load_lookup
set load_status = 'Holding'
where load_status = 'EPG-linker';
commit;

EXECUTE citeam.logger_add_event @Promo_logging_ID, 3, 'C03: Complete! (EPG programme linkage)';
commit;

/**************** D01: DEDUPLICATION? ****************/

-- ##

/**************** D02: OTHER POST-LOAD PROCESSING / CLEANSING? ****************/

-- ##

/**************** E02: DUMPING THE NEW PROMOS INTO THE BIG TABLE ****************/

-- Get a list of all the good loads
select load_id, file_MD5_hash
into #good_loads
from vespa_analysts.promos_load_lookup
where load_status = 'Holding';
-- The load_id is the main key, but the MD5 has is the one that's
-- part of the PK on the promo holding table.

commit;
-- not many rows to worry about here but whatever
create unique index joinzors_by_hash    on #good_loads (file_MD5_hash);
create unique index joinzors_by_ID      on #good_loads (load_id);
commit;

-- The formats and everything should be alligned
insert into vespa_analysts.promos_all (
    load_id
    ,infile_id
    ,promo_date
    ,channel
    ,Barb_code
    ,promo_start_time
    ,promo_duration
    ,promo_id
    ,cart_no
    ,promo_product_description
    ,preceeding_programme_trans_sk
    ,succeeding_programme_trans_sk
)
select 
        sdv.load_id
        ,sdv.infile_id
        ,sdv.promo_date
        ,sdv.channel
        ,sdv.Barb_code
        ,sdv.promo_start_time
        ,sdv.promo_duration
        ,sdv.promo_id
        ,sdv.cart_no
        ,sdv.promo_product_description
        ,null
        ,null
        --sdv.preceeding_programme_trans_sk
        --sdv.succeeding_programme_trans_sk
from vespa_analysts.promos_holding_pen as shp
inner join vespa_analysts.promos_derived_variables as sdv
        on shp.id = sdv.id
inner join #good_loads
        on shp.file_MD5_hash = #good_loads.file_MD5_hash;

commit;
EXECUTE citeam.logger_add_event @Promo_logging_ID, 3, 'E02: Complete! (Main table dump)';
commit;

/**************** E03: CONTROL TOTALS ON FINAL STRUCTURE ****************/

-- Mark in the loading table the control totals of the counts of things
-- moved into the full promos table:
update vespa_analysts.promos_load_lookup
set count_moved_to_all = t.hits
from vespa_analysts.promos_load_lookup as sll
inner join
(
        select sa.load_id, count(1) as hits
        from vespa_analysts.promos_all as sa
        inner join #good_loads
        on sa.load_id = #good_loads.load_id
        group by sa.load_id
) as t
on sll.load_id = t.load_id;

-- 

-- Mark as successful:
update vespa_analysts.promos_load_lookup
set load_status = 'Complete'
from vespa_analysts.promos_load_lookup as sll
inner join #good_loads
        on sll.load_id = #good_loads.load_id;

commit;
EXECUTE citeam.logger_add_event @Promo_logging_ID, 3, 'E03: Complete! (Final QA)';
commit;

/**************** E04: TIDYING UP VARIOUS JUNKS ****************/

-- Don't run this section if the previous stuff didn't work because
-- all your data will dissappear and you'll have to start the INPUT
-- INTO section again which is annoying because it takes ages.

-- Clip the good things out of the holding table, but leave all the various failures:
delete from vespa_analysts.promos_holding_pen
where file_MD5_hash in (select file_MD5_hash from #good_loads);
-- Can't use a join because DELETE doesn't play nice with them
commit;

-- Also need to cull out of the derived variables table
delete from vespa_analysts.promos_derived_variables
where file_MD5_hash in (select file_MD5_hash from #good_loads);
commit;

EXECUTE citeam.logger_add_event @Promo_logging_ID, 3, 'E04: Complete! (Tidying)';
commit;

/**************** F01: NOW YOU SHOULD LOOK INTO THESE FAILURES YOURSELF: ****************/
-- QA on what's still borked:
/*
select shp.load_id
        ,sll.log_station_code    
        ,sll.date_of_transmission
        ,sll.load_status as failure_type
        ,count(1) as hits
from vespa_analysts.promos_holding_pen as shp
inner join vespa_analysts.promos_load_lookup as sll
        on shp.load_id = sll.load_id;
*/
-- don't need filters as we removed everything that wasn't borked already.

-- Yeah, you should resolve all of those manually. Actually, the default
-- behaviour will be to remove everything from the holding pen (##!) and
-- dump everything that's weird into the quarantine (##!) so you'd just
-- have to check that table.
