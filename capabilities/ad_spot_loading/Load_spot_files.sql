/****************************************************************
**
**      PROJECT VESPA: SPOT DATA LOADING
**
** So we've got advertisment data in a bunch of plain text files.
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
** later.
**
** This script can be run as your own user. The common tables are
** created in a different script.
**
** Code sections:
**
**      Part 0:
**              001: Logger initiation
**              002: Remove antiquated spots
**
**      Part A: Loading data!
**              A01: Load spot headers
**              A02: Load spot data
**
**      Part B:
**              B01: Processing headers
**              B02: Removing duplicate files
**              B03: Segregating conflicting logs
**
**      Part C: Spot processing
**              C01: Data cleansing & formatting
**              C02: Linkage to Clearcast
**              C03: Linkage to EPG
**
**      Part D: Fancy detailed processing? (not yet in scope)
**              D01: Placeholder for Spot-level deduplication?
**              D02: Placeholder for other stuff?
**
**      Part E: Data migration!
**              E01: Putting linking statistics on the lookup
**              E02: Moving new spot data into the main table
**              E03: Tidying up various stuff
**
**      Part F: Failures.
**              F01: Looking into failures (currently manual)
**
** To do:
**     15/ Linking to Clearcast data, when that's available.
**     16/ catch and trap the quarantined spots at various important points.
**     17/ do we want to quarentine spots that have missing / invalid values for fields we
**          consider important? yes. Well, they're never going to be null, because they come
**          in fixed width, but essential field might end up empty or they might fail to be
**          numeric or something like that.
**     21/ There are still a bunch of items on the EPG table that don't have barb codes. We
**          need to get those added too I think.
**     23/ Okay, so for the full load, we've got hash duplicates, and we've also got logs
**          for the same channel that don't have hash duplicates, ie, we've got conflicting
**          records of stuff. Awesome. This guy is going to be a *big* change.
**     24/ Turns out that the extraction date goes into the header line, which is the source
**          of all the distinct hashes. Spots are mostly the same (maybe) but we still might
**          have to rejig it for regional differences.
**
** Recently done:
**
**     20/ Pull the counts of quarentined items back onto the loading lookup table
**     18/ Check that the duplicate log import functionality works like we want it to
**     22/ In the case where there are load files collisions with matching hashes, we want to
**          take the first of them rather than mark them all as colissions. Meaning we have to
**          rank on ID based on file hash etc. This guy really wants to be implemented before
**          the full build is rolled out.
**
****************************************************************/

-- Are we doing anything else to record which channels do and do
-- not have advertisment data supplied? Could be useful to see
-- the kind of spot coverage that we have ##!Feature!Request!##

-- OK, so the whole process works, things are going into the full
-- spots table. Now I should check that the duplicate handling does
-- what it should before rolling in the whole set of all that data.
-- Heh, still need to talk to Ops about having them upload it :-/

-- Also noted: count loaded raw will differ from the control totals
-- in cases of dupes within the dame load. That's okay, we'll take
-- that, the other processing sorts it out, and the control totals
-- end up back to what they should be when we're on the linkage and
-- moved to spots_all controls.

/**************** 001: PREPARE THE LOGGER! ****************/

-- Because this kind of automated thing, it's good to be able to fire
-- it off and then check later what it's up to.

CREATE VARIABLE @Spot_logging_ID bigint;
CREATE VARIABLE @Refresh_identifier varchar(40);

set @Refresh_identifier = dateformat(getdate(),'yyyy-mm-dd hh:mm') || ' spot load batch'
EXECUTE citeam.logger_create_run 'VespaAdSpotLoading', @Refresh_identifier, @Spot_logging_ID output;

-- Push something in to note that the whole process has started
EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, '001: Starting the spot load batch.'; 

commit;

/**************** 002: REMOVE ANY SPOTS STILL BEING KEPT AROUND ****************/

-- Yeah, not especially threadsafe, but will we ever be running this
-- more than once at a time? I hope not. Maybe? Um.... *MEGA* not thread
-- safe, in fact.
delete from vespa_analysts.spots_holding_pen;
delete from vespa_analysts.spots_derived_variables;

commit;
-- Mark any outstanding things that didn't get flushed last time the process finished
update vespa_analysts.spots_load_lookup
set load_status = 'Aborted!'
where load_status in ('Pending','Pre-link','CC-linker','EPG-linker','Holding');
-- We're not marking off the various failure codes, only the ones that represent
-- valid internal states and thus aborted procedures.
commit;
-- There may yet be things in the Quarantine too if the last run
-- wasn't properly resolved. But those are expected to be treated
-- manually, so we're not culling them.

EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, '002: Complete! (Purge ancients)';

commit;

/**************** A01: ADD ITEMS TO THE LOADING LOOKUP ****************/

-- Okay, with the current build, you have to run it from inside p5x1,
-- and you have to use the (face-palmingly bad) Sybase Interactive
-- SQL Java interface.
input into vespa_analysts.spots_load_lookup (
        file_MD5_hash
        ,count_from_preprocessing
        ,count_from_internal_controlls
        ,original_full_path
        )
from 'D:\\Vespa spot loading\\DCL_parse_headers.csv'
format ascii;

commit;
EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, 'A01: Complete! (Import headers)';
commit;

/**************** A02: IMPORT ALL THE AD SPOTS ****************/

-- And the actual spot data:
input into vespa_analysts.spots_holding_pen (
        file_MD5_hash
        ,r_infile_id
        ,record_type
        ,r_date_of_transmission
        ,log_station_code
        ,break_split_transmission
        ,break_platform_indicator
        ,r_break_start_time
        ,r_break_total_duration
        ,break_type
        ,r_broadcasters_break_id
        ,spot_type
        ,r_broadcasters_spot_number
        ,log_station_code_for_spot
        ,spot_split_transmission_indicator
        ,hd_simulcast
        ,spot_platform_indicator
        ,r_spot_start_time
        ,r_spot_duration
        ,clearcast_commercial_number
        ,sales_house_brand_description
        ,preceeding_programme_name
        ,succeeding_programme_name
        ,sales_house_identifier
        ,campaign_approval_id
        ,campaign_approval_id_version_number
        ,interactive_spot_platform_indicator
        ,isan_number
)
from 'D:\\Vespa spot loading\\DCL_parse_dump.csv'
format ascii;

-- This is mildly faster than the format with all the derived values in
-- there too, though not as fast as we'd remembered / hoped. The ID might
-- be slowing it down a bit? But that'd be truly tragic :/

commit;
EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, 'A02: Complete! (Import logs)';
commit;

-- ##Indices## do we want to add some indices here after it's loaded?
-- we'd have to remove them afterwards, or they'd otherwise slow down
-- the load, and futzing with the indices will force the script to be
-- run as vespa_analysts... but we only need the one index on the IDs
-- and so we could add that to the structures and not slow the import
-- down too much? hopefully? Dunno. We'll see. Kind of but not really
-- because processing goes through the derived values table, which is
-- indexed properly all over the place.

/**************** B01: POST-IMPORT HEADER PROCESSING ****************/

-- Deriving the other values: the transmission date and station ID:
update vespa_analysts.spots_load_lookup
set
        log_station_code = substring(original_full_path, length(original_full_path) - 16, 5)
        ,date_of_transmission = convert(date,
                substring(original_full_path, length(original_full_path) - 11, 4) || '-' ||
                substring(original_full_path, length(original_full_path) - 7, 2) || '-' ||
                substring(original_full_path, length(original_full_path) - 5, 2))
where load_status = 'Pending';

-- Pull out the QA totals for the number of records in each file:
update vespa_analysts.spots_load_lookup
set count_loaded_raw = t.tehcounts
from vespa_analysts.spots_load_lookup
left join (
        select
                file_MD5_hash
                ,coalesce(count(1), 0) as tehcounts
        from vespa_analysts.spots_holding_pen
        group by file_MD5_hash
) as t
on t.file_MD5_hash = spots_load_lookup.file_MD5_hash
where spots_load_lookup.load_status = 'Pending';
-- This guy is not stable with respect to possible MD5 hash collision
-- within a single load set, but hey, they'll all get isolated anyway
-- in the next steps.

commit;
EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, 'B01: Complete! (Header processing)';
commit;

/**************** B02: REMOVAL OF DUPLICATED LOG FILES ****************/

-- Check that the file isn't a duplicate of some other file we've already processed
select sll.load_id, sll.file_MD5_hash
into #Duplicate_load_hashes
from vespa_analysts.spots_load_lookup as sll
inner join vespa_analysts.spots_load_lookup as sll2
on sll.file_MD5_hash = sll2.file_MD5_hash
and sll.load_id <> sll2.load_id
and sll2.load_status = 'Complete'
and sll.load_status = 'Pending'
group by sll.load_id, sll.file_MD5_hash;

commit;

update vespa_analysts.spots_load_lookup
set sll.load_status = 'Prior Dupe!'
from vespa_analysts.spots_load_lookup as sll
inner join #Duplicate_load_hashes as dlh
on sll.load_id = dlh.load_id;
-- Marking it as collision kicks it out of the load process, so we don't
-- need to do anything else with it. The hashes are the same so all the
-- data matches (We're only using MD5, so still vulnerable to deliberate
-- cryptographic attacks on our as-run spot logs, but wtf? Unlikely.)

-- But yeah, may as well clean the table up:
delete from vespa_analysts.spots_holding_pen where file_MD5_hash in (
        select file_MD5_hash from #Duplicate_load_hashes
);

drop table #Duplicate_load_hashes;

commit;
EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, 'B02: Midway (Remove prior dupes)';
commit;

-- Next: isolate any duplicated files within the same load. Because the
-- hashes match, the file contents should be identical and so it doesn't
-- actually matter which of the duplicates we take for each line, because
-- they're all the same:
select file_MD5_hash, min(load_id) as first_load
into #Loading_samefiles
from vespa_analysts.spots_load_lookup
where load_status = 'Pending'
group by file_MD5_hash
having count(1) > 1;

commit;

create unique index fake_pk on #Loading_samefiles (file_MD5_hash);

-- Mark as dupes all the subsequent loads of the same hash
update vespa_analysts.spots_load_lookup
set load_status = 'Collision!'
from vespa_analysts.spots_load_lookup as sll
inner join #Loading_samefiles as ls
on sll.file_MD5_hash = ls.file_MD5_hash
and sll.load_status = 'Pending'
and sll.load_id <> ls.first_load;

-- ## Automated QA option; check that the dates match for these dupes
-- (otherwise it's super weird - identical content, different day?) ##

-- So given all the content is identical, we're going to pull out the first
-- record for each infile_ID we see, associate each of those with the load
-- we picked, associate all other spot log records with an identifiable bad
-- hash and then cull them.

select ls.file_MD5_hash, shp.r_infile_id, min(shp.id) as keeper
into #spots_log_items_to_keep
from vespa_analysts.spots_holding_pen as shp
inner join #Loading_samefiles as ls
on shp.file_MD5_hash = ls.file_MD5_hash
group by ls.file_MD5_hash, r_infile_id;

-- This is not super cool because the holding pen isn't indexed, but the
-- temp tables are so it's not super awful....

commit;

create unique index fake_PK on #spots_log_items_to_keep (file_MD5_hash, r_infile_id);

-- Don't need to worry about tacking on the load ID now, that happens later
-- anyways, now we're just removing the duplicated log rows
update vespa_analysts.spots_holding_pen
set shp.file_MD5_hash = '#borked#' -- 8 characters won't confused with a valid MD5, and the #'s as well...
from vespa_analysts.spots_holding_pen as shp
inner join #spots_log_items_to_keep as slitk
on shp.file_MD5_hash = slitk.file_MD5_hash
and shp.r_infile_id = slitk.r_infile_id
where shp.id <> keeper;

-- And now kill the ones we don't want
delete from vespa_analysts.spots_holding_pen
where file_MD5_hash = '#borked#';

drop table #Loading_samefiles;
drop table #spots_log_items_to_keep;

commit;
EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, 'B02: Complete! (Remove load dupes)';
commit;

/**************** B03: SEGREGATION OF CONFLICTING LOGS ****************/

-- (this section not yet tested thoroughly)

-- Check each spot file doesn't overlap time and station with some other *different* file:
select distinct sll.load_id, sll.file_MD5_hash
into #Colliding_load_files
from vespa_analysts.spots_load_lookup as sll
inner join vespa_analysts.spots_load_lookup as sll2
on sll.log_station_code = sll2.log_station_code
and sll.date_of_transmission = sll2.date_of_transmission
and sll.file_MD5_hash <> sll2.file_MD5_hash
and sll.load_id <> sll2.load_id
and sll2.load_status = 'Complete' -- only check against spot sets that are fine
and sll.load_status = 'Pending';

commit;

update vespa_analysts.spots_load_lookup
set load_status = 'Overlap'
from vespa_analysts.spots_load_lookup as sll
inner join #Colliding_load_files as clf
on sll.load_id = clf.load_id;

-- Move all the items into the quarantine, out of the active processing area
insert into vespa_analysts.spots_quarantine (
        failed_because
        ,failed_key
        ,file_MD5_hash
        ,r_infile_id
        ,record_type
        ,r_date_of_transmission
        ,log_station_code
        ,break_split_transmission
        ,break_platform_indicator
        ,r_break_start_time
        ,r_break_total_duration
        ,break_type
        ,r_broadcasters_break_id
        ,spot_type
        ,r_broadcasters_spot_number
        ,log_station_code_for_spot
        ,spot_split_transmission_indicator
        ,hd_simulcast
        ,spot_platform_indicator
        ,r_spot_start_time
        ,r_spot_duration
        ,clearcast_commercial_number
        ,sales_house_brand_description
        ,preceeding_programme_name
        ,succeeding_programme_name
        ,sales_house_identifier
        ,campaign_approval_id
        ,campaign_approval_id_version_number
        ,interactive_spot_platform_indicator
        ,isan_number
)
select
        'Distinct MD5 hashes but time / station overlaps with existing log file.'
        ,null -- even with the default, Sybase insists on columns featuring in a multi-column index to be explicitly INSERT'ed :-/
        ,shp.file_MD5_hash
        ,shp.r_infile_id
        ,shp.record_type
        ,shp.r_date_of_transmission
        ,shp.log_station_code
        ,shp.break_split_transmission
        ,shp.break_platform_indicator
        ,shp.r_break_start_time
        ,shp.r_break_total_duration
        ,shp.break_type
        ,shp.r_broadcasters_break_id
        ,shp.spot_type
        ,shp.r_broadcasters_spot_number
        ,shp.log_station_code_for_spot
        ,shp.spot_split_transmission_indicator
        ,shp.hd_simulcast
        ,shp.spot_platform_indicator
        ,shp.r_spot_start_time
        ,shp.r_spot_duration
        ,shp.clearcast_commercial_number
        ,shp.sales_house_brand_description
        ,shp.preceeding_programme_name
        ,shp.succeeding_programme_name
        ,shp.sales_house_identifier
        ,shp.campaign_approval_id
        ,shp.campaign_approval_id_version_number
        ,shp.interactive_spot_platform_indicator
        ,shp.isan_number
from vespa_analysts.spots_holding_pen as shp
inner join #Colliding_load_files as clf
on shp.file_MD5_hash = clf.file_MD5_hash;

commit;
EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, 'B03: Complete! (Segregate conflicts)';
commit;

/**************** C01: ROW-BY-ROW DERIVATIONS FOR SPOTS ****************/

-- First, forge all the text columns into some other data format, with
-- any date processing etc as required.
insert into vespa_analysts.spots_derived_variables (
        id
        ,file_MD5_hash
        ,infile_id
        ,barb_code
        ,date_of_transmission
        ,break_start_time
        ,break_total_duration
        ,broadcasters_break_id
        ,broadcasters_spot_number
        ,spot_sequence_id
        ,spot_start_time
        ,spot_duration
        ,barb_code_for_spot
        ,clearcast_commercial_number
)
select
        id
        ,file_MD5_hash
        ,convert(int, r_infile_id)
        ,convert(decimal(10,0), log_station_code)
        ,convert(datetime, substring(r_date_of_transmission,1,4) || '-' ||
                        substring(r_date_of_transmission,5,2) || '-' ||
                        substring(r_date_of_transmission,7,2) || ' 00:00:00')
        ,dateadd(hour,
                convert(int, substring(r_break_start_time,1,2)), -- Special treatment for hour part to handle the 0000 to 0600 items which get logged as between 2400 and 3000; we calculate the rest of the date, then add on that many hours.
                convert(datetime,
                        substring(r_date_of_transmission,1,4) || '-' ||
                        substring(r_date_of_transmission,5,2) || '-' ||
                        substring(r_date_of_transmission,7,2) || ' 00:' ||
                        substring(r_break_start_time,3,2) || ':' ||
                        substring(r_break_start_time,5,2))) -- break_start_time without BST correction yet
        ,convert(int, r_break_total_duration)
        ,convert(bigint, r_broadcasters_break_id)
        ,convert(bigint, r_broadcasters_spot_number)
        ,rank() over (partition by file_MD5_hash, r_break_start_time order by r_spot_start_time)
        ,dateadd(hour,
                convert(int, substring(r_spot_start_time,1,2)), -- Similar treatment for the same 2400 - 3000 dates apprearing in spot times
                convert(datetime,
                        substring(r_date_of_transmission,1,4) || '-' ||
                        substring(r_date_of_transmission,5,2) || '-' ||
                        substring(r_date_of_transmission,7,2) || ' 00:' ||
                        substring(r_spot_start_time,3,2) || ':' ||
                        substring(r_spot_start_time,5,2))) -- spot_start_time - also without BST correction at this point
        ,convert(int, r_spot_duration)
        ,convert(decimal(10,0), log_station_code_for_spot)
        ,ltrim(rtrim(clearcast_commercial_number))
from vespa_analysts.spots_holding_pen;

-- ##Automated QA option##: compare the total duration of spots (based on this partitioning)
-- to the total stated length of the break.

-- Okay, we have to deal with the BST->UTC conversion; everything between
-- the last Sundays in March and October are bumped forwards an hour. Our
-- processing so far has caused overlap at the October boundary, but that
-- is okay, we still have the date of transmission so we can see the days
-- that need the hour clipped off them.

create variable @earliest_log_year   int;
create variable @latest_log_year     int;

select
    @earliest_log_year  = datepart(year, min(date_of_transmission))
    ,@latest_log_year   = datepart(year, max(date_of_transmission))
from vespa_analysts.spots_derived_variables;

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
from vespa_analysts.spots_derived_variables as shp
inner join #BST_boundary_days as bd
    on shp.date_of_transmission <= bd.sky_date
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
update vespa_analysts.spots_derived_variables
set
    break_start_time    = dateadd(hour, -1, break_start_time)
    ,spot_start_time    = dateadd(hour, -1, spot_start_time)
from vespa_analysts.spots_derived_variables as sdv
inner join #BST_correction_lookup as bcl
on sdv.id = bcl.id;

commit;
    
-- Then other load processing actions:

-- Now we patch in some stuff to get the reversed sequence ID, working around the sybase
-- only tolerating single window functions (bleargh) first by counting how many spots there
-- are in each break...
select
    barb_code,
    break_start_time,
    count(1) as spots_in_break
into #break_spots_counts
from vespa_analysts.spots_derived_variables
group by barb_code, break_start_time;

commit;
create index not_even_trying_to_be_a_PK on #break_spots_counts (barb_code, break_start_time);
commit;

-- And then subtracting off the forward spot identifier:
update vespa_analysts.spots_derived_variables
set spot_reverse_sequence_id = bsc.spots_in_break + 1 - sdv.spot_sequence_id
from vespa_analysts.spots_derived_variables as sdv
inner join #break_spots_counts as bsc
on sdv.barb_code = bsc.barb_code and sdv.break_start_time = bsc.break_start_time;

commit;

-- More post-load processing stuff:

-- Get the autonumbered load IDs from the loading header onto the spots:
update vespa_analysts.spots_derived_variables
set load_id = sll.load_id
from vespa_analysts.spots_derived_variables
inner join vespa_analysts.spots_load_lookup as sll
on vespa_analysts.spots_derived_variables.file_MD5_hash = sll.file_MD5_hash
and sll.load_status = 'Pending';
-- Okay, so that's not quite header processing, but its the best place for it
-- because those states might change in a bit.

-- Oh wait, we also need to LTRIM(RTRIM(.)) all of the text fields, and we're
-- going to trim the HEX fields too since we're not going to be converting them
-- to any kind of number.
update vespa_analysts.spots_holding_pen
set
        break_type                              = ltrim(rtrim(break_type)),
        spot_type                               = ltrim(rtrim(spot_type)),
        hd_simulcast                            = ltrim(rtrim(hd_simulcast)),
        spot_platform_indicator                 = ltrim(rtrim(spot_platform_indicator)),
        clearcast_commercial_number             = ltrim(rtrim(clearcast_commercial_number)),
        sales_house_brand_description           = ltrim(rtrim(sales_house_brand_description)),
        preceeding_programme_name               = ltrim(rtrim(preceeding_programme_name)),
        succeeding_programme_name               = ltrim(rtrim(succeeding_programme_name)),
        interactive_spot_platform_indicator     = ltrim(rtrim(interactive_spot_platform_indicator)),
        isan_number                             = ltrim(rtrim(isan_number))
;

commit;

-- Need something to test that the spots are all good with their barb codes?

EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, 'C01: Complete! (Spot processing)';
-- Oh, and mark that we've done all the pre-linking stuff on the headers table
update vespa_analysts.spots_load_lookup
set load_status = 'Pre-link'
where load_status = 'Pending';
commit;

/**************** C02: LINKING CLEARCAST DATA ON THE HOLDING PEN ****************/

-- From here: all these guys need to be rebuilt as a SELECT into the
-- supporting derivations table.

-- This section is for deduplication, cleansing, linking, all the sutff
-- that is more sophisticated that basic line by line processing on the
-- forged data.

update vespa_analysts.spots_load_lookup
set load_status = 'CC-linker'
where load_status = 'Pre-link';
commit;

-- From here, chances are all this stuff will still be important and
-- in play once the automated feed comes in. Probably also document
-- these steps and what they do, as well as add requirements for the
-- other things that we introduce here, eg, primary keys on the spot
-- data, because that stuff is super useful too.
-- Can we put channel marks on it?
-- Can we link to EPG things?
-- Are there duplicates to worry about? Update: These might have thrown some errors earlier when we put our unique keys on.
-- What other tidying does it need? (See the Spot Data QA Test Script)

-- First we get the counts of everything in the holding pen for QA continuity:

-- ## <-

-- Uncomment these once we're actually doing work here

EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, 'C02: Complete! (Clearcast linkage)';
commit;

/**************** C03: LINKING PROGRAMME KEYS FROM THE EPG ****************/

update vespa_analysts.spots_load_lookup
set load_status = 'EPG-linker'
where load_status = 'CC-linker';
commit;

-- First build a lookup for the station, break start time and break end times for each
-- and then populate this smaller table, and then stitch those details back into the
-- spot stuff.
select 
        barb_code
        ,break_start_time
        ,dateadd(second, break_total_duration, break_start_time) as break_end_time 
        ,rank() over (partition by barb_code, break_start_time order by id) as rankage
        ,convert(bigint, null) as prior_epg_key
        ,convert(bigint, null) as following_epg_key
into #break_instances
from vespa_analysts.spots_derived_variables;
-- Better than doing GROUP BY apparently.
delete from #break_instances where rankage <> 1;

commit;

-- Because we have a couple of joins to do on these things:
create unique index break_start_joining on #break_instances (barb_code, break_start_time);
create unique index break_end_joining   on #break_instances (barb_code, break_end_time);
commit;

-- Pull in the EPGs for the prior show based on when the add break starts (should be in
-- the middle of the show that just finished?)
update #break_instances
set bi.prior_epg_key = ved.programme_trans_sk
from #break_instances as bi
inner join sk_prod.vespa_epg_dim as ved
on ved.barb_code = bi.barb_code
and ved.tx_start_datetime_utc < bi.break_start_time
and ved.tx_end_datetime_utc > bi.break_start_time;

-- EPG for following show based on what's playing when the ad break ends:
update #break_instances
set bi.following_epg_key = ved.programme_trans_sk
from #break_instances as bi
inner join sk_prod.vespa_epg_dim as ved
on ved.barb_code = bi.barb_code
and ved.tx_start_datetime_utc < bi.break_end_time
and ved.tx_end_datetime_utc > bi.break_end_time;

commit;

-- Now push those marks back onto the spot detail table:
update vespa_analysts.spots_derived_variables
set
        preceeding_programme_trans_sk = bi.prior_epg_key
        ,succeeding_programme_trans_sk = bi.following_epg_key
from vespa_analysts.spots_derived_variables as svd
inner join #break_instances as bi
on bi.barb_code = svd.barb_code
and bi.break_start_time = svd.break_start_time;

commit;

-- Afterwards: check if there are any cases of an internal ad break having different
-- programmes before and after, or a transitional ad break having the same programme
-- on either side. We'll probably get this eventually since we're comparing planned
-- stuff to actual broadcasts, but it's a good start.


-- Maybe move some stuff to Quarentine, if it don't link no good.
-- ##

-- Pull out some control totals on volumes of good EPG linkage.
update vespa_analysts.spots_load_lookup
set preceeding_programme_linked = t.tehcounts
from vespa_analysts.spots_load_lookup
inner join (
        select
                load_id
                ,count(1) as tehcounts
        from vespa_analysts.spots_derived_variables
        where preceeding_programme_trans_sk is not null
        group by load_id
) as t
on t.load_id = vespa_analysts.spots_load_lookup.load_id;

update vespa_analysts.spots_load_lookup
set succeeding_programme_linked = t.tehcounts
from vespa_analysts.spots_load_lookup
inner join (
        select
                load_id
                ,count(1) as tehcounts
        from vespa_analysts.spots_derived_variables
        where succeeding_programme_trans_sk is not null
        group by load_id
) as t
on t.load_id = vespa_analysts.spots_load_lookup.load_id;

commit;

-- Mark all the things that are still good (hahaha not processing anything into the
-- quarantine based on linkage fails yet) as sitting around doing their thing

update vespa_analysts.spots_load_lookup
set load_status = 'Holding'
where load_status = 'EPG-linker';
commit;

EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, 'C03: Complete! (EPG programme linkage)';
commit;

/**************** D01: DEDUPLICATION? ****************/

-- ##

/**************** D02: OTHER POST-LOAD PROCESSING / CLEANSING? ****************/

-- ##

/**************** E02: DUMPING THE NEW SPOTS INTO THE BIG TABLE ****************/

-- Get a list of all the good loads
select load_id, file_MD5_hash
into #good_loads
from vespa_analysts.spots_load_lookup
where load_status = 'Holding';
-- The load_id is the main key, but the MD5 has is the one that's
-- part of the PK on the spot holding table.

commit;
-- not many rows to worry about here but whatever
create unique index joinzors_by_hash    on #good_loads (file_MD5_hash);
create unique index joinzors_by_ID      on #good_loads (load_id);
commit;

-- The formats and everything should be alligned
insert into vespa_analysts.spots_all
select 
        sdv.load_id
        ,sdv.infile_id
        ,sdv.barb_code
        ,shp.break_split_transmission
        ,shp.break_platform_indicator
        ,sdv.break_start_time
        ,sdv.break_total_duration
        ,shp.break_type
        ,sdv.broadcasters_break_id
        ,shp.spot_type
        ,sdv.broadcasters_spot_number
        ,sdv.barb_code_for_spot
        ,shp.spot_split_transmission_indicator
        ,shp.hd_simulcast  
        ,shp.spot_platform_indicator
        ,sdv.spot_sequence_id
        ,sdv.spot_reverse_sequence_id
        ,sdv.spot_start_time
        ,sdv.spot_duration
        ,sdv.clearcast_commercial_number
        ,sdv.cc_product_description
        ,sdv.cc_client_name
        ,shp.sales_house_brand_description
        ,shp.preceeding_programme_name
        ,shp.succeeding_programme_name
        ,sdv.preceeding_programme_trans_sk
        ,sdv.succeeding_programme_trans_sk
        ,shp.sales_house_identifier
        ,shp.campaign_approval_id
        ,shp.campaign_approval_id_version_number
        ,shp.interactive_spot_platform_indicator
        ,shp.isan_number
from vespa_analysts.spots_holding_pen as shp
inner join vespa_analysts.spots_derived_variables as sdv
        on shp.id = sdv.id
inner join #good_loads
        on shp.file_MD5_hash = #good_loads.file_MD5_hash;

commit;
EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, 'E02: Complete! (Main table dump)';
commit;

/**************** E03: TIDYING UP VARIOUS JUNKS ****************/

-- Clip the good things out of the holding table, but leave all the various failures:
delete from vespa_analysts.spots_holding_pen
where file_MD5_hash in (select file_MD5_hash from #good_loads);
-- Can't use a join because DELETE doesn't play nice with them
commit;

-- Also need to cull out of the derived variables table
delete from vespa_analysts.spots_derived_variables
where file_MD5_hash in (select file_MD5_hash from #good_loads);
commit;

-- Mark in the loading table the control totals of the counts of things
-- moved into the full spots table:
update vespa_analysts.spots_load_lookup
set count_moved_to_all = t.hits
from vespa_analysts.spots_load_lookup as sll
inner join
(
        select sa.load_id, count(1) as hits
        from vespa_analysts.spots_all as sa
        inner join #good_loads
        on sa.load_id = #good_loads.load_id
        group by sa.load_id
) as t
on sll.load_id = t.load_id

-- Mark as successful:
update vespa_analysts.spots_load_lookup
set load_status = 'Complete'
from vespa_analysts.spots_load_lookup as sll
inner join #good_loads
        on sll.load_id = #good_loads.load_id;

commit;
EXECUTE citeam.logger_add_event @Spot_logging_ID, 3, 'E03: Complete! (Tidying)';
commit;

/**************** F01: NOW YOU SHOULD LOOK INTO THESE FAILURES YOURSELF: ****************/
-- QA on what's still borked:
/*
select shp.load_id
        ,sll.log_station_code    
        ,sll.date_of_transmission
        ,sll.load_status as failure_type
        ,count(1) as hits
from vespa_analysts.spots_holding_pen as shp
inner join vespa_analysts.spots_load_lookup as sll
        on shp.load_id = sll.load_id;
*/
-- don't need filters as we removed everything that wasn't borked already.

-- Yeah, you should resolve all of those manually. Actually, the default
-- behaviour will be to remove everything from the holding pen (##!) and
-- dump everything that's weird into the quarantine (##!) so you'd just
-- have to check that table.
