/******************************************************************************
**
** Project Vespa: Capping 2 - Core table creation
**
** This script initiates all the tables used in the Capping refresg build. They
** get built once in vespa_analysts, then we just refer to it later. Makes a
** lot of things a lot easier and faster, at the expense of a slightly more
** complicated preparation phase and increased overnight load, but hey, we have
** to do those things anyway at some point and this moves all the effort to a
* strictly less high stress part of the week.
**
** See also:
**
**  http://rtci/Vespa1/Capping.aspx
**
** Though, heh, currently that still refers to Capping 1, but that'll get an
** update soon, and will be much happier for it.
**
** Also: even though we are defining all these tables here, the tables that
** actually do the cache capping for us are dynamically built, one for each
** day, with the naming convention vespa_analysts.Vespa_daily_augs_YYYYMMDD
** where YYYYMMDD is the timestamp of the corresponding daily table.
**
** Code sections / Table categories:
**
**      Part A:       Processing tables: things that data goes through
**              A01 - The table of raw logs
**              A02 - The table of events and their start times
**              A03 - Table of first programme in each viewing event
**              A04 - A table of channel lookups
**              A05 - The box lookup for profiling
**
**      Part B:       Processing tables for BARB minute stuff
**              B01 - The fuill minutes spanned by viewing events
**              B02 - The bits overhanging minute boundires at the start and end of the events
**              B03 -
**              B04 -
**              B05 -
**
**
**      Part C:       Processing tables for calculation & storing caps
**              C01 - The table containing cap decisions
**              C02 - Bucket assignment lookup
**              C03 - Holding pen - last stop before dynamically named tables
**
**      Part D: D01 - Structural placeholder for daily tables
**
**      Part Q:       QA tables: things where we track suitability
**              Q01 - Ongoing QA totals of total viewing before / after caping
**              Q02 - Ongoing totals of BARB minute-by-minute consistency totals
**              Q03 - Historic tracking of total viewing per day
**              Q04 - ....
**
** Sybase 15 client note: Sybase has some annoying behavioural changes from the
** client for Sybase 12, and if you have the wrong client settings you might get
** syntax errors (really? WTF?). The setting you need are hidden under:
**  Tools->Options->SybaseIQ->Commands
** There are two check boxes, "Commit after every statement" and "Commit on exit
** or disconnect" and you want to make sure both are ticked.
**
** Also: there aren't many permissions here because all of the results tables
** are mostly dynamically generated in the weekly refresh script. There's kind
** of a placeholder here, so you can see what it should look like though.
**
******************************************************************************/

-- Stil to do: hehe, everything...

/****************** PART A05: BOX LOOKUP FOR PROFILING ******************/

-- There are some things we need for account profiling, but we're not going to
-- denormalise them onto the viewing tables... probably...
if object_id('CP2_box_lookup') is not null drop table CP2_box_lookup;

-- This table used to be called "all_boxes_info" but this one is slightly better.
create table CP2_box_lookup (
    subscriber_id                       bigint          primary key
    ,account_number                     varchar(20)     not null
    ,service_instance_id                varchar(50)
    ,PS_flag                            varchar(1)      default 'U'
    -- What else do we use at box level? At account level even?
);
-- Also note that this guy isn't built in the regular daily cycle; it's built
-- as it's own thing, and it's refreshed as each 7th day is processed. The
-- profiling is done as of the beginning of the period, for consistnecy with
-- Scaling workstream's approach to box segmentations.

commit;

-- Because some of the updates come from the customer database via the service_instance_id link:
create index service_instance_index on CP2_box_lookup (service_instance_id);

commit;

-- There are also a couple of other collection / processing tables that we need
-- because they can't be temporary as we populate them dynamically.
if object_id('CP2_relevant_boxes') is not null drop table CP2_relevant_boxes;
create table CP2_relevant_boxes (
    account_number                      varchar(20)
    ,subscriber_id                      bigint
    ,service_instance_id                varchar(50)
);

commit;
go



/****************** PART C01: TABLES CONTAINING THE CAPS BY BUCKET ******************/

-- Week caps is important enough to want to get split out into his own permanent-like
-- table... This table holds the caps we calculate for each "bucket"
if object_id('CP2_calculated_viewing_caps') is not null drop table CP2_calculated_viewing_caps;
create table CP2_calculated_viewing_caps (
    -- If we ever want to try to roll out some kind of bucket key:
    sub_bucket_id                       integer identity primary key
    ,bucket_id                          integer         -- We don't use pack_grp or box_subscription in the buckets, this gets picked up based just on event_start_day, event_start_hour, initial_genre and Live

    -- The composite PK columns: these define a "bucket"
    ,Live                               bit
    ,event_start_day                    tinyint
    ,event_start_hour                   tinyint
    ,box_subscription                   varchar(1)      -- 'P' or 'S' or 'U'
    ,pack_grp                           varchar(30)
    ,initial_genre                      varchar(25)

    -- Important derived columns
    ,max_dur_mins                       integer         -- the length of the cap to be applied, in minutes

);
-- That table will hold all the caps for one day, since we're looping to build cap
-- viewing data one day at a a time.

commit;
-- Indices: still not convinced we need all of these, that they all do anything useful...
create hng index idx1 on CP2_calculated_viewing_caps(event_start_day);
create hng index idx2 on CP2_calculated_viewing_caps(event_start_hour);
create hng index idx4 on CP2_calculated_viewing_caps(box_subscription);
create hng index idx5 on CP2_calculated_viewing_caps(pack_grp);
create hng index idx6 on CP2_calculated_viewing_caps(initial_genre);
commit;
-- This one, however, supports the application of caps to viewing data:
create unique index forcing_uniqueness on CP2_calculated_viewing_caps
    (event_start_hour, event_start_day, initial_genre, box_subscription, pack_grp, Live);
-- Unique forces the bucketing we're expecting to observe. But this one:
create index for_the_joining_group on CP2_calculated_viewing_caps
    (bucket_id, box_subscription, pack_grp);
-- That's the one that actually gets used in joins, since the bucket_ID does
-- a lot of simplification for the DB.

-- I dunno why anyone else needs this, but they don't need more than SELECT
grant select on CP2_calculated_viewing_caps to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;

commit;
go



/****************** PART C02: CAPPING BUCKETS LOOKUP ******************/

-- This guy is a composite key that summarises event_start_hour, event_start_day,
-- initial_genre and live into one integer that's easy to use (/index/join). Helps
-- reduce the number of columns needed in some summaries and joins by 3, so that's
-- a good thing.
if object_id('CP2_capping_buckets') is not null drop table CP2_capping_buckets;
create table CP2_capping_buckets (
    bucket_id                           integer identity primary key
    ,event_start_hour                   tinyint not null
    ,event_start_day                    tinyint not null
    ,initial_genre                      varchar(30) not null
    ,live                               bit
);

-- So this table still isn't as wildely used as it could be in the build, it's
-- implemented in a few places to facilitate a few things, but the big messy
-- middle bit of the code which makes the caps according to the various rules
-- doesn't really use it. But stuff there is split up enough to not really
-- need it. Maybe pushing it back onto the viewing data will need it, but we
-- are okay so far.

create unique index for_uniqueness on CP2_capping_buckets
    (event_start_hour, event_start_day, initial_genre, live);

commit;
go



/****************** PART C03: HOLDING PEN PRIOR TO DYNAMIC TABLE ******************/

-- This table is where we prepare all the cap details that we want, just before we
-- chuck it all into the dynamically named daily caps table.
if object_id('CP2_capped_data_holding_pen') is not null drop table CP2_capped_data_holding_pen;
create table CP2_capped_data_holding_pen (
    cb_row_id                   bigint              primary key     -- Links to the viewing data daily table of the same day
    ,subscriber_id              bigint              not null
    ,account_number             varchar(20)         not null
    ,scaling_segment_id         bigint                              -- To help with the MBM proc builds....                     -- NYIP!
    ,scaling_weighting          float                               --                                                          -- NYIP!
    ,programme_trans_sk         bigint                              -- To make the minute-by-minute stuff real easy
    ,viewing_starts             datetime                            -- Capped viewing start time
    ,viewing_stops              datetime
    ,viewing_duration           bigint                              -- Capped viewing in seconds
    ,BARB_minute_start          datetime                            -- Viewing with Capping treatment + BARB minute allocation  -- NYIP!
    ,BARB_minute_end            datetime                            -- BARB minutes are pulled back to broadcast time           -- NYIP!
    ,timeshifting               varchar(10)                         -- 'LIVE' or 'VOSDAL' (same day as live) or 'PLAYBACK7' (playback within 7 days) or 'PLAYBACK28' (otherwise)
    ,capped_flag                tinyint                             -- 0-3 depending on capping treatment, or 11 if there are lingering events that are not yet treated
    ,capped_event_end_time      datetime
    -- So those are the columns that go into the dynamically named table,
    -- but there are a few others used to process those out:
    ,adjusted_event_start_time  datetime
    ,X_Adjusted_Event_End_Time  datetime
    ,x_viewing_start_time       datetime
    ,x_viewing_end_time         datetime
    -- Other things we only need to maintain our control totals:
    ,program_air_date           date
    ,live                       tinyint
    ,genre                      varchar(50)
);

-- Indices? what else are we doing here?

/****************** PART D01: STRUCTURAL PLACEHOLDER FOR DALIES ******************/

-- So these guys are built dynamically for each day, but the sctucture should be
-- identical to this for each day:
/* (Commented out because we don't actually build the things like this)
create table vespa_analysts.Vespa_daily_augs_YYYYMMDD (
    cb_row_id                   bigint              primary key     -- Links to the viewing data daily table of the same day
    ,subscriber_id              bigint              not null
    ,account_number             varchar(20)         not null
    ,programme_trans_sk         bigint                              -- to help out with the minute-by-minute stuff
    ,scaling_segment_id         bigint                              -- To help with the MBM proc builds....                         -- NYIP!
    ,scaling_weighting          float                               -- Also assisting with the MBM proc builds                      -- NYIP!
    ,viewing_starts             datetime                            -- Capped viewing start time
    ,viewing_stops              datetime
    ,viewing_duration           bigint                              -- Capped viewing in seconds
    ,BARB_minute_start          datetime                            -- Viewing with Capping treatment + BARB minute allocation      -- NYIP!
    ,BARB_minute_end            datetime                                                                                            -- NYIP!
    ,timeshifting               varchar(10)                         -- 'LIVE' or 'VOSDAL' (same day as live) or 'PLAYBACK7' (playback within 7 days) or 'PLAYBACK28' (otherwise)
    ,capped_flag                tinyint                             -- 0-2 depending on capping treatment: 0 -> event not capped, 1 -> event capped but doesn't effect viewing, 2 -> event capped & shortens viewing, 3 -> event capped & excludes viewing (actually 3 doesn't turn up in the table, but that's what it means during processing)
    ,capped_event_end_time      datetime                            -- Only populated for capped events
);

create index for_MBM            on vespa_analysts.Vespa_daily_augs_YYYYMMDD (scaling_segment_id, viewing_starts, viewing_stops)
create index for_barb_MBM       on vespa_analysts.Vespa_daily_augs_YYYYMMDD (scaling_segment_id, BARB_minute_start, BARB_minute_end)
create index subscriber_id      on vespa_analysts.Vespa_daily_augs_YYYYMMDD (subscriber_id);
create index account_number     on vespa_analysts.Vespa_daily_augs_YYYYMMDD (account_number);

grant select on Vespa_daily_augs_YYYYMMDD to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;

commit;

*/
-- Initial studies show these tables to be about 800MB per day - on the pre-rampup panel of 210k
-- boxes returning data (190k accounts). That's quite a bit, means we're guessing at... 150GB of
-- capping cache stuff to go back to November 2011. Awesome. That's not actually a whole lot in
-- the scheme of things (though yeah, it's a lot more than the scaling builds.)

/****************** PART Q01: TABLES TRACKING VIEWING TOTALS ******************/

-- We're storing the totals of viewing for each major stage of processing, and also for
-- each capping strand
IF object_id('CP2_QA_viewing_control_totals') IS NOT NULL DROP TABLE CP2_QA_viewing_control_totals;
create table CP2_QA_viewing_control_totals (
    build_date                  date                not null -- The date that the caps apply to
    ,data_state                 varchar(20)         not null
    ,program_air_date           date                not null
    ,live                       bit
    ,genre                      varchar(25)
    ,viewing_records            int
    ,total_viewing_in_days      decimal(8,2)        not null
    ,primary key (build_date, data_state, program_air_date, live, genre)
);
/* What we expect for the data states in the above table (for each build_date):
    *. '1.) Collect' should match '2.) Pre-Cap'
    *. '4a.) Uncapped' + '4c.) Truncated' should add up to '3.) Capped',
    *. '4a.) Uncapped' + '4b.) Excluded' + '4c.) Truncated' + '4d.) T-Margin' should add up to '1.) Collect'
They should match pretty much exactly, since we've rounded everything to 2dp in hours.
*/

commit;

-- We're also tracking how many viewing events fal into each category of the capping
IF object_id('CP2_QA_viewing_control_distribs') IS NOT NULL DROP TABLE CP2_QA_viewing_control_distribs;
create table CP2_QA_viewing_control_distribs (
    build_date                  date                not null -- The date that the caps apply to
    ,data_state                 varchar(20)         not null -- '1.) Uncapped' or '2.) Capped'
    ,duration_interval          int                 not null -- batched into 10s chunks, so 0 means viewing durations between 0s and 10s
    ,viewing_events             int                          -- Er... but these are not events, but viewing bits... oh well
    ,primary key (build_date, data_state, duration_interval)
);

-- Now also doign the same thing not for viewing items, but for event durations
-- and with a resolution of 1 minute because these things are much longer.
IF object_id('CP2_QA_event_control_distribs') IS NOT NULL DROP TABLE CP2_QA_event_control_distribs;
create table CP2_QA_event_control_distribs (
    build_date                  date                not null -- The date that the caps apply to
    ,data_state                 varchar(20)         not null -- '1.) Uncapped' or '2.) Capped'
    ,duration_interval          int                 not null -- batched into 1m chunks, so 0 means viewing durations between 0s and 1 minute
    ,viewing_events             int
    ,primary key (build_date, data_state, duration_interval)
);

commit;

grant select on CP2_QA_viewing_control_totals      to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
grant select on CP2_QA_viewing_control_distribs    to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
grant select on CP2_QA_event_control_distribs      to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;

commit;
go

/****************** PART Q02: BARB MINUTE BY MINUTE CONTROL TOTALS ******************/

-- Tables which track the daily viewing totals before and after the BARB minute batching

/****************** PART Q03: HISTORICAL TRACKING OF DAILY TOTAL VIEWING ******************/

-- Tables which track the total viewing for the various stages of processing through both
-- capping and BARB minute allocation. These averages are just over people who watch *some*
-- TV at all, so will be higher than the average TV watching since boxes supplying only
-- logs don't get considered here. Also notice that this is daily viewing *on panel* and
-- the Sky Base average isn't calculated here (because it depends on Scaling and we're not
-- sure that we have the appropriate eights prepared when the capping gets done).
IF object_id('CP2_QA_daily_average_viewing') IS NOT NULL DROP TABLE CP2_QA_daily_average_viewing;
create table CP2_QA_daily_average_viewing (
    build_date                  date                not null primary key
    ,subscriber_count           int                 not null        -- Number of boxes noticed in the build
    ,average_uncleansed_viewing int                 default null    -- All the viewing counts are in minutes per box
    ,average_uncapped_viewing   int                 default null
    ,average_capped_viewing     int                 default null
    ,average_BARB_viewing       int                 default null    -- IE the average viewing per box after BARB minute-by-minute processing has been applied (NYIP)
);

grant select on CP2_QA_daily_average_viewing       to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;

/****************** PART Q04: HISTORICAL TRACKING OF MAGNITUDE OF CALCULATED CAPS ******************/

-- We want to know how big the various caps are that we're calculating, just
-- to see how much viewing we think is okay for each case
IF object_id('CP2_QA_viewing_control_cap_distrib') IS NOT NULL DROP TABLE CP2_QA_viewing_control_cap_distrib;
create table CP2_QA_viewing_control_cap_distrib (
    build_date                  date                not null -- The date that the caps apply to
    ,max_dur_mins               int                 not null
    ,cap_instances              int                 not null
    ,primary key (build_date, max_dur_mins)
);

commit;

grant select on CP2_QA_viewing_control_cap_distrib to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;

commit;
go
