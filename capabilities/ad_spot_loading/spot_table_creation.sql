/****************************************************************
**
**      PROJECT VESPA: SPOT DATA TABLE SETUP
**
** DON'T RUN THIS SCRIPT. IT WILL RESET ALL THE SPOT DATA.
**
** These are the table creation scripts for the spot loading.
** This script needs to be run in the "vespa_analysts" user and
** will reset all the spot tables there, and grant permissions
** on them to everyone (raw data is not Vespa sensitive).
**
** Refer to the "Load_spot_files.sql" script to actually do some
** spot file loading.
**
** Code sections:
**
**      Part A: Tables
**              A01: The spot loading header table
**              A02: The main spot table
**              A03: The temporary loading table
**              A04: Table for derived values
**              A05: The quarantine (for bad / strange spots)
**
**      Part B:
**              B01: Permissions
**
****************************************************************/

/**************** A01: SPOT LOADING HEADER TABLE ****************/

-- This is the listing of all the spot files we've got and where we
-- are on processing them all.
IF object_id('vespa_analysts.spots_load_lookup') IS NOT NULL DROP TABLE vespa_analysts.spots_load_lookup;
create table vespa_analysts.spots_load_lookup
(
        load_id                                 int             identity not null primary key
        ,upload_date                            smalldatetime   not null default cast(getdate() as smalldatetime)
        ,uploaded_by                            varchar(20)     not null default user
        ,load_status                            varchar(10)     not null default 'Pending'
        -- The load status might be:
        --      'Pending'       - if the spot file has only been added to the lookup
        --      'Duplicate'     - if the hash matches something we've already loaded
        --      'Pre-link'      - if the spot data is in the holding table but has not yet been linked to EPG and Clearcast data
        --      'EPG-linker'
        --      'CC-linker'
        --      'Holding'       - if it's sitting in the holding table
        --      'Complete'      - if it's all done
        --      'Load fail!'    - if something went bad with the loading (maybe we'll get to track the failures)
        --      'Link fail!'    - if something went bad in the linking
        --      'Collision!'    - if two files have the same MD5 hash in the same load
        --      'Overlap!'      - if the log station code and the date of transmission match but the file hash doesn't
        --      'Aborted!'      - if header's weren't finished processing when the script was restarted
        -- Okay, so the various error codes ('xxxx!') aren't in play, except for Aborted. Maybe later.
        ,file_MD5_hash                          varchar(32)     not null
        ,log_station_code                       varchar(5)      default null -- it's important, but gets set after load
        ,date_of_transmission                   date            default null -- this one too
        ,original_full_path                     varchar(250)    not null
        --,processed_full_path                    varchar(250)    not null -- Going to have to handle this guy separately - maybe? we already have upload date ##
        -- Various QA totals we're maintaining. Well, okay, this table is almost entirely for QA
        ,count_from_preprocessing               int             not null
        ,count_from_internal_controlls          int             not null
        ,count_loaded_raw                       int             default null
        ,clearcast_linked_count                 int             default null
        ,preceeding_programme_linked            int             default null
        ,succeeding_programme_linked            int             default null
        ,count_moved_to_all                     int             default null
        ,count_quarentined                      int             default null
        -- So we can track how effectively things are being processed
);
create hg index MD5_hash_index          on vespa_analysts.spots_load_lookup(file_MD5_hash);
create hg index station_and_day_index   on vespa_analysts.spots_load_lookup(log_station_code, date_of_transmission);

/**************** A02: MAIN SPOT TABLE ****************/

-- This is the eventual complete listing of all spot data. There's a whole
-- bunch of processing that goes into building this table, the import goes
-- through a couple of staging tables first.
IF object_id('vespa_analysts.spots_all') IS NOT NULL DROP TABLE vespa_analysts.spots_all;
create table vespa_analysts.spots_all (
        load_id                                 int             not null
        ,infile_id                              int             not null
        ,barb_code                              decimal(10,0)                   -- so definition matches barb_code on the EPG
        ,break_split_transmission               smallint
        ,break_platform_indicator               smallint
        ,break_start_time                       datetime        not null
        ,break_total_duration                   int                             -- measured in seconds
        ,break_type                             varchar( 2)
        ,broadcasters_break_id                  bigint
        ,spot_type                              varchar( 2)
        ,broadcasters_spot_number               bigint          not null
        ,barb_code_for_spot                     decimal(10,0)                   -- this one is for the spot, not sure why it's different to the one for the break?
        ,spot_split_transmission_indicator      smallint
        ,hd_simulcast                           varchar( 2)                     -- Hex
        ,spot_platform_indicator                varchar( 2)                     -- Hex
        ,spot_sequence_id                       int             not null        -- Gives order of spots with a break, because broadcasters_spot_number is not a clean ordering
        ,spot_reverse_sequence_id               int             not null        -- Gives reverse ordering, indicates out which spot was last in a break
        ,spot_start_time                        datetime        not null
        ,spot_duration                          int             not null
        ,clearcast_commercial_number            varchar(15)     not null
        ,cc_product_description                 varchar(35)     default null    -- all the cc_ prefix fields are added from clearcast data during linking
        ,cc_client_name                         varchar(20)     default null  
        ,sales_house_brand_description          varchar(35)
        ,preceeding_programme_name              varchar(40)
        ,succeeding_programme_name              varchar(40)
        ,preceeding_programme_trans_sk          bigint          default null    -- Might not be populated if the match is not good
        ,succeeding_programme_trans_sk          bigint          default null    -- These will get added in post-processing, because we don't expect it to be simple
        ,sales_house_identifier                 int
        ,campaign_approval_id                   bigint
        ,campaign_approval_id_version_number    int
        ,interactive_spot_platform_indicator    varchar( 2)
        ,isan_number                            varchar(24)                     -- Hex too
        ,primary key                            (load_id, infile_id)
        -- We were going to do it with a BIGINT IDENTITY but we'd still need
        -- a unique composite key for completeness, so why not just use that
        -- as the primary?
);
-- Unique indices for consistency:
--create unique index   spot_identifier_using_IDs       on vespa_analysts.spots_all (barb_code, broadcasters_break_id, broadcasters_spot_number); -- Broken! So it turns out that the broadcaster's break ID is recuclyed over days. Do we want to keep the day around as a separate field? But then we have to figure out which way we're treating the nightly offset; might just delete that unique index, since we've already got one unique index sorting out various duplication... and preprocessing... hopefully etc.
-- Indices for linking and analysis: haven't thought in detail about what
-- kind of queries are going to need composite indices, just guessing at
-- the basic ones at the moment. First for grouping by station, and maybe
-- we're also wanting some minute-by-minute stuff:
create unique index   spot_identifier_using_time      on vespa_analysts.spots_all (barb_code, spot_start_time);
-- Do we want both start time and the sequence to be indexed? do we care about the 3rd item vs the 5th? we might I guess.
create unique index   spot_identifier_using_sequence  on vespa_analysts.spots_all (barb_code, break_start_time, spot_sequence_id);
-- Reverse spot sequence can't be unique because they all start off NULL :/
create index   spot_by_reverse_sequence        on vespa_analysts.spots_all (barb_code, break_start_time, spot_reverse_sequence_id);
-- We'd also like to analyse by the shows they were in between (probably)
create index          preceeding_trask_sk_index       on vespa_analysts.spots_all (preceeding_programme_trans_sk);
create index          succeeding_trask_sk_index       on vespa_analysts.spots_all (succeeding_programme_trans_sk);
-- Maybe we want to see how often a particular add has turned up on all channels?
create index          commercial_index                on vespa_analysts.spots_all (clearcast_commercial_number);

/**************** A03: TEMPORARY LOADING TABLE(S?) ****************/

-- This is where all the new spots go when they're first loaded, and
-- where we process them into the columns we want.
IF object_id('vespa_analysts.spots_holding_pen') IS NOT NULL DROP TABLE vespa_analysts.spots_holding_pen;
create table vespa_analysts.spots_holding_pen (
        id                                      bigint                  identity primary key
        ,file_MD5_hash                          varchar(32)             -- How we tell which raw file the spot is from
        ,r_infile_id                            varchar(10)
        ,record_type                            varchar( 2)
        ,r_date_of_transmission                 varchar( 8)
        ,log_station_code                       varchar( 5)             -- This is the linkage to a channel, gets pushed into barb_code
        ,break_split_transmission               varchar( 2)
        ,break_platform_indicator               varchar( 2)
        ,r_break_start_time                     varchar( 6)             -- r_ prefix for raw unprocessed value
        ,r_break_total_duration                 varchar( 5)
        ,break_type                             varchar( 2)
        ,r_broadcasters_break_id                varchar(12)             -- Uniquely identifies each spot? within a station ID?
        ,spot_type                              varchar( 2)
        ,r_broadcasters_spot_number             varchar(12)             -- Orders adds within the spot? No, but it might turn out to be unique within a station
        ,log_station_code_for_spot              varchar( 5)
        ,spot_split_transmission_indicator      varchar( 2)
        ,hd_simulcast                           varchar( 2)
        ,spot_platform_indicator                varchar( 2)
        ,r_spot_start_time                      varchar( 6)
        ,r_spot_duration                        varchar( 5)
        ,clearcast_commercial_number            varchar(15)
        ,sales_house_brand_description          varchar(35)
        ,preceeding_programme_name              varchar(40)
        ,succeeding_programme_name              varchar(40)
        ,sales_house_identifier                 varchar( 5)
        ,campaign_approval_id                   varchar(10)
        ,campaign_approval_id_version_number    varchar( 5)
        ,interactive_spot_platform_indicator    varchar( 2)
        ,isan_number                            varchar(24)
);
-- No indices here, we just want this table to load as fast as
-- possible, the loading step is the bottleneck.
commit;

/**************** A04: DERIVED VARIABLES ****************/
-- So the table we're loading into is now contiguous, fully varchar, and
-- that should help get the data in as fast as possible or something. We
-- also need another table, with the same key structure, which will hold
-- all the other variables we derive based on these things; all the ints
-- and and datetimes and stuff in their useful formats.

IF object_id('vespa_analysts.spots_derived_variables') IS NOT NULL DROP TABLE vespa_analysts.spots_derived_variables;
create table vespa_analysts.spots_derived_variables (
        id                                      bigint          not null primary key -- not identity since we're getting the keys from the holding pen
        ,file_MD5_hash                          varchar(32)     not null        -- How we tell which raw file the spot is from
        ,load_id                                int             default null
        ,infile_id                              int             not null
        ,barb_code                              decimal(10,0)   default null    -- so definition matches barb_code on the EPG
        ,date_of_transmission                   date            not null        -- doesn't go into spots_all, just used for processing...
        ,break_start_time                       datetime        not null
        ,break_total_duration                   int             not null        -- in seconds
        ,broadcasters_break_id                  bigint          not null
        ,broadcasters_spot_number               bigint          not null
        ,spot_sequence_id                       int             not null        -- Gives order of spots with a break, from first to last
        ,spot_reverse_sequence_id               int             default null    -- The sequence from the end of the ad; and because Sybase can't handle multiple window functions in the same query, it gets updated later and so starts NULL
        ,spot_start_time                        datetime        not null
        ,spot_duration                          int             not null        -- also in seconds
        ,barb_code_for_spot                     decimal(10,0)   default null    -- why is there a separate code for the spot?
        ,clearcast_commercial_number            varchar(15)     not null        -- for linking to subsequent clearcast stuff
        ,cc_product_description                 varchar(35)     default null    -- all the cc_ prefix fields are added from clearcast data during linking
        ,cc_client_name                         varchar(20)     default null  
        ,preceeding_programme_trans_sk          bigint          default null    -- Might not be populated if the match is not good
        ,succeeding_programme_trans_sk          bigint          default null    -- These will get added in post-processing, because we don't expect it to be simple
);

-- Okay, this table can take the various unique indices we'll enforce:
create unique index spot_identifier_using_IDs        on vespa_analysts.spots_derived_variables (file_MD5_hash, infile_id);
create unique index spot_identifier_using_time       on vespa_analysts.spots_derived_variables (barb_code, spot_start_time);

-- Here we're also going to add the indices that we want; mainly
-- these are to join to other stuff and pull in other data
create index    load_header_index       on vespa_analysts.spots_derived_variables (file_MD5_hash);
create index    clearcast_linkage       on vespa_analysts.spots_derived_variables (clearcast_commercial_number);
create index    channel_linkage         on vespa_analysts.spots_derived_variables (barb_code, break_start_time);

commit;

/**************** A05: THE SPOT QUARANTINE ****************/

-- This is where we put all the spots we can't automatically resolve. These
-- probably need to be looked into & treated by hand.

IF object_id('vespa_analysts.spots_quarantine') IS NOT NULL DROP TABLE vespa_analysts.spots_quarantine;
create table vespa_analysts.spots_quarantine (
        id                                      bigint          not null identity primary key -- this guy doesn't need to match up with the ID in the loading thing, because the holding pen gets erased at the end of each cycle whereas these IDs persist
        ,failed_because                         varchar(60)     -- to hold notes on which stage caused the ejection from the valid spots
        ,failed_key                             int             default null -- for cases like duplication, we link them into groups of failure
        -- The raw values from the file:
        ,file_MD5_hash                          varchar(32)
        ,r_infile_id                            varchar(10)
        ,record_type                            varchar( 2)
        ,r_date_of_transmission                 varchar( 8)
        ,log_station_code                       varchar( 5)
        ,break_split_transmission               varchar( 2)
        ,break_platform_indicator               varchar( 2)
        ,r_break_start_time                     varchar( 6)
        ,r_break_total_duration                 varchar( 5)
        ,break_type                             varchar( 2)
        ,r_broadcasters_break_id                varchar(12)
        ,spot_type                              varchar( 2)
        ,r_broadcasters_spot_number             varchar(12)
        ,log_station_code_for_spot              varchar( 5)
        ,spot_split_transmission_indicator      varchar( 2)
        ,hd_simulcast                           varchar( 2)
        ,spot_platform_indicator                varchar( 2)
        ,r_spot_start_time                      varchar( 6)
        ,r_spot_duration                        varchar( 5)
        ,clearcast_commercial_number            varchar(15)
        ,sales_house_brand_description          varchar(35)
        ,preceeding_programme_name              varchar(40)
        ,succeeding_programme_name              varchar(40)
        ,sales_house_identifier                 varchar( 5)
        ,campaign_approval_id                   varchar(10)
        ,campaign_approval_id_version_number    varchar( 5)
        ,interactive_spot_platform_indicator    varchar( 2)
        ,isan_number                            varchar(24)
        -- The various derived values:
        ,load_id                                int
        ,infile_id                              int
        ,barb_code                              decimal(10,0)
        ,break_start_time                       datetime
        ,break_total_duration                   int
        ,spot_sequence_id                       int
        ,spot_reverse_sequence_id               int
        ,spot_start_time                        datetime
        ,spot_duration                          int
        ,barb_code_for_spot                     decimal(10,0)
        ,cc_product_description                 varchar(35)
        ,cc_client_name                         varchar(20)
        ,preceeding_programme_trans_sk          bigint
        ,succeeding_programme_trans_sk          bigint
);

create index failzors on vespa_analysts.spots_quarantine (failed_because, failed_key);

commit;

/**************** B01: PERMISSIONS ****************/

-- Permissions: for everyone, it's raw advertising data, not special at all.
grant all on vespa_analysts.spots_all                 to public;
grant all on vespa_analysts.spots_load_lookup         to public;
grant all on vespa_analysts.spots_holding_pen         to public;
grant all on vespa_analysts.spots_derived_variables   to public;
grant all on vespa_analysts.spots_quarantine          to public;

commit;