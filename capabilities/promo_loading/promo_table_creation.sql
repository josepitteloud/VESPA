/****************************************************************
**
**      PROJECT VESPA: PROMO DATA TABLE SETUP
**
** DON'T RUN THIS SCRIPT. IT WILL RESET ALL THE PROMO DATA.
**
** These are the table creation scripts for the promo loading.
** This script needs to be run in the "vespa_analysts" user and
** will reset all the promo tables there, and grant permissions
** on them to everyone (raw data is not Vespa sensitive).
**
** Refer to the "Load_promo_files.sql" script to actually do some
** promo file loading.
**
** Code sections:
**
**      Part A: Tables
**              A01: The promo loading header table
**              A02: The main promo table
**              A03: The temporary loading table
**              A04: Table for derived values
**              A05: The quarantine (for bad / strange promos)
**
**      Part B:
**              B01: Permissions
**
****************************************************************/

/**************** A01: PROMO LOADING HEADER TABLE ****************/

-- This is the listing of all the promo files we've got and where we
-- are on processing them all.
IF object_id('vespa_analysts.promos_load_lookup') IS NOT NULL DROP TABLE vespa_analysts.promos_load_lookup;
create table vespa_analysts.promos_load_lookup
(
        load_id                                 int             identity not null primary key
        ,upload_date                            smalldatetime   not null default cast(getdate() as smalldatetime)
        ,uploaded_by                            varchar(20)     not null default user
        ,load_status                            varchar(10)     not null default 'Pending'
        -- The load status might be:
        --      'Pending'       - if the promo file has only been added to the lookup
        --      'Duplicate'     - if the hash matches something we've already loaded
        --      'Pre-link'      - if the promo data is in the holding table but has not yet been linked to EPG and Clearcast data
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
        ,count_loaded_raw                       int             default null
        ,clearcast_linked_count                 int             default null
        ,preceeding_programme_linked            int             default null
        ,succeeding_programme_linked            int             default null
        ,count_moved_to_all                     int             default null
        ,count_quarentined                      int             default null
        -- So we can track how effectively things are being processed
);
create hg index MD5_hash_index          on vespa_analysts.promos_load_lookup(file_MD5_hash);
create hg index station_and_day_index   on vespa_analysts.promos_load_lookup(log_station_code, date_of_transmission);

/**************** A02: MAIN PROMO TABLE ****************/

-- This is the eventual complete listing of all promo data. There's a whole
-- bunch of processing that goes into building this table, the import goes
-- through a couple of staging tables first.
IF object_id('vespa_analysts.promos_all') IS NOT NULL DROP TABLE vespa_analysts.promos_all;
create table vespa_analysts.promos_all (
        load_id                                 int             not null
        ,infile_id                              int             not null
        ,promo_date                             datetime        not null
        ,channel                                varchar(4)
        ,Barb_code                              decimal(10,0)
        ,promo_start_time                       time            not null
        ,promo_duration                         int
        ,promo_id                               integer
        ,cart_no                                varchar(8)
        ,promo_product_description              varchar(50)
        ,preceeding_programme_trans_sk          bigint          default null    -- Might not be populated if the match is not good
        ,succeeding_programme_trans_sk          bigint          default null    -- These will get added in post-processing, because we don't expect it to be simple
        -- We were going to do it with a BIGINT IDENTITY but we'd still need
        -- a unique composite key for completeness, so why not just use that
        -- as the primary?
);

create index   promo_identifier_using_time      on vespa_analysts.promos_all (barb_code, promo_start_time);


/**************** A03: TEMPORARY LOADING TABLE(S?) ****************/

-- This is where all the new promos go when they're first loaded, and
-- where we process them into the columns we want.
IF object_id('vespa_analysts.promos_holding_pen') IS NOT NULL DROP TABLE vespa_analysts.promos_holding_pen;
create table vespa_analysts.promos_holding_pen (
        id                                      bigint                  identity primary key
        ,file_MD5_hash                          varchar(32)             -- How we tell which raw file the promo is from
        ,r_infile_id                            varchar(10)
        ,promo_date                             varchar(8)               not null
        ,channel                                varchar(4)
        ,Barb_code                              varchar(5)               not null
        ,promo_start_time                       varchar(6)               not null
        ,promo_duration                         varchar(3)
        ,promo_id                               varchar(9)
        ,cart_no                                varchar(8)
        ,promo_product_description              varchar(50)
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

IF object_id('vespa_analysts.promos_derived_variables') IS NOT NULL DROP TABLE vespa_analysts.promos_derived_variables;
create table vespa_analysts.promos_derived_variables (
        id                                      bigint          not null primary key -- not identity since we're getting the keys from the holding pen
        ,file_MD5_hash                          varchar(32)     not null        -- How we tell which raw file the promo is from
        ,load_id                                int             default null
        ,infile_id                              int             not null
        ,promo_date                             date        not null
        ,channel                                varchar(4)
        ,Barb_code                              decimal(10,0)      not null
        ,promo_start_time                       datetime            not null
        ,promo_duration                         int
        ,promo_id                               integer
        ,cart_no                                varchar(8)
        ,promo_product_description              varchar(50)
        ,preceeding_programme_trans_sk          bigint          default null    -- Might not be populated if the match is not good
        ,succeeding_programme_trans_sk          bigint          default null    -- These will get added in post-processing, because we don't expect it to be simple

);

/*
select barb_code, promo_start_time, count(*), count(distinct channel)
from promos_derived_variables
group by barb_code, promo_start_time
having count(*)>1;
select * from promos_derived_variables where barb_code = 4201 and promo_start_time = '2011-08-15 06:19:30.000000'

duplicate records for each promo - same barb code and start time but different channels - one may be freeview
*/

-- Okay, this table can take the various unique indices we'll enforce:
create unique index promo_identifier_using_IDs        on vespa_analysts.promos_derived_variables (file_MD5_hash, infile_id);
create index promo_identifier_using_time       on vespa_analysts.promos_derived_variables (barb_code, promo_start_time);

-- Here we're also going to add the indices that we want; mainly
-- these are to join to other stuff and pull in other data
create index    load_header_index       on vespa_analysts.promos_derived_variables (file_MD5_hash);

commit;


/**************** A05: THE PROMO QUARANTINE ****************/

-- This is where we put all the promos we can't automatically resolve. These
-- probably need to be looked into & treated by hand.

IF object_id('vespa_analysts.promos_quarantine') IS NOT NULL DROP TABLE vespa_analysts.promos_quarantine;
create table vespa_analysts.promos_quarantine (
        id                                      bigint          not null identity primary key -- this guy doesn't need to match up with the ID in the loading thing, because the holding pen gets erased at the end of each cycle whereas these IDs persist
        ,failed_because                         varchar(60)     -- to hold notes on which stage caused the ejection from the valid promos
        ,failed_key                             int             default null -- for cases like duplication, we link them into groups of failure
        -- The raw values from the file:
        ,file_MD5_hash                          varchar(32)
        ,r_infile_id                            varchar(10)
        ,promo_date                             datetime        not null
        ,channel                                varchar(4)
        ,Barb_code                              decimal(10,0)      not null
        ,promo_start_time                       time            not null
        ,promo_duration                         int
        ,promo_id                               integer
        ,cart_no                                varchar(8)
        ,promo_product_description              varchar(50)
        ,preceeding_programme_trans_sk          bigint
        ,succeeding_programme_trans_sk          bigint
);

create index failzors on vespa_analysts.promos_quarantine (failed_because, failed_key);

commit;

/**************** B01: PERMISSIONS ****************/

-- Permissions: for everyone, it's raw advertising data, not special at all.
grant all on vespa_analysts.promos_all                 to public;
grant all on vespa_analysts.promos_load_lookup         to public;
grant all on vespa_analysts.promos_holding_pen         to public;
grant all on vespa_analysts.promos_derived_variables   to public;
grant all on vespa_analysts.promos_quarantine          to public;

commit;
