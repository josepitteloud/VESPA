/******************************************************************************
**
** Project Vespa: Sky View Dashboard Report
**                  - Persistent Table Creation
**
** This is the script that builds all the persistent tables required by the
** Sky View Dashboard, or SVD. This script *does not* need to be executed
** during standard weekly refreshes of the SVDash report.
**
** This script, when necessary, should be run under  It's not
** necessary to run it any more though. Update: Except when it changes. And
** even then, you're hopefully not going to run this thing in it's entirety
** (because you'll kill the historical stuff), just the new parts you need.
**
** A lot of this is just like the Vespa panel Operational Dashboard, because
** that was the origin of this stuff. A lot of bits have changed, though, as
** Sky View is stored completely differently (and so has new foibles).
**
******************************************************************************/

-- Section headings remain from the previous all-in-one script (previous in
-- that it used to be a Vespa panel Operational Dashboard report)

-- Wait, do we even want big persisnent ongoing historic tables for the Sky View
-- dashboard report thingy?

/****************** B01: CREATE PERSISTENT TABLES ******************/
-- These tables are populated at the end; if something weird happens and you need
-- to rebuild the lot from scratch, you just create these tables. The automatic date
-- selection should archive things up to 5 days ago, but this happens at the end, and
-- expect it to be mega slow if you're processing *all* of those logs; the events
-- view is growing at about 100m records a week or something crazy like that.

/* Block commented since this isn't required for standard operaional runs

-- Summarising previous daily info into the exact statistics we report, so we don't
-- need to pull in every single log each time
create table vespa_SVD_log_aggregated_archive (
        doc_creation_date_from_6am      date            not null primary key
        ,log_count                      bigint          default null
        ,distinct_accounts              bigint          default null
        ,distinct_boxes                 bigint          default null
        ,reporting_primary_boxes        bigint          default null
        ,reporting_secondary_boxes      bigint          default null
        ,reporting_primary_anytimes     bigint          default null
        ,reporting_secondary_anytimes   bigint          default null
        ,enabled_primary_boxes          bigint          default null
        ,enabled_secondary_boxes        bigint          default null
        ,date_archived                  date            not null default cast(getdate() as date)
        ,user_reporting                 varchar(20)     not null default user
)
;

-- Permissions!
grant select, insert on vespa_SVD_log_aggregated_archive  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;

-- No boxes_returning_archive because we've got no way to track who's selected or not
-- selected for data return or anything else like that at any time. 

COMMIT
;

*/

/****************** B02: CREATE TRANSIENT TABLES ******************/

-- These ones are all tables that get cleared out and fully repopulated each run
if object_id('vespa_SVD_subscriber_dashboard') is not null
   drop table vespa_SVD_subscriber_dashboard;
go

create table vespa_SVD_subscriber_dashboard (
    account_number                  varchar(20)     not null
    ,subscriber_id                  bigint          not null primary key
    ,enabled_date                   date
    ,has_returned_data_ever         bit             default 0
    ,previously_returned_data       bit             default 0
    ,In_stb_log_snapshot            bit             default 0
    ,PS_flag                        varchar(1)      default 'U'
    ,Box_type_physical              varchar(20)     default 'Unknown'
    ,HD_box_subs                    bit             default 0
    ,box_subscription_group         varchar(60)
    ,Account_anytime_plus           bit             default 0
    ,Box_has_anytime_plus           bit             default 0
    ,PVR                            bit             default 0
);
-- Some of those are account level things, but hey, it comes straight off
-- the single box view so it's not difficult at all.
    
go

create index account_index on vespa_SVD_subscriber_dashboard (account_number);

go

if object_id('vespa_SVD_log_collection_dump') is not null
   drop table vespa_SVD_log_collection_dump;
go
-- Not sure we bother with any indices here, there's a lot of stuff being added to it
-- and at best we're going to add the indices after the table is fully populated, ie,
-- after we've junked a bunch of stuf into it from the daily events tables.


create table vespa_SVD_log_collection_dump (
        subscriber_id                   decimal(8)      not null
        ,LOG_START_DATE_TIME_UTC          datetime        not null
        ,account_number                 varchar(20)     not null
        ,doc_creation_date_from_6am     date            not null
        ,log_id                         varchar(100)    default null -- gets built later from subscriber_id and LOG_START_DATE_TIME_UTC
);
-- We're not indexing this guy because all we do with it is build the log listing
-- once and that's it. We don't even keep tags indicating which daily table the
-- log bits come from, so good luck trying to get it to do anything useful.

go

if object_id('vespa_SVD_new_log_listing') is not null
   drop table vespa_SVD_new_log_listing
-- Summary of suitably new log entries. They get pulled out of the daily tables above, and
-- then get deduplicated (across daily tables) as they arrive in this log listing. Older
-- log numbers are just pulled out of the archives though.
go

create table vespa_SVD_new_log_listing (
--        id                              bigint          identity not null primary key -- only useful in the discontinued deduplication procedure
        log_id                          varchar(100)    not null primary key
        ,subscriber_id                  decimal(8)      not null
        ,account_number                 varchar(20)     not null
--        ,document_creation_date         date            not null -- never gets used, he can get left out
        ,doc_creation_date_from_6am     date            not null
        ,box_P_or_S                     varchar(1)      default 'U' -- P/S and Anytime+ get used for grouping
        ,Account_anytime_plus           bit             default 0
        ,Box_has_anytime_plus           bit             default 0
);

go

create index subscriber_index   on vespa_SVD_new_log_listing (subscriber_id);
create index account_index      on vespa_SVD_new_log_listing (account_number);
create index log_date_index     on vespa_SVD_new_log_listing (doc_creation_date_from_6am);

go

if object_id('vespa_SVD_account_level_summary') is not null
   drop table vespa_SVD_account_level_summary;
go
-- Summary at account level:


create table vespa_SVD_account_level_summary (
        account_number                                  varchar(20)             not null primary key
        ,enabled_date_min                               datetime                not null
        ,primary_box_enabled                            bit                     not null default 0
        ,primary_box_enabled_and_returned_data          bit                     not null default 0
        ,non_primary_box_enabled                        bit                     not null default 0
        ,non_primary_box_enabled_and_returned_data      bit                     not null default 0
        ,account_premiums                               varchar(12)             default null
        ,highest_related_box                            varchar(12)             default null
);

go

create index enablement_date_index on vespa_SVD_account_level_summary (enabled_date_min)

go

if object_id('vespa_SVD_sky_base_listing') is not null
   drop table vespa_SVD_sky_base_listing;
go
-- For profiling across the entire Sky base


CREATE TABLE vespa_SVD_sky_base_listing (
        -- Sky Base profiling things:
        account_number                      varchar(30)     NOT NULL -- PRIMARY KEY - we've got a 6 dupes whoch make it not unique
        ,Box_type                           varchar(30)     DEFAULT NULL
        ,Premiums                           varchar(30)     DEFAULT NULL
        ,Value_segment                      varchar(30)     DEFAULT NULL
        ,Tenure                             varchar(30)     DEFAULT NULL
        ,SkyView_flag                         tinyint         DEFAULT NULL
        -- Other items for merged Opt-Out section:
        ,rtm                                varchar(50)     NOT NULL
        ,cust_viewing_data_capture_allowed  varchar(1)
--        ,most_recent_DTV_booking            date - doesn't see mto ever get used
        ,DTV_customer                       smallint
        ,is_new_customer                    tinyint
);

go

create index not_ceven_close_to_a_PK    on vespa_SVD_sky_base_listing (account_number)
create index rtm_for_opt_out            on vespa_SVD_sky_base_listing (rtm, DTV_customer, cust_viewing_data_capture_allowed, is_new_customer)

/****************** T01: PERMISSIONS ON CORE TABLES ******************/
-- Moved these into the creation script since otherwise we're trying to
-- continually grant the same permissions every run and that's silly.
-- (Though we're still doing that for the output tables :-/ )

-- We're running a proc from vespa_analysts now, so users need permissions to
-- pull the reports they need from the various tables. Though core stuff is
-- arguably only useful if the report doesn't pass QA...
grant select on vespa_SVD_account_level_summary      to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
grant select on vespa_SVD_log_collection_dump        to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
grant select on vespa_SVD_new_log_listing            to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
grant select on Vespa_SVD_sky_base_listing           to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
grant select on vespa_SVD_subscriber_dashboard       to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;

commit;
go
