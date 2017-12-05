/******************************************************************************
**
** Project Vespa: Operational Dashboard Report
**                  - Persistent Table Creation
**
** This is the script that builds all the persistent tables required by the
** Operational Dashboard, or OpDash. This script *does not* need to be executed
** during standard weekly refreshes of the OpDash report.
**
** This script, when necessary, should be run under vespa_analysts. It's not
** necessary to run it any more though. Update: Except when it changes. And
** even then, you're hopefully not going to run this thing in it's entirety
** (because you'll kill the historical stuff), just the new parts you need.
**
******************************************************************************/

-- Section headings remain from the previous all-in-one script.

/****************** B01: CREATE PERSISTENT TABLES ******************/
-- These tables are populated at the end; if something weird happens and you need
-- to rebuild the lot from scratch, you just create these tables. The automatic date
-- selection should archive things up to 5 days ago, but this happens at the end, and
-- expect it to be mega slow if you're processing *all* of those logs; the events
-- view is growing at about 100m records a week or something crazy like that.

/* Block commented since this isn't required for standard operaional runs

-- Summarising previous daily info into the exact statistics we report, so we don't
-- need to pull in every single log each time
create table vespa_analysts.vespa_OpDash_log_aggregated_archive (
        doc_creation_date_from_9am      date            not null primary key
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

-- For remembering which boxes have ever previuosly returned data, again, to stop us
-- from having to trawl through the huge amount of historical log info.
create table vespa_analysts.vespa_OpDash_boxes_returning_archive (
        subscriber_id                   decimal(8)       not null primary key
        ,account_number                 varchar(20)      not null -- not strictly required, but might be useful
        ,active_subscriber              varchar(5)       not null default 'Y' -- might also be DISAB for disabled or CHURN for Churners
        ,date_archived                  date             not null default cast(getdate() as date)
        ,user_reporting                 varchar(20)      not null default user
)
;
-- subscriber_IDs shouldn't change ever... probably :/

-- For remembering the Opt Out / RTM state for accounts within a week of when
-- they activated, for tracking whether or not the Opt Out-ness changes.
create table vespa_analysts.vespa_OpDash_new_joiners_RTMs (
        rtm                             varchar(30)     not null
        ,viewing_allowed                int             not null
        ,viewing_not_allowed            int             not null
        ,viewing_capture_is_question    int             not null
        ,viewing_capture_is_NULL        int             not null
        ,total_records                  int             not null
        ,date_archived                  date            not null default cast(getdate() as date)
        ,user_reporting                 varchar(20)     not null default user
        ,primary key (rtm, date_archived)
)
;

-- Permissions!
grant select, insert on vespa_analysts.vespa_OpDash_log_aggregated_archive  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
grant all            on vespa_analysts.vespa_OpDash_boxes_returning_archive to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
grant select, insert on vespa_analysts.vespa_OpDash_new_joiners_RTMs        to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;

COMMIT
;

call dba.sp_create_table  ('vespa_analysts','vespa_OpDash_from_optin','context    varchar(20)
                                                                ,weekending date
                                                                ,value      float'
);

commit;
create date index idx1 on vespa_analysts.vespa_OpDash_from_optin(weekending);

call dba.sp_create_table ('vespa_analysts','vespa_OpDash_optouts','account_number varchar(30)
                                                                ,weekending date'
);

commit;
call dba.sp_create_table ('vespa_analysts','consent_last_week','account_number varchar(30)
');

call dba.sp_create_table  ('vespa_analysts','vespa_OpDash_11_Adsmartable_Accounts','flag      varchar(15)
                                                                                   ,sky_base  int
                                                                                   ,vespa     int
                                                                                   ,reporting int
');
                                                                                   
commit;

call dba.sp_create_table  ('vespa_analysts','vespa_OpDash_12_Adsmartable_Boxes_Types','box_model          varchar(20)
                                                                                      ,sky_boxes             int
                                                                                      ,vespa_boxes           int
                                                                                      ,vespa_boxes_reporting int
');
                                                                                   
commit;
    -- below are the source tables that are used to populate the two tables above (11&12) cortb (22-04-2014)
    --account level adsmart source tables
call dba.sp_create_table ('vespa_analysts','active_uk_cust_vc','account_number                varchar(20)      not null
    ')
    ;
    create unique index act_ac on active_uk_cust_vc (account_number);

call dba.sp_create_table ('vespa_analysts','adsmart_boxes_al','account_number                 varchar(20)      not null
                                                             ,flag                            bit              default 0
    ')
    ;
    create unique index ab_ac_al on adsmart_boxes_al (account_number);

call dba.sp_create_table ('vespa_analysts','DP_active_accounts_al','account_number            varchar(20)      not null
    ')
    ;
    create unique index DP_ac_al on DP_active_accounts_al (account_number);

call dba.sp_create_table ('vespa_analysts','DP_active_accounts_rep_al','account_number        varchar(20)      not null
    ')
    ;
    create unique index DP_ac_al on DP_active_accounts_rep_al (account_number);

    --box level adsmart source tables
call dba.sp_create_table ('vespa_analysts','adsmart_boxes_bl','account_number                 varchar(20)      not null
                                                             ,service_instance_id             varchar(50)
                                                             ,flag                            bit              default 0
                                                             ,box_model                       varchar(123)
    ')
    ;
    create unique index ab_ac_bl on adsmart_boxes_bl (account_number, service_instance_id);

call dba.sp_create_table ('vespa_analysts','DP_active_accounts_bl','account_number            varchar(20)      not null
                                                                  ,service_instance_id        varchar(50)
    ')
    ;
    create unique index DP_ac_bl on DP_active_accounts_bl (account_number, service_instance_id);

call dba.sp_create_table ('vespa_analysts','DP_active_accounts_rep_bl','account_number        varchar(20)      not null
                                                                      ,service_instance_id    varchar(50)
    ')
    ;
    create unique index DP_ac_bl on DP_active_accounts_rep_bl (account_number, service_instance_id);
---------------------*********************end of adsmart source tables

commit;

Call dba.sp_create_table ('vespa_analysts','vespa_opdash_13_hhs_count','households  int
                                                                       ,opt_in      varchar(3)
                                                                       ,adsmartable varchar(3)
                                                                       ,box_type    varchar(50)
                                                                       ,daily_panel varchar(3)
                                                                       ,vespa       varchar(3)
');

Call dba.sp_create_table ('vespa_analysts','vespa_opdash_14_subs_count','boxes       int
                                                                        ,opt_in      varchar(3)
                                                                        ,adsmartable varchar(3)
                                                                        ,box_type    varchar(50)
                                                                        ,daily_panel varchar(3)
                                                                        ,vespa       varchar(3)
');

call dba.sp_create_table ('vespa_analysts', 'vespa_opdash_15_adsm_history', 'month                 date
                                                                            ,adsm_boxes            int
                                                                            ,adsm_hhs              int
                                                                            ,adsm_hhs_1box         int
                                                                            ,adsm_hhs_all_adsm     int
                                                                            ,adsm_hhs_non_adsm_box int
');

call dba.sp_create_table ('vespa_analysts', 'vespa_opdash_16_adsm_history_4Xdash',  'weekending                         date
                                                                                    ,adsm_hhs_1box                      int
                                                                                    ,adsm_hhs_morethan1box_1box_adsm    int
                                                                                    ,adsm_hhs_all_adsm                  int
                                                                                    ,non_adsm_hhs                       int
                                                                                    ,adsm_hhs_reporting                 int
');



*/

     
/****************** B02: CREATE TRANSIENT TABLES ******************/

-- These ones are all tables that get cleared out and fully repopulated each run
if object_id('vespa_OpDash_subscriber_dashboard') is not null
   drop table vespa_OpDash_subscriber_dashboard;

go

create table vespa_OpDash_subscriber_dashboard (
    account_number                  varchar(20) not null
    ,subscriber_id                  bigint not null primary key
    ,Vespa_box_state                varchar(20)
    ,enabled_date                   date
    ,has_hd_subscription            bigint default 0
    ,verified_panel_id_4            bit default 0
    ,has_returned_data_ever         bit default 0
    ,previously_returned_data       bit default 0
    ,x_primary_box_subscription     tinyint default 0
    ,x_secondary_box_subscription   tinyint default 0
    ,src_system_id                  varchar(50)
    ,x_box_type                     varchar(20)
    ,box_subscription_group         varchar(60)
    ,box_has_anytime                bit default 0
    ,account_anytime                bit default 0
);

go

create index account_index on vespa_OpDash_subscriber_dashboard (account_number);
create index for_some_joining on vespa_OpDash_subscriber_dashboard (src_system_id);

go

if object_id('vespa_OpDash_log_collection_dump') is not null
   drop table vespa_OpDash_log_collection_dump;
go
-- Not sure we bother with any indices here, there's a lot of stuff being added to it
-- and at best we're going to add the indices after the table is fully populated, ie,
-- after we've junked a bunch of stuf into it from the daily events tables.

go
create table vespa_OpDash_log_collection_dump (
        subscriber_id                   decimal(8)      not null
        ,log_start_date_time_utc        datetime        not null
        ,account_number                 varchar(20)     not null
        ,doc_creation_date_from_9am     date            not null
        ,log_id                         varchar(100)    default null -- gets built later from subscriber_id and stb_log_creation_date
);
-- We're not indexing this guy because all we do with it is build the log listing
-- once and that's it. We don't even keep tags indicating which daily table the
-- log bits come from, so good luck trying to get it to do anything useful.
go


if object_id('vespa_OpDash_new_log_listing') is not null
   drop table vespa_OpDash_new_log_listing;
go
-- Summary of suitably new log entries. They get pulled out of the daily tables above, and
-- then get deduplicated (across daily tables) as they arrive in this log listing. Older
-- log numbers are just pulled out of the archives though.
go

create table vespa_OpDash_new_log_listing (
--        id                              bigint          identity not null primary key -- only useful in the discontinued deduplication procedure
        log_id                          varchar(100)    not null primary key
        ,subscriber_id                  decimal(8)      not null
        ,account_number                 varchar(20)     not null
--        ,document_creation_date         date            not null -- never gets used, he can get left out
        ,doc_creation_date_from_9am     date            not null
        ,box_P_or_S                     varchar(1)      default 'U' -- P/S and Anytime+ get used for grouping
        ,box_has_anytime                bit             default 0
        ,account_anytime                bit             default 0
);
go

create index subscriber_index   on vespa_OpDash_new_log_listing (subscriber_id);
create index account_index      on vespa_OpDash_new_log_listing (account_number);
create index log_date_index     on vespa_OpDash_new_log_listing (doc_creation_date_from_9am);
go

if object_id('vespa_OpDash_account_level_summary') is not null
   drop table vespa_OpDash_account_level_summary;
go
-- Summary at account level:

go

create table vespa_OpDash_account_level_summary (
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

create index enablement_date_index on vespa_OpDash_account_level_summary (enabled_date_min)

go

if object_id('vespa_analysts.vespa_OpDash_sky_base_listing') is not null
   call dba.sp_drop_table ('vespa_analysts','vespa_OpDash_sky_base_listing');
go
-- For profiling across the entire Sky base
go

call dba.sp_create_table ('vespa_analysts','vespa_OpDash_sky_base_listing','
        -- Sky Base profiling things:
        account_number                      varchar(30)     NOT NULL -- PRIMARY KEY - we've got a 6 dupes whoch make it not unique
        ,Box_type                           varchar(30)     DEFAULT NULL
        ,Premiums                           varchar(30)     DEFAULT NULL
        ,Value_segment                      varchar(30)     DEFAULT NULL
        ,Tenure                             varchar(30)     DEFAULT NULL
        ,Vespa_flag                         tinyint         DEFAULT NULL
        -- Other items for merged Opt-Out section:
        ,rtm                                varchar(50)     NOT NULL
--        ,most_recent_DTV_booking            date - doesn't see mto ever get used
        ,DTV_customer                       smallint
        ,is_new_customer                    tinyint
        ,opt_in_this_week                   bit             default 1
        ,cust_viewing_data_capture_allowed varchar(1)
');

go

create index not_ceven_close_to_a_PK    on vespa_analysts.vespa_OpDash_sky_base_listing (account_number)
create index rtm_for_opt_out            on vespa_analysts.vespa_OpDash_sky_base_listing (rtm, DTV_customer, cust_viewing_data_capture_allowed, is_new_customer)




-- Since 21/02/2013, this new table will have the historical view of the opt out figures produced by the opdash...

if object_id('vespa_OpDash_hist_optout') is not null
 drop table vespa_OpDash_hist_optout;

go

create table vespa_OpDash_hist_optout (
 context     varchar(20)  not null
 ,weekending    date    not null
 ,dir_internet   decimal(15,3) not null
 ,dir_internet_tel  decimal(15,3) not null
 ,dir_tel    decimal(15,3) not null
 ,events     decimal(15,3) not null
 ,existing_cust_sales decimal(15,3) not null
 ,retail_indep   decimal(15,3) not null
 ,retail_multi   decimal(15,3) not null
 ,Sky_Homes    decimal(15,3) not null
 ,sky_retail_stores  decimal(15,3) not null
 ,tesco     decimal(15,3) not null
 ,walkers_cobra   decimal(15,3) not null
 ,walkers_north   decimal(15,3) not null
 ,[all]     decimal(15,3) not null
)

commit
go

create lf  index opdash_hist_optout1 on vespa_OpDash_hist_optout(context)
create date index opdash_hist_optout2 on vespa_OpDash_hist_optout(weekending)

commit
go

-- Since 17/04/2013, we are keeping track of viewing consent for new customers...

if object_id('vespa_OpDash_hist_new_optout') is not null
 drop table vespa_OpDash_hist_new_optout;

go

create table vespa_OpDash_hist_new_optout (
 context     varchar(20)  not null
 ,weekending    date    not null
 ,dir_internet   decimal(15,3) not null
 ,dir_internet_tel  decimal(15,3) not null
 ,dir_tel    decimal(15,3) not null
 ,events     decimal(15,3) not null
 ,retail_indep   decimal(15,3) not null
 ,retail_multi   decimal(15,3) not null
 ,Sky_Homes    decimal(15,3) not null
 ,sky_retail_stores  decimal(15,3) not null
 ,walkers_cobra   decimal(15,3) not null
 ,walkers_north   decimal(15,3) not null
 ,[all]     decimal(15,3) not null
)

commit
go

create lf  index opdash_hist_optout1 on vespa_OpDash_hist_new_optout(context)
create date index opdash_hist_optout2 on vespa_OpDash_hist_new_optout(weekending)

commit
go

/****************** T01: PERMISSIONS ON CORE TABLES ******************/
-- Moved these into the creation script since otherwise we're trying to
-- continually grant the same permissions every run and that's silly.
-- (Though we're still doing that for the output tables :-/ )

-- We're running a proc from vespa_analysts now, so users need permissions to
-- pull the reports they need from the various tables. Though core stuff is
-- arguably only useful if the report doesn't pass QA...
grant select on vespa_OpDash_account_level_summary      to vespa_group_low_security, sk_prodreg;
grant select on vespa_OpDash_log_collection_dump        to vespa_group_low_security, sk_prodreg;
grant select on vespa_OpDash_new_log_listing            to vespa_group_low_security, sk_prodreg;
grant select on Vespa_OpDash_sky_base_listing           to vespa_group_low_security, sk_prodreg;
grant select on vespa_OpDash_subscriber_dashboard       to vespa_group_low_security, sk_prodreg;
grant select on vespa_OpDash_hist_optout    to vespa_group_low_security, sk_prodreg;
grant select on vespa_OpDash_hist_new_optout   to vespa_group_low_security, sk_prodreg;
grant select on vespa_opdash_16_adsm_history_4Xdash to vespa_group_low_security, sk_prodreg;

go












select count(1) from vespa_analysts.vespa_OpDash_12_Adsmartable_Boxes_Types


