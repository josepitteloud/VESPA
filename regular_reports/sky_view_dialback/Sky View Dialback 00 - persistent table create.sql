/******************************************************************************
**
** Sky View panel: Dialback Report
**                  - Persistent Table Creation
**
** These are all the non-reporting tables that get built along the course of
** the Sky View Panel Dialback Report. This is all mostly built on the Vespa
** Dilback report, mostly with search-replace to change the names, because
** we don't want those to clash.
**
******************************************************************************/

/****************** A01: CREATE TRANSIENT TABLES ******************/

if object_id('sky_view_dialback_log_collection_dump') is not null
    drop table sky_view_dialback_log_collection_dump;
go
-- A staging table for pulling all the things out of the daily tables


create table sky_view_dialback_log_collection_dump (
        subscriber_id                   decimal(8)      not null
        ,LOG_START_DATE_TIME_UTC        datetime        not null
        ,doc_creation_date_from_9am     date            not null
        ,first_event_mark               datetime        not null
        ,last_event_mark                datetime        not null
        ,log_event_count                int             not null
        ,hour_received                  tinyint         not null            -- could make doc_creation_date_from_9am a date, but don't know what other knock-on effects that'd have
);

go

create index maybe_some_kind_of_fake_PK on sky_view_dialback_log_collection_dump (subscriber_id, doc_creation_date_from_9am);

go

if object_id('sky_view_dialback_log_daily_summary') is not null
    drop table sky_view_dialback_log_daily_summary
-- Summarising into one record per box per day
go

create table sky_view_dialback_log_daily_summary (
        subscriber_id                   decimal(8)      not null
        ,log_date                       date            not null default 0
        ,logs_sent                      int             not null default 0
        ,coverage_starts                datetime        not null            -- might end up summarising over multiple logs for same
        ,coverage_ends                  datetime        not null            -- day. Some pathological cases missed, probably fine.
        ,log_event_count                int             not null
        ,hour_received                  tinyint         not null
        ,primary key (subscriber_id, log_date)
);

go

if object_id('sky_view_dialback_box_listing') is not null
    drop table sky_view_dialback_box_listing;
go
-- Has one record per interval (which are specific to boxes)


create table sky_view_dialback_box_listing (
    subscriber_id                       decimal(8)      not null primary key
    ,account_number                     varchar(20)     not null
--    ,Vespa_box_state                    varchar(20)     not null          -- This guy comes from Vespa_subscriber_status which doesn't have Sky View Panel peopel on it.
--    ,enabled_date                       date            not null          -- Don't have date of enabling of boxes for Sky View stuff.
    ,enabled_7d                         bit             not null default 0
    ,enabled_30d                        bit             not null default 0
    ,confirmed_activation_7d            bit             not null default 0
    ,confirmed_activation_30d           bit             not null default 0
    ,total_logs_7d                      int             not null default 0
    ,distinct_days_7d                   int             not null default 0
    ,largest_interval_7d                int             not null default 0
    ,interval_count_7d                  int             not null default 0
    ,total_logs_30d                     int             not null default 0
    ,distinct_days_30d                  int             not null default 0
    ,largest_interval_30d               int             not null default 0
    ,interval_count_30d                 int             not null default 0
    -- And then whatever profiliing stuff we want...
    ,box_rank                           varchar(10)                     -- Primary or Secondary
    ,box_type                           varchar(20)                     -- HD, combi, 1TB, all that stuff
    ,premiums                           varchar(30)
    ,value_segment                      varchar(20)
    -- Oh hey some actual math, we're looking at state transition coeficients...
    ,t_00_counts_30d                    int             default null    -- observed instances of not reporting -> not reporting transitions for 30d window
    ,t_01_counts_30d                    int             default null    -- observed instances of not reporting -> *is* reporting transitions for 30d window
    ,t_10_counts_30d                    int             default null    -- observed instances of *is* reporting -> not reporting transitions for 30d window
    ,t_11_counts_30d                    int             default null    -- observed instances of *is* reporting -> *is* reporting transitions for 30d window
    ,transition_determinant_30d         double          default null    -- Determinants as a way of measuring how close to degenerate the transition matrix
--    ,transition_determinant_7d          double          default null    --  is, ie, how close it is to independent reporting chance from day to day.
);
go

-- Indices we'll add after they're populated? Makes sense for log items because
-- we're adding to them many times with daily tables, but for boxes, we build
-- this all in one go so it makes no difference where he indices get created.
create index for_joins_1 on sky_view_dialback_box_listing (account_number);

-- Similar to the Vespa Dialback thingy, this index might help overall or it might hinder...
create index for_mass_grouping  on sky_view_dialback_box_listing (box_rank, box_type, premiums, value_segment);


-- Because these things are central in vespa_analysts but people need to be
-- able to get to them...

grant select on sky_view_dialback_log_collection_dump      to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
grant select on sky_view_dialback_log_daily_summary        to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
grant select on sky_view_dialback_box_listing              to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;

commit;
go
