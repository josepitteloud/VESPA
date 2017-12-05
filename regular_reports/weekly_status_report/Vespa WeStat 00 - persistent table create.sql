/******************************************************************************
**
** Project Vespa: Weekly Status Report
**                  - Persistent Table Creation
**
** Permanent tables for the Weekly Status report, which looks at account flux
** through both the Sky View Panel and Vespa Panel.
**
******************************************************************************/

-- Accounts and profiling:

if object_id('vespa_westat_population_breakdown') is not null
   drop table vespa_westat_population_breakdown; 
go  
-- um, heh, this guy might change (a lot?) for the new historical build


create table vespa_westat_population_breakdown (
    subscriber_id                   decimal(10,0)
    ,account_number                 varchar(20)         not null    -- Only using for profiling; can't pivot in reports because different boxes will enable on different days, leading to account number duplication
    ,reporting_in_last_week         bit                 default 0
    ,reporting_quality              float
    ,profiling_day                  date                not null
    ,initial_or_final               varchar(1)          not null
    ,panel                          varchar(10)                     -- now with support for ALT6 and ALT7, and NULL means open loop enablement
    ,account_churns                 bit                 default 0
    ,churn_type                     varchar(10)         default null
    ,account_activates              bit                 default 0
    ,new_in_final_non_acquisition   bit                 default 0
    ,transition_state               varchar(40)                     -- for summarising the flags into human-readable-friendly notes.
    ,transition_sequence            tinyint                         -- for ordering the graph sections in the right way
);
-- Yeah, stil lneed to add the transitions to this guy...

commit;
go

create unique   index other_fake_pk on vespa_westat_population_breakdown (subscriber_id, profiling_day);
create          index fake_pk       on vespa_westat_population_breakdown (subscriber_id);
create          index for_joining   on vespa_westat_population_breakdown (account_number, profiling_day);
commit;

-- Another log collection dump:
go

if object_id('vespa_WeStat_log_collection_dump') is not null
   drop table vespa_WeStat_log_collection_dump;
go

create table vespa_WeStat_log_collection_dump (
    subscriber_id                   decimal(10,0)
    ,doc_creation_date_from_6am     date
);

/**************** HISTORICAL TRACKING! ****************/

-- So in the new plan, we're saving a lot more stuff to disk and pulling it back to
-- compare to it the next week. This goes both for the boxes active (are we doing by
-- box or by household here?) and the important numbers week to week.

-- This table holds what the panel looked like last week:
/* Kind of has historical stuff, we don't want to kill it without being careful about it
if object_id('vespa_westat_prior_population') is not null
   drop table vespa_westat_prior_population;
create table vespa_westat_prior_population (
    subscriber_id                   decimal(10,0)
    ,archive_date                   date            -- We'll keep the last 3 weeks of things, so that if a report build fails midway through we don't lose the historical data
    ,account_number                 varchar(20)     -- We need to archive these as we won't be able to find all of the records on the next week's SBV
    ,panel                          varchar(10)     -- needs to tolerate nulls because that's how we track open loop enabled boxes...
    ,reporting_quality              float
    ,primary key (subscriber_id, archive_date)
);
*/

-- OK, and this guy is where we cache all the results of each week so we can pull them
-- out and view them nicely:
/* commented out because it's operational and historical and shouldn't really be dropped
if object_id('vespa_westat_results_cache') is not null
   drop table vespa_westat_results_cache;
create table vespa_westat_results_cache (
    profile_date                    date            primary key                 -- the Thursday we use for profiling
    ,cache_date                     date            not null default today()    -- the day when the report is run
    -- All these totals are *as of* the profiling Thursday:
    ,vespa_enabled_closed_loop      int             default null
    ,vespa_enabled_open_loop        int             default null
    ,alternate_6_closed_loop        int             default null                -- Are we reporting this or not? Track it though.
    ,alternate_7_closed_loop        int             default null                -- Are we reporting this or not? Track it though.
    -- All of these deltas are over the week ending on the profiling Thursday:
    ,recently_requested_boxes       int             default null
    ,enablement_completed           int             default null
    ,CUSCAN_churner                 int             default null
    ,SYSCAN_churner                 int             default null
    ,Acquired_customer              int             default null
    ,flux_in_from_panel_6           int             default null
    ,flux_in_from_panel_7           int             default null
    ,flux_out                       int             default null
    -- We've set all the defaults to null so that we can easily detect anything that didn't
    -- get properly updated. We're also not caching any other profiling details on this guy,
    -- eg, reporting wuality isn't avaiable. This is just for the table at the bottom.
);
*/

/**************** PERMISSIONS! ****************/
go

grant select on vespa_westat_population_breakdown   to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg, angeld;
grant select on vespa_westat_results_cache          to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg, angeld;

commit;
