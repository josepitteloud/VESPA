/******************************************************************************
**
** Project Vespa: Weekly Status Report
**                  - Weekly Refresh Script
**
** This guy details changes in the panel bases for both Vespa and Sky View
** panels. Week to week we want to be able to look at what makes up the
** customer base; last week will be either common or churned, this week will
** be either churned or acquired. We might throw it into pivot graphs so that
** we can look at it by region or by premiums or affluence or lifestage. How
** many rows is that to export? Hopefully not many.
**
** Refer to:
**  http://rtci/vespa1/Weekly%20Status%20Report.aspx
**
** So it turns out we can get all of these variables from the central scaling
** lookup table. Do we want to be putting that dependency in there? Scaling
** will be changing soon. Dunno. Do we also want to mark reporting or not as
** a thing? That'd be pretty cute really, might see if we can. Again, we can
** get that from the current scaling build, just look at the intervals. Sweet.
**
** Code sections:
**      Part A: A01 -  Initialise Logger
**              A02 -  Table resets
**
**      Part B:         Population
**              B01 -  Identify open loop boxes
**              B02 -  Closed loop boxes
**              B03 -  Form initial population
**              B04 -  Get final population
**              B05 -  Population numbers into reporting archive
**
**      Part C:         Panel juggling
**              C01 -  Panel migration, as in, boxes that move between panels
**              C02 -  Vespa enablement confirmations (Open -> Closed loop)
**              C03 -  New enablement requests
**
**      Part D:         Structural flagging
**              D01 -  Flag Churn
**              D02 -  Flag Acquisition
**              D03 -  Flag data returns (initial profiling)
**              D04 -  Flag data returns (final profiling)
**              D09 -  Summarising structural flags into states
**              D10 -  Reporting on applied flags
**
**      Part E:         Flagging for profiling
**              E01 -  ... currently no other profiling flags yet?
**
**      Part P: P01 -  Archiving current states into the cache tables
**
**      Part Q          Automated QA (not in play yet...)
**
**      Part R:         Reporting build
**              R01 -  The basic tables. So far this is all.
**
**      Part T: T01 -  Permissions!
**
**
** Still to do:
**  3/ Documentation on the wiki of how the thing works etc
**  6/ We want figures for people dissappearing due to opting out. Where does that data come
**      from? Is there a historical record of that at all? Update: If we're pulling from SBV
**      then we can look for opt-out flags in the difference... (which we're also not checking
**      for Vespa panel live right now? heh...)
** 10/ Get the data feed for Sky View panel. We're waiting on a CCN for that; it'll be a while.
**      Update: this guy is still a bunch of slow manual hacks...
** 15/ Number the transition state labels so they come out ordered nice?
** 17/ Going to need a bunch of live testing to make sure the various left joins are working how
**      they should, with both the ON and WHERE clauses doing specific things.
** 18/ Maybe also numbers on ALT6 and ALT7 when they arrive. Currently we're tracking things in
**      and out of Vespa, which is kind of incidental, but it's not reporting a whole lot on
**      anything else. 
**
** Recently done:
**  8/ Automation testing etc.
**  5/ Work in the reporting-ever flags? Can get the reporting quality easily from SBV.
** 11/ The week-to-week differences want to be summarised and saved into a historical view, so
**      we have more than just the last week in the report.
** 12/ We're going to pull the active panel details from SBV and then cache the relevant details
**      into another historical table. It'll make tracking all the different fluxes perhaps
**      annoying, but might actually make the processing easier? We'll see.
**  1/ Additional revisions to build identified through feedback.
** 13/ Want to add items for panel flux; ie, Vespa -> Alt6, Alt 7 -> Vespa, etc. Do we want the
**      status of the alternate panels too? Probably...
** 14/ Do we also want box reporting quality as any kind of metric? It'd be easy to get from SBV,
**      though that statistic is really being tracked in other reports, so dunno.
**  7/ What other profiling flags do we want?
**  9/ We've got a category for new enablement requests, but we don't have a category for new
**      loop closings. might want one? Update: of questionable usefulness now we're doing the
**      historic rebuild with the SBV cacheing.
** 16/ Might also need a different key on the pivot thing, so that we don't have to keep changing
**      the dates that are visible in order to keep the graph up to date. Then drive the graph
**      by the content instead of the column or something? Wait, could we even get around that
**      with some other filter, say on 'VESPA', just to exclude the other blanks? That's all the
**      filter on the dates was for... Update: now have ordered panel listing...
**
******************************************************************************/

-- OK, so in order to build the various historicals like we want, probably large parts
-- of this whole thing are going to change... may need almost total redesign, we'll see.

if object_id('Weekly_Status_make_report') is not null
   drop procedure Weekly_Status_make_report;

go

create procedure Weekly_Status_make_report
as
begin

/****************** A01: SETTING UP THE LOGGER ******************/

DECLARE @westat_logging_ID      bigint
DECLARE @Refresh_identifier     varchar(40)
declare @run_Identifier         varchar(20)
-- For putting control totals in prior to logging:
DECLARE @QA_catcher             integer
-- For reporting boundaries:
declare @report_end_date        date
declare @report_start_date      date    -- this guy also might end up being redundant?
declare	@dk_event_startdate		integer
declare	@dk_event_enddate		integer

-- Master scheduler ping comes from Kuntal Mandal, so if that's our user then we're
-- doing a propper overnight run.
if lower(user) = 'kmandal'
    set @run_Identifier = 'VespaWeStat'
else
    set @run_Identifier = 'WeStat test ' || upper(right(user,1)) || upper(left(user,2))

set @Refresh_identifier = convert(varchar(10),today(),123) || ' WS refresh'
EXECUTE citeam.logger_create_run @run_Identifier, @Refresh_identifier, @westat_logging_ID output

-- We're not doing viewing data, we're doing Olive profiling stuff which means the
-- whole thing is set back by a week, and best case is to profile on Thursdays with
-- the refresh.

execute vespa_analysts.Regulars_Get_report_end_date @report_end_date output -- A Saturday

set @report_end_date    = dateadd(day, -2, @report_end_date) -- A Thursday
set @report_start_date  = dateadd(day, -7, @report_end_date) -- The previous thursday

commit

-- So identifying churn in a period is easy. So now... do any of these show up
-- in our Vespa table?

EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'A01: Complete! (Report setup)'
commit

/****************** A02: TABLE RESETS ******************/

execute WeStat_clear_transients
commit

EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'A02: Complete! (Table resets)'
commit

/****************** B01: IDENTIFYING THE OPEN LOOP BOXES ******************/

-- Now we just rely on SBV to get these numbers together...
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'B01: Discontinued! (Open loop population)'
commit

/****************** B02: GETTING CLOSED LOOP FROM VESPA HISTORY TABLE ******************/

-- Ha, no, all this is now sorted out by the SBV build...
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'B02: Discontinued! (Closed loop population)'
commit

/****************** B03: IDENTIFYING THE INITIAL POPULATION ******************/

-- Now all we need to do for this section is pull the stuff out of the historical table....

insert into vespa_westat_population_breakdown (
    subscriber_id
    ,account_number
    ,panel
    ,reporting_quality
    ,profiling_day
    ,initial_or_final
)
select 
    subscriber_id
    ,account_number
    ,panel
    ,reporting_quality
    ,@report_start_date
    ,'I'
from vespa_westat_prior_population
where archive_date = @report_start_date

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_westat_population_breakdown
where initial_or_final = 'I'

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'B03: Complete! (Initial population)', coalesce(@QA_catcher, -1)
commit

/****************** B04: IDENTIFYING THE FINAL POPULATION ******************/

-- Oh hey, the final population is now just the current SBV build!
insert into vespa_westat_population_breakdown (
    subscriber_id
    ,account_number
    ,panel
    ,reporting_quality
    ,profiling_day
    ,initial_or_final
)
select
    subscriber_id
    ,account_number
    ,panel
    ,reporting_quality
    ,@report_end_date
    ,'F'
from vespa_analysts.vespa_single_box_view
where (panel is null) or panel in ('VESPA','ALT6', 'ALT7')
and status_vespa = 'Enabled'

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_westat_population_breakdown
where initial_or_final = 'F'

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'B04: Complete! (Final population)', coalesce(@QA_catcher, -1)
commit

/****************** B05: STARTING THIS WEEK'S STUFF IN THE RESULTS CACHE ******************/

-- All the reporting goes from a central table, and we'll start building that up now as we
-- get the various pieces we need. Some of them go into the table but not the graph, so they
-- don't go as updates into the main population table. First though, if we're ever rerunning
-- the report for the same period (eg if something broke), we'll want to clear the existing
-- results out of it:
delete from vespa_westat_results_cache
where profile_date = @report_end_date

commit

-- First step: get the population totals, may as well from SBV again:
insert into vespa_westat_results_cache (
    profile_date
    ,vespa_enabled_closed_loop
    ,vespa_enabled_open_loop  
    ,alternate_6_closed_loop  
    ,alternate_7_closed_loop  
)
select
    @report_end_date
    ,sum(case when panel = 'VESPA' then 1 else 0 end)
    ,sum(case when panel is null then 1 else 0 end)
    ,sum(case when panel = 'ALT6' then 1 else 0 end)
    ,sum(case when panel = 'ALT7' then 1 else 0 end)
from vespa_analysts.vespa_single_box_view
where status_vespa = 'Enabled'

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'B05: Complete! (Population numbers)'
commit

/****************** C01: PANEL JUGGLING FLAGS ******************/

-- Now with support for ALT6 and ALT7, we want to track how stuff is moving
-- between these panels.

update vespa_westat_population_breakdown
set vespa_westat_population_breakdown.transition_state = case
    when vespa_westat_population_breakdown.panel = 'VESPA' and vespa_westat_population_breakdown.initial_or_final = 'I'                        then '08) Outbound panel transfer'
    when vespa_westat_population_breakdown.panel = 'VESPA' and vespa_westat_population_breakdown.initial_or_final = 'F' and r.panel = 'ALT6'   then '06) Imported from ALT6'
    when vespa_westat_population_breakdown.panel = 'VESPA' and vespa_westat_population_breakdown.initial_or_final = 'F' and vespa_westat_population_breakdown.panel = 'ALT7'   then '07) Imported from ALT7'
    when vespa_westat_population_breakdown.panel = 'VESPA' and vespa_westat_population_breakdown.initial_or_final = 'F'                        then '09) Other inbound transfer'
                                                                               else 'Immaterial transfer'
  end
from vespa_westat_population_breakdown
inner join vespa_westat_population_breakdown as r
on vespa_westat_population_breakdown.subscriber_id = r.subscriber_id
and vespa_westat_population_breakdown.initial_or_final <> r.initial_or_final
and vespa_westat_population_breakdown.panel <> r.panel -- This will exclude the NULLs of open loop enablement, but we'll pick that up in the next section

commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_westat_population_breakdown
where transition_state is not null

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'C01: Complete! (Panel juggling)', coalesce(@QA_catcher, -1)
commit

/****************** C02: TRANSITION OPEN -> CLOSED LOOP ******************/

-- So this guy isn't his own status, but we're tracking him in the listing
-- but not the pivot table... 

declare @enablement_closures int

select @enablement_closures = count(1)
from vespa_westat_population_breakdown as l
left join vespa_westat_population_breakdown as r
on l.subscriber_id = r.subscriber_id
and l.initial_or_final <> r.initial_or_final -- LEFT JOIN means that ON and WHERE clauses behave very differently
where l.initial_or_final = 'I'
and l.panel = 'VESPA'
and r.panel is null -- cheeky trick; it might be NULL if theres' no record, or if it was only open loop enabled, but we want both of those options to count

commit

update vespa_westat_results_cache
set enablement_completed = @enablement_closures
where profile_date = @report_end_date

EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'C02: Complete! (Loop closure)', coalesce(@enablement_closures, -1)
commit

/****************** C03: NEW ENABLEMENT REQUESTS ******************/

-- Another item not on the graph: the number of new boxes which have been
-- added to the enablement request list.

declare @new_requests int

select @new_requests = count(1)
from vespa_westat_population_breakdown as l
left join vespa_westat_population_breakdown as r
on l.subscriber_id = r.subscriber_id
and l.initial_or_final <> r.initial_or_final -- LEFT JOIN means that ON and WHERE clauses behave very differently
where l.initial_or_final = 'I'
and r.subscriber_id is null -- that were not around in the previuos week
and l.panel is null -- that were not around in the previuos week

commit

update vespa_westat_results_cache
set recently_requested_boxes = @new_requests
where profile_date = @report_end_date

EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'C03: Complete! (New enablement requests)', coalesce(@new_requests, -1)
commit

/****************** D01: APPLY CHURN FLAG TO INITIAL POPULATION ******************/

-- Okay, so we need all churns between @report_start_date and @report_end_date...
select  account_number
       ,effective_from_dt as churn_date
       ,case when status_code = 'PO'
             then 'CUSCAN'
             else 'SYSCAN'
         end as churn_type
       ,RANK() OVER (PARTITION BY  csh.account_number
                     ORDER BY  csh.effective_from_dt,csh.cb_row_id) AS 'RANK'  --Rank to get the first event
  into #panel_churns
  from sk_prod.cust_subs_hist as csh
 where subscription_sub_type ='DTV Primary Viewing'     --DTV stack
   and status_code in ('PO','SC')                       --CUSCAN and SYSCAN status codes
   and effective_from_dt between @report_start_date and @report_end_date
   and effective_from_dt != effective_to_dt

delete from #panel_churns where rank > 1

commit
create unique index fake_PK on #panel_churns (account_number)
commit

update vespa_westat_population_breakdown
set account_churns = 1
    ,churn_type = pc.churn_type
from vespa_westat_population_breakdown as wpb
inner join #panel_churns as pc
on wpb.account_number = pc.account_number
where initial_or_final = 'I'
-- Only mark churn on intial position; not sure it makes a whole lot of sense getting
-- churned accounts turning up on the final profiling day.

-- OK, that was not difficult.
commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_westat_population_breakdown
where account_churns = 1

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'D01: Complete! (Churn flags)', coalesce(@QA_catcher, -1)
commit

/****************** D02: APPLY ACQUISITION FLAG TO FINAL POPULATION ******************/

-- And now all activations between @report_start_date and @report_end_date...
select distinct account_number
into #panel_acquisitions
from sk_prod.cust_single_account_view
where acct_first_account_activation_dt between @report_start_date and @report_end_date

commit
create unique index fake_PK on #panel_acquisitions (account_number)
commit

update vespa_westat_population_breakdown
set account_activates = 1
from vespa_westat_population_breakdown as wpb
inner join #panel_acquisitions as pc
on wpb.account_number = pc.account_number
and wpb.initial_or_final = 'F'
-- Activation only makes sense on the second of the profiling days

commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_westat_population_breakdown
where account_activates = 1

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'D02: Complete! (Acquisition flags)', coalesce(@QA_catcher, -1)
commit

/****************** D03: PROFILING ON WHETHER A BOX RETURNED DATA IN THE LAST WEEK ******************/

-- Profiling by whether or not the box returned data in the last week
declare @SQL_Hurg               varchar(2000)
declare @scanning_day           date

delete from vespa_WeStat_log_collection_dump

-- we want to use the time dimension fields on the Select to speed up the time response (however still there is work to do to achieve a proper
-- state, as removing data manipulation commands from date fields on this Select and manipulating them after extraction... but it's a start)
-- so the shape that the time dimension fields have is YYYYMMDDHH and btw they are integer, so yeah is a numeric representation of a date and hour


set @dk_event_startdate	= convert(integer,dateformat(dateadd(day, -6, @report_start_date),'yyyymmddhh')) 	-- YYYYMMDD00
set @dk_event_enddate 	= convert(integer,dateformat(dateadd(day,1,@report_start_date),'yyyymmdd')+'23')	-- YYYYMMDD23

insert into vespa_WeStat_log_collection_dump (
        subscriber_id
        ,doc_creation_date_from_6am
)
select distinct
        subscriber_id
        ,convert(date, dateadd(hh, -6, LOG_RECEIVED_START_DATE_TIME_UTC))
from sk_prod.vespa_dp_prog_viewed_current
where panel_id in (4, 12)
and LOG_RECEIVED_START_DATE_TIME_UTC is not null
and subscriber_id is not null
and	dk_event_start_datehour_dim between @dk_event_startdate and @dk_event_enddate
union
select distinct
        subscriber_id
        ,convert(date, dateadd(hh, -6, LOG_RECEIVED_START_DATE_TIME_UTC))
from sk_prod.vespa_dp_prog_non_viewed_current
where panel_id in (4, 12)
and LOG_RECEIVED_START_DATE_TIME_UTC is not null
and subscriber_id is not null
and	dk_event_start_datehour_dim between @dk_event_startdate and @dk_event_enddate


--Adding this comment to the log, I think is nice to have it...
execute citeam.logger_add_event @westat_logging_ID, 3, 'D03: Weekly table scanned... (' || dateformat(dateadd(day, -6, @report_start_date),'yyyymmdd') || ')'
commit 


-- Want the ones that are pre-6AM, but not the ones that are post-6AM on the last day.
select distinct subscriber_id
into #initial_reporting_boxes
from vespa_WeStat_log_collection_dump
where doc_creation_date_from_6am <= @report_start_date

commit
create unique index fake_pk on #initial_reporting_boxes (subscriber_id)
commit

update vespa_westat_population_breakdown
set reporting_in_last_week = 1
from vespa_westat_population_breakdown as wpb
inner join #initial_reporting_boxes as irb
on wpb.subscriber_id = irb.subscriber_id
and wpb.initial_or_final = 'I'

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_westat_population_breakdown
where reporting_in_last_week = 1
and initial_or_final = 'I'

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'D03: Complete! (Returns in prior week)', coalesce(@QA_catcher, -1)
commit

/****************** D04: BOX RETURNING DATA DURING REPORT WEEK ******************/

delete from vespa_WeStat_log_collection_dump

-- Basically here, re-using the same code from above but in this case considering the date range for the report's week (report_end_date - 6 days)...

insert into vespa_WeStat_log_collection_dump (
        subscriber_id
        ,doc_creation_date_from_6am
)
select distinct
        subscriber_id
        ,convert(date, dateadd(hh, -6, LOG_RECEIVED_START_DATE_TIME_UTC))
from sk_prod.vespa_dp_prog_non_viewed_current
where panel_id in (4, 12)
and LOG_RECEIVED_START_DATE_TIME_UTC is not null
and subscriber_id is not null
and	dk_event_start_datehour_dim between @dk_event_startdate and @dk_event_enddate
union
select distinct
        subscriber_id
        ,convert(date, dateadd(hh, -6, LOG_RECEIVED_START_DATE_TIME_UTC))
from sk_prod.vespa_dp_prog_non_viewed_current
where panel_id in (4, 12)
and LOG_RECEIVED_START_DATE_TIME_UTC is not null
and subscriber_id is not null
and	dk_event_start_datehour_dim between @dk_event_startdate and @dk_event_enddate


--Adding this comment to the log, I think is nice to have it...
execute citeam.logger_add_event @westat_logging_ID, 3, 'D04: Daily table scanned... (' || dateformat(@scanning_day,'yyyymmdd') || ')'
commit

select distinct subscriber_id
into #final_reporting_boxes
from vespa_WeStat_log_collection_dump
where doc_creation_date_from_6am <= @report_end_date

commit
create unique index fake_pk on #final_reporting_boxes (subscriber_id)
commit

update vespa_westat_population_breakdown
set reporting_in_last_week = 1
from vespa_westat_population_breakdown as wpb
inner join #final_reporting_boxes as irb
on wpb.subscriber_id = irb.subscriber_id
and wpb.initial_or_final = 'F'

commit

-- Eventually we'll rebuild this with the scaling tables when scaling is rebuilt to use boxes instead.
-- hey, even before then, we might rebuild and then maintain those dialback tables to assist with box
-- reported-back-even, assist with the dialback and other reports that do stuff like this. hmmm...

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_westat_population_breakdown
where reporting_in_last_week = 1
and initial_or_final = 'F'

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'D04: Complete! (Returns during report week)', coalesce(@QA_catcher, -1)
commit

/****************** D09: SUMMARISING STRUCTURAL FLAGS INTO STATES ******************/

-- So this section mixes in the transition states for the churn and acquisition flags,
-- we already did the panel transfer stuff in sections C01 - C03

-- Maybe we want to identify people who weren't in the panel at the start but
-- were in the end? Yeah, might be worthwhile. 
select distinct subscriber_id
into #initial_boxes
from vespa_westat_population_breakdown
where initial_or_final = 'I'

commit
create unique index fake_pk on #initial_boxes (subscriber_id)
commit

update vespa_westat_population_breakdown set new_in_final_non_acquisition = 1
where initial_or_final = 'F' and account_activates = 0

update vespa_westat_population_breakdown
    set new_in_final_non_acquisition = 0
from vespa_westat_population_breakdown
inner join #initial_boxes as ib
on vespa_westat_population_breakdown.subscriber_id = ib.subscriber_id
and initial_or_final = 'F'
commit
-- Okay, that's cool, now let's build a graph or something.

-- So: turn all the flags into human readable stuff
update vespa_westat_population_breakdown
set transition_state = case
    when account_activates = 1 then '10) Acquired customer'
    when account_churns = 1 and churn_type = 'SYSCAN' then '11) SYSCAN churner'
    when account_churns = 1 and churn_type = 'CUSCAN' then '12) CUSCAN churner'
    when new_in_final_non_acquisition = 1 then '05) Recently requested box'
    when Panel = 'VESPA' then '01) Vespa panel 12'
    when Panel = 'ALT6' then '03) Alternate panel 6'
    when Panel = 'ALT7' then '04) Alternate panel 7'
    when Panel is null then '02) Enablement requested (Not yet in Panels)'
end
where transition_state is null

commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_westat_population_breakdown
where transition_state is not null

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'D09: Complete! (Summarise states)', coalesce(@QA_catcher, -1)
commit

/****************** D10: PUTTING FLAG COUNTS INTO THE RESULTS CACHE ******************/

-- Now we have all the totals we want, we can put them into the results structure
update vespa_westat_results_cache
set 
    CUSCAN_churner              = t.CUSCANs
    ,SYSCAN_churner             = t.SYSCANs
    ,Acquired_customer          = t.acquisitions
    ,flux_in_from_panel_6       = t.in_from_6
    ,flux_in_from_panel_7       = t.in_from_7
    ,flux_out                   = t.flux_out
from (
    select
        sum(case when transition_state = '12) CUSCAN churner' then 1 else 0 end) as CUSCANs
        ,sum(case when transition_state = '11) SYSCAN churner' then 1 else 0 end) as SYSCANs
        ,sum(case when transition_state = '10) Acquired customer' then 1 else 0 end) as acquisitions
        ,sum(case when transition_state = '06) Imported from ALT6' then 1 else 0 end) as in_from_6
        ,sum(case when transition_state = '07) Imported from ALT7' then 1 else 0 end) as in_from_7
        ,sum(case when transition_state = '08) Outbound panel transfer' then 1 else 0 end) as flux_out
    from vespa_westat_population_breakdown
    where panel = 'VESPA' or panel is null -- restrict to VESPA open & closed loop enablements
) as t
where profile_date = @report_end_date

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'D10: Complete! (Profiled reporting)'
commit

/****************** E01: OTHER PROFILING FLAGS ******************/

-- Maybe not any in the first build? but we've got the numebrs and the profiling
-- date in case we do.

/****************** P01: ARCHIVING ACTIVE BOX POPULATION ******************/

-- The last week's worth of stuff we report on is already cached, which is
-- fine, but we also need to store the list of boxes that are active now to
-- be the prior week's boxes for next week's run, and that comes straight
-- off SBV again:

delete from vespa_westat_prior_population
where archive_date + 11 < @report_start_date
or archive_date = @report_end_date          -- In case we're recovering from a broken build this week
commit

insert into vespa_westat_prior_population
select
        subscriber_id
        ,@report_end_date
        ,account_number
        ,panel
        ,reporting_quality
from vespa_analysts.vespa_single_box_view
where (panel is null) or panel in ('VESPA','ALT6', 'ALT7')
and status_vespa = 'Enabled'

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_westat_prior_population

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'P01: Complete! (Active panel)', coalesce(@QA_catcher, -1)
commit


/****************** Q01: AUTOMATED QA ******************/

-- Do we have anything we want to test here? geenrally there are going to be all
-- kinds of messy contingencies to worry about...


/****************** R01: REPORTING BUILDS ******************/

-- There's only one graph at the moment. So... yeah. Might be more later?

if object_id('vespa_analysts.vespa_WeStat_01_BigForPivot') is not null
   drop table vespa_analysts.vespa_WeStat_01_BigForPivot

commit

-- Currently not reporting the alternate panel 6 and 7 stuff.
select
    case
        when panel = 'VESPA'    then '1) VESPA live panel'
        when panel is null      then '2) Enablement requested'
        when panel = 'ALT6'     then '3) Alternate panel 6'
        when panel = 'ALT7'     then '4) Alternate panel 7'
      end as box_panel
    ,case
		when initial_or_final = 'I' then 'Initial'
		when initial_or_final = 'F' then 'Final'
									else 'Unknown'
	end as initial_or_final
    ,profiling_day
    ,transition_state
    ,count(1) as distinct_boxes
into vespa_analysts.vespa_WeStat_01_BigForPivot
from vespa_westat_population_breakdown
group by panel, initial_or_final, profiling_day, transition_state
order by panel, initial_or_final, profiling_day, transition_state
-- This looks like a big pivot, but there are a lot of dependencies in there; only 11 rows come out.

-- It'll also help with the profiling, having fewer rows coming out tracking the
-- states. So, yeah. Might need another column for Vespa vs Sky View too.

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_analysts.vespa_WeStat_01_BigForPivot

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'R01: Complete! (Report constructions)', coalesce(@QA_catcher, -1)
commit


/****************** T01: PERMISSIONS ON REPORT TABLES ******************/
-- These permissions, however, are essential run-to-run.
grant select on vespa_analysts.vespa_WeStat_01_BigForPivot to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, angeld

commit
EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'T01: Complete! (Report permissions)'
COMMIT

/****************** X01: AND WE'RE DONE! ******************/

EXECUTE citeam.logger_add_event @westat_logging_ID, 3, 'WeStat: weekly refresh complete!'
COMMIT

end;

go

-- And somethign else to clean up the junk that was built:
if object_id('WeStat_clear_transients') is not null
   drop procedure WeStat_clear_transients;

go

create procedure WeStat_clear_transients
as
begin
    delete from vespa_analysts.vespa_westat_population_breakdown
    delete from vespa_analysts.vespa_WeStat_log_collection_dump
    if object_id( 'vespa_analysts.vespa_WeStat_01_BigForPivot') is not null
        drop table vespa_analysts.vespa_WeStat_01_BigForPivot

end;

go
grant execute on Weekly_Status_make_report   to public;
grant execute on WeStat_clear_transients     to public;
go
-- Need the central scheduler thing to be able to call the procs. But it gets
-- run within the vespa_analytics account, so it doesn't mean that any random
-- public person can see what's in the resulting tables.


