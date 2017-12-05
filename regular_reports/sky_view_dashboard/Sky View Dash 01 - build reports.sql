/******************************************************************************
**
** Project Vespa: Sky View Dashboard Report
**                  - Weekly Refresh Script
**
** There's actually nothing in this script tht you run. It creates a big huge
** procedure which builds all the reports tables. You don't even directly run
** this procedure; it get's kicked off instead by the central report scheduler
** guy (currently in dev as 'VES024_Make_Reports') and you just turn up later,
** review the QA options ('Sky View Dialback 02 - check progress, manual QA.sql')
** then pull the report out via the Vespa Report Suite, which automates all
** the different pulls and organises the formatting etc. Refer to:
**
**      http://rtci/vespa1/Sky%20View%20Dashboard.aspx
**
** OD Spec: There is currently no spec for the Sky View dashboard beyond
** what's in the SQL. Or rather: "Make it like the Vespa Operational Dashboard
** except about Sky View". A couple of the tabs didn't survive because there
** was no comparable data source: specifically Opt Out and the weekly KPI files
** (external source) were removed, and the Weekly Enablement tab has been
** significantly cut back, because our Sky View panel has only one enablement
** load. We're working on other data sources though.
**
** Other information on the report scheduler can be found here:
**
**      http://rtci/vespa1/Analytics%20Scheduler.aspx
**
**
** Outstanding code dev actions:
**
** 14. Add the "any box active" and "any box returning data" to the reports. It's getting
**      big, but those fields might be useful. Small things.
** 55. So the current RTM is still broken, because there's a new table which has turned
**      and is providing the details for all new aquisitions or something. Until we fix
**      that, the RTM is going to be brokens.
** 59. Build (and run) the historical rebuild of the P/S and anytime split and enablement
**      stuff for the old bits of the daily sheet. It'll take an age though.
** 65. Note that the unknown boxes aren't lumped into Secondary boxes any more, and so
**      there's a hole in the control totals until we figure out the P/S flags.
**
** Look for '##' to help identify specific places where feature requests have been flagged.
**
** Recently implemented changes:
**
** 64. Now using as the base list not the Sky View enabled listing, but the boxes which are
**      selected for data return. Though, this is a once-off feed, as of 7 Feb 2012, so
**      dunno how long this will last...
** 61. Ahahaha hilarious.... The figure of box enablement quoted on the summary tab isn't
**      the open loop box enablement requests; it's the total enablements including both
**      panel 4 and 5. The cumulative table doesn't reset anywhere, and all the panel 5
**      boxes are still contributing. Might want to look into correcting that at some
**      point, or at least standardising it, because the numbers are not going to add up
**      with what's on the historical daily build thing. Update: Yeah, I think we fixed
**      that a while ago? when we filled the subscriber status specifically only with
**      those open loop enablement boxes? But in any case, that's super not relevant to
**      Sky View.
**
** Code sections:
**      Part A: A01 -  Initialise Logger
**              A02 -  Temporal Filters
**
**      Part B: B01 -  Persistent tables (now in "Sky View Dialback 00 - persistent table create")
**              B02 -  Create transient tables (now also in "Sky View Dialback 00 - persistent table create" too) 
**              B03 -  Clean out any persistent report holding tables
**
**      Part C:        Log item processing
**              C01 -  Extract new log items
**
**      Part E:        Box level processing
**              E01 -  Boxes returning data this cycle
**              E02 -  Assemble box listing
**              E03 -  Stitch on box details
**   ^          E04 -  Yet more box level details
**              E05 -  Pushing box details bax onto logs (to ease P/S reporting)
**
**      Part G:        Account level processing - Vespa only
**              G01 -  Prepare account summary
**              G02 -  Add account details
**
**      Part P:        Updating Vespa log archives
**              P01 -  Adding items to persistent aggregate tables
**              P02 -  Mark any churned / disabled boxes as such
**
**      Part S:        Account level - whole Sky base
**              S01 -  The whole Opt Out report costruction
**   ^          S02 -  Actions over the whole sky base (some duplication involved)
**              S03 -  Archiving RTM for customers activating in last week
**
**      Part Q:     -  Assembling reports into transient tables
**              Q01 -  Opt Out tables
**              Q02 -  Daily Summary stuff
**              Q03 -  Weekly Enablement results
**              Q04 -  Box Type & premiums details
**
**      Part T:
**              T01 -  Permissions on core tables
**              T02 -  Permissions on report structure tables
**
**      Part X:     -  The end of the refresh procedure!
**
**      Part Y:
**              Y01 -  (Different procdure) - clean out transient tables
**
** ^ : indicates current bottleneck sections (E04: 8min; S02: 14min) though
**      some of these involved downtime due to fixes too.
**
*****************************************************************************************/

if object_id('SkyView_Dashboard_make_report') is not null
   drop procedure SkyView_Dashboard_make_report;

go

create procedure SkyView_Dashboard_make_report
as
begin

/****************** A01: SETTING UP THE LOGGER ******************/

DECLARE @SVD_logging_ID         bigint
DECLARE @Refresh_identifier     varchar(40)
declare @run_Identifier         varchar(20)
-- For putting control totals in prior to logging:
DECLARE @QA_catcher             integer

-- Now automatically detecting if it's a test build and logging appropriately...
if lower(user) = 'kmandal'
    set @run_Identifier = 'SkyViewDashboard'
else
    set @run_Identifier = 'SVDash test ' || upper(right(user,1)) || upper(left(user,2))

set @Refresh_identifier = convert(varchar(10),today(),123) || ' SVD refresh'
EXECUTE citeam.logger_create_run @run_Identifier, @Refresh_identifier, @SVD_logging_ID output

/****************** A02: TEMPORAL FILTERY STUFF ******************/

DECLARE @aggregate_up_to 		date
DECLARE @already_archived_to 	date
DECLARE @latest_full_date 		date
declare @events_from_date 		integer
declare @events_to_date			integer


-- Figure out where bring logs from the vespa events view in: start from where-ever the
-- historical view stops.
select @already_archived_to = max(doc_creation_date_from_6am) from vespa_analysts.vespa_SVD_log_aggregated_archive

-- No longer looking at reporting relative to which day the report is actioned on, instead
-- running it on a fixed Sunday -> Saturday data set for the week before. Well, whatever
-- the standardised end day of weekly reporting is as defined in our procedure.
execute vespa_analysts.Regulars_Get_report_end_date @latest_full_date output
-- Since aggregate_up_to also kind of controls report outputs (ew!), that gets set as a
-- result of the report end;
set @aggregate_up_to = @latest_full_date - 6

-- we want to use the time dimension fields on the Select to speed up the time response (however still there is work to do to achieve a proper
-- state, as removing data manipulation commands from date fields on this Select and manipulating them after extraction... but it's a start)
-- so the shape that the time dimension fields have is YYYYMMDDHH and btw they are integer, so yeah is a numeric representation of a date and hour
-- hence, lets create the date range parting from @latest_full_date and @already_archived_to...

set @events_from_date 	= convert(integer,dateformat(@already_archived_to,'yyyymmddhh')) 					-- YYYYMMDD00
set @events_to_date 	= convert(integer,dateformat(dateadd(day,1,@latest_full_date),'yyyymmdd')+'23')		-- YYYYMMDD23


EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'A02: Complete! (Temporal filtering)'

COMMIT

/****************** B02: CREATE IMPORTANT TRANSIENT TABLES ******************/

-- Not maintained in this file any more! (It might fix these weird Sybase bugs,
-- it might not, but hopefully it helps...)

/****************** B03: RESET ANY RESIDUAL REPORT TABLES ******************/

-- Okay, all these are reset elsewhere so we just call that procedure...
execute SVD_clear_transients -- proc doesn't exist yet, but we're still building so sure.

commit
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'B02/3: Complete! (Clean old data)'
COMMIT

/****************** C01: POPULATING NEW LOGS TABLE ******************/

-- This largely follows the same scheme as the Vespa Operational Dashboard.
-- 1/ Scan daily tables for people returning logs
-- 2/ Summarise into a simpler lookup
-- 3/ Archive older results so that we don't need to go back through a lot
--      of daily tables all the time.

DECLARE @SQL_daily_kludge       varchar(2000)

-- Querying for all records received from the last day the historical view stoped until the defined end date for the report...
insert into vespa_SVD_log_collection_dump (
        subscriber_id
        ,LOG_START_DATE_TIME_UTC
        ,account_number
        ,doc_creation_date_from_6am
)
select
        subscriber_id
        ,LOG_START_DATE_TIME_UTC
        ,min(account_number)
		-- for a given log day D... from (D - 1) at 11 pm until D at 10:59... logs will be allocated to (D - 1)
        ,case 
				when convert(integer,dateformat(min(LOG_RECEIVED_START_DATE_TIME_UTC),'hh')) < 23
					then cast(min(LOG_RECEIVED_START_DATE_TIME_UTC) as date)-1 
				else
					cast(min(LOG_RECEIVED_START_DATE_TIME_UTC) as date)
				end as doc_creation_date_from_9am
from 	sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
where 	panel_id 							= 1 
and 	LOG_RECEIVED_START_DATE_TIME_UTC	is not null
and 	subscriber_id 						is not null
and 	LOG_START_DATE_TIME_UTC 			is not null
and 	account_number 						is not null
and 	dk_event_start_datehour_dim 		between @events_from_date and @events_to_date
group 	by 		subscriber_id
				,LOG_START_DATE_TIME_UTC
		having 	doc_creation_date_from_9am	is not null
union all
select
        subscriber_id
        ,LOG_START_DATE_TIME_UTC
        ,min(account_number)
		-- for a given log day D... from (D - 1) at 11 pm until D at 10:59... logs will be allocated to (D - 1)
        ,case 
				when convert(integer,dateformat(min(LOG_RECEIVED_START_DATE_TIME_UTC),'hh')) < 23
					then cast(min(LOG_RECEIVED_START_DATE_TIME_UTC) as date)-1 
				else
					cast(min(LOG_RECEIVED_START_DATE_TIME_UTC) as date)
				end as doc_creation_date_from_9am
from 	sk_prod.VESPA_DP_PROG_NON_VIEWED_CURRENT
where 	panel_id 							= 1 
and 	LOG_RECEIVED_START_DATE_TIME_UTC	is not null
and 	subscriber_id 						is not null
and 	LOG_START_DATE_TIME_UTC 			is not null
and 	account_number 						is not null
and 	dk_event_start_datehour_dim 		between @events_from_date and @events_to_date
group 	by 		subscriber_id
				,LOG_START_DATE_TIME_UTC
		having 	doc_creation_date_from_9am	is not null
		
execute citeam.logger_add_event @SVD_logging_ID, 3, 'C01: Daily table scanned...'

-- And now our logs table is populated! with duplicates. Now to build the table
-- we'll actually use.

-- And some control totals so we're confident things are happening:
set @QA_catcher = -1

select @QA_catcher = count(1)
from vespa_SVD_log_collection_dump

commit
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'C01: Midway 1/2 (daily logs sucked)', coalesce(@QA_catcher, -1)
commit

-- Also: because we're using daily table presence as our filter, we're getting some
-- logs turning up from today()-1 that go into the prior day's table, might also be
-- some messy stuff with the from-9-AM-deal, but in any case, we've got a tiny bit
-- of cleansing to do:
delete from vespa_SVD_log_collection_dump
where doc_creation_date_from_6am > @latest_full_date
commit

-- Now figure out the log ID for each item (there will still be duplicates at this point)
update vespa_SVD_log_collection_dump
set log_id = cast((subscriber_id||' '||LOG_START_DATE_TIME_UTC) as varchar(100))
commit

-- Should be redundant given the table was recreated and dropped, but Sybase is
-- being weird about it...
delete from vespa_SVD_new_log_listing
commit

-- Now we can summarise those into one record per log batch (those batches
-- started off spread over different daily event tables)
insert into vespa_SVD_new_log_listing (log_id, subscriber_id, account_number, doc_creation_date_from_6am)
select
        log_id
        ,min(subscriber_id)
        ,min(account_number)
        ,min(doc_creation_date_from_6am)
from vespa_SVD_log_collection_dump
group by log_id

-- All the subsequent fields should be the same of any given log ID, but hey, whatever.

commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_SVD_new_log_listing

COMMIT
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'C01: Complete! (Populate new logs)', coalesce(@QA_catcher, -1)
COMMIT

/****************** E01: ASSEMBLE BOX LISTING ******************/

-- Ok, so this build instead can be taken from the Single Box View, because
-- we kind of rebuilt that specifically to have all the stuff we wanted on
-- it. In fact, a whole lot of this process can probably go straight from
-- that....

delete from vespa_SVD_subscriber_dashboard

commit

insert into vespa_SVD_subscriber_dashboard (
    account_number
    ,subscriber_id
    ,In_stb_log_snapshot
    ,enabled_date
    ,PS_flag
    ,Box_type_physical
    ,HD_box_subs
    ,Account_anytime_plus
    ,Box_has_anytime_plus
    ,PVR
)
select
    account_number
    ,subscriber_id
    ,In_stb_log_snapshot
    ,coalesce(Enablement_date, '2011-05-10') -- by default, the only data source date we've got for the Sky View source
    ,PS_flag
    ,Box_type_physical
    ,HD_box_subs
    ,Account_anytime_plus
    ,Box_has_anytime_plus
    ,PVR
from vespa_analysts.vespa_single_box_view
where Is_Sky_View_Selected = 1
 
commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_SVD_subscriber_dashboard
commit
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'E01: Complete! (Box listing)', coalesce(@QA_catcher, -1)
COMMIT

/****************** E02: BOXES RETURNING DATA THIS CYCLE ******************/

-- First from boxes that returned data this week
select subscriber_id
into #boxes_returning_data_this_cycle
from vespa_SVD_new_log_listing
--where doc_creation_date_from_6am<=@latest_full_date filter moved to the log import instead
group by subscriber_id

commit
create unique index subscriber_index on #boxes_returning_data_this_cycle (subscriber_id)
commit

update vespa_SVD_subscriber_dashboard
set has_returned_data_ever = 1
from vespa_SVD_subscriber_dashboard as a
inner join #boxes_returning_data_this_cycle as b
on a.subscriber_id=b.subscriber_id

commit

-- That's the last time it's used.
drop table #boxes_returning_data_this_cycle

-- Update the listing based on what's in the log snapshot: anything in there has returned logs sometime.
update vespa_SVD_subscriber_dashboard
set has_returned_data_ever = 1
from vespa_SVD_subscriber_dashboard as vsvdsd
inner join sk_prod.VESPA_STB_LOG_SUMMARY  as sls
on vsvdsd.subscriber_id = sls.subscriber_id

commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_SVD_subscriber_dashboard
where has_returned_data_ever = 1
commit

EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'E02: Complete! (Boxes returning)', coalesce(@QA_catcher, -1)
COMMIT

/****************** E03: STITCHING ON BOX DETAILS TO BOX LEVEL SUMMARY ******************/

-- This whole section is irrelevant now we're playing with Single Box View.

/****************** E04: PATCHING IN MORE DETAILS AT BOX LEVEL ******************/

-- We've got the various flags we need from the single box view, and now we have to massage
-- then into the form that the report expects.

commit

update  vespa_SVD_subscriber_dashboard
set box_subscription_group = case
    when PS_Flag = 'P' and Box_type_physical = 'Sky+HD' and HD_box_subs=1   then '01: Primary Box - HD with HD Subscription'
    when PS_Flag = 'P'                                  and HD_box_subs=1   then '01: Primary Box - HD with HD Subscription'
    when PS_Flag = 'P' and Box_type_physical = 'Sky+HD' and HD_box_subs=0   then '02: Primary Box - HD with no HD Subscription'
    when PS_Flag = 'P' and Box_type_physical = 'Sky+'   and HD_box_subs=0   then '03: Primary Box - Sky+'
    when PS_Flag = 'P' and Box_type_physical = 'Basic'  and HD_box_subs=0   then '04: Primary Box - Basic'
    when PS_Flag = 'P'                                                      then '05: Other/Unknown Primary Box Type'

    when PS_Flag = 'S' and Box_type_physical = 'Sky+HD' and HD_box_subs=1   then '06: Secondary Box - HD with HD Subscription'
    when PS_Flag = 'S'                                  and HD_box_subs=1   then '06: Secondary Box - HD with HD Subscription'
    when PS_Flag = 'S' and Box_type_physical = 'Sky+HD' and HD_box_subs=0   then '07: Secondary Box - HD with no HD Subscription'
    when PS_Flag = 'S' and Box_type_physical = 'Sky+'   and HD_box_subs=0   then '08: Secondary Box - Sky+'
    when PS_Flag = 'S' and Box_type_physical = 'Basic'  and HD_box_subs=0   then '09: Secondary Box - Basic'
    when PS_Flag = 'S'                                                      then '10: Other/Unknown Secondary Box Type'

    -- New case for PS_flag being unknown;
    when                   Box_type_physical = 'Sky+HD' and HD_box_subs=1   then '11: Unknown Box - HD with HD Subscription'
    when                                                    HD_box_subs=1   then '11: Unknown Box - HD with HD Subscription'
    when                   Box_type_physical = 'Sky+HD' and HD_box_subs=0   then '12: Unknown Box - HD with no HD Subscription'
    when                   Box_type_physical = 'Sky+'   and HD_box_subs=0   then '13: Unknown Box - Sky+'
    when                   Box_type_physical = 'Basic'  and HD_box_subs=0   then '14: Unknown Box - Basic'
    else                                                                         '15: Unknown Box of Unknown Type Type'
    
end
from vespa_SVD_subscriber_dashboard

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_SVD_subscriber_dashboard
where box_subscription_group is not null

commit
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'E04: Complete! (More box details)', coalesce(@QA_catcher, -1)
COMMIT

/****************** E05: PUSHING BOX DETAILS BACK ONTO LOGS ******************/

-- Because we're reporting proportions of primary & secondary boxes that return logs, so
-- we want to pull those flags back onto the log lookup (rather than joining again as we're
-- building the output table). And now we need the Anytime stuff too:

update vespa_SVD_new_log_listing
set box_P_or_S = PS_flag
    ,Box_has_anytime_plus = svdsd.Box_has_anytime_plus
    ,Account_anytime_plus = svdsd.Account_anytime_plus
from vespa_SVD_new_log_listing as nll
inner join vespa_SVD_subscriber_dashboard as svdsd
on nll.subscriber_id = svdsd.subscriber_id

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_SVD_new_log_listing
where box_P_or_S in ('P', 'S')

commit
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'E05: Complete! (Box details onto logs)', coalesce(@QA_catcher, -1)
COMMIT

/****************** G01: POPULATING SUMMARY AT ACCOUNT LEVEL ******************/

-- The account level also summarises which boxes have which subscriptions and
-- which have returned data etc, so rather than populating it above then updating
-- everything, we just leave the build until after that other data has been
-- stitched in.

insert into vespa_SVD_account_level_summary (
        account_number
        ,enabled_date_min
        ,primary_box_enabled
        ,primary_box_enabled_and_returned_data
        ,non_primary_box_enabled
        ,non_primary_box_enabled_and_returned_data
)
select account_number
        ,min(enabled_date)
        ,max(case when PS_Flag = 'P' then 1 else 0 end)
        ,max(case when PS_Flag = 'P' and has_returned_data_ever=1 then 1 else 0 end)
        ,max(case when PS_Flag = 'S' then 1 else 0 end)
        ,max(case when PS_Flag = 'S' and has_returned_data_ever=1 then 1 else 0 end)
from vespa_SVD_subscriber_dashboard
group by account_number

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_SVD_account_level_summary
commit
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'G01: Complete! (Account summary)', coalesce(@QA_catcher, -1)
COMMIT

/****************** G02: PATCHING IN OTHER DETAILS AT ACCOUNT LEVEL ******************/

-- Pull the premium details out of Olive
select
        vals.account_number,
        case
                when max(cel.prem_sports) = 2 and max(cel.prem_movies) = 2 then 'Top Tier'
                when max(cel.prem_sports) = 2 and max(cel.prem_movies) = 0 then 'DS'
                when max(cel.prem_sports) = 0 and max(cel.prem_movies) = 2 then 'DM'
                when max(cel.prem_sports) = 2 and max(cel.prem_movies) = 1 then 'DS+SM'
                when max(cel.prem_sports) = 1 and max(cel.prem_movies) = 2 then 'DM+SS'
                when max(cel.prem_sports) = 1 and max(cel.prem_movies) = 1 then 'SS+SM'
                when max(cel.prem_sports) = 0 and max(cel.prem_movies) = 0 then 'No premiums'
        end as account_premiums
into            #premiums_lookup
from            vespa_SVD_account_level_summary as vals
inner join      sk_prod.cust_subs_hist as csh
        on      vals.account_number = csh.account_number
inner join      sk_prod.cust_entitlement_lookup as cel
        on      csh.current_short_description = cel.short_description
WHERE           csh.subscription_sub_type ='DTV Primary Viewing'
       AND      csh.subscription_type = 'DTV PACKAGE'
       AND      csh.status_code in ('AC','AB','PC')
       AND      csh.effective_from_dt <= @latest_full_date
       AND      csh.effective_to_dt   >  @latest_full_date
       AND      csh.effective_from_dt != csh.effective_to_dt
group by vals.account_number

commit
-- Currently taking like 4 minutes, it's fine.
create unique index account_number_index on #premiums_lookup (account_number)
commit

-- Now stitch those details back in
update          vespa_SVD_account_level_summary
set             vals.account_premiums = pl.account_premiums
from            vespa_SVD_account_level_summary as vals
inner join      #premiums_lookup as pl
on              vals.account_number = pl.account_number


-- That's it's last use:
drop table #premiums_lookup

commit

-- Now for the box types detail at account level: we're summarising the subscriber ID level we previously used
select      account_number
            ,min(convert(int, substring(box_subscription_group,0,2))) as highest_box_rank
            ,max(case when substring(box_subscription_group,0,2) in ('06','07','08','09','10') then 1 else 0 end) as has_secondary
            -- Kind of a misnomer, highest priority boxes are ranked from 01 to the lowest priority at the larger numbers
into        #highest_box_type
from        vespa_SVD_subscriber_dashboard
group by    account_number

commit

create index account_number_index on #highest_box_type (account_number)
commit
-- Stitch that back in
update          vespa_SVD_account_level_summary
set             vals.highest_related_box =
        case
                when hbt.has_secondary = 1 then convert(varchar(20), highest_box_rank) || '+MR'
                else convert(varchar(20), highest_box_rank)
        end
from            vespa_SVD_account_level_summary as vals
inner join      #highest_box_type as hbt
on              vals.account_number = hbt.account_number

commit
drop table #highest_box_type


commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_SVD_account_level_summary
where account_premiums is not null and highest_related_box is not null
commit
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'G02: Complete! (Account details)', coalesce(@QA_catcher, -1)
COMMIT

/****************** P01: (RE)POPULATION OF PERSISTENT TABLES ******************/

-- In fact, these updates can be run even if the sync to Olive doesn't work out,
-- ie, if the Olive sync is delayed.

-- This is the bit where we take mostly the same results we just built for the
-- result queries (maybe we should rephrase as temporary tables?) and push them
-- back into the aggregated archive tables so that next time we don't have to
-- pull as many things from the events view.
insert into vespa_SVD_log_aggregated_archive (
        doc_creation_date_from_6am
        ,log_count
        ,distinct_accounts
        ,distinct_boxes
        ,reporting_primary_boxes
        ,reporting_secondary_boxes
        ,reporting_primary_anytimes
        ,reporting_secondary_anytimes
)
select
        convert(date, doc_creation_date_from_6am)
        ,count(*) as logs
        ,count(distinct account_number) as distinct_accounts
        ,count(distinct subscriber_id) as distinct_boxes
        ,sum(case when box_P_or_S = 'P' then 1 else 0 end) as reporting_primary_boxes
        ,sum(case when box_P_or_S = 'S' then 1 else 0 end) as reporting_secondary_boxes
        ,sum(case when box_P_or_S = 'P' and Box_has_anytime_plus = 1 then 1 else 0 end) as reporting_primary_anytimes
        ,sum(case when box_P_or_S = 'S' and Box_has_anytime_plus = 1 then 1 else 0 end) as reporting_secondary_anytimes
from vespa_SVD_new_log_listing
where doc_creation_date_from_6am >= @already_archived_to + 1
-- converting to date basically sets the time to 00:00:00 so we start
-- archiving from the begining of the day we haven't archived at all yet
and doc_creation_date_from_6am < @aggregate_up_to
group by doc_creation_date_from_6am

commit

--And now patch in the enablement details:

-- Now we're archiving the enablement stuff to, so we'll need this table:
select enablement_date, PS_flag, count(1) as hits
into #sky_view_daily_enablements
from vespa_analysts.vespa_single_box_view
where Is_Sky_View_Selected = 1
group by enablement_date, PS_flag
-- Okay, so this used to be about panel 4 but I'm guessing it should really be about sky view?
-- Hopefully it doesn't break anything? Why is it pointing there? Some leftover thing...

commit

-- Okay, so, turns out we can't use dates to order cumulative sum window functions? pants.
-- Which is actually kind of important because we want to build the totals for each day
-- and doing cumulative isn't going to fill in the gaps of days that didn't see enablements.
select r.doc_creation_date_from_6am, l.PS_flag,
    sum(l.hits) as cumulative_enablements
into #sky_view_cumulative_enablements
from vespa_SVD_log_aggregated_archive as r
left join #sky_view_daily_enablements as l
on l.enablement_date <= r.doc_creation_date_from_6am
where doc_creation_date_from_6am >= @already_archived_to + 1 -- to only update the recently added
group by r.doc_creation_date_from_6am, l.PS_flag

-- Okay, so this is not stable against no logs arriving in some day. It won't turn up as zero,
-- it just won't be there at all. Could point it to the Sky Calendar instead, but hey.

commit
-- Not the most efficient formulation (not even indexed!) but this isn't going to be a bottleneck.

-- Doing two records into two columns requires two updates:
update vespa_SVD_log_aggregated_archive
set enabled_primary_boxes = cumulative_enablements
from vespa_SVD_log_aggregated_archive as laa
inner join #sky_view_cumulative_enablements as ce
on laa.doc_creation_date_from_6am = ce.doc_creation_date_from_6am
where PS_flag = 'P'

update vespa_SVD_log_aggregated_archive
set enabled_secondary_boxes = cumulative_enablements
from vespa_SVD_log_aggregated_archive as laa
inner join #sky_view_cumulative_enablements as ce
on laa.doc_creation_date_from_6am = ce.doc_creation_date_from_6am
where PS_flag = 'S'

commit

-- Clip archived items out of the recent table so as to not duplicate when
-- reporting about them
delete from vespa_SVD_new_log_listing
where doc_creation_date_from_6am < @aggregate_up_to
-- But! This happens after the cumulative enablements is built, so the later days
-- still exist in the cumulative enablements table. Which we'll then use later in
-- the script (section Q02) to get the enablement totals for the more recent stuff.
-- Don't really like having temporary table use spanning sections, but hey, that's
-- what happens with speed maintenance. Will we ever get a refactoring opportunity?

commit
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'P01: Complete! (Repopulate aggregates)'
COMMIT

/****************** P02: MARK DISABLES AND CHURNED BOXES ******************/

-- No longer tracking our own historical box listings! just using vespa_stb_log_snapshot instead.

/****************** S01: Sky base - population with RTM ******************/

-- Table population: pulling in RTM flag at the same time (based on
-- Opt Out construction)

INSERT INTO vespa_SVD_sky_base_listing (
    account_number
    ,rtm
    ,cust_viewing_data_capture_allowed
--    ,most_recent_DTV_booking - not used in a any report build
    ,DTV_customer
    ,is_new_customer
)
SELECT sav.account_number
      ,rtm.rtm_detail
      ,min(sav.cust_viewing_data_capture_allowed) -- required specifically for opt out report
--      ,max(sav.booking_dt) as most_recent_DTV_booking - not used in any report?
      ,max(case when sav.prod_latest_dtv_status_code in ('AB','AC','PC') then 1 else 0 end)
      ,convert(tinyint, case  -- it's about RTMs collecting data, which is something that happens at the point of booking, hence booking date.
          when dateadd(day, 7, max(sav.booking_dt)) > @latest_full_date then 3 -- activated within the last week
          when max(sav.booking_dt) >= '2011-05-26' then 2 -- Chordant Fix in place from 26th of May
          when max(sav.booking_dt) between '2011-04-28' and '2011-05-25' then 1 -- RTMs collecting opt-out data since 28th of April
          else 0
       end)
FROM CITEAM.RTM_DO_NOT_DELETE AS rtm
inner JOIN
sk_prod.cust_single_account_view AS sav
ON rtm.account_number = sav.account_number
WHERE sav.prod_latest_dtv_status_code IN ('AC','AB','PC')
GROUP BY sav.account_number,rtm_detail
commit
-- Turns out that account_number is not unique on the whole of SAV (why not?),
-- but limiting to actie accounts there are only 6 dupes in here. That's well
-- below the affecting-percentages threshold, so, whatever.

commit
set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_SVD_sky_base_listing
commit

EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'S01: Complete! (Sky base pop)', coalesce(@QA_catcher, -1)
COMMIT

/****************** S02: Account level details over sky base ******************/

-- PART A - now dissapeared into code merged with Opt Out report.

--------------------------------------------------------------------------------
-- PART B Events
--------------------------------------------------------------------------------

/*
PART B   - Populate table
     B01 - Box type
     B02 - Premiums
     B03 - Value_segment
     B04 - Tenure
*/

--------------------------------------------------------------- B01 - Box Type

-- B01 - Box type

--Creates a list of accounts with active HD capable boxes
SELECT  stb.account_number
       ,max(CASE WHEN x_description like '%HD%' THEN 1
                ELSE 0
             END) AS HD
       ,max(CASE WHEN x_description like '%HD%1TB%' THEN 1
                        ELSE 0
             END) AS HD1TB
	    ,max(CASE WHEN x_description like '%HD%2TB%' THEN 1
                        ELSE 0
             END) AS HD2TB
INTO #hda -- drop table #hda
FROM sk_prod.CUST_SET_TOP_BOX AS stb INNER JOIN vespa_SVD_sky_base_listing AS acc
                                             on stb.account_number = acc.account_number
WHERE box_installed_dt <= @latest_full_date
AND box_replaced_dt   > @latest_full_date
AND x_description like '%HD%'
GROUP BY stb.account_number
-- Acutally, the bottleneck could be either the following (now refactored) one, or
-- otherwise this one just passed...

commit
CREATE UNIQUE hg INDEX idx2 ON #hda(account_number)
commit

-- Get Counts of the different Box Types; this form now differs a bit from what's on the wiki.
-- Will the new structure make it faster? Faster than the 14 minutes it nominally takes? We'll
-- see, I guess.
SELECT  csh.account_number
       ,max(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Primary Viewing'    THEN 1 ELSE 0  END) AS TV
       ,max(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
       ,max(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
       ,max(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
       ,max(CASE  WHEN #hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
       ,max(CASE  WHEN #hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
	   ,max(CASE  WHEN #hda.HD2TB = 1                                      THEN 1 ELSE 0  END) AS HD2TBstb
       ,convert(varchar(30), null) as box_type
  INTO #box_type
  FROM sk_prod.cust_subs_hist AS csh
       INNER JOIN vespa_SVD_sky_base_listing AS acc ON csh.account_number = acc.account_number --< Limits to your universe
       LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel
                       ON csh.current_short_description = cel.short_description
       LEFT OUTER JOIN #hda ON csh.account_number = #hda.account_number --< Links to the HD Set Top Boxes
 WHERE csh.effective_FROM_dt <= @latest_full_date
   AND csh.effective_to_dt    > @latest_full_date
   AND csh.status_code IN  ('AC','AB','PC')
   AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
   AND csh.effective_FROM_dt <> csh.effective_to_dt
GROUP BY csh.account_number
HAVING TV = 1
-- ?? Row(s) affected

commit
create unique index maybe_fake_pk on #box_type (account_number)
commit

update #box_type
set box_type =  CASE	WHEN HD =1 AND MR = 1 AND HD2TBstb = 1      THEN 'A) HD Combi 2TB'
                        WHEN HD =1 AND HD2TBstb = 1                 THEN 'B) HD 2TB'
						WHEN HD =1 AND MR = 1 AND HD1TBstb = 1      THEN 'A) HD Combi 1TB'
                        WHEN HD =1 AND HD1TBstb = 1                 THEN 'B) HD 1TB'
                        WHEN HD =1 AND MR = 1 AND HDstb = 1         THEN 'A) HD Combi'
                        WHEN HD =1 AND HDstb = 1                    THEN 'B) HD'
						WHEN SP =1 AND MR = 1 AND HD2TBstb = 1      THEN 'C) HDx Combi 2TB'
                        WHEN SP =1 AND HD2TBstb = 1                 THEN 'D) HDx 2TB'
                        WHEN SP =1 AND MR = 1 AND HD1TBstb = 1      THEN 'C) HDx Combi 1TB'
                        WHEN SP =1 AND HD1TBstb = 1                 THEN 'D) HDx 1TB'
                        WHEN SP =1 AND MR = 1 AND HDstb = 1         THEN 'C) HDx Combi'
                        WHEN SP =1 AND HDstb = 1                    THEN 'D) HDx'
                        WHEN SP =1 AND MR = 1                       THEN 'E) SkyPlus Combi'
                        WHEN SP =1                                  THEN 'F) SkyPlus '
                        WHEN MR =1                                  THEN 'G) Multiroom'
                        ELSE                                        'H) FDB'
        END

commit

-- This should match single_box_view.box_type_subs except the single box view doesn't cover
-- the whole sky base, and we need that here for the indices.

UPDATE vespa_SVD_sky_base_listing
SET     csb.box_type = coalesce(bt.box_type, 'Unknown')
from vespa_SVD_sky_base_listing as csb
left join #box_type as bt on bt.account_number = csb.account_number
-- ?? Row(s) affected

commit
drop table #hda
commit

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_SVD_sky_base_listing
where box_type is not null and box_type <> 'Unknown'
commit

EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'S02: Midway 1/3 (Box type)', coalesce(@QA_catcher, -1)
COMMIT

--------------------------------------------------------------- B02 - Premiums

-- B02 - Premiums

UPDATE vespa_SVD_sky_base_listing
SET   Premiums = CASE   WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN 'top_tier'
                        WHEN cel.prem_sports = 1 AND cel.prem_movies = 2 THEN 'one_sport_two_movies'
                        WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN 'no_sports_two_movies'
                        WHEN cel.prem_sports = 2 AND cel.prem_movies = 1 THEN 'two_sports_one_movie'
                        WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN 'two_sports_no_movies'
                        WHEN cel.prem_sports = 1 AND cel.prem_movies = 1 THEN 'one_sport_one_movie'
                        WHEN cel.prem_sports = 0 AND cel.prem_movies = 0 THEN 'basic' ELSE 'unknown' END
      FROM vespa_SVD_sky_base_listing as csb
           inner join sk_prod.cust_subs_hist AS csh on csh.account_number = csb.account_number
           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel on csh.current_short_description = cel.short_description
     WHERE csh.subscription_sub_type ='DTV Primary Viewing'
       AND csh.subscription_type = 'DTV PACKAGE'
       AND csh.effective_from_dt <= @latest_full_date
       AND csh.effective_to_dt    > @latest_full_date
       AND csh.effective_from_dt <> csh.effective_to_dt

commit

--10097309 Row(s) affected

set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_SVD_sky_base_listing
where Premiums is not null and Premiums <> 'unknown'
commit

EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'S02: Midway 2/3 (Premiums)', coalesce(@QA_catcher, -1)
COMMIT

--------------------------------------------------------------- B02 - Value_segment

-- B02 - Value_segment

UPDATE vespa_SVD_sky_base_listing
   SET value_segment = coalesce(tgt.value_seg, 'Bedding In') -- because anything that isn't in the lookup because they're new will be new
  FROM vespa_SVD_sky_base_listing AS base
       left JOIN sk_prod.VALUE_SEGMENTS_DATA AS tgt ON base.account_number = tgt.account_number

--9936757 Row(s) affected
--------------------------------------------------------------- B02 - Tenure
commit

-- B02 - Tenure

UPDATE vespa_SVD_sky_base_listing
SET     Tenure = case   when datediff(day,acct_first_account_activation_dt,@latest_full_date) <=   91 then 'A) 0-3 Months'
                        when datediff(day,acct_first_account_activation_dt,@latest_full_date) <=  182 then 'B) 4-6 Months'
                        when datediff(day,acct_first_account_activation_dt,@latest_full_date) <=  365 then 'C) 6-12 Months'
                        when datediff(day,acct_first_account_activation_dt,@latest_full_date) <=  730 then 'D) 1-2 Years'
                        when datediff(day,acct_first_account_activation_dt,@latest_full_date) <= 1095 then 'E) 2-3 Years'
                        when datediff(day,acct_first_account_activation_dt,@latest_full_date) <= 1825 then 'F) 3-5 Years'
                        when datediff(day,acct_first_account_activation_dt,@latest_full_date) <= 3650 then 'G) 5-10 Years'
                         else                                                                    'H) 10 Years+ '
                        end
    from vespa_SVD_sky_base_listing as base
         inner join sk_prod.cust_single_account_view as sav on sav.account_number = base.account_number
   where cust_active_dtv = 1
--10085888 Row(s) affected
--------------------------------------------------------------- B02 - SkyView_flag
commit

-- B02 - SkyView_flag

update vespa_SVD_sky_base_listing
set SkyView_flag = 0
--10097309 Row(s) affected
Update vespa_SVD_sky_base_listing
SET     base.SkyView_flag = 1
from vespa_SVD_sky_base_listing as base
        inner join vespa_SVD_subscriber_dashboard as vespa on vespa.account_number = base.account_number
--268884 Row(s) affected


set @QA_catcher = -1
select @QA_catcher = count(1)
from vespa_SVD_sky_base_listing
where Tenure is not null and SkyView_flag is not null

commit
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'S02: Complete! (Full Sky base)', coalesce(@QA_catcher, -1)
COMMIT

/****************** S03: ARCHIVING RTM AT ACCOUNT ACTIVATION ******************/

-- This is all RTM archiving, and we're not doing RTM anymore with the Sky View panel. RTM
-- might be interesting, but that'll be a different secion in a report?

/****************** Q01: OPT OUT TABLES ******************/

-- Opt out no longer relevant for the Sky View Panel (also, no idea how it's tracked for this case)

/****************** Q02: DAILY SUMMARY STUFF ******************/

-- Report Output 5: Daily Summary

-- So the enablement numbers quoted on the front page and the cumulative thing on the
-- enablement tab don't exclude panel 5, which is a bit of a laugh. We're going to cut
-- panel 5 out of the loop and just take confirmed panel 4 activations, which we...
-- actually can't recognise via the enabled date. We need to get the panel 4 listing
-- but... we can join directly to the single box lookup for that. No need to pull those
-- things into the subscriber dashboard again (might even be able to rebuild this report
-- without that guy existing at all...

select
        doc_creation_date_from_6am as document_from_6am
        ,log_count
        ,distinct_accounts
        ,distinct_boxes
        ,reporting_primary_boxes
        ,reporting_secondary_boxes
        ,reporting_primary_anytimes
        ,reporting_secondary_anytimes
        ,enabled_primary_boxes
        ,enabled_secondary_boxes
into vespa_SVD_05_DailySummary_historics
from vespa_SVD_log_aggregated_archive
union all
select 
        convert(date, doc_creation_date_from_6am) as document_from_6am
        ,count(*) as log_count
        ,count(distinct account_number) as distinct_accounts
        ,count(distinct subscriber_id) as distinct_boxes
        ,sum(case when box_P_or_S = 'P' then 1 else 0 end) as reporting_primary_boxes
        ,sum(case when box_P_or_S = 'S' then 1 else 0 end) as reporting_secondary_boxes
        ,sum(case when box_P_or_S = 'P' and Box_has_anytime_plus = 1 then 1 else 0 end) as reporting_primary_anytimes
        ,sum(case when box_P_or_S = 'S' and Box_has_anytime_plus = 1 then 1 else 0 end) as reporting_secondary_anytimes
        ,convert(int, null) as enabled_primary_boxes
        ,convert(int, null) as enabled_secondary_boxes
from vespa_SVD_new_log_listing
group by doc_creation_date_from_6am
-- Not using indices, report tables are small.

-- Okay, suck, we need to get the distincts in there for the reporting boxes etc.
-- Which means creation and multiple updates :/

commit

-- And then update the enablement numbers for each day...
-- This uses a temporary table # that was built way up in section P01. That's
-- not so cool to have temporary tables built so far in the past still being
-- important, but if the session fails we'd just restard the dashboard anyway.
update vespa_SVD_05_DailySummary_historics
set     enabled_primary_boxes       = cumulative_enablements
from vespa_SVD_05_DailySummary_historics as od5dh
inner join #sky_view_cumulative_enablements as ce
on od5dh.document_from_6am = ce.doc_creation_date_from_6am
where PS_flag = 'P'

update vespa_SVD_05_DailySummary_historics
set     enabled_secondary_boxes       = cumulative_enablements
from vespa_SVD_05_DailySummary_historics as od5dh
inner join #sky_view_cumulative_enablements as ce
on od5dh.document_from_6am = ce.doc_creation_date_from_6am
where PS_flag = 'S'
-- OK so this will actually overwrite the older enablement numbers too, but with
-- exacly the same numbers as were used earlier in the report. Probably.

-- Report Output 10: this week's numbers (10 because it was developed last)
select
    case datepart(weekday, log_counts.log_date)
        when 1 then 'Sunday'
        when 2 then 'Monday'
        when 3 then 'Tuesday'
        when 4 then 'Wednesday'
        when 5 then 'Thursday'
        when 6 then 'Friday'
        when 7 then 'Saturday'
        else 'ERROR!'
      end as day_of_week
    ,log_counts.log_date
    ,min(log_counts.reporting_that_day)             as reporting_boxes
    ,sum(enabled_counts.boxes_enabled_today)        as boxes_enabled
    ,convert(double, null)                          as reporting_proportion
    ,min(primary_counts.reporting_primaries)        as reporting_primary_boxes
    ,sum(enabled_counts.enabled_primaries_today)    as primary_boxes_enabled
    ,convert(double, null)                          as primary_reporting_proportion
    ,min(secondary_counts.reporting_secondaries)    as reporting_secondary_boxes
    ,sum(enabled_counts.enabled_secondaries_today)  as secondary_boxes_enabled
    ,convert(double, null)                          as secondary_reporting_proportion
    ,min(primary_anytime_active_counts.reporting_primary_anytime_activated)         as reporting_primary_anytime_active_boxes
    ,sum(enabled_counts.enabled_primary_anytime_activated_today)                    as primary_anytime_active_boxes
    ,convert(double, null)                                                          as primary_anytime_active_reporting_proportion
    ,min(secondary_anytime_active_counts.reporting_secondary_anytime_activated)     as reporting_secondary_anytime_active_boxes
    ,sum(enabled_counts.enabled_secondary_anytime_activated_today)                  as secondary_anytime_active_boxes
    ,convert(double, null)                                                          as secondary_anytime_active_reporting_proportion
    ,min(primary_anytime_eligible_counts.reporting_primary_anytime_eligible)        as reporting_primary_anytime_eligible_boxes
    ,sum(enabled_counts.enabled_primary_anytime_eligible_today)                     as primary_anytime_eligible_boxes
    ,convert(double, null)                                                          as primary_anytime_eligible_reporting_proportion
    ,min(secondary_anytime_eligible_counts.reporting_secondary_anytime_eligible)    as reporting_secondary_anytime_eligible_boxes
    ,sum(enabled_counts.enabled_secondary_anytime_eligible_today)                   as secondary_anytime_eligible_boxes
    ,convert(double, null)                                                          as secondary_anytime_eligible_reporting_proportion
into vespa_SVD_10_DailySummary_thisweek -- 10 because it was developed last
from (
    select
        doc_creation_date_from_6am as log_date, 
        count(distinct subscriber_id) as reporting_that_day
    from vespa_SVD_new_log_listing -- this table at this point only has left in it the 7 days we're interested in
    group by doc_creation_date_from_6am
) as log_counts
inner join ( -- Can't put them all in the same pull from vespa_SVD_new_log_listing because there are subscriber_ID duplicates in there...
    select
        doc_creation_date_from_6am as log_date, 
        count(distinct subscriber_id) as reporting_primaries
    from vespa_SVD_new_log_listing
    where box_P_or_S = 'P'
    group by doc_creation_date_from_6am
) as primary_counts
on log_counts.log_date = primary_counts.log_date
inner join (
    select
        doc_creation_date_from_6am as log_date, 
        count(distinct subscriber_id) as reporting_secondaries
    from vespa_SVD_new_log_listing
    where box_P_or_S = 'S'
    group by doc_creation_date_from_6am
) as secondary_counts
on log_counts.log_date = secondary_counts.log_date
inner join (
    select
        doc_creation_date_from_6am as log_date, 
        count(distinct subscriber_id) as reporting_primary_anytime_activated
    from vespa_SVD_new_log_listing
    where box_P_or_S = 'P' and Account_anytime_plus = 1
    group by doc_creation_date_from_6am
) as primary_anytime_active_counts
on log_counts.log_date = primary_anytime_active_counts.log_date
inner join (
    select
        doc_creation_date_from_6am as log_date, 
        count(distinct subscriber_id) as reporting_secondary_anytime_activated
    from vespa_SVD_new_log_listing
    where box_P_or_S = 'S' and Account_anytime_plus = 1
    group by doc_creation_date_from_6am
) as secondary_anytime_active_counts
on log_counts.log_date = secondary_anytime_active_counts.log_date
inner join (
    select
        doc_creation_date_from_6am as log_date, 
        count(distinct subscriber_id) as reporting_primary_anytime_eligible
    from vespa_SVD_new_log_listing
    where box_P_or_S = 'P' and Box_has_anytime_plus = 1
    group by doc_creation_date_from_6am
) as primary_anytime_eligible_counts
on log_counts.log_date = primary_anytime_eligible_counts.log_date
inner join (
    select
        doc_creation_date_from_6am as log_date, 
        count(distinct subscriber_id) as reporting_secondary_anytime_eligible
    from vespa_SVD_new_log_listing
    where box_P_or_S = 'S' and Box_has_anytime_plus = 1
    group by doc_creation_date_from_6am
) as secondary_anytime_eligible_counts
on log_counts.log_date = secondary_anytime_eligible_counts.log_date
inner join (
    select
        enabled_date,
        count(1) as boxes_enabled_today,
        sum(case when PS_flag = 'P' then 1 else 0 end) as enabled_primaries_today,
        sum(case when PS_flag = 'S' then 1 else 0 end) as enabled_secondaries_today,
        sum(case when PS_flag = 'P' and Account_anytime_plus = 1 then 1 else 0 end) as enabled_primary_anytime_activated_today,
        sum(case when PS_flag = 'S' and Account_anytime_plus = 1 then 1 else 0 end) as enabled_secondary_anytime_activated_today,
        sum(case when PS_flag = 'P' and Box_has_anytime_plus = 1 then 1 else 0 end) as enabled_primary_anytime_eligible_today,
        sum(case when PS_flag = 'S' and Box_has_anytime_plus = 1 then 1 else 0 end) as enabled_secondary_anytime_eligible_today
    from vespa_SVD_subscriber_dashboard
    --where enabled_date > '2011-10-01'
    -- Sky View Panel doesn't need the dirty Panel 4 / Panel 5 hack.
    group by enabled_date
) as enabled_counts
on enabled_counts.enabled_date <= log_counts.log_date -- To do cumulative sum
group by log_counts.log_date
-- OK, so this is kind of cheeky, we want a date filter which limits this to
-- the last week. The way we do that is to hack the bounds on when we archive
-- data to the permanent summary. Then another part of the process clips all
-- of the recently archived data out of the transient table to make another
-- report easier, with the side effect of leaving in the transient table only
-- the stuff that we want to report on here. Without having to establish what
-- "today" is whenever this report was run.

commit

-- Now update the ratios that will be our percentages:
update vespa_SVD_10_DailySummary_thisweek
set reporting_proportion                                = convert(double, reporting_boxes) / boxes_enabled
    ,primary_reporting_proportion                       = convert(double, reporting_primary_boxes) / primary_boxes_enabled
    ,secondary_reporting_proportion                     = convert(double, reporting_secondary_boxes) / secondary_boxes_enabled
    ,primary_anytime_active_reporting_proportion        = convert(double, reporting_primary_anytime_active_boxes) / primary_anytime_active_boxes
    ,secondary_anytime_active_reporting_proportion      = convert(double, reporting_secondary_anytime_active_boxes) / secondary_anytime_active_boxes
    ,primary_anytime_eligible_reporting_proportion      = convert(double, reporting_primary_anytime_eligible_boxes) / primary_anytime_eligible_boxes
    ,secondary_anytime_eligible_reporting_proportion    = convert(double, reporting_secondary_anytime_eligible_boxes) / secondary_anytime_eligible_boxes

COMMIT
create unique index fake_pk on vespa_SVD_05_DailySummary_historics   (document_from_6am)
create unique index fake_pk on vespa_SVD_10_DailySummary_thisweek    (log_date)
COMMIT
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'Q02: Complete! (Daily summaries)'
COMMIT

/****************** Q03: WEEKLY ENABLEMENT RESULTS ******************/

-- Report Output 6: Enablement summary by Box
select
    convert(date, enabled_date)
    ,count(*) as boxes
    ,sum(case when PS_Flag = 'P' then 1 else 0 end) as primary_box_enabled
    ,sum(case when PS_Flag = 'P' and has_returned_data_ever=1 then 1 else 0 end) as primary_box_enabled_and_returned_data
    ,sum(case when PS_Flag = 'S' then 1 else 0 end) as non_primary_box_enabled
    ,sum(case when PS_Flag = 'S' and has_returned_data_ever=1 then 1 else 0 end) as non_primary_box_enabled_and_returned_data
    ,null as leaving_space_for_percentages_1
    ,null as leaving_space_for_percentages_2
    ,null as leaving_space_for_percentages_3
    ,sum(case when box_subscription_group = '01: Primary Box - HD with HD Subscription' then 1 else 0 end) as Primary_Box_HD_with_HD_Subs
    ,sum(case when box_subscription_group = '02: Primary Box - HD with no HD Subscription' then 1 else 0 end) as Primary_Box_HD_without_HD_Subs
    ,sum(case when box_subscription_group = '03: Primary Box - Sky+' then 1 else 0 end) as Primary_Box_Sky_plus
    ,sum(case when box_subscription_group = '04: Primary Box - Basic' then 1 else 0 end) as Primary_Box_Basic
    ,sum(case when box_subscription_group = '05: Other/Unknown Primary Box Type' then 1 else 0 end) as Unknown_Primary_Box_Type
    ,sum(case when box_subscription_group = '06: Secondary Box - HD with HD Subscription' then 1 else 0 end) as Secondary_Box_HD_with_HD_Subs
    ,sum(case when box_subscription_group = '07: Secondary Box - HD with no HD Subscription' then 1 else 0 end) as Secondary_Box_HD_without_HD_Subs
    ,sum(case when box_subscription_group = '08: Secondary Box - Sky+' then 1 else 0 end) as Secondary_Box_Sky_plus
    ,sum(case when box_subscription_group = '09: Secondary Box - Basic' then 1 else 0 end) as Secondary_Box_Basic
    ,sum(case when box_subscription_group = '10: Other/Unknown Secondary Box Type' then 1 else 0 end) as Unknown_Secondary_Box_Type
into vespa_SVD_06_Enablement_bybox
from vespa_SVD_subscriber_dashboard
group by enabled_date

-- Report Output 7: Enablement summary by account
select
    convert(date, enabled_date_min)
    ,count(*) as accounts
    ,sum(case when primary_box_enabled>0 then 1 else 0 end) as total_primary_boxes_enabled
    ,sum(case when primary_box_enabled_and_returned_data>0 then 1 else 0 end) as total_primary_boxes_enabled_and_returning_data
    ,sum(case when  non_primary_box_enabled=0 then primary_box_enabled else 0 end) as primary_box_only
    ,sum(case when  non_primary_box_enabled=0 then primary_box_enabled_and_returned_data else 0 end ) as primary_box_only_box_returned_data
    --,sum(non_primary_box_enabled) as non_primary_box
    --,sum(non_primary_box_enabled_and_returned_data) as non_primary_box_returned_data
    ,sum(case when primary_box_enabled>0 and non_primary_box_enabled>0 then 1 else 0 end) as multiple_boxes_enabled
    ,sum(case when primary_box_enabled>0 and non_primary_box_enabled>0 and primary_box_enabled_and_returned_data>0 and non_primary_box_enabled_and_returned_data>0 then 1 else 0 end) as multiple_boxes_enabled_multiple_returning_data
    ,sum(case when primary_box_enabled>0 and non_primary_box_enabled>0 and primary_box_enabled_and_returned_data=0 and non_primary_box_enabled_and_returned_data=0 then 1 else 0 end) as multiple_boxes_enabled_multiple_returning_data_no_boxes
    ,sum(case when primary_box_enabled=0 and non_primary_box_enabled>0  then 1 else 0 end) as only_non_primary_boxes_enabled
    ,null as leaving_space_for_percentages_1
    ,null as leaving_space_for_percentages_2
    -- Now for the package categories:
    ,sum(case when account_premiums = 'Top Tier'    then 1 else 0 end) as enabled_TT
    ,sum(case when account_premiums = 'DS'          then 1 else 0 end) as enabled_DS
    ,sum(case when account_premiums = 'DM'          then 1 else 0 end) as enabled_DM
    ,sum(case when account_premiums = 'DS+SM'       then 1 else 0 end) as enabled_DS_SM
    ,sum(case when account_premiums = 'DM+SS'       then 1 else 0 end) as enabled_DM_SS
    ,sum(case when account_premiums = 'SS+SM'       then 1 else 0 end) as enabled_SS_SM
    ,sum(case when account_premiums = 'No premiums' or account_premiums is null then 1 else 0 end) as enabled_no_premiums
    -- Now for the box types:
    ,sum(case when highest_related_box = '1+MR'     then 1 else 0 end) as HD_primary_box_w_HD_subs_and_multiroom
    ,sum(case when highest_related_box = '1'        then 1 else 0 end) as HD_primary_box_w_HD_subs
    ,sum(case when highest_related_box = '2+MR'     then 1 else 0 end) as HD_primary_box_w_multiroom_no_HD_subs
    ,sum(case when highest_related_box = '2'        then 1 else 0 end) as HD_primary_box_no_HD_subs
    ,sum(case when highest_related_box = '3+MR'     then 1 else 0 end) as SkyPlus_primary_box_w_multiroom
    ,sum(case when highest_related_box = '3'        then 1 else 0 end) as SkyPlus_primary_box
    ,sum(case when highest_related_box = '4+MR'     then 1 else 0 end) as Basic_primary_box_w_multiroom
    ,sum(case when highest_related_box = '4'        then 1 else 0 end) as Basic_primary_box
    ,sum(case when highest_related_box = '5+MR'     then 1 else 0 end) as Unknown_primary_box_w_multiroom
    ,sum(case when highest_related_box = '5'        then 1 else 0 end) as Unknown_primary_box
    ,sum(case when highest_related_box = '6+MR'     then 1 else 0 end) as No_primary_box_HD_w_HD_subs_secondary_box
    ,sum(case when highest_related_box = '7+MR'     then 1 else 0 end) as No_primary_box_HD_secondary_box_no_HD_subs
    ,sum(case when highest_related_box = '8+MR'     then 1 else 0 end) as No_primary_box_sky_plus_secondary_box
    ,sum(case when highest_related_box = '9+MR'     then 1 else 0 end) as No_primary_box_basic_secondary_box
    ,sum(case when highest_related_box = '10+MR'     then 1 else 0 end) as No_primary_box_unknown_secondary_box
into vespa_SVD_07_Enablement_byaccount
from vespa_SVD_account_level_summary
group by enabled_date_min

COMMIT
create unique index fake_pk on vespa_SVD_06_Enablement_bybox     (enabled_date)
create unique index fake_pk on vespa_SVD_07_Enablement_byaccount (enabled_date_min)
COMMIT
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'Q03: Complete! (Weekly enablement results)'
COMMIT

/****************** Q04: BOX TYPE & PREMIUMS DETAILS ******************/

-- Report Output 8: Box Type & Premiums - Box Type
if object_id('vespa_SVD_08_boxtype') is not null
	drop table vespa_SVD_08_boxtype

commit

create table vespa_SVD_08_boxtype(
box_type                varchar(30) not null
,sky_base               integer not null default 0
,SkyView                integer default 0
,Sky_base_less_SkyView  integer default 0 
,sequencer				integer
)

commit

insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('A) HD Combi 2TB',0,1)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('B) HD 2TB',0,2)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('A) HD Combi 1TB',0,3)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('B) HD 1TB',0,4)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('A) HD Combi',0,5)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('B) HD',0,6)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('C) HDx Combi 2TB',0,7)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('D) HDx 2TB',0,8)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('C) HDx Combi 1TB',0,9)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('D) HDx 1TB',0,10)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('C) HDx Combi',0,11)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('D) HDx',0,12)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('E) SkyPlus Combi',0,13)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('F) SkyPlus ',0,14)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('G) Multiroom',0,15)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('H) FDB',0,16)
insert into vespa_SVD_08_boxtype (box_type,sky_base,sequencer) values('Unknown',0,17)

commit

Select  Box_type
        ,count(distinct base.account_number) as Sky_base
        ,sum( case when SkyView_flag = 1 and PS_Flag = 'P' and has_returned_data_ever>0 then 1 else 0 end) as SkyView
        ,sum( case when SkyView_flag = 0 then 1 else 0 end) as Sky_base_less_SkyView
        ,case   when box_type = 'A) HD Combi 2TB' 	then 1
                when box_type = 'B) HD 2TB' 		then 2
				when box_type = 'A) HD Combi 1TB' 	then 3
                when box_type = 'B) HD 1TB' 		then 4
                when box_type = 'A) HD Combi' 		then 5
                when box_type = 'B) HD' 			then 6
                when box_type = 'C) HDx Combi 2TB' 	then 7
                when box_type = 'D) HDx 2TB' 		then 8
				when box_type = 'C) HDx Combi 1TB' 	then 9
                when box_type = 'D) HDx 1TB' 		then 10
                when box_type = 'C) HDx Combi' 		then 11
                when box_type = 'D) HDx' 			then 12
                when box_type = 'E) SkyPlus Combi' 	then 13
                when box_type = 'F) SkyPlus' 		then 14
                when box_type = 'G) Multiroom' 		then 15
                when box_type = 'H) FDB' 			then 16
				else 17
        end as sequencer
into #vespa_SVD_08_boxtype
from    Vespa_SVD_sky_base_listing as base
        left join vespa_SVD_subscriber_dashboard as vbr on vbr.account_number = base.account_number
group by Box_type

update 	vespa_SVD_08_boxtype
set		sky_base 				= temp.sky_base
		,SkyView				= temp.SkyView
		,Sky_base_less_SkyView	= temp.Sky_base_less_SkyView
from 		#vespa_SVD_08_boxtype as temp
inner join	vespa_SVD_08_boxtype as o8 on temp.sequencer = o8.sequencer


-- Report Output 9: Box Type & Premiums - Premiums
Select  Premiums
        ,count(distinct base.account_number) as Sky_base
        ,sum( case when SkyView_flag = 1 and PS_Flag = 'P' and has_returned_data_ever>0 then 1 else 0 end) as SkyView
        ,sum( case when SkyView_flag = 0 then 1 else 0 end) as Sky_base_less_SkyView
        ,case   when premiums = 'top_tier' then 1
                when premiums = 'one_sport_two_movies' then 2
                when premiums = 'no_sports_two_movies' then 3
                when premiums = 'two_sports_one_movie' then 4
                when premiums = 'two_sports_no_movies' then 5
                when premiums = 'one_sport_one_movie' then 6
                when premiums = 'basic' then 7
                else 8
            end as sequencer
into vespa_SVD_09_premiums
from    Vespa_SVD_sky_base_listing as base
        left join vespa_SVD_subscriber_dashboard as vbr on vbr.account_number = base.account_number
group by premiums

COMMIT
create unique index fake_pk on vespa_SVD_08_boxtype      (sequencer)
create unique index fake_pk on vespa_SVD_09_premiums     (sequencer)
COMMIT
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'Q04: Complete! (Box type & premium details)'
COMMIT

/****************** T02: PERMISSIONS ON REPORT TABLES ******************/
-- These permissions, however, are essential run-to-run.
grant select on vespa_SVD_05_DailySummary_historics  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_SVD_06_Enablement_bybox        to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_SVD_07_Enablement_byaccount    to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_SVD_08_boxtype                 to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_SVD_09_premiums                to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
grant select on vespa_SVD_10_DailySummary_thisweek   to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security

commit
EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'T01: Complete! (Report permissions)'
COMMIT

/****************** X01: AND WE'RE DONE! ******************/

EXECUTE citeam.logger_add_event @SVD_logging_ID, 3, 'SVDash: weekly refresh complete!'
COMMIT

end;
go
grant execute on SkyView_Dashboard_make_report to public;
go
-- Need the central scheduler thing to be able to call the procs. But it gets
-- run within the vespa_analytics account, so it doesn't mean that any random
-- public person can see what's in the resulting tables.

/****************** Y01: CLEAN OUT TRANSIENT TABLES ******************/
-- This guys needs to be in a different file because all the tables end
-- up in vespa_analysts, which regular users won't have permission to
-- drop afterwards.

if object_id('SVD_clear_transients') is not null
   drop procedure SVD_clear_transients;

go

create procedure SVD_clear_transients
as
begin
    delete from vespa_analysts.vespa_SVD_account_level_summary
    delete from vespa_analysts.vespa_SVD_log_collection_dump
    delete from vespa_analysts.vespa_SVD_new_log_listing
    delete from vespa_analysts.Vespa_SVD_sky_base_listing
    delete from vespa_analysts.vespa_SVD_subscriber_dashboard
    if object_id( 'vespa_analysts.vespa_SVD_05_DailySummary_historics') is not null
        drop table vespa_analysts.vespa_SVD_05_DailySummary_historics
    if object_id( 'vespa_analysts.vespa_SVD_06_Enablement_bybox') is not null
        drop table vespa_analysts.vespa_SVD_06_Enablement_bybox
    if object_id( 'vespa_analysts.vespa_SVD_07_Enablement_byaccount') is not null
        drop table vespa_analysts.vespa_SVD_07_Enablement_byaccount
    if object_id( 'vespa_analysts.vespa_SVD_08_boxtype') is not null
        drop table vespa_analysts.vespa_SVD_08_boxtype
    if object_id( 'vespa_analysts.vespa_SVD_09_premiums') is not null
        drop table vespa_analysts.vespa_SVD_09_premiums
    if object_id( 'vespa_analysts.vespa_SVD_10_DailySummary_thisweek') is not null
        drop table vespa_analysts.vespa_SVD_10_DailySummary_thisweek
end;
go
grant execute on SVD_clear_transients to public;
