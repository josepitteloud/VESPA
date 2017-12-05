 /******************************************************************************
**
** Project Vespa: Operational Dashboard Report
**                  - Weekly Refresh Script
**
** There's actually nothing in this script tht you run. It creates a big huge
** procedure which builds all the reports tables. You don't even directly run
** this procedure; it get's kicked off instead by the central report scheduler
** guy (currently in dev as 'VES024_Make_Reports') and you just turn up later,
** review the QA options ('Vespa OpDash 02 - check progress, manual QA.sql')
** then pull the report out via the Vespa Report Suite, which automates all
** the different pulls and organises the formatting etc. You do have to worry
** about making sure the external files are in the right format, and writing
** the box of facts on the summary tab though. Refer to:
**
**      http://rtci/vespa1/The%20Operational%20Dashboard.aspx
**
** OD Spec: There is currently no spec for the operational dashboard beyond
** what's in the SQL. Essentially we're building a high level summary of how
** many boxes have been enabled, how many are sending back data, breaking those
** down by box type and premium channel configurations. This has expanded
** significantly since it was first introduced, absorbing the OO report, and
** now combines data from a collection of sources and does RTM stuff and
** suchlike. An actual brief might appear soon (as in November 2012). For more
** info and run instrucitons, see:
**
**
** Outstanding code dev actions:
**
** 14. Add the "any box active" and "any box returning data" to the reports. It's getting
**      big, but those fields might be useful. Small things.
** 36. Table Vespa_stb_log_snapshot might remove the need to keep around the archive
**      table vespa_OpDash_boxes_returning_archive - though the other one we still need
**      since we have no vilisbility of log placement in time, but the current aggregates
**      do allow us to claculate all the boxes reporting ever without our own cache.
**      Update: no, it doesn't quite meet our needs but we might get an aggregate table
**      soon tht does.
** 46. Churn marking section might be overstating churn because of the population being
**      based on campaign cells, but then also getting marked off the active list because
**      they're not in the subscriber status... oh well.
**
** Look for '##' to help identify specific places where feature requests have been flagged.
**
** Recently implemented changes:
**
** 58. Add the P/S and Anytime+ split to the big historical daily table
** 60. Do we want to standardise a bunch of stuff via the new single box view we've built?
**      Update: Yes. Yes we do. Before the CCNs start rolling in.
** 61. Ahahaha hilarious.... The figure of box enablement quoted on the summary tab isn't
**      the open loop box enablement requests; it's the total enablements including both
**      panel 4 and 5. The cumulative table doesn't reset anywhere, and all the panel 5
**      boxes are still contributing. Might want to look into correcting that at some
**      point, or at least standardising it, because the numbers are not going to add up
**      with what's on the historical daily build thing. Update: Pretty sure we fixed that
**      a while back when we populate the subscriber dashboard with just the Open Loop
**      enablement boxes? Wait, no, we didn't it's not Open Loop Enablement, it's Open
**      Loop Enablement that's *also* somewhere on the subscriber dashboard. Not even
**      enabled there. So it is a weird overlap hack of Panel 4 + residual panel 5. Oh
**      dear. Yeah, borked. But, that will get sorted out by (6) so yeah.
** 62. Convert the builds to Closed Loop Enablement, since the CCN should fix all of those
**      issues now.
** 55. So the current RTM is still broken, because there's a new table which has turned
**      and is providing the details for all new aquisitions or something. Until we fix
**      that, the RTM is going to be brokens. Update: this is in the spec of the RTM table
**      rebuild, we don't need to worry about it here.
** 59. Build (and run) the historical rebuild of the P/S and anytime split and enablement
**      stuff for the old bits of the daily sheet. It'll take an age though. Update: No.**
** 63. Update OpDash for Phase 2 data structures, specifically the daily tables and ting.
**
** Code sections:
**      Part A: A01 -  Initialise Logger
**              A02 -  Temporal Filters
**
**      Part B: B01 -  Persistent tables (now in "Vespa OpDash 00 - persistent table create")
**              B02 -  Create transient tables (now also in "Vespa OpDash 00 - persistent table create" too)
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
**              Q05 -  Historical Optout figures view
**
**      Part R:     -  Adsmartable accounts and boxes with viewing consent
**              R01 -  Adsmartable and non-adsmartable Sky base and vespa accounts volumes
**              R02 -  Adsmartable box type/desc combo, with Sky base and vespa box volumes
**
**      Part U:     -  Adsmartable boxes
**              U01 -  Adsmartable Households
**              U02 -  Adsmartable History
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

if object_id('OpDash_make_report') is not null
   drop procedure OpDash_make_report;

go

create procedure OpDash_make_report -- execute OpDash_make_report
as
begin

     /****************** A01: SETTING UP THE LOGGER ******************/

     DECLARE @Model_logging_ID       bigint
     DECLARE @Refresh_identifier     varchar(40)
     declare @run_Identifier         varchar(20)
     -- For putting control totals in prior to logging:
     DECLARE @QA_catcher             integer

     -- Now automatically detecting if it's a test build and logging appropriately...
     if lower(user) = 'kmandal'
         set @run_Identifier = 'VespaOpDash'
     else
         set @run_Identifier = 'OpDash test ' || upper(right(user,1)) || upper(left(user,2))

     set @Refresh_identifier = convert(varchar(10),today(),123) || ' OD refresh'
     EXECUTE citeam.logger_create_run @run_Identifier, @Refresh_identifier, @Model_logging_ID output

     /****************** A02: TEMPORAL FILTERY STUFF ******************/

     DECLARE @aggregate_up_to   date
     DECLARE @already_archived_to  date
     DECLARE @latest_full_date   date
     declare @events_from_date   integer
     declare @events_to_date   integer

     -- Figure out where bring logs from the vespa events view in: start from where-ever the
     -- historical view stops.
     select @already_archived_to = max(doc_creation_date_from_9am) from vespa_analysts.vespa_OpDash_log_aggregated_archive

     -- No longer looking at reporting relative to which day the report is actioned on, instead
     -- running it on a fixed Sunday -> Saturday data set for the week before. Well, whatever
     -- the standardised end day of weekly reporting is as defined in our procedure.
     execute vespa_analysts.Regulars_Get_report_end_date @latest_full_date output
     -- Even in testing, may as well leave this guy pointing to the same report date
     -- reference builder

     -- Archive limit: nothing with doc_creation_date_from_9am on this day will archived, but
     -- everything before it will (that hasn't been archived already).
     set @aggregate_up_to = @latest_full_date - 6
     -- This needs to be a bit in the past as we never rescan the earlier period once they're
     -- archived. That said, we're looking at document creation date, and we don't expect
     -- these to be backdated much at all. Update again: we've been really cheeky, now we're
     -- managing this so that the time filter for report #10 is exactly what's left in the
     -- transient table after everything before this date is archived out. Oh well. And since
     -- we're reporting on fixed day boundries now, it's now tied to the @latest_full_date.

     -- we want to use the time dimension fields on the Select to speed up the time response (however still there is work to do to achieve a proper
     -- state, as removing data manipulation commands from date fields on this Select and manipulating them after extraction... but it's a start)
     -- so the shape that the time dimension fields have is YYYYMMDDHH and btw they are integer, so yeah is a numeric representation of a date and hour
     -- hence, lets create the date range parting from @latest_full_date and @already_archived_to...

     set @events_from_date  = convert(integer,dateformat(@already_archived_to,'yyyymmddhh'))      -- YYYYMMDD00
     set @events_to_date  = convert(integer,dateformat(dateadd(day,1,@latest_full_date),'yyyymmdd')+'23')  -- YYYYMMDD23

     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'A02: Complete! (Temporal filtering)'

     COMMIT

     /****************** B02: CREATE IMPORTANT TRANSIENT TABLES ******************/

     -- Not maintained in this file any more! (It might fix these weird Sybase bugs,
     -- it might not, but hopefully it helps...)

     /****************** B03: RESET ANY RESIDUAL REPORT TABLES ******************/

     -- Okay, all these are reset elsewhere so we just call that procedure...
     execute OpDash_clear_transients

     commit
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'B02/3: Complete! (Clean old data)'
     COMMIT

     /****************** C01: POPULATING NEW LOGS TABLE ******************/

     -- Okay, so the Vespa events view is now so broken that it's not functional
     -- at all, and we're dropping back to the raw daily tables to do everything.

     -- We've got a couple of options as to how we want to archive stuff:
     -- 1/ No archiving, scan everything, will take ages (although, less time
     --      than scanning the whole events view).
     -- 2/ Archive results from daily tables and don't go back to those tables.
     --      Disadvantage: have to leave quite a lot of lag and rescan quite a
     --      few tables in order to get decent accuracy. Advantage: kind of simple
     --      to code, get quite good results, only skip results in cases where a
     --      log *only* contains heavily timeshifted stuff, since any newer events
     --      will appear in a recent table and still give us the stb_log_cretion_dt
     -- 3/ Archive results from daily tables and update those results with further
     --      rescans. Disadvantage: messy to code. Still need to scan a bunch of
     --      daily tables a long way back, even if the indices make it easier. Ugly
     --      to code, worse to maintain. Advantages: highly complete data treatment.
     --
     -- So we're going to go with (2), since it will catch very nearly everything,
     -- and is reasonably easy to code. Still only need to set the archive update
     -- back maybe two weeks or so, to catch the logs that showed up a bit late.

     -- For Phase 2, we no longer need to loop through daily tables, just refer to
     -- the one big table of all the stuff:

     /*
     angeld:

     --30/10
     We used to have a fixed set of hours based on the assumption that boxes would transmit the data during such window, this was from 00:00 until
     08:59. All LOGS_RECEIVED at these hours of a Day belong to sum( day - 1 )...

     Now this concept has changed due the fact that we found a high number of logs being captured at 23 hours of that day that were been dropped
     because of the fixed hours on the Select...

     The new concept refers to all logs received from 23:00 on day A until 22:59 on next day (A+1) will belong to A... For Example:

     Logs for the 18th will be composed of:
     + logs received between 23:00 - 23:59 of the 18th
     + And logs received between 00:00 - 22:59 of the 19th

     */
       insert into vespa_analysts.vespa_OpDash_log_collection_dump (
              subscriber_id
             ,log_start_date_time_utc
             ,account_number
             ,doc_creation_date_from_9am
     )
       select subscriber_id
             ,log_start_date_time_utc
             ,min(account_number)
             ,case
         when convert(integer,dateformat(min(LOG_RECEIVED_START_DATE_TIME_UTC),'hh')) < 23
          then cast(min(LOG_RECEIVED_START_DATE_TIME_UTC) as date)-1
         else
          cast(min(LOG_RECEIVED_START_DATE_TIME_UTC) as date)
         end as doc_creation_date_from_9am
         from sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
        where panel_id       in (11, 12) -- Broadband daily panel 11 added
          and dk_event_start_datehour_dim between @events_from_date and @events_to_date
          and subscriber_id      is not null
       and account_number     is not null
       and log_start_date_time_utc   is not null
     group by subscriber_id
        ,log_start_date_time_utc
       having doc_creation_date_from_9am is not null


     commit

     -- And now our logs table is populated! with duplicates. Now to build the table
     -- we'll actually use.

     -- And some control totals so we're confident things are happening:
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_OpDash_log_collection_dump

     commit
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'C01: Midway 1/2 (daily logs sucked)', coalesce(@QA_catcher, -1)
     commit

     -- Also: because we're using daily table presence as our filter, we're getting some
     -- logs turning up from today()-1 that go into the prior day's table, might also be
     -- some messy stuff with the from-9-AM-deal, but in any case, we've got a tiny bit
     -- of cleansing to do:
     delete from vespa_analysts.vespa_OpDash_log_collection_dump
     where doc_creation_date_from_9am > @latest_full_date
     commit

     -- Now figure out the log ID for each item (there will still be duplicates at this point)
     update vespa_analysts.vespa_OpDash_log_collection_dump
     set log_id = cast((subscriber_id||' '||log_start_date_time_utc) as varchar(100))
     commit

     -- Should be redundant given the table was recreated and dropped, but Sybase is
     -- being weird about it...
     delete from vespa_analysts.vespa_OpDash_new_log_listing
     commit

     -- Now we can summarise those into one record per log batch (those batches
     -- started off spread over different daily event tables)
     insert into vespa_analysts.vespa_OpDash_new_log_listing (log_id, subscriber_id, account_number, doc_creation_date_from_9am)
     select
             log_id
             ,min(subscriber_id)
             ,min(account_number)
             ,min(doc_creation_date_from_9am)
     from vespa_analysts.vespa_OpDash_log_collection_dump
     group by log_id

     -- All the subsequent fields should be the same of any given log ID, but hey, whatever.

     commit

     set @QA_catcher = -1
     select @QA_catcher = count(1)
     from vespa_analysts.vespa_OpDash_new_log_listing

     COMMIT
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'C01: Complete! (Populate new logs)', coalesce(@QA_catcher, -1)
     COMMIT

     /****************** E01: ASSEMBLE BOX LISTING ******************/

     -- Okay, now we have the single box view and the vocabulary to describe this,
     -- the Operational Dashboard goes off the Closed Loop enablement thusly:

     insert into vespa_analysts.vespa_OpDash_subscriber_dashboard (
         account_number
         ,subscriber_id
         ,src_system_id
         ,enabled_date
         ,x_box_type
         ,account_anytime
         ,box_has_anytime
         ,x_primary_box_subscription
         ,x_secondary_box_subscription
         ,has_hd_subscription
     )
     select
         account_number
         ,subscriber_id
         ,service_instance_id
         ,coalesce(Enablement_date, Selection_date)
         ,Box_type_physical
         ,account_anytime_plus
         ,box_has_anytime_plus
         ,case when PS_Flag = 'P' then 1 else 0 end
         ,case when PS_Flag = 'S' then 1 else 0 end
         ,HD_box_subs
     from vespa_analysts.vespa_single_box_view
     where  (in_vespa_panel = 1 or in_vespa_panel_11 = 1) -- Broadband daily panel 11 added
     and  status_vespa = 'Enabled'

     -- Oh, and for free, we're pulling out a bunch of those profiling details
     -- that we've also centralised onto SBV at the same time...

     -- ## Mention: what's the difference between the src_system_id and the
     -- service_instance_id of the sk_prod.CUST_SERVICE_INSTANCE table? The
     -- Old build took the src_system_id from sk_prod.CUST_SERVICE_INSTANCE
     -- and later joined it to service_instance_id of CUST_SET_TOP_BOX...##

     commit
     -- That's so much easier than what we previously had! Ok, the ugliness is centralised
     -- in Single Box view though...

     set @QA_catcher = -1
     select @QA_catcher = count(1)
     from vespa_analysts.vespa_OpDash_subscriber_dashboard
     commit
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'E01: Complete! (Box listing)', coalesce(@QA_catcher, -1)
     COMMIT

     /****************** E02: BOXES RETURNING DATA THIS CYCLE ******************/

     -- First from boxes that returned data this week
     select subscriber_id
     into #boxes_returning_data_this_cycle
     from vespa_analysts.vespa_OpDash_new_log_listing
     --where doc_creation_date_from_9am<=@latest_full_date filter moved to the log import instead
     group by subscriber_id

     commit
     create unique index subscriber_index on #boxes_returning_data_this_cycle (subscriber_id)
     commit

     update vespa_analysts.vespa_OpDash_subscriber_dashboard
     set has_returned_data_ever = 1
     from vespa_analysts.vespa_OpDash_subscriber_dashboard as a
     inner join #boxes_returning_data_this_cycle as b
     on a.subscriber_id=b.subscriber_id


     commit

     -- That's the last time it's used.
     drop table #boxes_returning_data_this_cycle



     /* -- Commented as we only want to check the returning % for boxes on the week under analysis...

     -- Now add historical "returnment" data from boxes we remember returning data in previous cycles
     update vespa_OpDash_subscriber_dashboard
     set     has_returned_data_ever = 1,
             previously_returned_data = 1
     from vespa_OpDash_subscriber_dashboard as vsd
     inner join vespa_analysts.vespa_OpDash_boxes_returning_archive as b
     on vsd.subscriber_id=b.subscriber_id
     where active_subscriber = 'Y'

     */

     commit
     set @QA_catcher = -1
     select @QA_catcher = count(1)
     from vespa_analysts.vespa_OpDash_subscriber_dashboard
     where has_returned_data_ever = 1
     commit

     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'E02: Complete! (Boxes returning)', coalesce(@QA_catcher, -1)
     COMMIT

     /****************** E03: STITCHING ON BOX DETAILS TO BOX LEVEL SUMMARY ******************/

     -- Don't need to go to SBV to get them, they came in with the population.

     -- think that build works out fine, but we'll need to check eh?

     commit
     set @QA_catcher = -1
     select @QA_catcher = count(distinct src_system_id)
     from vespa_analysts.vespa_OpDash_subscriber_dashboard
     commit
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'E03: Complete! (Box details)', coalesce(@QA_catcher, -1)
     COMMIT

     /****************** E04: PATCHING IN MORE DETAILS AT BOX LEVEL ******************/

     -- So the constituent parts of this guy are also built in SBV. But the profiling date
     -- is different (the recent thursday over there, the date of enablement here). Check
     -- that the resulting discrepancies aren't too big...

     update  vespa_analysts.vespa_OpDash_subscriber_dashboard
     set box_subscription_group=
     case when x_primary_box_subscription=1 and x_box_type = 'Sky+HD'  and has_hd_subscription=1  then '01: Primary Box - HD with HD Subscription'
     when x_primary_box_subscription=1          and has_hd_subscription=1  then '01: Primary Box - HD with HD Subscription'
     when x_primary_box_subscription=1   and x_box_type = 'Sky+HD'  and has_hd_subscription=0  then '02: Primary Box - HD with no HD Subscription'
     when x_primary_box_subscription=1   and x_box_type = 'Sky+'  and has_hd_subscription=0  then '03: Primary Box - Sky+'
     when x_primary_box_subscription=1   and x_box_type = 'Basic'  and has_hd_subscription=0  then '04: Primary Box - Basic'
     when x_primary_box_subscription=1                  then '05: Other/Unknown Primary Box Type'

     when x_primary_box_subscription=0   and x_box_type = 'Sky+HD'  and has_hd_subscription=0  then '07: Secondary Box - HD with no HD Subscription'
     when x_primary_box_subscription=0   and x_box_type = 'Sky+'                              then '08: Secondary Box - Sky+'
     when x_primary_box_subscription=0   and x_box_type = 'Basic'                              then '09: Secondary Box - Basic'
     when x_primary_box_subscription=0   and x_box_type = 'Sky+HD' and has_hd_subscription=1  then '06: Secondary Box - HD with HD Subscription'
     when x_primary_box_subscription=0          and has_hd_subscription=1  then '06: Secondary Box - HD with HD Subscription'
     when x_primary_box_subscription=0                  then '10: Other/Unknown Secondary Box Type' else null end

     from vespa_analysts.vespa_OpDash_subscriber_dashboard

     commit
     set @QA_catcher = -1
     select @QA_catcher = count(1)
     from vespa_analysts.vespa_OpDash_subscriber_dashboard
     where box_subscription_group is not null

     commit
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'E04: Complete! (More box details)', coalesce(@QA_catcher, -1)
     COMMIT

     /****************** E05: PUSHING BOX DETAILS BACK ONTO LOGS ******************/

     -- Because we're reporting proportions of primary & secondary boxes that return logs, so
     -- we want to pull those flags back onto the log lookup (rather than joining again as we're
     -- building the output table). And now we need the Anytime stuff too:

     update vespa_analysts.vespa_OpDash_new_log_listing
     set box_P_or_S = case
             when x_primary_box_subscription = 1 and x_secondary_box_subscription = 0 then 'P'
             when x_primary_box_subscription = 0 and x_secondary_box_subscription = 1 then 'S'
             else 'U' end
         ,box_has_anytime = osd.box_has_anytime
         ,account_anytime = osd.account_anytime
     from vespa_analysts.vespa_OpDash_new_log_listing as nll
     inner join vespa_analysts.vespa_OpDash_subscriber_dashboard as osd
     on nll.subscriber_id = osd.subscriber_id

     commit
     set @QA_catcher = -1
     select @QA_catcher = count(1)
     from vespa_analysts.vespa_OpDash_new_log_listing
     where box_P_or_S in ('P', 'S')

     commit
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'E05: Complete! (Box details onto logs)', coalesce(@QA_catcher, -1)
     COMMIT

     /****************** G01: POPULATING SUMMARY AT ACCOUNT LEVEL ******************/

     -- The account level also summarises which boxes have which subscriptions and
     -- which have returned data etc, so rather than populating it above then updating
     -- everything, we just leave the build until after that other data has been
     -- stitched in.

     insert into vespa_analysts.vespa_OpDash_account_level_summary (
             account_number
             ,enabled_date_min
             ,primary_box_enabled
             ,primary_box_enabled_and_returned_data
             ,non_primary_box_enabled
             ,non_primary_box_enabled_and_returned_data
     )
     select account_number
             ,min(enabled_date)
             ,max(case when x_primary_box_subscription=1 then 1 else 0 end)
             ,max(case when x_primary_box_subscription=1 and has_returned_data_ever=1 then 1 else 0 end)
             ,max(case when x_secondary_box_subscription=1 then 1 else 0 end)
             ,max(case when x_secondary_box_subscription=1 and has_returned_data_ever=1 then 1 else 0 end)
     from vespa_analysts.vespa_OpDash_subscriber_dashboard
     group by account_number

     commit
     set @QA_catcher = -1
     select @QA_catcher = count(1)
     from vespa_analysts.vespa_OpDash_account_level_summary
     commit
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'G01: Complete! (Account summary)', coalesce(@QA_catcher, -1)
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
     from            vespa_analysts.vespa_OpDash_account_level_summary as vals
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
     update          vespa_analysts.vespa_OpDash_account_level_summary
     set             vals.account_premiums = pl.account_premiums
     from            vespa_analysts.vespa_OpDash_account_level_summary as vals
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
     from        vespa_analysts.vespa_OpDash_subscriber_dashboard
     group by    account_number

     commit

     create index account_number_index on #highest_box_type (account_number)
     commit
     -- Stitch that back in
     update          vespa_analysts.vespa_OpDash_account_level_summary
     set             vals.highest_related_box =
             case
                     when hbt.has_secondary = 1 then convert(varchar(20), highest_box_rank) || '+MR'
                     else convert(varchar(20), highest_box_rank)
             end
     from            vespa_analysts.vespa_OpDash_account_level_summary as vals
     inner join      #highest_box_type as hbt
     on              vals.account_number = hbt.account_number

     commit
     drop table #highest_box_type


     commit
     set @QA_catcher = -1
     select @QA_catcher = count(1)
     from vespa_analysts.vespa_OpDash_account_level_summary
     where account_premiums is not null and highest_related_box is not null
     commit
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'G02: Complete! (Account details)', coalesce(@QA_catcher, -1)
     COMMIT

     /****************** P01: (RE)POPULATION OF PERSISTENT TABLES ******************/

     -- In fact, these updates can be run even if the sync to Olive doesn't work out,
     -- ie, if the Olive sync is delayed.

     -- This is the bit where we take mostly the same results we just built for the
     -- result queries (maybe we should rephrase as temporary tables?) and push them
     -- back into the aggregated archive tables so that next time we don't have to
     -- pull as many things from the events view.
     insert into vespa_analysts.vespa_OpDash_log_aggregated_archive (
             doc_creation_date_from_9am
             ,log_count
             ,distinct_accounts
             ,distinct_boxes
             ,reporting_primary_boxes
             ,reporting_secondary_boxes
             ,reporting_primary_anytimes
             ,reporting_secondary_anytimes
     )
     select
             convert(date, doc_creation_date_from_9am)
             ,count(*) as logs
             ,count(distinct account_number) as distinct_accounts
             ,count(distinct subscriber_id) as distinct_boxes
             ,sum(case when box_P_or_S = 'P' then 1 else 0 end) as reporting_primary_boxes
             ,sum(case when box_P_or_S = 'S' then 1 else 0 end) as reporting_secondary_boxes
             ,sum(case when box_P_or_S = 'P' and box_has_anytime = 1 then 1 else 0 end) as reporting_primary_anytimes
             ,sum(case when box_P_or_S = 'S' and box_has_anytime = 1 then 1 else 0 end) as reporting_secondary_anytimes
     from vespa_analysts.vespa_OpDash_new_log_listing
     where doc_creation_date_from_9am >= @already_archived_to + 1
     -- converting to date basically sets the time to 00:00:00 so we start
     -- archiving from the begining of the day we haven't archived at all yet
     and doc_creation_date_from_9am < @aggregate_up_to
     group by doc_creation_date_from_9am

     commit

     --And now patch in the enablement details:

     -- Now we're archiving the enablement stuff to, so we'll need this table:
     select enablement_date, PS_flag, count(1) as hits
     into #daily_enablements
     from vespa_analysts.vespa_single_box_view
     where (panel = 'VESPA' or panel = 'VESPA11') -- Broadband daily panel 11 added
     group by enablement_date, PS_flag

     commit

     -- Okay, so, turns out we can't use dates to order cumulative sum window functions? pants.
     -- Which is actually kind of important because we want to build the totals for each day
     -- and doing cumulative isn't going to fill in the gaps of days that didn't see enablements.
     select r.doc_creation_date_from_9am, l.PS_flag,
         sum(l.hits) as cumulative_enablements
     into #cumulative_enablements
     from vespa_analysts.vespa_OpDash_log_aggregated_archive as r
     left join #daily_enablements as l
     on l.enablement_date <= r.doc_creation_date_from_9am
     where doc_creation_date_from_9am >= @already_archived_to + 1 -- to only update the recently added
     group by r.doc_creation_date_from_9am, l.PS_flag

     -- Okay, so this is not stable against no logs arriving in some day. It won't turn up as zero,
     -- it just won't be there at all. Could point it to the Sky Calendar instead, but hey.

     commit
     -- Not the most efficient formulation (not even indexed!) but this isn't going to be a bottleneck.
     drop table #daily_enablements
     commit

     -- Doing two records into two columns requires two updates:
     update vespa_analysts.vespa_OpDash_log_aggregated_archive
     set enabled_primary_boxes = cumulative_enablements
     from vespa_analysts.vespa_OpDash_log_aggregated_archive as laa
     inner join #cumulative_enablements as ce
     on laa.doc_creation_date_from_9am = ce.doc_creation_date_from_9am
     where PS_flag = 'P'

     update vespa_analysts.vespa_OpDash_log_aggregated_archive
     set enabled_secondary_boxes = cumulative_enablements
     from vespa_analysts.vespa_OpDash_log_aggregated_archive as laa
     inner join #cumulative_enablements as ce
     on laa.doc_creation_date_from_9am = ce.doc_creation_date_from_9am
     where PS_flag = 'S'

     commit

     -- Clip archived items out of the recent table so as to not duplicate when
     -- reporting about them
     delete from vespa_analysts.vespa_OpDash_new_log_listing
     where doc_creation_date_from_9am < @aggregate_up_to
     -- But! This happens after the cumulative enablements is built, so the later days
     -- still exist in the cumulative enablements table. Which we'll then use later in
     -- the script (section Q02) to get the enablement totals for the more recent stuff.
     -- Don't really like having temporary table use spanning sections, but hey, that's
     -- what happens with speed maintenance. Will we ever get a refactoring opportunity?

     commit

     -- Mark those boxes which have returned data for the first time this report cycle
     insert into vespa_analysts.vespa_OpDash_boxes_returning_archive (
             subscriber_id
             ,account_number
     )
     select  subscriber_id
             ,account_number
     from    vespa_analysts.vespa_OpDash_subscriber_dashboard
     where   has_returned_data_ever = 1 and previously_returned_data = 0

     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'P01: Complete! (Repopulate aggregates)'

     COMMIT

     /****************** P02: MARK DISABLES AND CHURNED BOXES ******************/

     -- So our way of managing chureners and opt-outs is to reduce the list after
     -- each run to those who are active on the panel.

     update vespa_analysts.vespa_OpDash_boxes_returning_archive
     set active_subscriber = 'Y'
     from vespa_analysts.vespa_OpDash_boxes_returning_archive
     inner join vespa_analysts.vespa_single_box_view as sbv
     on vespa_analysts.vespa_OpDash_boxes_returning_archive.subscriber_id = sbv.subscriber_id
     and (sbv.in_vespa_panel = 1 or in_vespa_panel_11 = 1) -- Broadband daily panel 11 added

     -- This is the easy way of handling any reinstates rther than wory about marking
     -- them and then carying on all of that administration and case-by-case junk, we
     -- just re-add them later if they start returning data again.
     delete from vespa_analysts.vespa_OpDash_boxes_returning_archive
     where active_subscriber = 'CULL'

     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'P02: Complete! (Disables, Churn)'
     COMMIT
     -- This section might be kind of broken if the snapshot isn't being
     -- updated right; we might end up over-representing churn because every
     -- week the same portion of boxes are added (based on the campaign cells)
     -- and then removed because they're not in the subscriber status...

     /****************** S01: Sky base - population with RTM ******************/
     declare @lastthursday date

     -- Table population: pulling in RTM flag at the same time (based on
     -- Opt Out construction)

     set @lastthursday = @latest_full_date - 2  -- Falling in line with the week definition mentioned by Acquisition team
                -- we want to go from friday to thursday... hence Thursday is the cutoff point...
/* -- cortb commented out (22-04-2014) as v16 had error on temp space estimation.
     INSERT INTO vespa_analysts.vespa_OpDash_sky_base_listing (
          account_number
         ,rtm
     --    ,most_recent_DTV_booking - not used in a any report build
         ,DTV_customer
         ,is_new_customer
         ,cust_viewing_data_capture_allowed
         )
       select account_number
             ,case when RTM is null then 'Unknown'
                                    else RTM  --cortb removed hard-coded RTMs to capture all 09/10/2013
              end as fix_rtm
             ,max(case when cust_status in ('AB','AC','PC') then 1 else 0 end)
             ,convert(tinyint, case  -- it's about RTMs collecting data, which is something that happens at the point of booking, hence booking date.
                                    when dateadd(day, 7, max(cust_active_dt)) > @lastthursday then 3 -- activated within the last week
                                    when max(cust_active_dt) >= '2011-05-26' then 2 -- Chordant Fix in place from 26th of May
                                    when max(cust_active_dt) between '2011-04-28' and '2011-05-25' then 1 -- RTMs collecting opt-out data since 28th of April
                                    else 0 end  ) as flag
             ,min(cust_viewing_data_capture_allowed) as cust_viewing_data_capture_allowed
         from (
               SELECT base.account_number
                     ,RANK() OVER (PARTITION BY ord.account_number
                                   ORDER BY ord.cb_row_id ASC
                                  ) AS rank
                     ,case WHEN ord.currency_code = 'EUR' AND ord.route_to_market LIKE '%Direct%'                                    THEN 'ROI Direct'
                           WHEN ord.currency_code = 'EUR'                                                                            THEN 'ROI Retail'
                           WHEN ( ord.retailer_ASA_GROUP_NUMBER ) IN ('11164','11167') AND ord.retailer_ASA_BRANCH_NUMBER LIKE '8%'  THEN 'Tesco'
                           WHEN ( ord.retailer_ASA_GROUP_NUMBER ) IN ('43000')                                                       THEN 'Events'
                           WHEN ord.retailer_asa_group_number IN ('42000','48000')                                                   THEN 'Walkers North'
                           WHEN ord.route_to_market LIKE '%Walkers%' OR ord.retailer_asa_group_number IN ('45000')                   THEN 'Walkers Cobra'
                           WHEN ord.ROUTE_TO_MARKET = 'Direct'                                                                       THEN 'Direct Telephone'
                           WHEN ord.route_to_market IN ('Direct Internet','Online')                                                  THEN 'Direct Internet'
                           ELSE ord.route_to_market END AS RTM
                     ,base.prod_latest_dtv_status_code        as cust_status
                     ,base.PROD_DTV_ACTIVATION_DT             as cust_active_dt
                     ,cust_viewing_data_capture_allowed
                 FROM sk_prod.cust_single_account_view    AS base
                      LEFT JOIN sk_prod.CUST_ORDER_DETAIL AS ord ON base.account_number = ord.account_number
                where base.CUST_ACTIVE_DTV = 1    -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
                  and base.pty_country_code = 'GBR'   -- To Exclude ROI which we don't use...
                  and base.PROD_DTV_ACTIVATION_DT <= @lastthursday
              ) as base
        where rank = 1
     group by account_number
             ,fix_rtm	*/
			 
			 --  cortb split up the the query above (22-04-2014) as v16 had error on temp space estimation and didn't like the rank function.
       SELECT base.account_number
              ,case WHEN ord.currency_code = 'EUR' AND ord.route_to_market LIKE '%Direct%' THEN 'ROI Direct'
                    WHEN ord.currency_code = 'EUR' THEN 'ROI Retail'
                    WHEN ( ord.retailer_ASA_GROUP_NUMBER ) IN ('11164','11167') AND ord.retailer_ASA_BRANCH_NUMBER LIKE '8%' THEN 'Tesco'
                    WHEN ( ord.retailer_ASA_GROUP_NUMBER ) IN ('43000') THEN 'Events'
                    WHEN ord.retailer_asa_group_number IN ('42000','48000') THEN 'Walkers North'
                    WHEN ord.route_to_market LIKE '%Walkers%' OR ord.retailer_asa_group_number IN ('45000') THEN 'Walkers Cobra'
                    WHEN ord.ROUTE_TO_MARKET = 'Direct' THEN 'Direct Telephone'
                    WHEN ord.route_to_market IN ('Direct Internet','Online') THEN 'Direct Internet'
                    ELSE ord.route_to_market END AS RTM
              ,base.prod_latest_dtv_status_code as cust_status
              ,base.PROD_DTV_ACTIVATION_DT as cust_active_dt
              ,cust_viewing_data_capture_allowed
              ,ord.account_number as ord_account_number
              ,ord.cb_row_id as ord_cb_row_id
              into #temp_base
          FROM sk_prod.cust_single_account_view AS base
               LEFT JOIN sk_prod.CUST_ORDER_DETAIL AS ord ON base.account_number = ord.account_number
         where base.CUST_ACTIVE_DTV = 1 -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
           and base.pty_country_code = 'GBR' -- To Exclude ROI which we don't use...
           and base.PROD_DTV_ACTIVATION_DT <= @lastthursday

        SELECT RANK() OVER (PARTITION BY ord_account_number
                                ORDER BY ord_cb_row_id ASC
                            ) AS rank
               ,*
          into #base
          FROM #temp_base

	 INSERT INTO vespa_analysts.vespa_OpDash_sky_base_listing (
          account_number
         ,rtm
     --    ,most_recent_DTV_booking - not used in a any report build
         ,DTV_customer
         ,is_new_customer
         ,cust_viewing_data_capture_allowed
         )
       select account_number
             ,case when RTM is null then 'Unknown'
                                    else RTM  --cortb removed hard-coded RTMs to capture all 09/10/2013
              end as fix_rtm
             ,max(case when cust_status in ('AB','AC','PC') then 1 else 0 end)
             ,convert(tinyint, case  -- it's about RTMs collecting data, which is something that happens at the point of booking, hence booking date.
                                    when dateadd(day, 7, max(cust_active_dt)) > @lastthursday then 3 -- activated within the last week
                                    when max(cust_active_dt) >= '2011-05-26' then 2 -- Chordant Fix in place from 26th of May
                                    when max(cust_active_dt) between '2011-04-28' and '2011-05-25' then 1 -- RTMs collecting opt-out data since 28th of April
                                    else 0 end  ) as flag
             ,min(cust_viewing_data_capture_allowed) as cust_viewing_data_capture_allowed
         from #base
        where rank = 1
     group by account_number
             ,fix_rtm			 

       update vespa_analysts.vespa_OpDash_sky_base_listing as bas
          set opt_in_this_week = 0 --default 1
         from sk_prod.cust_single_account_view as sav
        where bas.account_number = sav.account_number
          and sav.CUST_VIEWING_DATA_CAPTURE_ALLOWED <> 'Y'

       update vespa_analysts.vespa_OpDash_sky_base_listing as bas
          set opt_in_this_week = 0
             ,cust_viewing_data_capture_allowed = 'N'
         from vespa_analysts.ConsentIssue_05_Revised_Consent_Info as exc
        where bas.account_number = exc.account_number
	/*
       update vespa_analysts.vespa_OpDash_sky_base_listing as bas
          set opt_in_this_week = 0
             ,cust_viewing_data_capture_allowed = 'N'
         from sk_prod.SAM_REGISTRANT as sam
        where bas.account_number = sam.account_number
          and sam.TSA_OPT_IN = 'N'
	*/
     commit
     -- Turns out that account_number is not unique on the whole of SAV (why not?),
     -- but limiting to actie accounts there are only 6 dupes in here. That's well
     -- below the affecting-percentages threshold, so, whatever.

     commit
     set @QA_catcher = -1
     select @QA_catcher = count(1)
     from vespa_analysts.vespa_OpDash_sky_base_listing
     commit

     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'S01: Complete! (Sky base pop)', coalesce(@QA_catcher, -1)
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

     -- angeld:  I think this process of recreating the Boxes type is actually not necessary (70% sure on this)...
     --   We have the same information sitting down at vespa_single_box_view which by the time we construct this report
     --   SBV is already built... unless, well... can't think on any unless... this seems to be an enhancement spot...

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
     INTO #hda
     FROM sk_prod.CUST_SET_TOP_BOX AS stb INNER JOIN vespa_analysts.vespa_OpDash_sky_base_listing AS acc
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
            INNER JOIN vespa_analysts.vespa_OpDash_sky_base_listing AS acc ON csh.account_number = acc.account_number --< Limits to your universe
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
     set box_type =  CASE    WHEN HD =1 AND MR = 1 AND HD2TBstb = 1      THEN 'A) HD Combi 2TB'
                             WHEN HD =1 AND HD2TBstb = 1                 THEN 'B) HD 2TB'
        WHEN HD =1 AND MR = 1 AND HD1TBstb = 1      THEN 'C) HD Combi 1TB'
                             WHEN HD =1 AND HD1TBstb = 1                 THEN 'D) HD 1TB'
                             WHEN HD =1 AND MR = 1 AND HDstb = 1         THEN 'E) HD Combi'
                             WHEN HD =1 AND HDstb = 1                    THEN 'F) HD'
                             WHEN SP =1 AND MR = 1 AND HD2TBstb = 1      THEN 'G) HDx Combi 2TB'
                             WHEN SP =1 AND HD2TBstb = 1                 THEN 'H) HDx 2TB'
        WHEN SP =1 AND MR = 1 AND HD1TBstb = 1      THEN 'I) HDx Combi 1TB'
                             WHEN SP =1 AND HD1TBstb = 1                 THEN 'J) HDx 1TB'
                             WHEN SP =1 AND MR = 1 AND HDstb = 1         THEN 'K) HDx Combi'
                             WHEN SP =1 AND HDstb = 1                    THEN 'L) HDx'
                             WHEN SP =1 AND MR = 1                       THEN 'M) SkyPlus Combi'
                             WHEN SP =1                                  THEN 'N) SkyPlus '
                             WHEN MR =1                                  THEN 'O) Multiroom'
                             ELSE                                        'P) FDB'
             END

     commit

     UPDATE vespa_analysts.vespa_OpDash_sky_base_listing
     SET     csb.box_type = coalesce(bt.box_type, 'Unknown')
     from vespa_analysts.vespa_OpDash_sky_base_listing as csb
     left join #box_type as bt on bt.account_number = csb.account_number
     -- ?? Row(s) affected

     commit
     drop table #hda
     commit

     set @QA_catcher = -1
     select @QA_catcher = count(1)
     from vespa_analysts.vespa_OpDash_sky_base_listing
     where box_type is not null and box_type <> 'Unknown'
     commit

     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'S02: Midway 1/3 (Box type)', coalesce(@QA_catcher, -1)
     COMMIT

     --------------------------------------------------------------- B02 - Premiums

     -- B02 - Premiums

     UPDATE vespa_analysts.vespa_OpDash_sky_base_listing
     SET   Premiums = CASE   WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN 'top_tier'
                             WHEN cel.prem_sports = 1 AND cel.prem_movies = 2 THEN 'one_sport_two_movies'
                             WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN 'no_sports_two_movies'
                             WHEN cel.prem_sports = 2 AND cel.prem_movies = 1 THEN 'two_sports_one_movie'
                             WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN 'two_sports_no_movies'
                             WHEN cel.prem_sports = 1 AND cel.prem_movies = 1 THEN 'one_sport_one_movie'
                             WHEN cel.prem_sports = 0 AND cel.prem_movies = 0 THEN 'basic' ELSE 'unknown' END
           FROM vespa_analysts.vespa_OpDash_sky_base_listing as csb
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
     from vespa_analysts.vespa_OpDash_sky_base_listing
     where Premiums is not null and Premiums <> 'unknown'
     commit

     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'S02: Midway 2/3 (Premiums)', coalesce(@QA_catcher, -1)
     COMMIT

     --------------------------------------------------------------- B02 - Value_segment

     -- B02 - Value_segment

     UPDATE vespa_analysts.vespa_OpDash_sky_base_listing
        SET value_segment = coalesce(tgt.value_seg, 'Bedding In') -- because anything that isn't in the lookup because they're new will be new
       FROM vespa_analysts.vespa_OpDash_sky_base_listing AS base
            left JOIN sk_prod.VALUE_SEGMENTS_DATA AS tgt ON base.account_number = tgt.account_number

     --9936757 Row(s) affected
     --------------------------------------------------------------- B02 - Tenure
     commit

     -- B02 - Tenure

     UPDATE vespa_analysts.vespa_OpDash_sky_base_listing
     SET     Tenure = case   when datediff(day,acct_first_account_activation_dt,@latest_full_date) <=   91 then 'A) 0-3 Months'
                             when datediff(day,acct_first_account_activation_dt,@latest_full_date) <=  182 then 'B) 4-6 Months'
                             when datediff(day,acct_first_account_activation_dt,@latest_full_date) <=  365 then 'C) 6-12 Months'
                             when datediff(day,acct_first_account_activation_dt,@latest_full_date) <=  730 then 'D) 1-2 Years'
                             when datediff(day,acct_first_account_activation_dt,@latest_full_date) <= 1095 then 'E) 2-3 Years'
                             when datediff(day,acct_first_account_activation_dt,@latest_full_date) <= 1825 then 'F) 3-5 Years'
                             when datediff(day,acct_first_account_activation_dt,@latest_full_date) <= 3650 then 'G) 5-10 Years'
                              else                                                                    'H) 10 Years+ '
                             end
         from vespa_analysts.vespa_OpDash_sky_base_listing as base
              inner join sk_prod.cust_single_account_view as sav on sav.account_number = base.account_number
        where cust_active_dtv = 1
     --10085888 Row(s) affected
     --------------------------------------------------------------- B02 - Vespa_flag
     commit

     -- B02 - Vespa_flag

     update vespa_analysts.vespa_OpDash_sky_base_listing
     set vespa_flag = 0
     --10097309 Row(s) affected

     Update vespa_analysts.vespa_OpDash_sky_base_listing
     SET     base.Vespa_flag = 1
     from vespa_analysts.vespa_OpDash_sky_base_listing as base
             inner join vespa_analysts.vespa_OpDash_subscriber_dashboard as vespa on vespa.account_number = base.account_number
     --268884 Row(s) affected


     set @QA_catcher = -1
     select @QA_catcher = count(1)
     from vespa_analysts.vespa_OpDash_sky_base_listing
     where Tenure is not null and Vespa_flag is not null

     commit
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'S02: Complete! (Full Sky base)', coalesce(@QA_catcher, -1)
     COMMIT

     /****************** S03: ARCHIVING RTM AT ACCOUNT ACTIVATION ******************/

     -- So we're reporting RTMs for customers activating within the last week, added
     -- together with all similar last-week RTMs for all previous reports. So we put
     -- all of them into one table and then just summarise out of this for the report:

     -- Verifying for reprocessing...
     if exists (
         select first *
         from vespa_analysts.vespa_OpDash_new_joiners_RTMs
         where date_archived = @lastthursday
        )
      begin

       delete from vespa_analysts.vespa_OpDash_new_joiners_RTMs
       where date_archived = @lastthursday

       commit

      end

     -- Archiving Figures of the run...
     insert into vespa_analysts.vespa_OpDash_new_joiners_RTMs (
         rtm
         ,viewing_allowed
         ,viewing_not_allowed
         ,viewing_capture_is_question
         ,viewing_capture_is_NULL
         ,total_records
      ,date_archived
     )
     select
         rtm
         ,sum(case when cust_viewing_data_capture_allowed = 'Y' then 1 else 0 end) as viewing_allowed
         ,sum(case when cust_viewing_data_capture_allowed = 'N' then 1 else 0 end) as viewing_not_allowed
         ,sum(case when cust_viewing_data_capture_allowed ='?' then 1 else 0 end) as viewing_capture_is_question
         ,sum(case when cust_viewing_data_capture_allowed is null then 1 else 0 end) as viewing_capture_is_NULL
         ,count(*) as total_records
      ,@lastthursday
     from vespa_analysts.vespa_OpDash_sky_base_listing
     --where rtm not like '%ROI%' -- cortb commented out to capture customers who originated in ROI 09/10/2013
     where DTV_customer = 1
     and is_new_customer = 3
     group by rtm

     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'S03: Complete! (RTM activation)'
     COMMIT

     /****************** Q01: OPT OUT TABLES ******************/

     -- Report Output 1: Opt out - RTM All Accounts
     select rtm
           ,sum(case when cust_viewing_data_capture_allowed = 'Y' then 1 else 0 end) as viewing_allowed
           ,sum(case when cust_viewing_data_capture_allowed = 'N' then 1 else 0 end) as viewing_not_allowed
           ,sum(case when cust_viewing_data_capture_allowed = '?' then 1 else 0 end) as viewing_capture_is_question
           ,sum(case when cust_viewing_data_capture_allowed is null then 1 else 0 end) as viewing_capture_is_NULL
           ,count(*) as total_records
     into vespa_analysts.vespa_OpDash_01_OptOut_AllAccounts
     from vespa_analysts.vespa_OpDash_sky_base_listing
     --where rtm not like '%ROI%' -- cortb commented out to capture customers who originated in ROI 09/10/2013
     where DTV_customer = 1
     group by rtm
     -- Report Output 2: Opt out - RTM Accounts after 26th May
     select rtm
         ,sum(case when cust_viewing_data_capture_allowed = 'Y' then 1 else 0 end) as viewing_allowed
         ,sum(case when cust_viewing_data_capture_allowed = 'N' then 1 else 0 end) as viewing_not_allowed
         ,sum(case when cust_viewing_data_capture_allowed ='?' then 1 else 0 end) as viewing_capture_is_question
         ,sum(case when cust_viewing_data_capture_allowed is null then 1 else 0 end) as viewing_capture_is_NULL
         ,count(*) as total_records
     into vespa_analysts.vespa_OpDash_02_OptOut_AfterMay
     from vespa_analysts.vespa_OpDash_sky_base_listing
     --where rtm not like '%ROI%' -- cortb commented out to capture customers who originated in ROI 09/10/2013
     Where DTV_customer = 1
     and is_new_customer in (2, 3)
     group by rtm

     -- Report Output 3: Opt Out from April 28th (when RTMs started collecting opt out data)
     select rtm
         ,sum(case when cust_viewing_data_capture_allowed = 'Y' then 1 else 0 end) as viewing_allowed
         ,sum(case when cust_viewing_data_capture_allowed = 'N' then 1 else 0 end) as viewing_not_allowed
         ,sum(case when cust_viewing_data_capture_allowed ='?' then 1 else 0 end) as viewing_capture_is_question
         ,sum(case when cust_viewing_data_capture_allowed is null then 1 else 0 end) as viewing_capture_is_NULL
         ,count(*) as total_records
     into vespa_analysts.vespa_OpDash_03_OptOut_AfterApril
     from vespa_analysts.vespa_OpDash_sky_base_listing
     --where rtm not like '%ROI%' -- cortb commented out to capture customers who originated in ROI 09/10/2013
     where DTV_customer = 1
     and is_new_customer > 0
     group by rtm

     -- Report Output 4: Out out - RTM accounts from a week of activation
       select rtm
             ,sum(viewing_allowed)               as viewing_allowed
             ,sum(viewing_not_allowed)           as viewing_not_allowed
             ,sum(viewing_capture_is_question)   as viewing_capture_is_question
             ,sum(viewing_capture_is_NULL)       as viewing_capture_is_NULL
             ,sum(total_records)                 as total_records
         into vespa_analysts.vespa_OpDash_04_OptOut_Activation
         from vespa_analysts.vespa_OpDash_new_joiners_RTMs
        where date_archived = (select max(date_archived) from vespa_analysts.vespa_OpDash_new_joiners_RTMs)
     group by rtm

     COMMIT
     create unique index fake_pk on vespa_analysts.vespa_OpDash_01_OptOut_AllAccounts   (rtm)
     create unique index fake_pk on vespa_analysts.vespa_OpDash_02_OptOut_AfterMay      (rtm)
     create unique index fake_pk on vespa_analysts.vespa_OpDash_03_OptOut_AfterApril    (rtm)
     create unique index fake_pk on vespa_analysts.vespa_OpDash_04_OptOut_Activation    (rtm)
     COMMIT
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'Q01: Complete! (Opt Out reports)'
     COMMIT

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
             doc_creation_date_from_9am as document_from_9AM
             ,log_count
             ,distinct_accounts
             ,distinct_boxes
             ,reporting_primary_boxes
             ,reporting_secondary_boxes
             ,reporting_primary_anytimes
             ,reporting_secondary_anytimes
             ,enabled_primary_boxes
             ,enabled_secondary_boxes
     into vespa_analysts.vespa_OpDash_05_DailySummary_historics
     from vespa_analysts.vespa_OpDash_log_aggregated_archive
     union all
     select
             convert(date, doc_creation_date_from_9am) as document_from_9AM
             ,count(*) as log_count
             ,count(distinct account_number) as distinct_accounts
             ,count(distinct subscriber_id) as distinct_boxes
             ,sum(case when box_P_or_S = 'P' then 1 else 0 end) as reporting_primary_boxes
             ,sum(case when box_P_or_S = 'S' then 1 else 0 end) as reporting_secondary_boxes
             ,sum(case when box_P_or_S = 'P' and box_has_anytime = 1 then 1 else 0 end) as reporting_primary_anytimes
             ,sum(case when box_P_or_S = 'S' and box_has_anytime = 1 then 1 else 0 end) as reporting_secondary_anytimes
             ,convert(int, null) as enabled_primary_boxes
             ,convert(int, null) as enabled_secondary_boxes
     from vespa_analysts.vespa_OpDash_new_log_listing
     group by doc_creation_date_from_9am
     -- Not using indices, report tables are small.

     -- Okay, suck, we need to get the distincts in there for the reporting boxes etc.
     -- Which means creation and multiple updates :/

     commit

     -- And then update the enablement numbers for each day...
     -- This uses a temporary table # that was built way up in section P01. That's
     -- not so cool to have temporary tables built so far in the past still being
     -- important, but if the session fails we'd just restard the dashboard anyway.
     update vespa_analysts.vespa_OpDash_05_DailySummary_historics
     set     enabled_primary_boxes       = cumulative_enablements
     from vespa_analysts.vespa_OpDash_05_DailySummary_historics as od5dh
     inner join #cumulative_enablements as ce
     on od5dh.document_from_9AM = ce.doc_creation_date_from_9am
     where PS_flag = 'P'

     update vespa_analysts.vespa_OpDash_05_DailySummary_historics
     set     enabled_secondary_boxes       = cumulative_enablements
     from vespa_analysts.vespa_OpDash_05_DailySummary_historics as od5dh
     inner join #cumulative_enablements as ce
     on od5dh.document_from_9AM = ce.doc_creation_date_from_9am
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
     into vespa_analysts.vespa_OpDash_10_DailySummary_thisweek -- 10 because it was developed last initially
     from (
         select
             doc_creation_date_from_9am as log_date,
             count(distinct subscriber_id) as reporting_that_day
         from vespa_analysts.vespa_OpDash_new_log_listing -- this table at this point only has left in it the 7 days we're interested in
         group by doc_creation_date_from_9am
     ) as log_counts
     inner join ( -- Can't put them all in the same pull from vespa_OpDash_new_log_listing because there are subscriber_ID duplicates in there...
         select
             doc_creation_date_from_9am as log_date,
             count(distinct subscriber_id) as reporting_primaries
         from vespa_analysts.vespa_OpDash_new_log_listing
         where box_P_or_S = 'P'
         group by doc_creation_date_from_9am
     ) as primary_counts
     on log_counts.log_date = primary_counts.log_date
     inner join (
         select
             doc_creation_date_from_9am as log_date,
             count(distinct subscriber_id) as reporting_secondaries
         from vespa_analysts.vespa_OpDash_new_log_listing
         where box_P_or_S = 'S'
         group by doc_creation_date_from_9am
     ) as secondary_counts
     on log_counts.log_date = secondary_counts.log_date
     inner join (
         select
             doc_creation_date_from_9am as log_date,
             count(distinct subscriber_id) as reporting_primary_anytime_activated
         from vespa_analysts.vespa_OpDash_new_log_listing
         where box_P_or_S = 'P' and account_anytime = 1
         group by doc_creation_date_from_9am
     ) as primary_anytime_active_counts
     on log_counts.log_date = primary_anytime_active_counts.log_date
     inner join (
         select
             doc_creation_date_from_9am as log_date,
             count(distinct subscriber_id) as reporting_secondary_anytime_activated
         from vespa_analysts.vespa_OpDash_new_log_listing
         where box_P_or_S = 'S' and account_anytime = 1
         group by doc_creation_date_from_9am
     ) as secondary_anytime_active_counts
     on log_counts.log_date = secondary_anytime_active_counts.log_date
     inner join (
         select
             doc_creation_date_from_9am as log_date,
             count(distinct subscriber_id) as reporting_primary_anytime_eligible
         from vespa_analysts.vespa_OpDash_new_log_listing
         where box_P_or_S = 'P' and box_has_anytime = 1
         group by doc_creation_date_from_9am
     ) as primary_anytime_eligible_counts
     on log_counts.log_date = primary_anytime_eligible_counts.log_date
     inner join (
         select
             doc_creation_date_from_9am as log_date,
             count(distinct subscriber_id) as reporting_secondary_anytime_eligible
         from vespa_analysts.vespa_OpDash_new_log_listing
         where box_P_or_S = 'S' and box_has_anytime = 1
         group by doc_creation_date_from_9am
     ) as secondary_anytime_eligible_counts
     on log_counts.log_date = secondary_anytime_eligible_counts.log_date
     inner join (
         select
             enabled_date,
             count(1) as boxes_enabled_today,
             sum(case when x_primary_box_subscription = 1 then 1 else 0 end) as enabled_primaries_today,
             sum(case when x_secondary_box_subscription = 1 then 1 else 0 end) as enabled_secondaries_today,
             sum(case when x_primary_box_subscription = 1 and account_anytime = 1 then 1 else 0 end) as enabled_primary_anytime_activated_today,
             sum(case when x_secondary_box_subscription = 1 and account_anytime = 1 then 1 else 0 end) as enabled_secondary_anytime_activated_today,
             sum(case when x_primary_box_subscription = 1 and box_has_anytime = 1 then 1 else 0 end) as enabled_primary_anytime_eligible_today,
             sum(case when x_secondary_box_subscription = 1 and box_has_anytime = 1 then 1 else 0 end) as enabled_secondary_anytime_eligible_today
         from vespa_analysts.vespa_OpDash_subscriber_dashboard
         -- where enabled_date > '2011-10-01'
         -- Panel 4 vs Panel 5 discrepancy now handled in single box view?
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
     update vespa_analysts.vespa_OpDash_10_DailySummary_thisweek
     set reporting_proportion                                = convert(double, reporting_boxes) / boxes_enabled
         ,primary_reporting_proportion                       = convert(double, reporting_primary_boxes) / primary_boxes_enabled
         ,secondary_reporting_proportion                     = convert(double, reporting_secondary_boxes) / secondary_boxes_enabled
         ,primary_anytime_active_reporting_proportion        = convert(double, reporting_primary_anytime_active_boxes) / primary_anytime_active_boxes
         ,secondary_anytime_active_reporting_proportion      = convert(double, reporting_secondary_anytime_active_boxes) / secondary_anytime_active_boxes
         ,primary_anytime_eligible_reporting_proportion      = convert(double, reporting_primary_anytime_eligible_boxes) / primary_anytime_eligible_boxes
         ,secondary_anytime_eligible_reporting_proportion    = convert(double, reporting_secondary_anytime_eligible_boxes) / secondary_anytime_eligible_boxes

     COMMIT
     create unique index fake_pk on vespa_analysts.vespa_OpDash_05_DailySummary_historics   (document_from_9AM)
     create unique index fake_pk on vespa_analysts.vespa_OpDash_10_DailySummary_thisweek    (log_date)
     COMMIT
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'Q02: Complete! (Daily summaries)'
     COMMIT

     /****************** Q03: WEEKLY ENABLEMENT RESULTS ******************/

     -- Report Output 6: Enablement summary by Box
     select
         convert(date, enabled_date)
         ,count(*) as boxes
         ,sum(case when x_primary_box_subscription=1 then 1 else 0 end) as primary_box_enabled
         ,sum(case when x_primary_box_subscription=1 and has_returned_data_ever=1 then 1 else 0 end) as primary_box_enabled_and_returned_data
         ,sum(case when x_secondary_box_subscription=1 then 1 else 0 end) as non_primary_box_enabled
         ,sum(case when x_secondary_box_subscription=1 and has_returned_data_ever=1 then 1 else 0 end) as non_primary_box_enabled_and_returned_data
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
     into vespa_analysts.vespa_OpDash_06_Enablement_bybox
     from vespa_analysts.vespa_OpDash_subscriber_dashboard
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
     into vespa_analysts.vespa_OpDash_07_Enablement_byaccount
     from vespa_analysts.vespa_OpDash_account_level_summary
     group by enabled_date_min

     COMMIT
     create unique index fake_pk on vespa_analysts.vespa_OpDash_06_Enablement_bybox     (enabled_date)
     create unique index fake_pk on vespa_analysts.vespa_OpDash_07_Enablement_byaccount (enabled_date_min)
     COMMIT
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'Q03: Complete! (Weekly enablement results)'
     COMMIT

     /****************** Q04: BOX TYPE & PREMIUMS DETAILS ******************/

     -- Report Output 8: Box Type & Premiums - Box Type
     if (object_id('vespa_analysts.vespa_OpDash_08_boxtype') is not null) execute('call DBA.sp_drop_table (''vespa_analysts'', ''vespa_OpDash_08_boxtype'')')

     execute ('call dba.sp_create_table (''vespa_analysts'', ''vespa_OpDash_08_boxtype'',''box_type               varchar(30) not null
                                                                           ,sky_base               integer not null default 0
                                                                           ,vespa                  integer default 0
                                                                           ,sequencer              integer
     '')')

     commit

     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('A) HD Combi 2TB',0,1)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('B) HD 2TB',0,2)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('C) HD Combi 1TB',0,3)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('D) HD 1TB',0,4)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('E) HD Combi',0,5)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('F) HD',0,6)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('G) HDx Combi 2TB',0,7)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('H) HDx 2TB',0,8)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('I) HDx Combi 1TB',0,9)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('J) HDx 1TB',0,10)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('K) HDx Combi',0,11)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('L) HDx',0,12)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('M) SkyPlus Combi',0,13)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('N) SkyPlus ',0,14)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('O) Multiroom',0,15)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('P) FDB',0,16)
     insert into vespa_analysts.vespa_OpDash_08_boxtype (box_type,sky_base,sequencer) values('Unknown',0,17)

     commit

     Select  Box_type
             ,count(distinct base.account_number) as Sky_base
             ,sum( case when vespa_flag = 1 and x_primary_box_subscription=1 and has_returned_data_ever>0 then 1 else 0 end) as vespa
             ,case   when box_type = 'A) HD Combi 2TB'  then 1
                     when box_type = 'B) HD 2TB'   then 2
         when box_type = 'C) HD Combi 1TB'  then 3
                     when box_type = 'D) HD 1TB'   then 4
                     when box_type = 'E) HD Combi'   then 5
                     when box_type = 'F) HD'    then 6
                     when box_type = 'G) HDx Combi 2TB'  then 7
                     when box_type = 'H) HDx 2TB'   then 8
         when box_type = 'I) HDx Combi 1TB'  then 9
                     when box_type = 'J) HDx 1TB'   then 10
                     when box_type = 'K) HDx Combi'   then 11
                     when box_type = 'L) HDx'    then 12
                     when box_type = 'M) SkyPlus Combi'  then 13
                     when box_type = 'N) SkyPlus '   then 14
                     when box_type = 'O) Multiroom'   then 15
                     when box_type = 'P) FDB'    then 16
                     else 17
             end as sequencer
     into #vespa_OpDash_08_boxtype
     from    vespa_analysts.Vespa_OpDash_sky_base_listing as base
             left join vespa_analysts.vespa_OpDash_subscriber_dashboard as vbr on vbr.account_number = base.account_number
     group by Box_type

     update  vespa_analysts.vespa_OpDash_08_boxtype
     set  sky_base     = temp.sky_base
       ,vespa     = temp.vespa
     from   #vespa_OpDash_08_boxtype as temp
     inner join vespa_analysts.vespa_OpDash_08_boxtype as o8 on temp.sequencer = o8.sequencer

     -- Report Output 9: Box Type & Premiums - Premiums
     Select  Premiums
             ,count(distinct base.account_number) as Sky_base
             ,sum( case when vespa_flag = 1 and x_primary_box_subscription=1 and has_returned_data_ever>0 then 1 else 0 end) as vespa
             ,case   when premiums = 'top_tier' then 1
                     when premiums = 'one_sport_two_movies' then 2
                     when premiums = 'no_sports_two_movies' then 3
                     when premiums = 'two_sports_one_movie' then 4
                     when premiums = 'two_sports_no_movies' then 5
                     when premiums = 'one_sport_one_movie' then 6
                     when premiums = 'basic' then 7
                     else 8
                 end as sequencer
     into vespa_analysts.vespa_OpDash_09_premiums
     from    vespa_analysts.Vespa_OpDash_sky_base_listing as base
             left join vespa_analysts.vespa_OpDash_subscriber_dashboard as vbr on vbr.account_number = base.account_number
     where premiums is not null -- this is a temp fix... but need to investigate why are we getting premiums = null...
     group by premiums

     COMMIT
     create unique index fake_pk on vespa_analysts.vespa_OpDash_08_boxtype      (sequencer)
     create unique index fake_pk on vespa_analysts.vespa_OpDash_09_premiums     (sequencer)
     COMMIT
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'Q04: Complete! (Box type & premium details)'
     COMMIT



     /****************** Q05: HISTORICAL OPTOUT FIGURES VIEW ******************/

     declare @weekending date

     select @weekending =
          case when datepart(weekday,@latest_full_date) = 7
           then @latest_full_date
           else (@latest_full_date - datepart(weekday,@latest_full_date))
          end

     if exists (
         select first *
         from vespa_analysts.vespa_OpDash_hist_optout
         where  weekending = @weekending
        )
      begin

       delete from vespa_analysts.vespa_OpDash_hist_optout
       where weekending = @weekending

       commit
      end

     -- This is pretty much pivoting the optout table to get contexts in rows into columns with the respective values...
	 -- cortb Changed orientation of this table to show RTMs in rows instead of columns 09/10/2013
    insert  into vespa_analysts.vespa_OpDash_hist_optout
	Select  'number'
	  		,@weekending
			,rtm
			,coalesce(viewing_allowed,0)
	from   	vespa_analysts.vespa_OpDash_01_OptOut_AllAccounts
	union
	select  'number'
			,@weekending
			,'All'
			,coalesce(sum(viewing_allowed),0)
	from   	vespa_analysts.vespa_OpDash_01_OptOut_AllAccounts
	group   by	1,2,3
	union
	select  'percentage'
			,@weekending
			,rtm
			,cast(coalesce(viewing_allowed,0) as float) /cast(total_records as float)
	from   	vespa_analysts.vespa_OpDash_01_OptOut_AllAccounts
	union
	select  'percentage'
			,@weekending
			,'All'
			,cast(sum(viewing_allowed) as float) / cast(sum(total_records) as float)
	from   	vespa_analysts.vespa_OpDash_01_OptOut_AllAccounts
	group   by	1,2,3



     -- Also building history for viewing consent for new customers... (Let's see if sales is working)

     if exists (
         select first *
         from vespa_analysts.vespa_OpDash_hist_new_optout
         where  weekending = @weekending
        )
      begin

       delete from vespa_analysts.vespa_OpDash_hist_new_optout
       where weekending = @weekending

       commit
      end

	insert  into vespa_analysts.vespa_OpDash_hist_new_optout
	Select  'number'
			,@weekending
			,rtm
			,coalesce(viewing_allowed,0)
	from    vespa_analysts.vespa_OpDash_04_OptOut_Activation
	union
	select  'number'
			,@weekending
			,'All'
			,coalesce(sum(viewing_allowed),0)
	from    vespa_analysts.vespa_OpDash_04_OptOut_Activation
	group   by	1,2,3
	union
	select  'percentage'
			,@weekending
			,rtm
			,cast(coalesce(viewing_allowed,0) as float) /cast(total_records as float)
	from   	vespa_analysts.vespa_OpDash_04_OptOut_Activation
	union
	select  'percentage'
			,@weekending
			,'All'
			,cast(sum(viewing_allowed) as float) / cast(sum(total_records) as float)
	from    vespa_analysts.vespa_OpDash_04_OptOut_Activation
	group   by 	1,2,3

     commit

     --Opt-out Report (from previous opt-in) This is the table used for the Viewing Consent Report pages 5-6
       insert into vespa_analysts.vespa_OpDash_from_optin(context
                                                         ,weekending
                                                         ,value)
       select 'number'
             ,@weekending
             ,sum(case when bas.opt_in_this_week = 0 then 1 else 0 end)
         from vespa_analysts.consent_last_week                        as lst
              inner join vespa_analysts.vespa_OpDash_sky_base_listing as bas on bas.account_number = lst.account_number
         union all
       select 'percentage'
             ,@weekending
             ,1.0 * sum(case when bas.opt_in_this_week = 0 then 1 else 0 end) / count(lst.account_number)
         from vespa_analysts.consent_last_week                        as lst
              inner join vespa_analysts.vespa_OpDash_sky_base_listing as bas on bas.account_number = lst.account_number

     --history of account nos. that opt out (a history table, not used for any reports. it is possible for an account to appear here more than once)
       insert into vespa_analysts.vespa_OpDash_optouts(account_number
                                                      ,weekending)
       select bas.account_number
             ,@weekending
         from vespa_analysts.consent_last_week                        as lst
              inner join vespa_analysts.vespa_OpDash_sky_base_listing as bas on bas.account_number = lst.account_number
        where bas.opt_in_this_week = 0

     --store this week's consent info for next week
     truncate table vespa_analysts.consent_last_week

       insert into vespa_analysts.consent_last_week
       select account_number
         from vespa_analysts.vespa_OpDash_sky_base_listing
        where opt_in_this_week = 1

     commit

     /****************** U01: ADSMARTABLE PAGES ******************/
	/* --  cortb commented out (22-04-2014) as v16 had error on temp space estimation.
       Select sav.account_number
             ,stb.service_instance_id
             ,coalesce(x_model_number, 'Unknown') as x_model_number
             ,rank () over (partition by sav.account_number, stb.service_instance_id order by ph_non_subs_link_sk desc, stb.cb_row_id, sav.cb_row_id) as active_flag
             ,case when x_pvr_type in ('PVR5', 'PVR6') or (x_pvr_type = 'PVR4' and x_manufacturer in ('Samsung', 'Pace')) then 1 else 0 end as adsmartable
             ,ph_non_subs_link_sk as ph_non_subs_link_sk
             ,stb.cb_row_id as cb_row_id_stb
             ,sav.cb_row_id as cb_row_id_sav
         into #subs
         from sk_prod.cust_single_account_view as sav
              left join sk_prod.CUST_SET_TOP_BOX as stb on stb.account_number = sav.account_number
        where sav.cust_active_dtv = 1
          and sav.pty_country = 'Great Britain'
		  */		  
		  
		-- cortb split out rank function (22-04-2014) as temp space error was over estimated and didn't like rank function in query
       Select sav.account_number
             ,stb.service_instance_id
             ,coalesce(x_model_number, 'Unknown') as x_model_number
             ,case when x_pvr_type in ('PVR5', 'PVR6') and x_manufacturer not in ('Samsung') then 1 else 0 end as adsmartable
             ,ph_non_subs_link_sk as ph_non_subs_link_sk
             ,stb.cb_row_id as cb_row_id_stb
             ,sav.cb_row_id as cb_row_id_sav
         into #subs
         from sk_prod.cust_single_account_view as sav
              left join sk_prod.CUST_SET_TOP_BOX as stb on stb.account_number = sav.account_number
        where sav.cust_active_dtv = 1
          and sav.pty_country = 'Great Britain'		  

        select rank () over (partition by account_number
                                          ,service_instance_id
                                 order by ph_non_subs_link_sk desc
                                         ,cb_row_id_stb
                                         ,cb_row_id_sav
                            ) as active_flag
               ,*
         into #subs_rank
         from #subs
		
       select account_number
             ,service_instance_id
             ,cast(adsmartable as bit)
             ,x_model_number
             ,cast(0 as bit) as reporting
             ,cast(0 as bit) as opt_in
             ,cast(0 as bit) as daily_panel
             ,cast(0 as bit) as vespa
         into #all_subs
         from #subs_rank
        where active_flag = 1

       update #all_subs as bas
          set bas.opt_in = sky.opt_in_this_week
         from vespa_analysts.vespa_OpDash_sky_base_listing as sky
        where bas.account_number = sky.account_number

       update #all_subs as bas
          set vespa = 1
         from vespa_analysts.vespa_single_box_view as sbv
        where bas.service_instance_id = sbv.service_instance_id
          and status_vespa = 'Enabled'

       update #all_subs as bas
          set daily_panel = 1
         from vespa_analysts.vespa_single_box_view as sbv
        where bas.service_instance_id = sbv.service_instance_id
          and (panel = 'VESPA' or panel = 'VESPA11') -- Broadband daily panel 11 addded
          and status_vespa = 'Enabled'

       update #all_subs as bas
          set reporting = 1
         from vespa_analysts.vespa_single_box_view as sbv
        where bas.service_instance_id = sbv.service_instance_id
          and reporting_quality > 0

       insert into vespa_analysts.vespa_OpDash_14_subs_count(boxes
                                                            ,opt_in
                                                            ,adsmartable
                                                            ,box_type
                                                            ,daily_panel
                                                            ,vespa
     )
       select count(1) as boxes
             ,case when opt_in      = 1 then 'Yes' else 'No' end as opt_in
             ,case when adsmartable = 1 then 'Yes' else 'No' end as adsmartable
             ,x_model_number
             ,case when daily_panel = 1 then 'Yes' else 'No' end as daily_panel
             ,case when vespa       = 1 then 'Yes' else 'No' end as vespa
         from #all_subs
     group by opt_in
             ,adsmartable
             ,x_model_number
             ,daily_panel
             ,vespa

       select account_number
             ,count(1)         as boxes
             ,sum(opt_in)      as opt_in
             ,sum(adsmartable) as sum_adsmartable
             ,max(adsmartable) as max_adsmartable
             ,sum(daily_panel) as daily_panel
             ,sum(vespa)       as vespa
             ,sum(reporting)   as reporting
         into #all_hhs
         from #all_subs
     group by account_number

       insert into vespa_analysts.vespa_OpDash_13_hhs_count(households
                                                           ,opt_in
                                                           ,adsmartable
                                                           ,daily_panel
                                                           ,vespa
     )
       select count(1) as households
             ,case when opt_in          > 0 then 'Yes' else 'No' end as opt_in
             ,case when sum_adsmartable > 0 then 'Yes' else 'No' end as adsmartable
             ,case when daily_panel     > 0 then 'Yes' else 'No' end as daily_panel
             ,case when vespa           > 0 then 'Yes' else 'No' end as vespa
         from #all_hhs
     group by opt_in
             ,adsmartable
             ,daily_panel
             ,vespa

	/****************** U02: ADSMARTABLE HISTORY ******************/
			 
     declare @latest_month date
     select @latest_month = max(month) from vespa_opdash_adsm_history

     if @latest_full_date > dateadd(month, 1, @latest_month)  -- if we haven't added data for this month yet
     begin
          insert into vespa_analysts.vespa_OpDash_15_adsm_history(month
                                                                 ,adsm_boxes
                                                                 ,adsm_hhs
                                                                 ,adsm_hhs_1box
                                                                 ,adsm_hhs_all_adsm
                                                                 ,adsm_hhs_non_adsm_box
          )
          select @latest_full_date - datepart(dd,@latest_full_date) + 1 -- first of the month
                ,sum(sum_adsmartable)                                                        as adsm_boxes
                ,sum(max_adsmartable)                                                        as adsm_hhs
                ,sum(case when max_adsmartable = 1 and boxes = 1          then 1 else 0 end) as adsm_hhs_1box
                ,sum(case when boxes = sum_adsmartable                    then 1 else 0 end) as adsm_hhs_all_adsm
                ,sum(case when max_adsmartable = 1 and sum_adsmartable < boxes then 1 else 0 end) as adsm_hhs_non_adsm_box
            from #all_hhs
     end
	 
	 --cortb added the 2 queries below for collection and inclusion in the XDash report 05/11/2013
	 --count of Skybase broken down by adsmartable households with only 1 box, adsmartable households 
	 --with more than 1 box where 1 is adsmartble but not all of them, adsmartable households where all
	 --boxes are adsmartable and households that are not adsmartable
	 
	  select account_number
             ,count(1)         as boxes
             ,sum(adsmartable) as sum_adsmartable
             ,max(adsmartable) as max_adsmartable
         into #all_hhs_4xdash
         from #all_subs
     group by account_number
	 
	--checking to see if this week is already there
     select @weekending =
          case when datepart(weekday,@latest_full_date) = 7
           then @latest_full_date
           else (@latest_full_date - datepart(weekday,@latest_full_date))
          end

     if exists (
         select first *
         from vespa_analysts.vespa_opdash_16_adsm_history_4Xdash
         where  weekending = @weekending
        )
      begin

       delete from vespa_analysts.vespa_opdash_16_adsm_history_4Xdash
       where weekending = @weekending

       commit
      end
	 --now we can update it
	 insert into vespa_analysts.vespa_opdash_16_adsm_history_4Xdash (weekending
                                                              ,adsm_hhs_1box
                                                              ,adsm_hhs_morethan1box_1box_adsm
                                                              ,adsm_hhs_all_adsm
                                                              ,non_adsm_hhs
     )
     select @latest_full_date
            ,sum(case when max_adsmartable = 1
                      and boxes = 1                     then 1 else 0 end) as adsm_hhs_1box
            ,sum(case when boxes > 1
                      and sum_adsmartable >= 1
                      and  max_adsmartable = 1
                      and  sum_adsmartable <> boxes     then 1 else 0 end) as adsm_hhs_morethan1box_1box_adsm
            ,sum(case when boxes > 1
                      and  boxes = sum_adsmartable
                      and max_adsmartable = 1           then 1 else 0 end) as adsm_hhs_all_adsm
            ,sum(case when max_adsmartable = 0          then 1 else 0 end) as non_adsm_hhs
       from #all_hhs_4xdash
 
	commit
	   
     /****************** R01: ADSMARTABLE ACCOUNTS AND BOXES ******************/
     -- Adding adsmartable (non-adsmartable) information breakdown by Sky base and vespa accounts,
     -- with viewing consent and active boxes.
     -- cortb - 24/07/2013

	 /*insert	into vespa_analysts.vespa_OpDash_11_Adsmartable_Accounts  -- cortb (23-04-2014) commented out as v16 couldn't handle this due to low estimation of temp space required
     select  CASE WHEN adsmart.flag = 0 THEN 'Not Adsmartable'
                  WHEN adsmart.flag = 1 THEN 'Adsmartable'
             END AS Flag
             ,count(distinct sav.account_number) as Sky_Base
             ,count(distinct sbv.account_number) as vespa
             ,count(distinct sbvr.account_number) as vespa_reporting
     from    (

                 -- flagging from SAV DTH Active customer in UK, Viewing consent (optional))
                 select  distinct account_number
                 from        sk_prod.CUST_SINGLE_ACCOUNT_VIEW
                 where       CUST_ACTIVE_DTV = 1             -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
                 and         pty_country_code = 'GBR'
                 and         cust_viewing_data_capture_allowed = 'Y' -- [ ENABLE/DISABLE this criteria to consider viewing consent ]

             )   as sav
             left join   (

                             -- Flag Adsmartable boxes based on Adsmart definition
                             select  account_number
                                     ,max(   CASE    WHEN x_pvr_type ='PVR6'                                 THEN 1
                                                     WHEN x_pvr_type ='PVR5'                                 THEN 1
                                                     WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung'  THEN 1
                                                     WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'     THEN 1
                                                                                                             ELSE 0
                                             END) AS flag
                             from    (

                                         -- Extracting Active Boxes per account (one line per box per account)
                                         select  *
                                         from    (

                                                     -- Ranking STB based on service instance id to dedupe the table
                                                     Select  account_number
                                                             ,x_pvr_type
                                                             ,x_personal_storage_capacity
                                                             ,currency_code
                                                             ,x_manufacturer
                                                             ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
                                                     from    sk_prod.CUST_SET_TOP_BOX

                                                 )   as base
                                         where   active_flag = 1

                                     )   as active_boxes
                             where   currency_code = 'GBP'
                             group   by  account_number

                         )   as adsmart
             on  sav.account_number = adsmart.account_number
             left join   (
                             --Listing DP active Accounts
                             select  distinct account_number
                             from    vespa_analysts.vespa_single_box_view
                             where   (panel = 'VESPA' or panel = 'VESPA11') -- Broadband daily panel 11 added
                             and     status_vespa = 'Enabled'
                         )   as sbv
             on  sav.account_number = sbv.account_number
             left join   (
                             --Listing DP active Accounts that have reported at least 1 day amongst last 30 days
                             select  distinct account_number
                             from    vespa_analysts.vespa_single_box_view
                             where   (panel = 'VESPA' or panel = 'VESPA11') -- Broadband daily panel 11 added
                             and     status_vespa = 'Enabled'
                             and     reporting_quality > 0
                         )   as sbvr
             on  sav.account_number = sbvr.account_number
     where   adsmart.flag in (1, 0)
     group   by  adsmart.flag
     order   by  Flag asc
	 */
	 
	 -- cortb (22-04-2014) added break down of the query above into tables and put indexes to cater for lack of temp space estimation
	 -- inserting data into the tables at account level
	 if object_id( 'vespa_analysts.active_uk_cust_vc') is not null
        truncate table vespa_analysts.active_uk_cust_vc

            insert into vespa_analysts.active_uk_cust_vc(account_number)
            -- flagging from SAV DTH Active customer in UK, Viewing consent (optional))
            select  distinct account_number
            from        sk_prod.CUST_SINGLE_ACCOUNT_VIEW
            where       CUST_ACTIVE_DTV = 1             -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
            and         pty_country_code = 'GBR'
            and         cust_viewing_data_capture_allowed = 'Y' -- [ ENABLE/DISABLE this criteria to consider viewing consent ]

    commit

    if object_id( 'vespa_analysts.adsmart_boxes_al') is not null
        truncate table vespa_analysts.adsmart_boxes_al

            insert into vespa_analysts.adsmart_boxes_al(account_number, flag)
            -- Flag Adsmartable boxes based on Adsmart definition
            select  account_number
                    ,max(   CASE    WHEN x_pvr_type ='PVR6'                                 THEN 1
                                    WHEN x_pvr_type ='PVR5' and x_manufacturer not in ('Samsung') THEN 1
                                  --  WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung'  THEN 1
                                  --  WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'     THEN 1
                                                                                            ELSE 0
                                    END) AS flag
            from    (
                        -- Ranking STB based on service instance id to dedupe the table
                        Select  account_number
                                ,x_pvr_type
                                ,x_personal_storage_capacity
                                ,currency_code
                                ,x_manufacturer
                        from    sk_prod.CUST_SET_TOP_BOX
                        where   x_active_box_flag_new = 'Y'
                    )   as active_boxes
            where   currency_code = 'GBP'
            group   by  account_number

    commit

    if object_id( 'vespa_analysts.DP_active_accounts_al') is not null
        truncate table vespa_analysts.DP_active_accounts_al

            insert into vespa_analysts.DP_active_accounts_al(account_number)
            --Listing DP active Accounts
            select  distinct account_number
            from    vespa_analysts.vespa_single_box_view
            where   (panel = 'VESPA' or panel = 'VESPA11') -- Broadband daily panel 11 added
            and     status_vespa = 'Enabled'

    commit

    if object_id( 'vespa_analysts.DP_active_accounts_rep_al') is not null
        truncate table vespa_analysts.DP_active_accounts_rep_al

            insert into vespa_analysts.DP_active_accounts_rep_al(account_number)
            --Listing DP active Accounts that have reported at least 1 day amongst last 30 days
            select  distinct account_number
            from    vespa_analysts.vespa_single_box_view
            where   (panel = 'VESPA' or panel = 'VESPA11') -- Broadband daily panel 11 added
            and     status_vespa = 'Enabled'
            and     reporting_quality > 0
	
	
	-- inserting account level adsmart info to replace old query above	***************
	insert	into vespa_analysts.vespa_OpDash_11_Adsmartable_Accounts		
    select  CASE WHEN adsmart.flag = 0 THEN 'Not Adsmartable'
                  WHEN adsmart.flag = 1 THEN 'Adsmartable'
             END AS Flag
             ,count(distinct sav.account_number) as Sky_Base
             ,count(distinct sbv.account_number) as vespa
             ,count(distinct sbvr.account_number) as vespa_reporting
     from    vespa_analysts.active_uk_cust_vc as sav
             left join   vespa_analysts.adsmart_boxes_al as adsmart on  sav.account_number = adsmart.account_number
             left join   vespa_analysts.DP_active_accounts_al as sbv on  sav.account_number = sbv.account_number
             left join   vespa_analysts.DP_active_accounts_rep_al as sbvr on  sav.account_number = sbvr.account_number
     where   adsmart.flag in (1, 0)
     group   by  adsmart.flag
     order   by  Flag asc

     commit

     truncate table vespa_analysts.adsmart_boxes_al
     truncate table vespa_analysts.DP_active_accounts_al
     truncate table vespa_analysts.DP_active_accounts_rep_al

     Commit
	 
	 -- cortb added this update to insert adsmartable households reporting into XDash history table 
	 Update  vespa_opdash_16_adsm_history_4Xdash as voah
     set     adsm_hhs_reporting = vespa_reporting
     from    vespa_analysts.vespa_OpDash_11_Adsmartable_Accounts as voaa
     where   voaa.Flag = 'Not Adsmartable'
		

     /****************** R01: ADSMARTABLE ACCOUNTS AND BOXES ******************/
     -- Adding adsmartable only information breakdown by box type/desc, with box volumes for Sky base and vespa.
     -- cortb - 24/07/2013

	 /*insert	into vespa_analysts.vespa_OpDash_12_Adsmartable_Boxes_Types -- cortb (23-04-2014) commented out as v16 couldn't handle this due to low estimation of temp space required
     select  adsmart.box_model
             ,count(distinct adsmart.service_instance_id) as Sky_boxes
             ,count(distinct sbv.service_instance_id) as vespa_boxes
             ,count(distinct sbvr.service_instance_id) as vespa_boxes_reporting
     from    (

                 -- flagging from SAV DTH Active customer in UK, Viewing consent (optional))
                 select  distinct account_number
                 from        sk_prod.CUST_SINGLE_ACCOUNT_VIEW
                 where       CUST_ACTIVE_DTV = 1             -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
                 and         pty_country_code = 'GBR'
                 and         cust_viewing_data_capture_allowed = 'Y' -- [ ENABLE/DISABLE this criteria to consider viewing consent ]

             )   as sav
             inner join   (

                             -- Flag Adsmartable boxes based on Adsmart definition and model & description
                             select  account_number
                                     ,service_instance_id
                                     ,max(   CASE    WHEN x_pvr_type ='PVR6'                                 THEN 1
                                                     WHEN x_pvr_type ='PVR5'                                 THEN 1
                                                     WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung'  THEN 1
                                                     WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'     THEN 1
                                                                                                             ELSE 0
                                             END) AS flag
                                     ,x_model_number || ' / ' || x_description AS box_model
                             from    (

                                         -- Extracting Active Boxes per account (one line per box per account)
                                         select  *
                                         from    (

                                                     -- Ranking STB based on service instance id to dedupe the table
                                                     Select  account_number
                                                             ,x_pvr_type
                                                             ,x_personal_storage_capacity
                                                             ,currency_code
                                                             ,x_manufacturer
                                                             ,service_instance_id
                                                             ,x_model_number
                                                             ,x_description
                                                             ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
                                                     from    sk_prod.CUST_SET_TOP_BOX

                                                 )   as base
                                         where   active_flag = 1

                                     )   as active_boxes
                             where   currency_code = 'GBP'
                             group   by  account_number
                             ,service_instance_id
                             ,box_model

                         )   as adsmart
             on  sav.account_number = adsmart.account_number
             left join   (
                             -- Listing DP active Accounts & boxes
                             select  account_number
                                     ,service_instance_id
                             from    vespa_analysts.vespa_single_box_view
                             where   (panel = 'VESPA' or panel = 'VESPA11') -- Broadband daily panel 11 added
                             and     status_vespa = 'Enabled'
                         )   as sbv
             on  sav.account_number = sbv.account_number
             left join   (
                             -- Listing DP active Accounts & boxes that have reported at least 1 day amongst last 30 days
                             select  account_number
                                     ,service_instance_id
                             from    vespa_analysts.vespa_single_box_view
                             where   (panel = 'VESPA' or panel = 'VESPA11') -- Broadband daily panel 11 added
                             and     status_vespa = 'Enabled'
                             and     reporting_quality > 0
                         )   as sbvr
             on  sav.account_number = sbvr.account_number
     where adsmart.flag = 1
     group   by  adsmart.box_model
     order by Sky_boxes desc
	 */
	
	 -- cortb (22-04-2014) added break down of the query above into tables and put indexes to cater for lack of temp space estimation
	 -- inserting data into the tables at box level
	if object_id( 'vespa_analysts.adsmart_boxes_bl') is not null
        truncate table vespa_analysts.adsmart_boxes_bl

            insert into vespa_analysts.adsmart_boxes_bl(account_number, service_instance_id, flag, box_model)
            -- Flag Adsmartable boxes based on Adsmart definition
            select  account_number
                    ,service_instance_id
                    ,max(   CASE    WHEN x_pvr_type ='PVR6'                                 THEN 1
                                    WHEN x_pvr_type ='PVR5' AND x_manufacturer not in ('Samsung')   THEN 1
                                  --  WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung'  THEN 1
                                  --  WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'     THEN 1
                                                                                            ELSE 0
                                    END) AS flag
                    ,x_model_number || ' / ' || x_description AS box_model
            from    (
                        -- Ranking STB based on service instance id to dedupe the table
                        Select  account_number
                                ,x_pvr_type
                                ,x_personal_storage_capacity
                                ,currency_code
                                ,x_manufacturer
                                ,service_instance_id
                                ,x_model_number
                                ,x_description
                        from    sk_prod.CUST_SET_TOP_BOX
                        where   x_active_box_flag_new = 'Y'
                     )   as active_boxes
            where   currency_code = 'GBP'
            group   by  account_number
            ,service_instance_id
            ,box_model

    commit

    if object_id( 'vespa_analysts.DP_active_accounts_bl') is not null
        truncate table vespa_analysts.DP_active_accounts_bl

            insert into vespa_analysts.DP_active_accounts_bl(account_number, service_instance_id)
            --Listing DP active Accounts
            select  distinct account_number
                             ,service_instance_id
            from    vespa_analysts.vespa_single_box_view
            where   (panel = 'VESPA' or panel = 'VESPA11') -- Broadband daily panel 11 added
            and     status_vespa = 'Enabled'

    commit

    if object_id( 'vespa_analysts.DP_active_accounts_rep_bl') is not null
        truncate table vespa_analysts.DP_active_accounts_rep_bl

            insert into vespa_analysts.DP_active_accounts_rep_bl(account_number, service_instance_id)
            --Listing DP active Accounts that have reported at least 1 day amongst last 30 days
            select  distinct account_number
                             ,service_instance_id
            from    vespa_analysts.vespa_single_box_view
            where   (panel = 'VESPA' or panel = 'VESPA11') -- Broadband daily panel 11 added
            and     status_vespa = 'Enabled'
            and     reporting_quality > 0

     commit

	 	-- inserting box level adsmart info to replace old query above	***************
	 insert	into vespa_analysts.vespa_OpDash_12_Adsmartable_Boxes_Types	 
     select  adsmart.box_model
             ,count(distinct adsmart.service_instance_id) as Sky_boxes
             ,count(distinct sbv.service_instance_id) as vespa_boxes
             ,count(distinct sbvr.service_instance_id) as vespa_boxes_reporting
     from    vespa_analysts.active_uk_cust_vc as sav
             left join   vespa_analysts.adsmart_boxes_bl as adsmart on  sav.account_number = adsmart.account_number
             left join   vespa_analysts.DP_active_accounts_bl as sbv on  sav.account_number = sbv.account_number
             left join   vespa_analysts.DP_active_accounts_rep_bl as sbvr on  sav.account_number = sbvr.account_number
     where adsmart.flag = 1
     group   by  adsmart.box_model
     order by Sky_boxes desc

     commit

     truncate table vespa_analysts.active_uk_cust_vc
     truncate table vespa_analysts.adsmart_boxes_bl
     truncate table vespa_analysts.DP_active_accounts_bl
     truncate table vespa_analysts.DP_active_accounts_rep_bl

     commit
		
		

     /****************** T02: PERMISSIONS ON REPORT TABLES ******************/
     -- These permissions, however, are essential run-to-run.
     grant select on vespa_analysts.vespa_OpDash_01_OptOut_AllAccounts      to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_02_OptOut_AfterMay         to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_03_OptOut_AfterApril       to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_04_OptOut_Activation       to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_05_DailySummary_historics  to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_06_Enablement_bybox        to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_07_Enablement_byaccount    to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_08_boxtype                 to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_09_premiums                to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_10_DailySummary_thisweek   to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_11_Adsmartable_Accounts    to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_12_Adsmartable_Boxes_Types to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_13_hhs_count               to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_14_subs_count              to vespa_group_low_security
     grant select on vespa_analysts.vespa_OpDash_15_adsm_history            to vespa_group_low_security
	 grant select on vespa_analysts.vespa_opdash_16_adsm_history_4Xdash 	to vespa_group_low_security
		
     commit
     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'T01: Complete! (Report permissions)'
     COMMIT

     /****************** X01: AND WE'RE DONE! ******************/

     EXECUTE citeam.logger_add_event @Model_logging_ID, 3, 'OpDash: weekly refresh complete!'
     COMMIT

end;

grant execute on OpDash_make_report to public;

commit;
go
-- Need the central scheduler thing to be able to call the procs. But it gets
-- run within the vespa_analytics account, so it doesn't mean that any random
-- public person can see what's in the resulting tables.

/****************** Y01: CLEAN OUT TRANSIENT TABLES ******************/
-- This guys needs to be in a different proc because all the tables end
-- up in vespa_analysts, which regular users won't have permission to
-- drop afterwards. They're still not explicit with the schema though,
-- because someone might be maintaining a testing / dev build in their
-- own schema. So, yeah.

if object_id('OpDash_clear_transients') is not null
   drop procedure OpDash_clear_transients;

go

create procedure OpDash_clear_transients
as
begin
    delete from vespa_analysts.vespa_OpDash_account_level_summary
    delete from vespa_analysts.vespa_OpDash_log_collection_dump
    delete from vespa_analysts.vespa_OpDash_new_log_listing
    delete from vespa_analysts.Vespa_OpDash_sky_base_listing
    delete from vespa_analysts.vespa_OpDash_subscriber_dashboard
	if object_id( 'vespa_analysts.vespa_OpDash_01_OptOut_AllAccounts') is not null
        drop table vespa_analysts.vespa_OpDash_01_OptOut_AllAccounts
    if object_id( 'vespa_analysts.vespa_OpDash_02_OptOut_AfterMay') is not null
        drop table vespa_analysts.vespa_OpDash_02_OptOut_AfterMay
    if object_id( 'vespa_analysts.vespa_OpDash_03_OptOut_AfterApril') is not null
        drop table vespa_analysts.vespa_OpDash_03_OptOut_AfterApril
    if object_id( 'vespa_analysts.vespa_OpDash_04_OptOut_Activation') is not null
        drop table vespa_analysts.vespa_OpDash_04_OptOut_Activation
    if object_id( 'vespa_analysts.vespa_OpDash_05_DailySummary_historics') is not null
        drop table vespa_analysts.vespa_OpDash_05_DailySummary_historics
    if object_id( 'vespa_analysts.vespa_OpDash_06_Enablement_bybox') is not null
        drop table vespa_analysts.vespa_OpDash_06_Enablement_bybox
    if object_id( 'vespa_analysts.vespa_OpDash_07_Enablement_byaccount') is not null
        drop table vespa_analysts.vespa_OpDash_07_Enablement_byaccount
    if object_id( 'vespa_analysts.vespa_OpDash_08_boxtype') is not null
        drop table vespa_analysts.vespa_OpDash_08_boxtype
    if object_id( 'vespa_analysts.vespa_OpDash_09_premiums') is not null
        drop table vespa_analysts.vespa_OpDash_09_premiums
    if object_id( 'vespa_analysts.vespa_OpDash_10_DailySummary_thisweek') is not null
        drop table vespa_analysts.vespa_OpDash_10_DailySummary_thisweek
    truncate table vespa_analysts.vespa_OpDash_11_Adsmartable_Accounts
    truncate table vespa_analysts.vespa_OpDash_12_Adsmartable_Boxes_Types
    truncate table vespa_analysts.vespa_OpDash_13_hhs_count
    truncate table vespa_analysts.vespa_OpDash_14_subs_count
end;

grant execute on OpDash_clear_transients to public;

commit;
go








---



