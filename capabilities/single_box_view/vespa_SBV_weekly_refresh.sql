/******************************************************************************
**
** Project Vespa: Single box view
**                  - Weekly refresh
**
** Got to figure out when this refresh will happen, and put it in the scheduler.
** Probably at the beginning of every week, using the previous SAV refresh etc?
**
** Unfortunately the table population is messy. It's cleaned up a bit with the
** CCN coming into play, and we're now just using whatever is in the subscriber
** status table as our definitive listing of what's on Vespa.
**
** See also:
**      http://rtci/vespa1/Single%20box%20view.aspx
**
** Code sections:
**      Part A: A01 -  Initialise Logger
**              A02 -  Reset working & holding table
**
**      Part B:        Assembling box listing
**              B01 -  Confirmations from Midas campaign cells
**              B02 -  Forming the population
**              B03 -  Panel 4 marks by campaign cells
**              B04 -  Vespa flags fom Vespa DB
**              B05 -  Sky View Panel flags
**              B06 -  What is (probably) be Panel 4 based on enablement date
**              B07 -  Active DTV flag, standard UK account
**              B08 -  ACTIVE BOX FLAGS ON PANEL 12
**              B09 -  Flags for alternate panels 6 & 7
**              B10 -  Reducing population to active boxes (try to avoid temp space errors) - DISCONTINUED
**              B11 -  Definitive single flag for which panel each box is on
**
**      Part C:        Primary / Secondary flags
**              C01 -  From Olive
**              C02 -  From Vespa STB Log Lookup
**              C03 -  Attempting to infer the P/S flag in non-explicit cases...
**              C04 -  Combined decided flag (the one to use)
**
**      Part D:        Enablement dates
**              D01 -  Historical enablement dates from subscriber history
**              D02 -  Combined enablement date decision
**
**      Part E:        Other Vespa profiling / derived variables
**              E01 -  Viewing data present for each one of last 30 days
**              E02 -  Box reporting reliability metric
**
**      Part G:        Profiling variables from Customer Database
**              G01 -  Box type
**              G02 -  Anytime + flags
**              G03 -  PvR
**              G04 -  HD flags
**              G05 -  Premiums (they're an account thing, yes, but they get used all the time)
**              G06 -  3D TV flags
**              G07 -  cb_individual_key for linking to 3rd party data
**    G08 -  Archiving Quality metrics
**
** ^: these are bottleneck sections. They go to the Olive DB, not much can be
** done about it, they just slow. Sometimes a join will speed them up, sometimes
** not so much.
**
** Downside: we have three tables for which account_number should be a primary
** key; the subscriber status, the STB log snapshot, and now our single box
** view. Oh well.
**
** To do:
**  12. Realign the physical and subscription box types (might break OpDash)
**  13. Improve duplication checking when bringing items in from customer DB
**  14. Alternate panels 6 and 7 will turn up sometime soon and we'll need to
**          identify and flag those appropriately - see ##14## for line refs
**  18. We've got Service Instance ID on the daily tables. Are we going to use
**          that to fill in some of the gaps where we didn't get service instance
**          ID fro mthe customer database?
**  22. Remove the 'EnablePending' check once the 4->12 migration is complete
**          - see ##22## - Update: 'Trumped' is also on the list of things which
**          - now really mean Enabled too, awesome, messy.
**  23. Now there's treatment on the customer group wiki on how to link to
**          ConsumerView; see http://mktskyportal/Campaign%20Handbook/ConsumerView.aspx
**          and also look for the ##23## dev note.
**
** Done:
**  17. Additional reporting quality metric that also considers enablement date
**  19. Revoke team SELECT permissions at the beginning of the build and grant
**          them back again after the build is complete. (Could do that for the
**          scaling build too?)
**  20. Stability vs unexpected restarts: check who has permissions on SBV and
**          also make the index drop at the start of the daily pulls conditional,
**          because Sybase will stop and error if it tries to remove something
**          that isn't there.
**  21. Improved proc logging visibility, so that the logging name is determined
**          by the namespace the table is in, and not the name of the user running
**          the build proc. That's a bit better (though slightly messier to code).
**
******************************************************************************/

if object_id('SBV_refresh_single_box_view') is not null
   drop procedure SBV_refresh_single_box_view;

;

create procedure SBV_refresh_single_box_view -- execute SBV_refresh_single_box_view
as
begin

     /****************** A01: SETTING UP THE LOGGER ******************/

     DECLARE @SBV_build_ID           bigint
     DECLARE @Refresh_identifier     varchar(40)
     declare @run_Identifier         varchar(20)
     -- For putting control totals in prior to logging:
     DECLARE @QA_catcher             integer
     DECLARE @txt_clash    varchar(6)
     set @txt_clash = 'CLASH!'

     declare @profiling_day date
     -- so we're ;ing to set this to last Thursday when everything was updated?
     -- SAV refresh permitting of course. Oh, hey, there's a cheap way of doing it;

     select @profiling_day = max(cb_data_date) from cust_single_account_view

     set @Refresh_identifier = convert(varchar(10),@profiling_day,123) || ' SBV refresh'

     -- Okay, so we're again rebuilding how we're identifying live vs demo builds...
     -- kmandal is back in so that we can track the overnight ones, and we've ;t a
     -- bit of help from a cheeky dirty proc:

     declare @tablespacename         varchar(80)

     -- So this should give us back the name of whichever schema we're operating in....
     execute vespa_analysts.Regulars_whats_my_namespace @tablespacename output

     commit

     -- So if we're updating vespa_analysts.vespa_single_box_view then we're live,
     -- otherwise it's a test build in someone's personal schema:
     if lower(@tablespacename) = 'vespa_analysts'
         set @run_Identifier = 'VespaSingleBoxView'
     else
     begin
         -- First off, for those cases where the support procedure isn't set:
         set @tablespacename = coalesce(@tablespacename, user)
         -- OK, now the logging can begin...
         set @run_Identifier = 'SBV test ' || upper(right(@tablespacename,1)) || upper(left(@tablespacename,2))
     end

     -- That tablespacename will get used in a few other places in this build too...

     EXECUTE logger_create_run @run_Identifier, @Refresh_identifier, @SBV_build_ID output

     /****************** A02: CLEANING OUT THE JUNK ******************/

     -- First off, hide the table from the team while it's being rebuilt. Thing is we need
     -- to dynamically generate this list based on who has permissions, since if the SBV
     -- build dies midway through then Sybase will error out when it tries to revoke SELECT
     -- permission from people that don't have it:

     DECLARE @SQL_permissions_hack       varchar(1000)
     DECLARE @grantee                    varchar(40)
     DECLARE @gid                        int
     DECLARE @gidmax                     int

     set @SQL_permissions_hack = 'revoke select on vespa_analysts.vespa_single_box_view from '

     -- Build a list of everyone who has select permissions on SBV:
     select grantee
         ,rank() over (order by grantee) as id       -- as a kind of fake primary key thing
     into #selectibles
     from sys.systabauth
     where lower(tcreator) = @tablespacename         -- Now compatible with testing builds in other schemas!
     and lower(ttname) = 'vespa_single_box_view'
     and selectauth = 'Y'
     group by grantee

     -- How many people are there to loop through?
     select @gid = min(id), @gidmax = max(id)
     from #selectibles

     -- now ; through that list and revoke all their permissions;
     while @gid <= @gidmax
     begin
         select @grantee = grantee from #selectibles where id = @gid

--         execute(@SQL_permissions_hack || @grantee)

         set @gid = @gid + 1
     end

     drop table #selectibles

     -- Now reset the table:
     delete from vespa_analysts.vespa_single_box_view

     commit
     EXECUTE logger_add_event @SBV_build_ID, 3, 'A02: Complete! (Clean junk)'
     commit

     /****************** B01: CONFIRMATIONS FROM CAMPAIGN CELLS ******************/

     -- This was originally stolen from the OpDash build, but someone else
     -- gave us that code too and it just identified and then excluded with
     -- the result that and exclusions who were later re-introduced wouldn't
     -- show up in the analysis population, when they really should, so now
     -- we're doing it differently in a way that will handle that properly.
     -- Of course, by now this code has changed a bunch, check the commit logs.

     select
         account_number      -- not strictly required? Not used by the new process at all, it all ;es by boxes.
         ,card_subscriber_id
         ,cell_name
         ,writeback_datetime
         ,rank() over (partition by card_subscriber_id order by writeback_datetime desc) as most_recent
     into #tagged_vespa_boxes
     from sk_prod.campaign_history_cust  a
     inner join sk_prod.campaign_history_lookup_cust   b
     on a.cell_id = b.cell_id
     where (cell_name like 'Vespa Disablement%' or cell_name like 'Vespa Enablement%')
     and writeback_datetime >= cast('2011-10-01' as datetime)

     commit

     delete from #tagged_vespa_boxes
     where most_recent <> 1
     or cell_name like 'Vespa Disablement%'
     or card_subscriber_id in ('07411397','17613600','30242487','30724691','33614702') -- dirty hack continues for subscriber IDs '07411397' and '7411397'

     commit
     create unique index fake_pk on #tagged_vespa_boxes (card_subscriber_id)
     commit

     -- Okay, so now we're excluding by subscriber_id rather than by account, and
     -- we're also tolerating reenebalements. What do the totals look like? Should
     -- be higher generally. Hilariously, this build is also significantly faster.
     -- And returns an aditional 12k boxes (as of 13 Feb) of which 8.9k reported
     -- data on the 3rd of Feb alone! ;nna say it's my way that's ;od.

     -- Oh, but then: we've still ;t issues with old request stuff relating to
     -- boxes that have been pushed over to different accounts. So we need to cull
     -- from panel 4 all the things that don't relate to current box assignments...

     update #tagged_vespa_boxes
     set most_recent = 0
     from #tagged_vespa_boxes
     inner join cust_card_subscriber_link as ccsl
     on #tagged_vespa_boxes.card_subscriber_id = ccsl.card_subscriber_id
     and #tagged_vespa_boxes.account_number = ccsl.account_number
     and ccsl.current_flag = 'Y'

     commit

     -- Having identified them, now cull all the stuff relating to antiquated records...
     delete from #tagged_vespa_boxes
     where most_recent <> 0
     -- Might result in slow decreases of open loop enablement over time, but hey,
     -- I'm okay with that.

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #tagged_vespa_boxes

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B01: Complete! (Campaign cell confirmations)', coalesce(@QA_catcher, -1)
     commit

     /****************** B02: FORM POPULATION ******************/

     -- OK so the rebuild accomodating the Sky View panel is ;ing to put all
     -- the subscribers from different sources in and then flag them separately...

     select distinct account_number, card_subscriber_id
     into #duplicated_stb_population
     from #tagged_vespa_boxes
     -- No PK because we might get duplicated things from the Sky View panel
     -- or the vespa subscriber stuff...
     insert into #duplicated_stb_population
     select distinct account_number, vss.card_subscriber_id
     from sk_prod.VESPA_SUBSCRIBER_STATUS as vss
     where vss.card_subscriber_id <> '07411397' -- yeargh, still this messt guy
     -- And anything else that might be reporting back...
     insert into #duplicated_stb_population
     select distinct convert(varchar(12),account_number)
         ,right(replicate('0',8) || convert(varchar(20), subscriber_id), 8) as card_subscriber_id
         -- always want subscriber IDs to be 8 characters long
     from sk_prod.vespa_stb_log_summary --angeld: vespa_stb_log_snapshot replaced by vespa_stb_log_summary, this is because of phase 2 transittion...
     where account_number is not null
     -- (Shouldn't need to do that really, but sometimes Olive doesn't know about the boxes
     -- and we want to have a decent lookup of everything that might turn up on Sky View or
     -- Vespa or whatnot.)

     -- We'll also need panel 6 and 7 stuff in here when those are available... ##14##

     -- Okay... card_subscriber_ids are usually 8 characters long so we're ;ing to have to
     -- pad that out or something. Sucky. Oh well. Yeah, 8 is the standard.


     -- but... what? the Sky View panel has no subscriber IDs on it, only
     -- account numbers. suck. Okay, well, back into customer DB we ;...

     select distinct account_number
     into #skyview_accounts
     from vespa_sky_view_panel

     commit
     create unique index fake_pk on #skyview_accounts (account_number)
     commit

     -- Linkages to accounts comes from card_subscriber_link rather than the set_top_box table:
     insert into #duplicated_stb_population
     select
         csl.account_number
         ,csl.card_subscriber_id
     from cust_card_subscriber_link as csl
     inner join #skyview_accounts as sva
     on csl.account_number = sva.account_number
     where current_flag = 'Y'
     and @profiling_day between effective_from_dt and effective_to_dt

     commit

     -- Oh and we now have this confirmed list of Sky View panel seleted boxes... and we stitched
     -- account number onto that, cool.
     insert into #duplicated_stb_population
     select account_number, card_subscriber_id
     from vespa_analysts.verified_Sky_View_members
     where account_number is not null

     -- hahaha no, because we have duplicated account numbers in this list. First thing to do:
     -- remove any duplicates. Later we'll figure out where these duplicates came from.

     select card_subscriber_id
     into #awful_duplicated_subscribers
     from #duplicated_stb_population
     group by card_subscriber_id
     having count(distinct account_number) > 1

     commit
     set @QA_catcher = -1
     select @QA_catcher = count(1)
     from #awful_duplicated_subscribers

     if @QA_catcher > 0
     begin
         -- Awful structure, but that table should be very big.
         delete from #duplicated_stb_population where card_subscriber_id in (select card_subscriber_id from #awful_duplicated_subscribers)
         -- Later we'll figure out which of the sources has priority...

         -- And yeah, log any of those failures...
         EXECUTE logger_add_event @SBV_build_ID, 2, 'B02: Subscriber IDs with multiple accounts!', coalesce(@QA_catcher, -1)
     end
     commit

     -- Okay, now we can ; put those things into the main table:

     insert into vespa_analysts.vespa_single_box_view (
         subscriber_id
         ,card_subscriber_id
         ,account_number
     )
     select distinct
       convert(decimal(10), card_subscriber_id)
       ,card_subscriber_id
       ,account_number
     from  #duplicated_stb_population
     where convert(decimal(10), card_subscriber_id) is not null

     commit

     -- Things we no longer need:
     drop table #duplicated_stb_population
     drop table #skyview_accounts

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B02: Complete! (Population formed)', coalesce(@QA_catcher, -1)
     commit

     /****************** B03: ADD VESPA PANEL REQUEST MARKS ******************/

     -- OK, this list is more of a request that a confirmation (in terms of actual
     -- business flow Open->Closed loop), but at a time we were usig it to confirm
     -- Vespa while other things were slightly borked. The theme is that someone
     -- requesting the box through campaign elements confirms that the box we're
     -- seeing in the database should be there in our panel.

     -- OK, so now the big list is made, let's mark what's Vespa
     update vespa_analysts.vespa_single_box_view
     set Panel_ID_4_cells_confirm = 1            -- haha it's no longer about 4 (12?), but it's a confirmation
         ,Selection_date = tvb.writeback_datetime
     from vespa_analysts.vespa_single_box_view
     inner join #tagged_vespa_boxes as tvb
     on vespa_analysts.vespa_single_box_view.card_subscriber_id = tvb.card_subscriber_id

     commit

     -- So now creating from the subscriber history table a list of boxes whose last status was panel 12 (regardless they are disabled or enabled)
     -- to get last date of writeback_datetime value (panel_id derivation is done further in the code)...

     select  n.*
             ,vssh2.panel_no
     into    #subs_hist_vespa_boxes
     from    (
             select  sbv.card_subscriber_id
                     ,max(vssh.writeback_datetime) as writeback_datetime
             from    vespa_analysts.vespa_single_box_view as sbv
                     inner join vespa_subscriber_status_hist as vssh
                     on  sbv.card_subscriber_id = vssh.card_subscriber_id
             group   by  sbv.card_subscriber_id) as n
             inner join  vespa_subscriber_status_hist as vssh2
             on  n.card_subscriber_id = vssh2.card_subscriber_id
             and n.writeback_datetime = vssh2.writeback_datetime
     where vssh2.panel_no in (11,12)  -- cortb added panel 11 - 17/01/2014


     update  vespa_analysts.vespa_single_box_view
     set     Panel_ID_4_cells_confirm = 1
             ,Selection_date = shvb.writeback_datetime
     from    vespa_analysts.vespa_single_box_view as sbv
             inner join #subs_hist_vespa_boxes as shvb
             on sbv.card_subscriber_id = shvb.card_subscriber_id
     where   sbv.selection_date is null


     commit

     drop table #tagged_vespa_boxes
     drop table #subs_hist_vespa_boxes

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where Panel_ID_4_cells_confirm = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B03: Complete! (Olive box marks)', coalesce(@QA_catcher, -1)
     commit

     /****************** B04: VESPA DATABASE STATUS FLAGS ******************/

     -- We also want enablement date & status from the Vespa table:
     update vespa_analysts.vespa_single_box_view
     set Status_Vespa        = vss.result
         ,vss_request_dt     = case when vss.result = 'Enabled' then convert(date, vss.request_dt) else null end
         ,vss_created_date   = case when vss.result = 'Enabled' then convert(date, vss.created_dt) else null end
     from vespa_analysts.vespa_single_box_view
     inner join VESPA_SUBSCRIBER_STATUS as vss
     on vespa_analysts.vespa_single_box_view.card_subscriber_id = vss.card_subscriber_id

     commit

     -- Then there's also stuff (panel_ID etc) we wand from the log snapshot:

     /* ANGELD [READY TO QA]: So on this bit there is a lot to say. VESPA_STB_LOG_SNAPSHOT table needs to be
     changed to vespa_stb_log_summary, but the cool thing is that currently (03/10/2012) the
     panel_id field is completely empty (Sarcasm), so we need to work around a way to get the panel_id
     values at a box level meanwhile we se what's ;ing on with the summary table...

     The final query after we sort out these null fields should looks like below:

     update vespa_single_box_view
     set
         In_stb_log_snapshot = 1
         ,Panel_ID_Vespa = vsls.panel_id
     from vespa_analysts.vespa_single_box_view
     inner join vespa_stb_log_summary as vsls
     on vespa_single_box_view.subscriber_id = vsls.subscriber_id

     Meanwhile, we would have to ; with below work-around:
     */
     /*-----------------------------------------------------------------------------------------------------------------*/

     update vespa_analysts.vespa_single_box_view
     set
         In_stb_log_snapshot = 1
         ,Panel_ID_Vespa = vss.panel_no
     from vespa_analysts.vespa_single_box_view as sbv
      inner join VESPA_SUBSCRIBER_STATUS as vss on sbv.card_subscriber_id = vss.card_subscriber_id

     /*-----------------------------------------------------------------------------------------------------------------*/


     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where In_stb_log_snapshot = 1 or Status_Vespa is not null

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B04: Complete! (Vespa DB marks)', coalesce(@QA_catcher, -1)
     commit

     /****************** B05: SKY VIEW PANEL FLAGS DETAILS ******************/

     -- Finally we want to ; over to the Sky View panel stuff and pull out what's
     -- over there:
     update vespa_analysts.vespa_single_box_view
     set Is_Sky_View_candidate = 1
         ,Sky_View_load_date = cb_change_date -- This will get overwritten for verified members, but that's okay & intended
     from vespa_analysts.vespa_single_box_view
     inner join vespa_sky_view_panel as vsvp
     on vespa_analysts.vespa_single_box_view.account_number = vsvp.account_number

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where Is_Sky_View_candidate = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B05: Midway (Sky View enabled)', coalesce(@QA_catcher, -1)
     commit

     -- Oh, and then the marks about boxed selected for data return:
     update vespa_analysts.vespa_single_box_view
     set
         Is_Sky_View_Selected = 1
         ,Sky_View_load_date  = vsvm.load_date
     from vespa_analysts.vespa_single_box_view
     inner join vespa_analysts.verified_Sky_View_members as vsvm
     on vespa_analysts.vespa_single_box_view.subscriber_id = vsvm.subscriber_id

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where Is_Sky_View_Selected = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B05: Complete! (Sky View selected)', coalesce(@QA_catcher, -1)
     commit

     /****************** B06: PANEL 4 DECISION BY ENABLEMENT DATE ******************/

     -- So there are a lot of boxes which say Enabled in the Vespa DB, but with old
     -- enablement dates; active panel really should be enabled after October 2010...

     -- Is this actually what we want to be doing? I think not. This line is discontinued.

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B06: Discontinued.', coalesce(@QA_catcher, -1)
     commit

     /****************** B07: ACTIVE DTV MARKS & UK STANDARD ACCOUNTS ******************/

     -- We don't actually know if the boxes on our panel have active DTV marks or not,
     -- but it's kind of relevant yes?

     update vespa_analysts.vespa_single_box_view
     set CUST_ACTIVE_DTV = sav.CUST_ACTIVE_DTV
         ,uk_standard_account = case when sav.acct_type='Standard' and sav.account_number <>'?' and sav.pty_country_code ='GBR' then 1
             else 0 end
         -- Oh, and also, the individual's key since we're lready joining to SAV here
         ,cb_key_individual = sav.cb_key_individual
     from vespa_analysts.vespa_single_box_view
     inner join cust_single_account_view as sav
     on vespa_analysts.vespa_single_box_view.account_number = sav.account_number

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where CUST_ACTIVE_DTV = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B07: Complete! (Active DTV subs)', coalesce(@QA_catcher, -1)
     commit

     /****************** B08: ACTIVE BOX FLAGS ON PANEL 12 ******************/

     -- First off, the rules for Open Loop enablement:
     -- update vespa_single_box_view
     -- set Open_loop_enabled      = Panel_ID_4_cells_confirm & CUST_ACTIVE_DTV & uk_standard_account
     -- and then Closed Loop enablement:
         --angeld: This field I delelted ,Closed_loop_enabled = Panel_ID_4_cells_confirm & CUST_ACTIVE_DTV & uk_standard_account & case when Status_Vespa in ('Enabled', 'DisablePending', 'EnablePending', 'Trumped') then 1 else 0 end
      -- ##22## Now handling 'EnablePending' and (now 'Trumped' too) as the special case during the panel 4 -> 12 migration
     -- and then Sky_View selected: actually, we're just refering to the Is_Sky_View_Selected column so no update

     -- These rules are a it tentative at the moment, but we should really get them
     -- signed off (and then partition the exceptions and count them in the testing
     -- script, eh?)

      -- angeld: This replace above
     update vespa_analysts.vespa_single_box_view
     set in_vespa_panel = 1
     from vespa_analysts.vespa_single_box_view as sbv inner join vespa_subscriber_status as vss
     on sbv.card_subscriber_id = vss.card_subscriber_id
     where vss.panel_no = 12

     commit
  
  -- cortb added this flag to capture panel 11 - 17/01/2014
  update vespa_analysts.vespa_single_box_view
     set in_vespa_panel_11 = 1
     from vespa_analysts.vespa_single_box_view as sbv inner join vespa_subscriber_status as vss
     on sbv.card_subscriber_id = vss.card_subscriber_id
     where vss.panel_no = 11
  
  commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where in_vespa_panel_11 = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B08: Panel 11 Complete! (Canonical enablement)', coalesce(@QA_catcher, -1) -- cortb added logger for panel 11 - 17/01/2014
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where in_vespa_panel = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B08: Panel 12 Complete! (Canonical enablement)', coalesce(@QA_catcher, -1) -- cortb adapted logger label to show panel 12 - 17/01/2014
     commit

     /****************** B09: FLAGS FOR ALTERNATE PANELS 6 & 7 ******************/

     -- Not that we even have data for these guys yet... ##14##
     update   vespa_analysts.vespa_single_box_view
     set   alternate_panel_5 = case when panel_no = 5 then 1 else 0 end   -- cortb added this to include panel 5 - 17/01/2014
  ,alternate_panel_6  = case when panel_no = 6 then 1 else 0 end    -- currently just with placeholders for the time being
        ,alternate_panel_7  = case when panel_no = 7 then 1 else 0 end
     from   vespa_analysts.vespa_single_box_view as sbv
     inner join  vespa_subscriber_status as vss
     on    sbv.card_subscriber_id = vss.card_subscriber_id and vss.panel_no in (5,6,7)

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where alternate_panel_5 = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B09: Size of ALT 5 on VSS', coalesce(@QA_catcher, -1)  -- cortb added this to show panel 5 - 17/01/2014
     commit
  
  set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where alternate_panel_6 = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B09: Size of ALT6 on VSS', coalesce(@QA_catcher, -1)  -- cortb adapted this to split 6 & 7 - 17/01/2014
     commit
  
  set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where alternate_panel_7 = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B09: Size of ALT7 on VSS', coalesce(@QA_catcher, -1)  -- cortb adapted this to split 6 & 7 17/01/2014
     commit

     /****************** B10: POPULATION CULLING ******************/

     -- We're having some trouble with server capacity and temp space errors, so we're ;ing
     -- to reduce SBV only to the boxes which are active on one of the panels.

     /* We're hoping to avoid this if possible...

     delete from vespa_single_box_view
     where Open_loop_enabled = 0 and Closed_loop_enabled = 0 and Is_Sky_View_Selected = 0

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_single_box_view

     EXECUTE logger_add_event @SBV_build_ID, 3, 'B10: Complete! (Cleanse population)', coalesce(@QA_catcher, -1)
     commit
     */

     -- Thing is, this appears largely dependent on other server activity, but until we
     -- have a scheduler available to do this after hours, we're kind of bound by it.

     /****************** B11: CANONICAL PANEL MEMBERSHIP FLAG ******************/

     -- Okay, so we want a single flag which tells us which panel each box is on.
     /*update vespa_single_box_view
     set panel = case
         when Closed_loop_enabled + Is_Sky_View_Selected + alternate_panel_6 + alternate_panel_7 > 1 then 'CLASH!'
         when Closed_loop_enabled=1  and (Panel_ID_Vespa is null or Panel_ID_Vespa in (4,12)) then 'VESPA'    -- Need the NULLs to tolerate boxes that haven't returned data yet
         when Closed_loop_enabled=1  then 'CLASH!'                                                      -- Anything still flagged has some other data return key, which is bad
         when Is_Sky_View_Selected=1 and (Panel_ID_Vespa is null or Panel_ID_Vespa = 1) then 'SKYVIEW'
         when Is_Sky_View_Selected=1 then 'CLASH!'
         when alternate_panel_6=1    and (Panel_ID_Vespa is null or Panel_ID_Vespa = 6) then 'ALT6'
         when alternate_panel_6=1    then 'CLASH!'
         when alternate_panel_7=1    and (Panel_ID_Vespa is null or Panel_ID_Vespa = 7) then 'ALT7'
         when alternate_panel_7=1    then 'CLASH!'
     end*/


     update  vespa_analysts.vespa_single_box_view
        set  panel =
       case
        when in_vespa_panel = 1   and (Panel_ID_Vespa is null or Panel_ID_Vespa in (4,12)) then 'VESPA'    -- Need the NULLs to tolerate boxes that haven't returned data yet
  when in_vespa_panel_11 = 1 and (Panel_ID_Vespa is null or Panel_ID_Vespa = 11) then 'VESPA11'      -- cortb added this to include panel 11 - 17/01/2014
  when alternate_panel_5 = 1   and (Panel_ID_Vespa is null or Panel_ID_Vespa = 5) then 'ALT5'     -- cortb added this to include panel 5 - 17/01/2014
        when alternate_panel_6 = 1   and (Panel_ID_Vespa is null or Panel_ID_Vespa = 6) then 'ALT6'
        when alternate_panel_7 = 1   and (Panel_ID_Vespa is null or Panel_ID_Vespa = 7) then 'ALT7'
        when Is_Sky_View_Selected = 1  and (Panel_ID_Vespa is null or Panel_ID_Vespa = 1) then 'SKYVIEW'
        else @txt_clash
       end

     -- Boxes with NULL panels will include the Sky View Candidates that aren't accepted, plus
     -- Open Loop Vespa enablements. Known CLASH'ers include those guys who are still reporting
     -- under panel_ID = 5 but are somehow also tagged as Vespa. Which means they're Open Loop
     -- enabled as well, but hey. They were kind of known, but definately want to be flagged as
     -- errors. Update: Dunno what this will do with the panel 12 migration, but we're not
     -- excluding panel 4 yet.

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where panel is not null and panel <> 'CLASH!'

     commit
     EXECUTE logger_add_event @SBV_build_ID, 3, 'B11: Complete! (Canonical panel)', coalesce(@QA_catcher, -1)
     commit

     /****************** C01: PRIMARY / SECONDARY FLAGS FROM OLIVE ******************/

     select
         b.service_instance_id
         ,convert(integer,min(si_external_identifier)) as subscriber_id -- should be unique per service instance ID?
         ,convert(bit, max(case when si_service_instance_type = 'Primary DTV' then 1 else 0 end)) as primary_box
         ,convert(bit, max(case when si_service_instance_type = 'Secondary DTV (extra digiboxes)' then 1 else 0 end)) as secondary_box
     into #subscriber_details
     from CUST_SERVICE_INSTANCE as b
     inner join vespa_analysts.vespa_single_box_view as sbv
     on sbv.card_subscriber_id = b.si_external_identifier -- Hopefully this helps us avoid temp space errors?
     where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
     and @profiling_day between effective_from_dt and effective_to_dt
     group by b.service_instance_id

     commit
     create index for_stuff on #subscriber_details (subscriber_id)
     commit

     -- Then push those box types onto the subscriber level summary
     update vespa_analysts.vespa_single_box_view
     set
         service_instance_id = b.service_instance_id
         ,PS_Olive = case
             when b.subscriber_id is null then 'U'
             when b.primary_box = 1 and secondary_box = 0 then 'P'
             when b.primary_box = 0 and secondary_box = 1 then 'S'
             else '?' end -- Ambiguous thiungs get marked differently to missing things...
     from vespa_analysts.vespa_single_box_view as sbv
     left outer join #subscriber_details as b
     on sbv.subscriber_id = b.subscriber_id

     commit

     drop table #subscriber_details
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view where PS_Olive in ('P', 'S')

     EXECUTE logger_add_event @SBV_build_ID, 3, 'C01: Complete! (Olive P/S)', coalesce(@QA_catcher, -1)
     commit

     /****************** C02: PRIMARY / SECONDARY FLAGS FROM VESPA DB ******************/

     -- In the Vespa DB, all the P/S flags live on the STB log snapshot table...

     /*ANGELD: ok cool so now to say whether a box is flagged as P or S in Vespa side we then need to refer again to the
     vespa_stb_log_summary (which is replacing stb_log_snapshoot from phase 1)... again until today (03/10/2012) the issue
     regarding having null values throughout the fields we need in this table persist, hence we are doing a work around...

     However, once the null values issue gets fixed the code should be as below:

     update vespa_single_box_view
     set In_stb_log_snapshot = 1
         ,PS_Vespa = sls.service_instance_type
     from vespa_single_box_view
     inner join vespa_stb_log_summary as sls on vespa_single_box_view.subscriber_id = sls.subscriber_id

     But in the mean time, we need to do as below:*/

     -- getting the most updated record for each box( in this table identified as si_external_identifier)
     -- we want a fresh start -- section below adapted by cortb (12/02/2014)
     if object_id('vespa_analysts.csi') is not null
        truncate table vespa_analysts.csi
    commit

    select si_external_identifier
           ,si_service_instance_type
           ,effective_from_dt
      into #sbvcsi
      from cust_service_instance as spcsi
           inner join vespa_analysts.vespa_single_box_view as sbv on sbv.card_subscriber_id = spcsi.si_external_identifier
     where si_service_instance_type like '%DTV%'
       and si_external_identifier <> '-1'

    insert into vespa_analysts.csi(
           si_external_identifier
           ,rank_
           ,si_service_instance_type
           )
    select si_external_identifier
           ,rank() over(partition by si_external_identifier order by effective_from_dt desc)
           ,si_service_instance_type
      from #sbvcsi
   
   drop table #sbvcsi

    update vespa_analysts.vespa_single_box_view
       set In_stb_log_snapshot = 1
           ,PS_Vespa = left(csi.si_service_instance_type,1) -- This is to get either P or S rather than Primary or Secondary...
      from vespa_analysts.csi as csi
     where csi.si_external_identifier = vespa_analysts.vespa_single_box_view.card_subscriber_id
       and csi.rank_ = 1
  
  -- cortb (13/02/2014) commented this out to correct an update resource error
 /*select si_external_identifier
           ,rank() over(partition by si_external_identifier order by effective_from_dt desc) as rank_
           ,si_service_instance_type
       into #csi
       from cust_service_instance
      where si_service_instance_type like '%DTV%'

     update vespa_single_box_view
        set In_stb_log_snapshot = 1
           ,PS_Vespa = left(csi.si_service_instance_type,1) -- This is to get either P or S rather than Primary or Secondary...
       from vespa_single_box_view as sbv
            inner join #csi as CSI on sbv.card_subscriber_id = CSI.si_external_identifier
                                  and CSI.rank_ = 1
          */

     /*---------------------------------------------------------------------------------------------------------------*/

     -- going to have a lot of 'U' items, but that's fine
     update vespa_analysts.vespa_single_box_view
     set PS_Vespa = 'U'
     where PS_Vespa is null or PS_Vespa = ''


     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view where PS_Vespa in ('P', 'S')

     EXECUTE logger_add_event @SBV_build_ID, 3, 'C02: Complete! (Vespa P/S)', coalesce(@QA_catcher, -1)
     commit

     /****************** C03: INFERRING THE P/S FLAG ON SINGLE BOX ACCOUNTS ******************/

     -- Okay, so if we don't have a P/S flag yet, and there's only one box associated with
     -- the account, and the account doesn't have a multiroom subscription, that'd make
     -- it a P box, yes?

     select account_number
         ,convert(bit, 0) as has_MR
     into #maybe_single_households
     from vespa_analysts.vespa_single_box_view
     group by account_number
     having count(1) = 1 and sum(case when PS_Vespa = 'U' and PS_Olive = 'U' then 1 else 0 end)=1

     -- So this should get us to a pretty concise population that shouldn't take too long to process...

     commit
     create unique index fake_pk on #maybe_single_households (account_number)
     commit

     -- ok, so now let's figure out which have MR and which have multiple associated boxes...
     update #maybe_single_households
     set has_MR = 1
     from #maybe_single_households
     inner join cust_single_account_view as csh
     on #maybe_single_households.account_number = csh.account_number
     where prod_active_multiroom = 1

     commit

     -- Now we have the marks, put them back on SBV; can just join by account number, by
     -- construction these are households with only one box

     update vespa_analysts.vespa_single_box_view
     set PS_inferred_primary = 1
     from vespa_analysts.vespa_single_box_view
     inner join #maybe_single_households as msh
     on vespa_analysts.vespa_single_box_view.account_number = msh.account_number
     where msh.has_MR = 0

     commit
     drop table #maybe_single_households

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view where PS_inferred_primary = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'C03: Complete! (Inferred P/S)', coalesce(@QA_catcher, -1)
     commit

     /****************** C04: A CANONICAL PRIMARY / SECONDARY FLAG TO USE ******************/

     -- Okay, so the rules for this are easy; if Vespa and Olive agree, use that mark. If one
     -- knows and the other doesn't use the one that knows. If they disagree about P and S,
     -- flag it as a problem.

     update vespa_analysts.vespa_single_box_view
     set
     PS_flag = case
         when PS_Olive = PS_Vespa and PS_Olive <> 'U' then PS_Olive
         when PS_inferred_primary = 1 then 'P' -- Only populated for the questionable boxes
         when PS_Olive = 'U' or PS_Olive is null then PS_Vespa
         when PS_Vespa = 'U' or PS_Vespa is null then PS_Olive
         else '!'    -- this should only leave the case where one of Olive / Vespa says 'P' and the other says 'S'
       end
     ,PS_source = case
         when PS_Olive = PS_Vespa and PS_Olive <> 'U' then 'Both agree'
         when PS_inferred_primary = 1 then 'Inferred'
         when PS_Vespa = 'U' or PS_Vespa is null then 'Olive'
         when PS_Olive = 'U' or PS_Olive is null then 'Vespa'
         else 'Collision!'
       end

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view where PS_flag in ('P', 'S')

     EXECUTE logger_add_event @SBV_build_ID, 3, 'C04: Complete! (Canonical P/S)', coalesce(@QA_catcher, -1)
     commit

     /****************** D01: GET HISTORIC ENABLEMENT DATES ******************/

     -- Hey we kind of want to check that there are no subsequent disables here. And we
     -- also want the account number, to check that the most recent box detail is actually
     -- relevant to where it's currently deployed (we've been burnt by that before...)

     -- Oh boy, we've *still* ;t duplicates in here, *again*, meaning we have to do something
     -- completely different. Might still break next week too, and that'd be just swell.

     select
         account_number
         ,card_subscriber_id
         ,result
         ,coalesce(request_dt, modified_dt) as request_dt
         ,rank() over (partition by account_number, card_subscriber_id order by request_dt desc, modified_dt desc, created_dt desc) as most_recent
     into #prior_enablements
     from vespa_subscriber_status_hist
     where result in ('Enabled', 'Disabled')

     -- We don't want enablements that have a more recent disable. We also don't want to mark
     -- the enablement of a subscriber if it's since been moved to a different account.

     commit
     delete from #prior_enablements
     where most_recent <> 1
     or result = 'Disabled'

     commit
     create unique index fake_pk on #prior_enablements (card_subscriber_id, account_number)
     commit

     -- Now we have the most recent historic enablements, stitch those onto the current view...

     update vespa_analysts.vespa_single_box_view
     set historic_result_date = convert(date, pe.request_dt)
     from vespa_analysts.vespa_single_box_view
     inner join #prior_enablements as pe
     on vespa_analysts.vespa_single_box_view.card_subscriber_id = pe.card_subscriber_id
     and vespa_analysts.vespa_single_box_view.account_number = pe.account_number

     commit
     drop table #prior_enablements
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where historic_result_date is not null

     EXECUTE logger_add_event @SBV_build_ID, 3, 'D01: Complete! (Historic enablement dates)', coalesce(@QA_catcher, -1)
     commit

     /****************** D02: CANONICAL ENABLEMENT DATES ******************/

     -- Now that we have all the dates, we figure out which is the most important of each:

     update vespa_analysts.vespa_single_box_view
     set Enablement_date = case
         when vss_request_dt         is not null and Status_Vespa = 'Enabled'    then vss_request_dt         -- If the box doesn't say 'Enabled' then there should be a historic enablement to fall back with
         when Sky_View_load_date     is not null                                 then Sky_View_load_date
         when historic_result_date   is not null                                 then historic_result_date
         when Selection_date         is not null                                 then Selection_date
         when vss_created_date       is not null                                 then vss_created_date
     end
     ,Enablement_date_source = case
         when vss_request_dt         is not null and Status_Vespa = 'Enabled'    then 'vss_request_dt'
         when Sky_View_load_date     is not null                                 then 'Sky View'
         when historic_result_date   is not null                                 then 'historic'
         when Selection_date         is not null                                 then 'writeback'
         when vss_created_date       is not null                                 then 'vss_created_dt'
     end

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where Enablement_date is not null

     EXECUTE logger_add_event @SBV_build_ID, 3, 'D02: Complete! (Enablement date decision)', coalesce(@QA_catcher, -1)
     commit

     /****************** E01: LONG TERM DATA RETURN QUALITIES ******************/

     -- So we might be calculating this slightly differently than on the Operational
     -- dashboard, because we care not about when the dialback happens, but when it
     -- happens at all, is it ;od enough to scale with reliably, etc. We're also doing
     -- it by 6AM switcharound, since that's what BARB and our existing scaling builds
     -- do.

     declare @scanning_day       date
     declare @event_from_date  integer
     declare @event_to_date   integer

     set @event_from_date  = convert(integer,dateformat(dateadd(day, -60, @profiling_day),'yyyymmddhh'))  -- YYYYMMDD00
     set @event_to_date   = convert(integer,dateformat(dateadd(day,1,@profiling_day),'yyyymmdd')+'23') -- YYYYMMDD23

     delete from vespa_analysts.Vespa_SBV_logs_dump

     commit

     -- Get data from main events table
      insert into Vespa_analysts.Vespa_SBV_logs_dump
      select subscriber_id
             ,  dt
         from    vespa_analysts.panel_data
         where   dt >= dateadd(day, -60, @profiling_day)
         and     dt <= dateadd(day,1,@profiling_day)
         and     panel in (5,6,7)   -- cortb included panel 5 in filter - 17/10/2014
   and  data_received = 1
         group   by  subscriber_id
                 , dt


     commit

      insert into Vespa_analysts.Vespa_SBV_logs_dump
         select subscriber_id
        ,convert(date, dateadd(hh, -6, log_received_start_date_time_utc))
      from VESPA_DP_PROG_VIEWED_CURRENT
      where dk_event_start_datehour_dim >= @event_from_date
      and  dk_event_start_datehour_dim <= @event_to_date
      and  panel_id in (4,11,12)   -- cortb included panel 11 in filter - 17/01/2014
      group  by  subscriber_id
         ,log_received_start_date_time_utc
     /*
     union all
      select subscriber_id
        ,convert(date, dateadd(hh, -6, log_received_start_date_time_utc))
      from VESPA_DP_PROG_NON_VIEWED_CURRENT
      where dk_event_start_datehour_dim >= @event_from_date
      and  dk_event_start_datehour_dim <= @event_to_date
      and  panel_id in (4,12)
      group  by  subscriber_id
         ,log_received_start_date_time_utc
     */

     -- the 'EmptyLog' events still have event times, so that's cool

     -- We only need -30 for this section, but need a bit more for E02 because that
     -- looks at when a box first started reporting

     -- clip out those logs that we picked up when scanning the following day for empty logs
     -- relevant to the last day in our period
     delete from Vespa_analysts.Vespa_SBV_logs_dump where doc_date_from_6am > @profiling_day

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from Vespa_analysts.Vespa_SBV_logs_dump

     EXECUTE logger_add_event @SBV_build_ID, 3, 'E01: Midway (All logs sucked)', coalesce(@QA_catcher, -1)
     commit

     -- Cool, now we can summarise this into the number of reported days...
     select subscriber_id, count(distinct doc_date_from_6am) as log_counts
     into #Vespa_SBV_logs_summary
     from Vespa_analysts.Vespa_SBV_logs_dump
     where doc_date_from_6am > dateadd(day, -30, @profiling_day)
     group by subscriber_id

     commit
     create unique index fake_PK on #Vespa_SBV_logs_summary (subscriber_id)
     commit
     -- and then stitch the relevant columns into SBV
     update vespa_analysts.vespa_single_box_view
     set logs_every_day_30d      = case when log_counts > 29 then 1 else 0 end
         ,logs_returned_in_30d    = log_counts
     from vespa_analysts.vespa_single_box_view
     inner join #Vespa_SBV_logs_summary as sls
     on vespa_analysts.vespa_single_box_view.subscriber_id = sls.subscriber_id

     commit
     -- we need a similar but slightly different thing for the box reporting metric,
     -- this exact structure isn't so useful
     drop table #Vespa_SBV_logs_summary
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where logs_every_day_30d = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'E01: Complete (Recent reporting)', coalesce(@QA_catcher, -1)
     commit

     /****************** E02: BOX REPORTING QUALITY METRIC ******************/

     -- So this is a metric which is mostly based on the 30 days returning thing, but
     -- also takes the enablement date into account, so that now boxes aren't prenalised
     -- because they haven't had the opportunity to report enough to be considered.

     -- First of all, figure out the earliest reporting we have for each box, because
     -- that's where we want to start counting logs from...
     select subscriber_id
         ,min(doc_date_from_6am) as start_scan
     into #first_reports
     from Vespa_analysts.Vespa_SBV_logs_dump
     group by subscriber_id

     commit
     create unique index fake_PK on #first_reports (subscriber_id)
     commit

     -- ... if that was less than 30 days a;
     update #first_reports
     set start_scan = case
         when start_scan > dateadd(day, -30, @profiling_day) then start_scan
         else dateadd(day, -29, @profiling_day)
     end

     commit

     -- OK, so there might be a few minor errors relating to boxes that were reporting for a
     -- different account and then repurposed for a new account, so we'll also check the scan
     -- date against the enablement date for each box:
     update #first_reports
     set start_scan = case
         when start_scan > enablement_date then start_scan
         else enablement_date
     end
     from #first_reports
     inner join vespa_analysts.vespa_single_box_view as sbv
     on #first_reports.subscriber_id = sbv.subscriber_id

     commit

     select fr.subscriber_id
           ,count(distinct doc_date_from_6am) as relevant_logs
           ,case when sbv.panel_id_vespa in (11,12,5) then count(distinct doc_date_from_6am) / convert(float, datediff(day, start_scan, @profiling_day)+1)
                 when sbv.panel_id_vespa in (6,7)     then count(distinct doc_date_from_6am) / ((convert(float, datediff(day, start_scan, @profiling_day)+1))/2)
            end as reporting_quality
         -- ,case -- Only want to rate boxes that first reported at least 15 days a;
             -- when start_scan <= dateadd(day, -15, @profiling_day) then 1
             -- else 0
         -- end as ;od_for_judgement
       into #relevant_log_counts
       from #first_reports as fr
            inner join Vespa_analysts.Vespa_SBV_logs_dump   as ld  on fr.subscriber_id = ld.subscriber_id
                                                                  and fr.start_scan <= ld.doc_date_from_6am
            inner join vespa_analysts.vespa_single_box_view as sbv on fr.subscriber_id = sbv.subscriber_id
   group by fr.subscriber_id, start_scan, sbv.panel_id_vespa

     commit
     create unique index fake_PK on #relevant_log_counts (subscriber_id)
     commit
     -- OK, now we no longer need these tables
     drop table #first_reports
     delete from Vespa_analysts.Vespa_SBV_logs_dump

     commit
     -- Now we can patch these metrics into SBV, though we have to be slightly careful
     -- to get the right fallbacks:
     update vespa_analysts.vespa_single_box_view
  set reporting_quality = round(rlc.reporting_quality,3)
     -- set reporting_quality = case
         -- when ;od_for_judgement = 1                                 then round(rlc.reporting_quality,3)
         -- when ;od_for_judgement = 0                                 then null -- It's reporting, but not enough to judge yet
         -- when enablement_date <= dateadd(day, -15, @profiling_day)   then 0 -- If a box was enabled at least 15 days a; and hasn't returned data, it's no ;od
         -- else                                                        null -- remaining boxes haven't returned data and haven't been enabled long enough to judge them
     -- end
     from vespa_analysts.vespa_single_box_view
     left join #relevant_log_counts as rlc
     on vespa_analysts.vespa_single_box_view.subscriber_id = rlc.subscriber_id

     commit
     drop table #relevant_log_counts

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where reporting_quality > 0

     EXECUTE logger_add_event @SBV_build_ID, 3, 'E02: Complete (Reporting quality)', coalesce(@QA_catcher, -1)
     commit

     /*****************
     STB_ACTIVE table creation.  Basically creates the one sub-table that takes care of all queries that use cust_set_top_box
     ******************/

     select * into #stb_active
     from
     (
      select account_number
            ,service_instance_id
            ,active_box_flag
            ,box_installed_dt
            ,box_replaced_dt
            ,x_pvr_type
            ,x_anytime_enabled
            ,current_product_description
            ,x_anytime_plus_enabled
            ,x_box_type
            ,CASE WHEN x_description like '%HD%2TB%' THEN 1 ELSE 0 END AS HD2TB
            ,CASE WHEN x_description like '%HD%1TB%' THEN 1 ELSE 0 END AS HD1TB
            ,CASE WHEN x_description like '%HD%'     THEN 1 ELSE 0 END AS HD
            ,x_manufacturer
            ,x_description
            ,x_model_number
            ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) as active_flag
        from cust_set_top_box
       ) as t
       where active_flag = 1

     commit

     create index #stb_active_accnum on #stb_active (account_number)
     create index #stb_active_siid   on #stb_active (service_instance_id)

     /*****************
     STB_ACTIVE table creation end
     ******************/


     /****************** G01: BOX TYPE (BY SUBSCRIPTION) ******************/

     -- Box type nicked from the OpDash build (it's cool, we'll eventually just redirect
     -- OpDash actions to this table)

     --Creates a list of accounts with active HD capable boxes
       SELECT stb.account_number
             ,max(HD) AS HD
             ,max(HD1TB) AS HD1TB
             ,max(HD2TB) as HD2TB
         INTO #hda -- drop table #hda
         FROM #stb_active AS stb
              INNER JOIN vespa_analysts.vespa_single_box_view AS acc on stb.account_number = acc.account_number
     GROUP BY stb.account_number
     -- Acutally, the bottleneck could be either the following (now refactored) one, or
     -- otherwise this one just passed...

     commit
     CREATE UNIQUE hg INDEX idx2 ON #hda(account_number)

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
            ,max(CASE  WHEN #hda.HD2TB = 1            THEN 1 ELSE 0  END) AS HD2TBstb
            ,convert(varchar(30), null) as box_type
       INTO #box_type
       FROM cust_subs_hist AS csh
            INNER JOIN vespa_analysts.vespa_single_box_view AS acc ON csh.account_number = acc.account_number --< Limits to your universe
            LEFT OUTER JOIN cust_entitlement_lookup cel
                            ON csh.current_short_description = cel.short_description
            LEFT OUTER JOIN #hda ON csh.account_number = #hda.account_number --< Links to the HD Set Top Boxes
      WHERE csh.effective_FROM_dt <= @profiling_day
        AND csh.effective_to_dt    > @profiling_day
        AND csh.status_code IN  ('AC','AB','PC')
        AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
        AND csh.effective_FROM_dt <> csh.effective_to_dt
     GROUP BY csh.account_number
     HAVING TV = 1

     commit
     create unique index maybe_fake_pk on #box_type (account_number)
     commit

     update #box_type
     set box_type =  CASE    WHEN HD =1 AND MR = 1 AND HD2TBstb = 1      THEN 'A) HD Combi 2TB'
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

    UPDATE vespa_analysts.vespa_single_box_view
 SET  box_type_subs = coalesce(bt.box_type, 'Unknown')
 from  vespa_analysts.vespa_single_box_view as csb
   left join #box_type as bt 
   on csb.account_number = bt.account_number

     -- Wait... the join ;es on account number? But.. it's *box* type... why aren't we joining
     -- on service instance ID or something like that? That's what's in the wiki, so w/e. Update:
     -- because this is the box type by subscription. The physical box type does come from
     -- cust_set_top_box and we'll do that next. Though this guy still brings HDx into it, and
     -- that should probably be a separate flag or something?

     commit
     drop table #hda
     drop table #box_type
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view where box_type_subs <> 'Unknown'

     EXECUTE logger_add_event @SBV_build_ID, 3, 'G01: Midway. (Box type - subscription)', coalesce(@QA_catcher, -1)
     commit

     /****************** G01.5: BOX TYPE BY PHYSICAL BOX TYPE ******************/

     -- A new physical box type, we've reorganised how we're prioritising and
     -- deduplicating the stuff.

     select b.service_instance_id
           ,b.x_box_type
           ,b.box_installed_dt
           ,b.active_flag
           ,case when b.box_installed_dt <= @profiling_day and b.box_replaced_dt > @profiling_day then 1 else 0 end as apparently_active
           ,b.active_flag as rankage
       into #deduped_list
       from #stb_active as b
            inner join vespa_analysts.vespa_single_box_view as vsd on vsd.service_instance_id = b.service_instance_id

     -- we use the ph_non_subs_link_sk per service_instance_id to --define an active box

     --next statement should be redundant now as we are
     --defining active box in #stb_active creation table
     --delete from #deduped_list where rankage > 1

     commit
     create unique index src_index on #deduped_list (service_instance_id)

     update vespa_analysts.vespa_single_box_view
        set Box_type_physical = b.x_box_type
       from vespa_analysts.vespa_single_box_view as sbv
            left outer join #deduped_list as b on sbv.service_instance_id = b.service_instance_id

     -- It's not used again:
     drop table #deduped_list

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view where Box_type_physical is not null

     EXECUTE logger_add_event @SBV_build_ID, 3, 'G01.5: Complete! (Physical box type)', coalesce(@QA_catcher, -1)
     commit

     /****************** G02: ANYTIME + FLAGS ******************/

     -- This code comes to us from Philip Rimmer in Decisioning, though we've since added
     -- the effective dates check... because that's sensible, no?
     update vespa_analysts.vespa_single_box_view a
     set Account_anytime_plus = 1
     from cust_subs_hist    b
     where a.account_number = b.account_number
     and subscription_sub_type = 'PDL subscriptions'
     AND    status_code = 'AC'
     AND   @profiling_day between effective_from_dt and effective_to_dt

     commit
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view where Account_anytime_plus = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'G02: Midway.. (Anytime+ accounts)', coalesce(@QA_catcher, -1)
     commit

     -- And now the same for the Anytime+ box version...
     select convert(bigint, card_subscriber_id) as subscriber_id
     into #vespa_card_anytime_plus
     from cust_card_subscriber_link    c
     inner join #stb_active d
     on c.service_instance_id = d.service_instance_id
     where d.x_anytime_plus_enabled = 'Y'
     and c.current_flag = 'Y'

     commit
     create index subscriber_id_index on #vespa_card_anytime_plus (subscriber_id)
     commit

     /*
     update vespa_single_box_view
     set Box_has_anytime_plus = case when cap.subscriber_id is null then 0 else 1 end
     from vespa_single_box_view
     left join #vespa_card_anytime_plus as cap
     on vespa_single_box_view.subscriber_id = cap.subscriber_id
     */

     --TK – UPDATE BOX_HAS_ANYTIME_PLUS COLUMN

     update vespa_analysts.vespa_single_box_view a
     set Box_has_anytime_plus = 1
     where exists
     (select 1 from #vespa_card_anytime_plus b
     where a.subscriber_id = b.subscriber_id)

     commit

     update vespa_analysts.vespa_single_box_view
     set Box_has_anytime_plus = 0
     where Box_has_anytime_plus <> 1

     commit

     drop table #vespa_card_anytime_plus

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view where Box_has_anytime_plus = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'G02: Complete! (Anytime+ boxes)', coalesce(@QA_catcher, -1)
     commit

     /****************** G03: PVR FLAG ******************/

     -- PVR taken from Scaling build on the wiki, page version 21 timestamped 12/22/2011 2:43 PM
     -- Okay, though we streamlined it a bit so we're only pulling out the PvR items.

       SELECT csh.account_number
         INTO #PVR_lookup
          FROM cust_subs_hist as csh
              inner join #stb_active as stb on csh.service_instance_id = stb.service_instance_id
         WHERE csh.effective_from_dt <= @profiling_day
           AND csh.effective_to_dt > @profiling_day
           AND csh.subscription_sub_type in ('DTV Primary Viewing', 'DTV Extra subscription')
           AND stb.x_pvr_type like '%PVR%'
     GROUP BY csh.account_number

     commit -- this index wasn't in the wiki version either...
     create unique index fake_PK on #PVR_lookup (account_number)
     commit

       UPDATE vespa_analysts.vespa_single_box_view as bas
           SET bas.pvr = 1
          FROM #PVR_lookup
         WHERE bas.account_number = #PVR_lookup.account_number

     commit

     drop table #PVR_lookup

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view where pvr = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'G03: Complete! (PVR flag)', coalesce(@QA_catcher, -1)
     commit

     /****************** G04: HD FLAG  (BY SUBSCRIPTION) ******************/

     -- Code taken from the HD-obtaining bit of the Operational Dashboard;
     -- needs to be refined a bit, as there's again both box and subscription versions.

     update vespa_analysts.vespa_single_box_view
     set HD_box_subs = 0

     commit

     -- upgrading the way we flag HD subscription: now HD subscriptions are considered at account level rather than at box level...
     update  vespa_analysts.vespa_single_box_view
     set  HD_box_subs = 1
     where account_number in  (
             select distinct vsd.account_number
             from  vespa_analysts.vespa_single_box_view as vsd
               inner join cust_subs_hist as b
               on vsd.service_instance_id=b.service_instance_id
             where  subscription_sub_type='DTV HD' and  status_code in ('AC','AB','PC')
             and  b.effective_from_dt <= @profiling_day
             AND  b.effective_to_dt    > @profiling_day
            )

     -- That's the subscription version, though we've changed the HD subscription
     -- check from the day of enablement to the day of profiling. Oh, and we've changed
     -- the ugly outer join into two steps with an inner join.

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view where HD_box_subs = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'G04: Midway. (HD subscription)', coalesce(@QA_catcher, -1)
     commit


     /****************** G04.5: HD FLAG  (BY PHYSICAL BOX) ******************/

     -- So the physical box version of this is put together from the other OpDash
     -- build and the wiki example on cust_set_top_box. We could bundle the anytime+
     -- capabilities into the same pull, and just do one from CUST_SET_TOP_BOX
     -- especially as we end up with service_instance_id on SBV too. Refactor sometime?
     -- Could also pull the box manafacturer in too, that might be useful.

     SELECT  stb.service_instance_id
       ,stb.HD
             ,stb.HD1TB
     INTO #hda
     FROM #stb_active AS stb
     inner join vespa_analysts.vespa_single_box_view as sbv
     on stb.service_instance_id = sbv.service_instance_id
     WHERE HD = 1

     commit
     create unique index fake_pk on #hda (service_instance_id)
     commit

     update vespa_analysts.vespa_single_box_view
     set
         HD_box_physical     = HD
         ,HD_1TB_physical    = HD1TB
     from vespa_analysts.vespa_single_box_view
     inner join #hda
     on vespa_analysts.vespa_single_box_view.service_instance_id = #hda.service_instance_id

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view where HD_box_physical = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'G04: Complete! (HD physical box)', coalesce(@QA_catcher, -1)
     commit


     /****************** G05: PREMIUMS BY ACCOUNT ******************/

     -- These aren't strictly a by-box thing, but we use them all the time...

     select distinct account_number
     into #target_accounts
     from vespa_analysts.vespa_single_box_view

     commit
     create unique index fake_pk on #target_accounts (account_number)
     commit

     -- Premiums query fromthe wiki; it's dated Sept 2009 though, is this still how we're
     -- doing premiums? Generally matches the process in the Operational Dashboard too, so whatever
     SELECT     csh.account_number
                ,cel.prem_sports
                ,cel.prem_movies
                ,rank() over (partition by csh.account_number order by effective_from_dt desc) as rankage
     into       #account_premiums
     FROM       cust_subs_hist as csh
     inner join #target_accounts as t
             on csh.account_number = t.account_number
     inner join cust_entitlement_lookup as cel
             on csh.current_short_description = cel.short_description
          WHERE csh.subscription_sub_type ='DTV Primary Viewing'
            AND csh.subscription_type = 'DTV PACKAGE'
            AND csh.status_code in ('AC','AB','PC')
            AND csh.effective_from_dt <= @profiling_day
            AND csh.effective_to_dt   >  @profiling_day
            AND csh.effective_from_dt != csh.effective_to_dt

     -- Hey why don't we just pull over the entire short description? Then you'd see exactly what
     -- each account has...

     commit
     -- So there are dupes, but we only want the most recent:
     delete from #account_premiums where rankage > 1

     -- So we're using a different format for these, because different reports want to summarise
     -- then in different ways and group different things into "Other" and that's fine.
     commit
     drop table #target_accounts
     create unique index fake_pk on #account_premiums (account_number)
     commit

     -- Now we have the marks, they ; back onto SBV
     update vespa_analysts.vespa_single_box_view
     set prem_sports = ap.prem_sports
         ,prem_movies = ap.prem_movies
     from vespa_analysts.vespa_single_box_view
     inner join #account_premiums as ap
     on vespa_analysts.vespa_single_box_view.account_number = ap.account_number

     commit
     drop table #account_premiums
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where prem_sports is not null

     EXECUTE logger_add_event @SBV_build_ID, 3, 'G05: Complete! (Premiums)', coalesce(@QA_catcher, -1)
     commit


     /****************** G06: 3D TV FLAG ******************/

     -- Not using these yet, but it might be a useful thing to already have summarised

     SELECT distinct service_instance_id
     into #guys_with_3dtv
       FROM cust_subs_hist
      WHERE subscription_sub_type = '3DTV'
        AND status_code in ('AC','PC','AB')
        AND effective_from_dt <= @profiling_day
        AND effective_to_dt   >  @profiling_day

     commit
     create unique index fake_pk on #guys_with_3dtv (service_instance_id)
     commit

     update vespa_analysts.vespa_single_box_view
     set Box_is_3D = 1
     from vespa_analysts.vespa_single_box_view
     inner join #guys_with_3dtv as gw3d
     on vespa_analysts.vespa_single_box_view.service_instance_id = gw3d.service_instance_id

     commit
     drop table #guys_with_3dtv
     set @QA_catcher = -1
     commit

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where Box_is_3D = 1

     EXECUTE logger_add_event @SBV_build_ID, 3, 'G06: Complete! (3D TV)', coalesce(@QA_catcher, -1)
     commit

     /****************** G07: EXPERIAN CONSUMERVIEW LINKS ******************/

     -- So we thought about pulling in the Experian variables we need, but rather than do that (because
     -- we don't know what all those are yet), we're just ;ing to do the first bit of getting the link
     -- into the experian consumerview table. Then it's still reasonably easy for people to pull out
     -- whatever they need. Yes, it's account level stuff, but stuff like that is ;ing on VSBV in absence
     -- of any VSAV structure.

     -- So in section B07 we pulled in the cb_key_individual because we were already joining to SAV there.
     -- now we just need to get some corresponding ck_row_id keys to get the right records in the
     -- consumerview structure:

     select cb_key_individual, min(cb_row_id) as consumerview_cb_row_id
     into #consumerview_linkage
     from experian_consumerview
     -- um... what other conditions are we ;ing to try to force here? Just taking min(cb_row_id) isn't so
     -- appealing, but there's nothing on the wiki about what other conditions we'd look for, so w/e
     group by cb_key_individual
     -- Oh wait, now some guidance has appeared regarding ranking various conidtions on head_of_household
     -- and suchlike. Perhaps we'll merge that in sometime ##23##

     commit
     create unique index fake_pk on #consumerview_linkage (cb_key_individual)
     commit

     update vespa_analysts.vespa_single_box_view
     set consumerview_cb_row_id = cl.consumerview_cb_row_id
     from vespa_analysts.vespa_single_box_view
     inner join #consumerview_linkage as cl
     on vespa_analysts.vespa_single_box_view.cb_key_individual = cl.cb_key_individual

     commit
     drop table #consumerview_linkage
     set @QA_catcher = -1
     commit

     select @QA_catcher = count(1)
     from vespa_analysts.vespa_single_box_view
     where cb_key_individual is not null

     EXECUTE logger_add_event @SBV_build_ID, 3, 'G07: Complete! (Consumerview links)', coalesce(@QA_catcher, -1)
     commit

     /****************** G08: ARCHIVING QUALITY METRICS ******************/

     declare @weekending date

     select @weekending = case when datepart(weekday,@profiling_day) = 7 then @profiling_day
                                                                         else (@profiling_day + (7 - datepart(weekday,@profiling_day))) end

     if exists (
         select first *
         from vespa_analysts.vespa_sbv_hist_qualitycheck
         where weekending = @weekending
        )
      begin
       delete  from vespa_analysts.vespa_sbv_hist_qualitycheck where weekending = @weekending
       commit
      end
     -- Archiving Quality metrics derived on the run for each account aiming to trace the trend over time...

     insert  into vespa_analysts.vespa_sbv_hist_qualitycheck
     select  @weekending
             ,account_number
             ,subscriber_id
             ,panel_id_vespa
             ,reporting_quality
             ,logs_every_day_30d
    ,now()
     from    vespa_analysts.vespa_single_box_view
     where status_vespa = 'Enabled'

     commit

     if exists (
         select first *
         from  vespa_analysts.vespa_sbv_hist_qualitycheck
         where weekending = @weekending
        )
      begin
       EXECUTE logger_add_event @SBV_build_ID, 3, 'G08: Complete! (Archiving Quality)'
      end
     else
      begin
       EXECUTE logger_add_event @SBV_build_ID, 3, 'G08: Incomplete! (Archiving Quality)'
      end

     commit

     /****************** FINISHING OFF: ******************/

     -- Giving permissions back to the team:
--     grant select on vespa_analysts.vespa_single_box_view to vespa_group_low_security, sk_prodreg

     -- Are we going to fire off the QA procedure before we bail from this one? Maybe.
--     execute SBV_QA_single_box_view @SBV_build_ID
     -- ok, so, yes, we are.

     commit

     EXECUTE logger_add_event @SBV_build_ID, 3, 'Single box view: refresh complete!'
     commit

end;

;


grant execute on SBV_refresh_single_box_view to CITeam, DBA, vespa_analysts_admin_group;
-- Except for the scheduler, which runs through CITeam.
-- And now Cronacle using the DBA username...
commit;








