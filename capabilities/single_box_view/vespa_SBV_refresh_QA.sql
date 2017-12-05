/******************************************************************************
**
** Project Vespa: Single box view
**                  - Refresh QA script
**
** So following the weekly refresh we'll check that a bunch of things worked
** out, and like the scaling refresh, poke any weird values into the logger.
** Some of the QA is inbuilt into the table, eg, maintaining both Olive and
** Vespa flags for box type, Daily and Snapshot marks on having returned data
** sometimes the QA just compares these values.
**
** Generally all this QA just wants to test that the SBV got built properly.
** There's also the Data Control report, which looks more after business rules
** regarding what the data should actually be doing.
**
** We're currently getting a bunch of failures in various sections, but that's
** just business as usual now; 30 or so boxes attached to multiple accounts,
** a few hundred staff and ROI accounts, 30-40k boxes without P/S flags or
** marks in Olive. So, yeah. Mostly it's a nice catch on a bunch of business
** logic checks we like to do.
**
**
** Code sections:
**      Part A: A01 -  Initialise Logger
**
**      Part B:        Subscriber_ID level checks
**              B01 -  P/S aggregation checking
**              B02 -  Dates from the Vespa DB
**              B03 -  Completeness from Olive
**              B04 -  Service_instance_ID uniqueness
**              B05 -  Consistency of panel decision
**              B06 -  Bounds checking for reporting statistics
**
**      Part C:        Account level checks
**              C01 -  Premiums consistency
**
** Currently we're getting a bunch of failures through B02 & B03, which might
** each be sorted out by their respective CCNs, or they might not. We'll see.
** but at least not we have visibility of it.
**
** New tests:
**  1. P/S flags: mostly in play thus far
**  2. Enablement completeness: what about all the non-reporting boxes etc, and also, active boxes that don't have enablement dates
**  3. Sky View Panel: those active panel members which overlap with Vespa, or aren't active DTV customers...
**
******************************************************************************/

/****************** A01: SETTING UP THE QA CATCHING ******************/

if object_id('SBV_QA_single_box_view') is not null
   drop procedure SBV_QA_single_box_view;

go

create procedure SBV_QA_single_box_view
    @SBV_build_ID   bigint  -- so that the QA results go into the same logger object as the refresh run
as
begin

     declare @bad_count_checker      bigint

     /****************** B01: QA ON PRIMARY / SECONDARY BOX FLAGS ******************/

     -- Boxes where Vespa disagrees with Olive about box assignments
     set @bad_count_checker = -1

     select @bad_count_checker = count(1)
     from vespa_analysts.vespa_single_box_view
     where (PS_Olive = 'S' and PS_Vespa = 'P') or (PS_Olive = 'P' and PS_Vespa = 'S')

     commit

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B01a: Olive and Vespa disagree on P/S flag!', coalesce(@bad_count_checker, -1)

     commit

     -- Boxes where Olive isn't even internally consistent:
     set @bad_count_checker = -1

     select @bad_count_checker = count(1)
     from vespa_analysts.vespa_single_box_view
     where PS_Olive = '?' -- ? specifically means that box is flagged as both P and S in olive

     commit

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B01b: Olive disagrees with self on P/S flag!', coalesce(@bad_count_checker, -1)

     commit

     -- Boxes that aren't given any designation in Olive:
     set @bad_count_checker = -1

     select @bad_count_checker = count(1)
     from vespa_analysts.vespa_single_box_view
     where PS_Olive = 'U'

     commit

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B01c: Olive does not recognise box!', coalesce(@bad_count_checker, -1)

     commit

     -- Boxes that are marked as known P/S designation collissions:
     set @bad_count_checker = -1

     select @bad_count_checker = count(1)
     from vespa_single_box_view
     where PS_flag = '!'

     commit

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B01d: Total colliding P/S flags!', coalesce(@bad_count_checker, -1)

     commit

     execute citeam.logger_add_event @SBV_build_ID, 3, 'B01: Complete! (P/S consistency)'


     --      /****************** B02: FINDING DATES IN VESPA DATABASE ******************/
     --
     -- set @bad_count_checker = -1
     --
     -- -- Not yet in the right form, but it wants to be tested....
     -- select @bad_count_checker = count(1) from vespa_analysts.vespa_single_box_view
     -- where  Open_loop_enabled = 1 and Selection_date is null
     --
     -- if @bad_count_checker is null or @bad_count_checker <> 0
     --     execute citeam.logger_add_event @SBV_build_ID, 2, 'B02a: Open Loop accounts lack selection dates!', coalesce(@bad_count_checker, -1)
     --
     -- commit
     --
     -- set @bad_count_checker = -1
     --
     -- select @bad_count_checker = count(1) from vespa_single_box_view
     -- where  Closed_loop_enabled = 1 and enablement_date is null
     --
     -- if @bad_count_checker is null or @bad_count_checker <> 0
     --     execute citeam.logger_add_event @SBV_build_ID, 2, 'B02b: Closed Loop accounts lack enablement dates!', coalesce(@bad_count_checker, -1)
     --
     -- commit
     --
     -- execute citeam.logger_add_event @SBV_build_ID, 3, 'B02: Complete! (Date marks)'

     /****************** B03: COMPLETENESS FROM OLIVE ******************/

     -- We want the box types set to some kind of default (even in 'Unknown') rather than having floating nulls:

     set @bad_count_checker = -1

     select @bad_count_checker = count(1) from vespa_single_box_view
     where Box_type_subs is null

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B03a: Box type (subs) left NULL!', coalesce(@bad_count_checker, -1)

     commit

     set @bad_count_checker = -1

     select @bad_count_checker = count(1) from vespa_analysts.vespa_single_box_view
     where Box_type_physical is null

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B03b: Box type (physical) left NULL!', coalesce(@bad_count_checker, -1)

     commit

     -- OK, and the service instance ID linking

     set @bad_count_checker = -1

     select @bad_count_checker = count(1) from vespa_single_box_view
     where service_instance_id is null

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B03c: service_instance_id left NULL!', coalesce(@bad_count_checker, -1)

     commit

     -- Ditto with the Premium types:

     set @bad_count_checker = -1

     select @bad_count_checker = count(1) from vespa_single_box_view
     where prem_movies is null or prem_sports is null

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B03d: Premiums left NULL!', coalesce(@bad_count_checker, -1)

     commit

     execute citeam.logger_add_event @SBV_build_ID, 3, 'B03: Complete! (Completeness from Olive)'

     /****************** B04: LINKAGE CONSISTENCY ******************/

     -- We've got these service instance IDs, but we want to check if there are any duplicates,
     -- or if the links we picked up relate to current boxes or antiquated boxes..

     set @bad_count_checker = -1

     select @bad_count_checker = count(1) from
     (
         select service_instance_id
         from vespa_single_box_view
         where service_instance_id is not null
         group by service_instance_id
         having count(distinct account_number) > 1
     ) as t

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B04a: Service instance ID with multiple accounts!', coalesce(@bad_count_checker, -1)

     commit

     set @bad_count_checker = -1

     select @bad_count_checker = count(1) from
     (
         select service_instance_id
         from vespa_single_box_view
         where service_instance_id is not null
         group by service_instance_id
         having count(distinct account_number) = 1
         and count(1) > 1
     ) as t

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B04b: Service instance ID duplicates!', coalesce(@bad_count_checker, -1)

     commit

     -- OK, now let's check if the service instance IDs and account numbers are current:
     set @bad_count_checker = -1

     select @bad_count_checker = count(1) from
     (
         select max(case when active_box_flag = 'Y' then 1 else 0 end) as has_current
         from vespa_single_box_view as sbv
         inner join sk_prod.cust_set_top_box as stb
         on sbv.service_instance_id = stb.service_instance_id -- card subscriber id isn't in set top box...
         and sbv.account_number = stb.account_number
         group by sbv.service_instance_id, sbv.account_number
     ) as t
     where has_current = 0

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B04c: Non-current box association!', coalesce(@bad_count_checker, -1)

     execute citeam.logger_add_event @SBV_build_ID, 3, 'B04: Complete! (Linkage consistency)'

     /****************** B05: CHECKS ON PANEL DECISIONS ******************/

     -- Every box should fit into exactly one panel, but sometimes they dont, and we might
     -- get panel decisions clashes or whatever.

     set @bad_count_checker = -1

     select @bad_count_checker = count(1)
     from vespa_single_box_view
     where panel = 'CLASH!'

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B05a: Panel desicion failure!', coalesce(@bad_count_checker, -1)

     commit

     set @bad_count_checker = -1

     select @bad_count_checker = count(1)
     from vespa_single_box_view
     where panel not in ('ALT5', 'VESPA', 'VESPA11', 'SKYVIEW', 'ALT6', 'ALT7', 'CLASH!') -- cortb added (07-02-2014) VESPA11 and ALT5
     -- These are the only expected values in panel. NULLs are also fine, and this
     -- test won't fire for them.

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B05b: Unexpected panel designation!', coalesce(@bad_count_checker, -1)

     commit

     execute citeam.logger_add_event @SBV_build_ID, 3, 'B05: Complete! (Panel decisions)'

     /****************** B06: CHECKING BOUNDS OF BOX REPORTING QUALITY METRICS ******************/

     -- OK, so the number of dats of data return should be non-NULL and between 0 and 30 (inclusive):

     set @bad_count_checker = -1

     select @bad_count_checker = count(1)
     from vespa_single_box_view
     where logs_returned_in_30d is NULL

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B06a: NULLs in log return count!', coalesce(@bad_count_checker, -1)

     commit

     set @bad_count_checker = -1

     select @bad_count_checker = count(1)
     from vespa_single_box_view
     where logs_returned_in_30d < 0 or logs_returned_in_30d > 30

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B06b: Log return count out of bounds!', coalesce(@bad_count_checker, -1)

     commit

     -- Similarly, the box reporting quality metric should be between 0 and 1 inclusive, though
     -- NULLs are allowed here (new boxes that haven't bee naround long enough to be fairly rated)

     set @bad_count_checker = -1

     select @bad_count_checker = count(1)
     from vespa_single_box_view
     where reporting_quality < 0 or reporting_quality > 1

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B06c: Reporting quality metric out of bounds!', coalesce(@bad_count_checker, -1)

     commit

     -- Certain alignments of the various metrics are also expected:
	/*  cortb took this out (07-02-2014) as we recently included boxes that were enabled for less than 15 days
     set @bad_count_checker = -1

     select @bad_count_checker = count(1)
     from vespa_single_box_view
     where reporting_quality =1 and logs_returned_in_30d < 15

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B06d: Inconsistency with log return counts!', coalesce(@bad_count_checker, -1)

     commit */
	 
	 set @bad_count_checker = -1 -- cortb added (07-02-2014) 

     select @bad_count_checker = count(1)
     from   vespa_single_box_view
     where  Panel_ID_Vespa in (5, 6, 7) and    logs_returned_in_30d > 15

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B06d: AP log return counts >15!', coalesce(@bad_count_checker, -1)

     commit
	 
	 set @bad_count_checker = -1 -- cortb added (07-02-2014) 

     select @bad_count_checker = count(1)
     from   espa_single_box_view
     where  Panel_ID_Vespa in (11, 12) and    logs_returned_in_30d > 30

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B06e: DP log return counts >30!', coalesce(@bad_count_checker, -1)

     commit

     set @bad_count_checker = -1

     select @bad_count_checker = count(1)
     from vespa_single_box_view
     where logs_every_day_30d =1 and (reporting_quality <> 1 or reporting_quality is null)

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'B06f: Inconsistency in log return metrics!', coalesce(@bad_count_checker, -1)

     commit
     -- Currently this guy is failing due to boxes which have been enabled in the manual hack
     -- of Feb 21st, but are fairly reliably reporting since then. The logs_every_day_30d=1
     -- but the reporting metric also checks enablement date and only looks for stuff after
     -- the enablement, so this unit test is going to fail until... actually, only this week,
     -- because when profiling at the 8th, these boxes will all have been enabled for more
     -- than 15 days and will get reporting quality metrics assigned. Sweet. Update: this guy
     -- is still failing on boxes that are reliably returning data (30/30d) but for whatever
     -- reason only appear to have been enabled recently, and so aren't qualifying for any
     -- reporting metric value. We're okay with that.

     execute citeam.logger_add_event @SBV_build_ID, 3, 'B06: Complete! (Reporting bounuds)'

     /****************** C01: QA ON ACCOUNT LEVEL DETAILS ******************/

     -- Because no account should ever have more than one allocated premium type etc across it's boxes:

     select @bad_count_checker = count(1) from (
         select account_number
         from vespa_single_box_view
         group by account_number
         having count(distinct convert(varchar(2), prem_sports) || convert(varchar(2), prem_movies)) > 1
     ) as t

     if @bad_count_checker is null or @bad_count_checker <> 0
         execute citeam.logger_add_event @SBV_build_ID, 2, 'C01a: Splintered Premiums flags!', coalesce(@bad_count_checker, -1)

     commit

     execute citeam.logger_add_event @SBV_build_ID, 3, 'C01: Complete! (Account flag consistency)'

end;

go

grant execute on SBV_QA_single_box_view to CITeam; -- logger might need to call this QA sometime?
commit;



























---





