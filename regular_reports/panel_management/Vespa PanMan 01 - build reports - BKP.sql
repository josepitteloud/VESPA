/******************************************************************************
**
** Project Vespa: Panel Management Report
**                  - Weekly Refresh Script
**
** The Panel Management report looks at who stable the Vespa panel is with
** to scaling. Coverage of segments, over / under indexing of stuff, most
** needed boxes for upcoming activation, also one big metric that summarises
** the whole panel into a single performance percentage. This guy takes into
** account box reporting as well as  segmentation spread of the 
**
** See also:
**
**      http://rtci/vespa1/Panel%20Management%20Report.aspx
**
** OK So: we're building the main panel metrics over just the scaling variables,
** but a lot of the profiling and balance work we want to maintan vs these other
** variables as well.
**
** Still to do:
**
** 11. Build the rest of the box selection code, to mess around with segment indexes and box
**      reporting quality metrics to figure out how to fill the rest of panel after the 
**      stratified sampling has done the obvious candidates. Heh, givn the number of variables
**      we're trying to balance against, we're going to need to consider 1 box segments fairly
**      often... sadface. Refer to tags like ##11## and maybe also consider brief V068 for
**      basic things about how the post-ramp-up balance work was implemented.
** 13. Remaining item for implementation is the redundancy measure thing. Not that either of
**      the other panels is live yet, but hey.
** 26. Ensure the build is stable, check that the templating, VBA, everything like that lines up
** 28. Adjust (again) for the operational build of Scaling 2. Some table names have changed, the
**      output structure has changed, and we also now get to perhaps loop over that other table
**      of variables rather than have a huge section of hard-coding. Also might even abstract it
**      completely from the scaling build if we've got the variables in a lookup.
** 31. All the debug and suchlike. Can we actually run a build now, if we've got some scaling 2
**      structures in place?
** 32. Maybe recast the reset procedure so that it detects the schema and drops appropriately.
** 34. Ensure alignment of unit tests with Scaling 2 build - tables, new structures, etc
** 35. Add a parameter option to let us run panel management for any day, and default to the
**      standard profiling thursday if it's null. The mis-alignment with SBV won't matter so
**      much, we just want to be able to use it for test builds prior to us having the current
**      week of scaling build in play.
**
** Tables that are apparently in play in Vespa_Analysts:
** 
** Recently completed: (in order of completion)
** 10. Realign the box selection tricks to use the upgraded reporting quality metric now on SBV.
** 12. Look at archiving off the SHA1 keys so we don't have to rebuild them each week (because
**      Sybase seems pretty slow at it).
** 15. Turn the pair or bad reporting guys into a single good base coverage metric
** 16. Fill out the rest of the high level overview; as in, the traffic light things.
**  9. Still the automation stuff, including the transposing of the summary stuff and sorting
**      out all the title bits. And more termplating for changing requirements, heh.
** 18. We'll also need another household clasification for households that are new to Vespa
**      and haven't been around for long enough to have their reliability judged.
** 19. Need to change the SHA1 calculations into batching of 4k items per time, trying to do all
**      the hashes at once is prohibitively slow.
** 14. OnNet vs Offnet as an additional non-scaling variable
** 24. Fix the thing with the hash calculation omission
** 21. "Reliable" and "somewhat reliable" want to be combined into a single "acceptable" mark.
** 25. Add Sky Go users to the non-scaling variables breakdown
** 22. Add to unit tests checks on fixed length outputs
** 23. Add a thiung to the front page about reporting quality breakdown over the universes.
** 17. Maybe the data completeness metric only act over the scaling variables? Update: Yes
** 20. Upgrade for scaling 2!
**      i/ Check table & column names - DONE!
**      ii/ New single variable summaries - DONE
**      iii/ Recast sections using dialback intervals - DONE
**      iv/ Treatment for different universes? Update - DONE
**      v/ Templating - get the columns right, copy the formulas down, change OnNet traffic ligh to OnNet-OffNet - DONE
**      vi/ Demo & debug (will need to grant permissions lol)
**      vii/ Rebuild non-scaling variables so as to profile at same point as scaling... 10 days before or something? - DONE
**      viii/ Something to check that the manual section is completed, and re-queue the job in the scheduler if t's not - DONE
**      ix/ What else?
** 28. Break out non-scaling variables into sub-sections
** 29. Add the new fixed OnNet build detailed on the customer group wiki.
** 30. More QA control totals and suchlike, for internal consistency and suchlike.
** 27. Need a thing which checks that the dependent Scaling 2 build is complete [Update: No, it's autoamted now]
** 33. Table renamings for Operational build of Scaling 2
**
** Code sections:
**      Part A: A01 -  Initialise Logger
**              A02 -  Table Resets
**              A03 -  Temporal Bounds
**
**      Part B:        Cunstruction, numbered by requirements
**              B01 -  Variables for sampling that we don't use in scaling
**              B01a -  Value Segments
**              B01b -  Experian: MOSAIC & Financial Strategy Segments
**              B01c -  OnNet / OffNet
**              B01d -  Sky Go Users
**              B01e -   (placeholders)
**              B01x -  Getting a segmentation ID
**              B02 -  Indexing of panel segments against Sky Base (all panels at once)
**              B03 -  Aggregated view of segmentation variables (all panels too!)
**              B04 -  Data completeness - proportion of unrepresented / poorly represented segments
**              B05 -  Box "Swing" - boxes which have the largest weights
**   ^          B06 -  Redundancy measures from alternate panels
**              B07 -  Other high level metrics beside the data completeness - single variable traffic lights
**
**      Part D:        Building some specific report tables
**              D01 -  High level overviews for each panel
**              D02 -  Vespa reporting quality breakdown by scaling universe
**
**      Part M:        Panel modification work          - NYIP - this whole part isn't in play yet ##11##
**              M01 -  First round of box selection: up to stratification limit
**   ^          M02 -  Filling in selection gaps with Utility measure
**  [^]         M03 -  Final box selection round based on as-yet undecided magic
**              M04 -  Assembling lists of accounts to be moved about the place
**
**      Part Q:        Automated QA on stuff
**              Q01 -  Tests on Dashboard section
**              Q02 -  End-to-end control totals
**              Q04 -  Tests on panel migration lists   - NYIP - ##11##
**
**      Part R:        Are we having a report builds section?
**
**      Part T: T01 -  Permissions!
**
** ^ - still requires attention. Not many now.
**
** Definitions: We're considering a household to be "Acceptable" at reporting
** when every box returns data for at least 25 days, or 90% of days since
** enablement if the enablement happened more than 15 but less that 30 days
** ago (it's the reporting quality metric from SBV.) No data return on any
** box for the last 30 days is a "Zero reporting" rating, households with
** boxes enabled within the last 15 days are "Recently enabled", and anything
** else gets labeled "Unreliable".
**
** Underpinning this is a bit of an assumption that the Alternate Day panels
** will still return data for the whole period, and that we'll see them in every
** daily table. We might end up introducing a bias *away* from heavy TV watchers
** if a heavy TV watcher in the alternate day panels ends up without items in
** a daily table because it was a non-reporting day and the buffers got re-filled
** up with more recent stuff before the reporting happened. But hey, no idea how
** Alt6 and Alt7 will work until they actually turn up.
**
******************************************************************************/

-- This guy is mostly stable, though there are stil la few sections that need heavy dev.

if object_id(    'PanMan_make_report') is not null
   drop procedure PanMan_make_report;

commit;
go

create procedure  PanMan_make_report -- execute PanMan_make_report
   @profiling_thursday         date    = null
as
begin

     /****************** A01: SETTING UP THE LOGGER ******************/

     DECLARE @PanMan_logging_ID      bigint
     DECLARE @Refresh_identifier     varchar(40)
     declare @run_Identifier         varchar(20)
     declare @recent_profiling_date date
     DECLARE @TrafficLights_stdCount integer
     -- For putting control totals in prior to logging:
     DECLARE @QA_catcher             integer
     declare @exe_status             integer

     set @TrafficLights_stdCount = 11

     if lower(user) = 'kmandal'
         set @run_Identifier = 'VespaPanMan'
     else
         set @run_Identifier = 'PanMan test ' || upper(right(user,1)) || upper(left(user,2))

     set @Refresh_identifier = convert(varchar(10),today(),123) || ' PanMan refresh'
     EXECUTE citeam.logger_create_run @run_Identifier, @Refresh_identifier, @PanMan_logging_ID output

     commit
     EXECUTE citeam.logger_add_event @PanMan_logging_ID, 3, 'A01: Complete! (Report setup)'
     
     commit

     /****************** A02: TABLE RESETS ******************/

     execute PanMan_clear_transients

     -- Well that was easy. But we're also going to check that it worked:

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from Vespa_PanMan_all_households

     if @QA_catcher is null or @QA_catcher <> 0
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'A02: failed to realise clean table!', @QA_catcher

     commit

     EXECUTE citeam.logger_add_event @PanMan_logging_ID, 3, 'A02: Complete! (Table resets)'
     commit

     /****************** A03: TIME BOUNDS ******************/

     -- Might not use this for a whole lot, given how much gets done in the midway scaling tables
     if @profiling_thursday is null
     begin
         execute Regulars_Get_report_end_date @profiling_thursday output -- A Saturday
         set @profiling_thursday    = dateadd(day, -8, @profiling_thursday) -- Thursday of SAV flip data, but we're now profiling from the beginning of the period.
     end
     commit

     EXECUTE citeam.logger_add_event @PanMan_logging_ID, 3, 'A03: Complete! (Temporal bounds)'
     commit

     /****************** A04: DEPENDENCY COMPLETENESS CHECK ******************/

     -- So this build relies strongly on the most recent scaling build complting. But as of
     -- Scaling 2, this is no longer managed through the scheduler and is instead a manual
     -- process, so we need another check to see if the manual flips have been completed for
     -- the week. Yeeeash. Why isn't it just in the scheduler? It'd easily fit, that's why
     -- Scaling 1 was rebuilt the way that it was...

     -- Update: Nope, the Scaling 2 build is now fully automated!
	 
	 -- NOTE: -----------------------------------------------
	 -- Panman is tight up to Scaling to show the distribution of accounts of Scaling latest build
	 -- However the Sky Base displayed in the summary tab is independent from this process and details
	 -- the actual Sky Base as of today... Further tabs on Panman Excel file are to show Scaling Results
	 -- -----------------------------------------------------

     select @recent_profiling_date = max(profiling_date)
       from vespa_analysts.SC2_Sky_base_segment_snapshots

     select account_number,scaling_segment_id
       into #Scaling_weekly_sample
       from vespa_analysts.SC2_Sky_base_segment_snapshots
      where profiling_date = @recent_profiling_date
	
     /****************** B01: SEGMENTING BY VARIABLES NOT USED IN SCALING ******************/

     -- So there are other variables we're not scaling by, but we do want to involve in the
     -- segmentation. Currently there's only value segment, but this will get a lot easier as
     -- more of these variables are addded. But we start with the population of the scaling
     -- segmentation (since they are the guys we need).

     insert into Vespa_PanMan_this_weeks_non_scaling_segmentation  (
         account_number
     )
     select distinct(account_number)
       from #Scaling_weekly_sample
       
       -- Cronacle healthcheck...
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01-1 DML command status: '||@@error
     commit
    -- Cronacle healthcheck...
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01-1 DML command status: '||@@error
     
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from Vespa_PanMan_this_weeks_non_scaling_segmentation

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B01: Population', coalesce(@QA_catcher, -1)
     commit

     /****************** B01a: VALUE SEGMENTS ******************/

     -- First off the Value Segments data:
     update Vespa_PanMan_this_weeks_non_scaling_segmentation
        set value_segment = coalesce(value_seg, 'Bedding In') -- Anyone new is, by construction, new
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
            left join sk_prod.VALUE_SEGMENTS_DATA as vsd on Vespa_PanMan_this_weeks_non_scaling_segmentation.account_number = vsd.account_number

    -- Cronacle healthcheck...
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01a-1 DML command status: '||@@error
     commit

    -- Cronacle healthcheck...
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01a-2 DML command status: '||@@error
    
     -- Since it's a subsection, may as well control total it up
     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
      where value_segment <> 'Bedding In'

     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B01a: Value segments', coalesce(@QA_catcher, -1)
     commit

     /****************** B01b: EXPERIAN: MOSAIC AND FINANCIAL STRATEGY SEGMENTS ******************/

     -- Now for the Experian data: yes the keys are on SBV for Vespa population, but we actually need
     -- the whole base...
       select cb_key_individual
             ,cb_row_id as consumerview_cb_row_id
--           ,rank() over(partition by cb_key_individual ORDER BY head_of_household desc, cb_row_id desc) as rank_ind
             ,rank() over(partition by cb_key_individual ORDER BY cb_row_id) as rank_ind
         into #consumerview_lookup
         from sk_prod.experian_consumerview
     -- um... what other conditions are we going to try to force here? Just taking min(cb_row_id) isn't so
     -- appealing, but there's nothing on the wiki about what other conditions we'd look for, so w/e
     delete from #consumerview_lookup where rank_ind >1

    -- Cronacle healthcheck...
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01b-1 DML command status: '||@@error
    
     commit
     create unique index fake_pk on #consumerview_lookup (cb_key_individual)
     commit

    -- Cronacle healthcheck...
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01b-2 DML command status: '||@@error
    
     -- So in SAV the individual key ends up duplicated across a few accounts, and some of these account
     -- number duplicates even show different individual keys and tenure dates... whatever.
       select sav.account_number
             ,min(cl.consumerview_cb_row_id) as consumerview_cb_row_id -- this does bad things to the processing in the case that SAV is already broken. There's a longer suggested workaround coming from cust_subs_hist so we might be able to work that into SBV?
         into #consumerview_patch
         from sk_prod.cust_single_account_view as sav
              inner join #consumerview_lookup as cl on sav.cb_key_individual = cl.cb_key_individual
     group by sav.account_number

    -- Cronacle healthcheck...
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01b-3 DML command status: '||@@error
    
     commit
     create unique index for_joining on #consumerview_patch (account_number)
     commit

    -- Cronacle healthcheck...
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01b-4 DML command status: '||@@error
    
     -- OK, now get those keys onto the segmentation table...
     update Vespa_PanMan_this_weeks_non_scaling_segmentation
        set consumerview_cb_row_id = cp.consumerview_cb_row_id
       from Vespa_PanMan_this_weeks_non_scaling_segmentation as twnss
            inner join #consumerview_patch as cp on twnss.account_number = cp.account_number

     commit
     drop table #consumerview_lookup
     drop table #consumerview_patch
     commit
    -- Cronacle healthcheck...
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01b-5 DML command status: '||@@error
    
     --sybase bug workaround. Sybase 15.2 doesn't like this query, so a workaround follows
     --         update Vespa_PanMan_this_weeks_non_scaling_segmentation as bas
     --            set MOSAIC_segment             = coalesce(h_mosaic_uk_2009_group, 'U') -- NULLs default to 'U' for unknown
     --               ,Financial_strategy_segment = coalesce(h_fss2_groups, 'U')
     --           from sk_prod.EXPERIAN_CONSUMERVIEW as ec
     --          where bas.consumerview_cb_row_id = ec.cb_row_id
     --            and bas.consumerview_cb_row_id is not null and consumerview_cb_row_id > 0
     --         commit
     create table Vespa_PanMan_this_weeks_non_scaling_segmentation_bugfix (
          account_number                     varchar(20) primary key
         ,non_scaling_segment_id             int
         ,value_segment                      varchar(10)
         ,consumerview_cb_row_id             bigint
         ,MOSAIC_segment                     varchar(1)
         ,Financial_strategy_segment         varchar(1)
         ,is_OnNet                           bit         default 0
         ,uses_sky_go                        bit         default 0
     )

     commit
     create index for_updating   on Vespa_PanMan_this_weeks_non_scaling_segmentation_bugfix (consumerview_cb_row_id)
     
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01b-6 DML command status: '||@@error
    
       insert into Vespa_PanMan_this_weeks_non_scaling_segmentation_bugfix (
              account_number
             ,non_scaling_segment_id
             ,value_segment
             ,consumerview_cb_row_id
             ,MOSAIC_segment
             ,Financial_strategy_segment
             ,is_OnNet
             ,uses_sky_go)
       select bas.account_number
             ,bas.non_scaling_segment_id
             ,bas.value_segment
             ,bas.consumerview_cb_row_id
             ,coalesce(con.h_mosaic_uk_group, 'U') as MOSAIC_segment
             ,coalesce(con.h_fss_group, 'U')          as Financial_strategy_segment
             ,bas.is_OnNet
             ,bas.uses_sky_go
         from Vespa_PanMan_this_weeks_non_scaling_segmentation as bas
              left join sk_prod.EXPERIAN_CONSUMERVIEW          as con on bas.consumerview_cb_row_id = con.cb_row_id
--        where bas.consumerview_cb_row_id is not null
--          and bas.consumerview_cb_row_id > 0

    
     truncate table Vespa_PanMan_this_weeks_non_scaling_segmentation
     
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01b-7 DML command status: '||@@error
    
       insert into Vespa_PanMan_this_weeks_non_scaling_segmentation(
              account_number
             ,non_scaling_segment_id
             ,value_segment
             ,consumerview_cb_row_id
             ,MOSAIC_segment
             ,Financial_strategy_segment
             ,is_OnNet
             ,uses_sky_go)
       select account_number
             ,non_scaling_segment_id
             ,value_segment
             ,consumerview_cb_row_id
             ,MOSAIC_segment
             ,Financial_strategy_segment
             ,is_OnNet
             ,uses_sky_go
         from Vespa_PanMan_this_weeks_non_scaling_segmentation_bugfix

     drop table Vespa_PanMan_this_weeks_non_scaling_segmentation_bugfix
     --bug fix ends here

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
      where MOSAIC_segment <> 'U'
         or Financial_strategy_segment <> 'U'

     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B01b: Experian patch', coalesce(@QA_catcher, -1)
     commit
    
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01b-8 DML command status: '||@@error
    
     /****************** B01c: ONNET AND OFFNET  ******************/

     -- The OnNet goes by postcode, so...
       select twnss.account_number
             ,min(cb_address_postcode) as postcode -- it's arbitrary, if there are duplicates then SAV is bad...
             ,convert(bit, 0) as onnet
         into #onnet_patch
         from Vespa_PanMan_this_weeks_non_scaling_segmentation as twnss
              inner join sk_prod.cust_single_account_view      as sav on sav.account_number = twnss.account_number
        where sav.cust_active_dtv = 1 -- OK, so we're getting account number duplicates, that's annoying...
     group by twnss.account_number -- If there are account_number duplicates, they're postcodes for an active account, so whatever...

     update #onnet_patch
        set postcode = upper(REPLACE(postcode,' ',''))

     commit
     create unique index fake_pk on #onnet_patch (account_number)
     create index joinsy on #onnet_patch (postcode)
     commit

     -- 1) Get BROADBAND_POSTCODE_EXCHANGE postcodes

         SELECT cb_address_postcode as postcode, MAX(mdfcode) as exchID
           INTO #bpe
           FROM sk_prod.BROADBAND_POSTCODE_EXCHANGE
       GROUP BY postcode

     update #bpe
        set postcode = upper(REPLACE( postcode,' ',''))

     commit
     create unique index fake_pk on #bpe (postcode)


     -- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes
         SELECT postcode as postcode, MAX(exchange_id) as exchID
           INTO #p2e
           FROM sk_prod.BB_POSTCODE_TO_EXCHANGE
       GROUP BY postcode

     update #p2e
        set postcode = upper(REPLACE( postcode,' ',''))

     commit
     create unique index fake_pk on #p2e (postcode)

     -- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible

     SELECT COALESCE(#p2e.postcode, #bpe.postcode) AS postcode
           ,COALESCE(#p2e.exchID, #bpe.exchID) as exchange_id
           ,'OFFNET' as exchange
       INTO #onnet_lookup
       FROM #bpe FULL JOIN #p2e ON #bpe.postcode = #p2e.postcode

     commit
     create unique index fake_pk on #onnet_lookup (postcode)

     -- 4) Update with latest Easynet exchange information

     UPDATE #onnet_lookup
        SET exchange = 'ONNET'
       FROM #onnet_lookup AS base
            INNER JOIN sk_prod.easynet_rollout_data as easy on base.exchange_id = easy.exchange_id
      WHERE easy.exchange_status = 'ONNET'

     -- 5) Flag your base table with onnet exchange data. Note that this uses a postcode field with
     --   spaces removed so your table will either need to have a similar filed or use a REPLACE
     --   function in the join

     UPDATE #onnet_patch
        SET onnet = CASE WHEN tgt.exchange = 'ONNET'
                         THEN 1
                         ELSE 0
                    END
       FROM #onnet_patch AS base
            INNER JOIN #onnet_lookup AS tgt on base.postcode = tgt.postcode
     commit

     update Vespa_PanMan_this_weeks_non_scaling_segmentation
        set is_OnNet = op.onnet
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
            inner join #onnet_patch as op on Vespa_PanMan_this_weeks_non_scaling_segmentation.account_number = op.account_number

     commit

     -- Clear out all those tables that got sprayed about the place:
     drop table #onnet_patch
     drop table #onnet_lookup
     drop table #p2e
     drop table #bpe
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
      where is_OnNet = 1

     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B01c: OnNet (vs OffNet)', coalesce(@QA_catcher, -1)
     commit
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01c-1 DML command status: '||@@error
     /****************** B01d: SKY GO USERS  ******************/

     -- Finally (for now) the Sky Go use marks
     select distinct account_number
       into #skygousers
       from sk_prod.SKY_PLAYER_USAGE_DETAIL
      where activity_dt >= '2011-08-18'

     commit
     create unique index fakle_pk on #skygousers (account_number)
     commit

     update Vespa_PanMan_this_weeks_non_scaling_segmentation
        set uses_sky_go = 1
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
            inner join #skygousers as sgu on Vespa_PanMan_this_weeks_non_scaling_segmentation.account_number = sgu.account_number

     drop table #skygousers

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
      where uses_sky_go = 1

     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B01d: Sky Go users', coalesce(@QA_catcher, -1)
     commit
    
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01d-1 DML command status: '||@@error
    
     /****************** B01x: GETTING SEGMENTATION IDS  ******************/

     -- OK and now we need to pull the IDs off the lookup... and because we've already coalesced
     -- the NULLs into U's we don't need to do the double-join thing that the scaling did.
     update Vespa_PanMan_this_weeks_non_scaling_segmentation
        set non_scaling_segment_id = t.non_scaling_segment_id
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
            inner join Vespa_PanMan_non_scaling_segments_lookup as t on Vespa_PanMan_this_weeks_non_scaling_segmentation.value_segment              = t.value_segment
                                                                    and Vespa_PanMan_this_weeks_non_scaling_segmentation.MOSAIC_segment             = t.MOSAIC_segment
                                                                    and Vespa_PanMan_this_weeks_non_scaling_segmentation.Financial_strategy_segment = t.Financial_strategy_segment
                                                                    and Vespa_PanMan_this_weeks_non_scaling_segmentation.is_OnNet                   = t.is_OnNet
                                                                    and Vespa_PanMan_this_weeks_non_scaling_segmentation.uses_sky_go                = t.uses_sky_go

     -- Other join conditions too here as we get other variables... when they get added
     commit

     -- Check that we don't get NULL entries here:
     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
      where non_scaling_segment_id is null

     if @QA_catcher is null or @QA_catcher <> 0
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'B01: Failure establishing non-scaling segmentation IDs!', coalesce(@QA_catcher, -1)

     commit

     -- OK, and that's this week's segmentation done!

     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
      where non_scaling_segment_id is not null

     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'B01: Complete! (Non-scaling segmentation)', coalesce(@QA_catcher, -1)
     commit
    
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B01x-1 DML command status: '||@@error
    
     /****************** B02: INDEXING PANELS AGAINST THE SKY BASE ******************/

     -- We can do this for all panels at once with a few joins from SBV into last
     -- week's segmentation... nearly. We're scaling by accounts, which means that we
     -- first need to turn SBV into a statement about how well an entire household
     -- returns data:

       insert into Vespa_PanMan_all_households (
              account_number
             ,hh_box_count       -- not directly used? but might be interesting
             ,most_recent_enablement
             ,reporting_categorisation
             ,reporting_quality
             ,panel
       )
       select account_number
             ,count(1)
             ,max(Enablement_date)
             ,case when datediff(day, max(Enablement_date), @profiling_thursday) < 15      then 'Recently enabled'
                   when min(logs_every_day_30d) = 1                                        then 'Acceptable'
                   when min(logs_returned_in_30d) >= 27 or min(reporting_quality) >= 0.9   then 'Acceptable'
                   when max(logs_returned_in_30d) = 0                                      then 'Zero reporting'
                                                                                           else 'Unreliable'
                   end
             ,min(reporting_quality)  -- Used much later in the box selection bit, but may as well build it now
             ,min(panel)              -- This guy should be unique per account, we test for that coming off SBV
         from vespa_analysts.vespa_single_box_view
        where panel in ('VESPA','VESPA11')
        and status_vespa = 'Enabled'
     group by account_number
     
     -- Now, for alter panels the assigment for the reporting_categorisation is slightly different as the boxes in this panels
     -- have a live span of 15 days... hence the metric should be calculated based on this period...
     
       insert into Vespa_PanMan_all_households (
              account_number
             ,hh_box_count       -- not directly used? but might be interesting
             ,most_recent_enablement
             ,reporting_categorisation
             ,reporting_quality
             ,panel
       )
       select account_number
             ,count(1)
             ,max(Enablement_date)
             ,case when datediff(day, max(Enablement_date), @profiling_thursday) < 15      then 'Recently enabled'
                   when min(logs_every_day_30d) = 1                                      then 'Acceptable' -- This one should happen?...
                   when min(logs_returned_in_30d) >= 13 or min(reporting_quality) >= 0.4   then 'Acceptable'
                   when max(logs_returned_in_30d) = 0                                      then 'Zero reporting'
                                                                                           else 'Unreliable'
                   end
             ,min(reporting_quality)  -- Used much later in the box selection bit, but may as well build it now
             ,min(panel)              -- This guy should be unique per account, we test for that coming off SBV
         from vespa_analysts.vespa_single_box_view
        where panel in ('ALT6', 'ALT7','ALT5')
        and status_vespa = 'Enabled'
     group by account_number

     -- Don't need any of the profiling vaiables from there because we get those from the
     -- most recent Scaling Segmentation. Not getting the enablement dates, because putting
     -- things on or off the panel based on data return delay is done by box enablement.

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_all_households

     -- If there aren't any account put into the table, this is a super bad thing; it might
     -- have failed because different boxes within the same household are assigned to different
     -- panels, so we're going to check that some boxes made it into the table we want.

     if @QA_catcher is null or @QA_catcher = 0
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'B02: Error populating returning households! (Could be account / panel conflict.)'

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Households generated)'
     commit

     -- OK, so the hashes take ages to build, so we pick them up from this archive:
     update Vespa_PanMan_all_households
        set accno_SHA1 = sha1.accno_SHA1 -- Did some tests, MD5 actually takes *longer* than SHA1
       from Vespa_PanMan_all_households
            inner join vespa_analysts.Vespa_PanMan_SHA1_archive as sha1 on Vespa_PanMan_all_households.account_number = sha1.account_number

     commit
     -- But there might be new accounts, so:
     select account_number
           ,convert(varchar(40), null)  as accno_SHA1
       into #new_SHA1s
       from Vespa_PanMan_all_households
      where Vespa_PanMan_all_households.accno_SHA1 is null

     commit
     create unique index fake_pk on #new_SHA1s (account_number)
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #new_SHA1s

     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (New hash list)', @QA_catcher
     commit

     -- So it turns out that batching these has calculations into sets of 4k makes the
     -- whole thing a *lot* faster...

     create table #hash_cache (
         account_number          varchar(20)
     )

     commit
     declare @t integer
     set @t = 0
     commit

     while @t <= @QA_catcher / 4000 -- @QACatcher still has the number of new hashes we need to calculate
     begin

         insert into #hash_cache
         select top 4000 account_number
           from #new_SHA1s
          where accno_sha1 is null

         commit

         select account_number
               ,hash(account_number, 'SHA1') as accno_SHA1
          into #hash_results
          from #hash_cache

         commit
         create unique index fake_pk on #hash_results (account_number)
         commit

         update #new_SHA1s
            set accno_SHA1 = res.accno_SHA1
           from #new_SHA1s
                inner join #hash_results as res on #new_SHA1s.account_number = res.account_number

         commit
         drop table #hash_results
         delete from #hash_cache
         commit

         set @t = @t + 1

         if mod(@t, 5) = 0
             -- Ping the logger only every 20k of account numbers hashed
             execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Hashes batch processed)', @t * 4000
         commit

     end

     -- OK, so patch the new hashes onto the main table...
     update Vespa_PanMan_all_households
        set accno_SHA1 = sha1.accno_SHA1 -- Did some tests, MD5 actually takes *longer* than SHA1
       from Vespa_PanMan_all_households
            inner join #new_SHA1s as sha1 on Vespa_PanMan_all_households.account_number = sha1.account_number

     commit
     -- And also archive all those hashes so we don't calculate them again
     insert into vespa_analysts.Vespa_PanMan_SHA1_archive
     select account_number, accno_SHA1
       from #new_SHA1s

     commit
     drop table #new_SHA1s
     drop table #hash_cache
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_all_households
      where accno_SHA1 is null

     if @QA_catcher is null or @QA_catcher <> 0
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'B02: Households without hashes!', coalesce(@QA_catcher, -1)

     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Hashes tagged)'
     commit

     -- OK, so now grab the various segmentation marks:

     update Vespa_PanMan_all_households
        set scaling_segment_id = tws.scaling_segment_id
      from Vespa_PanMan_all_households
           inner join #Scaling_weekly_sample as tws on Vespa_PanMan_all_households.account_number = tws.account_number

     update Vespa_PanMan_all_households
        set non_scaling_segment_id = tsnss.non_scaling_segment_id
       from Vespa_PanMan_all_households
            inner join Vespa_PanMan_this_weeks_non_scaling_segmentation as tsnss on Vespa_PanMan_all_households.account_number = tsnss.account_number

     update Vespa_PanMan_all_households as bas
        set non_scaling_segment_id = nss.non_scaling_segment_id
       from Vespa_PanMan_this_weeks_non_scaling_segmentation as nss
      where bas.account_number = nss.account_number

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Segmentation applied)'
     commit

     -- So we need to start off with a view of the whole Sky base, and then add in the details for the stuff on each panel...
       select scaling_segment_id
             ,non_scaling_segment_id
         --  Doesn't include scaling_segment_name, we stitch that in later
             ,count(1) as Sky_Base_Households
         into #sky_base_segmentation
         from #Scaling_weekly_sample as tws
              inner join Vespa_PanMan_this_weeks_non_scaling_segmentation as tsnss on tws.account_number = tsnss.account_number
        where tws.scaling_segment_ID is not null and tsnss.non_scaling_segment_id is not null -- That annoying case of the region 'Eire' guy which is Ireland and therefore shouldn't be in the Vespa dataset, but whatever
     group by tws.scaling_segment_ID, tsnss.non_scaling_segment_id
     -- It has to go into a temp table because we duplicate all these number for each panel

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Sky totals built)'
     commit

     -- We need control totals for each panel later, but right now we need this to duplicate the sky base numbers for each panel...
       select panel
             ,count(1) as panel_reporters
         into #panel_totals
         from Vespa_PanMan_all_households
        where reporting_categorisation = 'Acceptable'
     group by panel

     commit

     insert into Vespa_PanMan_Scaling_Segment_Profiling (
            panel
           ,scaling_segment_id
           ,non_scaling_segment_id
       --  Doens't include scaling_segment_name, we stitch that in later
           ,Sky_Base_Households
     )
     select pt.panel
           ,sb.*
       from #sky_base_segmentation as sb
			cross join	(
							select	distinct panel
							from	Vespa_PanMan_all_households
						)	as pt
            /* cross join #panel_totals as pt */ -- want all the combinations of stuff

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Sky totals deployed)'
     commit

     -- Now with the marks in plac we can group things into segments: even though we have the scaling
     -- and non-scaling segments on the all households table, that's only for boxes on a panel and here
     -- we need the whole sky base. Good thing we've already got that built then, and added into the
     -- main table for each panel.
       select panel
             ,scaling_segment_id
             ,non_scaling_segment_id
             ,count(1) as Panel_Households
             ,sum(case when reporting_categorisation = 'Acceptable'       then 1 else 0 end) as Acceptably_reliable_households
             ,sum(case when reporting_categorisation = 'Unreliable'       then 1 else 0 end) as Unreliable_households
             ,sum(case when reporting_categorisation = 'Zero reporting'   then 1 else 0 end) as Zero_reporting_households
             ,sum(case when reporting_categorisation = 'Recently enabled' then 1 else 0 end) as Recently_enabled_households
         into #panel_segmentation
         from Vespa_PanMan_all_households as hr
        where scaling_segment_ID is not null and non_scaling_segment_id is not null -- That annoying case of the region 'Eire' guy which is Ireland and therefore shouldn't be in the Vespa dataset, but whatever
     group by panel, scaling_segment_ID, non_scaling_segment_id

     commit
     create unique index fake_pk on #panel_segmentation (panel, scaling_segment_id, non_scaling_segment_id)
     commit

     -- Now with the totals built for each panel, we can throw them into the table with the Sky base:
     update Vespa_PanMan_Scaling_Segment_Profiling
        set Panel_Households                = ps.Panel_Households
           ,Acceptably_reliable_households = ps.Acceptably_reliable_households
           ,Unreliable_households          = ps.Unreliable_households
           ,Zero_reporting_households      = ps.Zero_reporting_households
           ,Recently_enabled_households    = ps.Recently_enabled_households
       from Vespa_PanMan_Scaling_Segment_Profiling
            inner join #panel_segmentation as ps on Vespa_PanMan_Scaling_Segment_Profiling.panel                    = ps.panel
                                                and Vespa_PanMan_Scaling_Segment_Profiling.scaling_segment_id       = ps.scaling_segment_id
                                                and Vespa_PanMan_Scaling_Segment_Profiling.non_scaling_segment_id   = ps.non_scaling_segment_id

     commit
     drop table #sky_base_segmentation
     drop table #panel_segmentation
     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Profiler built)'
     commit

     -- Patch in the scaling segment name from the lookup...
     update Vespa_PanMan_Scaling_Segment_Profiling
        set scaling_segment_name = ssl.scaling_segment_name
       from Vespa_PanMan_Scaling_Segment_Profiling
            inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl on Vespa_PanMan_Scaling_Segment_Profiling.scaling_segment_ID = ssl.scaling_segment_ID

     update Vespa_PanMan_Scaling_Segment_Profiling
        set non_scaling_segment_name = nssl.non_scaling_segment_name
       from Vespa_PanMan_Scaling_Segment_Profiling
            inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on Vespa_PanMan_Scaling_Segment_Profiling.non_scaling_segment_ID = nssl.non_scaling_segment_ID

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Names imported)'
     commit

     -- Then yeah, that guy gets sucked out and he powers the various reporting views that we get.

     -- We do need the indices in-database though, since we make decisions based on them etc.
     declare @total_sky_base                 int
     -- With the new normalised structures, panel totals just go into a table...

     -- We need the size of the sky base for indexing calculations
     select @total_sky_base     = sum(Sky_Base_Households)
       from Vespa_PanMan_Scaling_Segment_Profiling
      where panel in ('VESPA','VESPA11')

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Totals set)'
     commit

     -- Now simplified because we'll only be dividing by things in cases where we've got
     -- the appropriate panel stuff in the table:
     update Vespa_PanMan_Scaling_Segment_Profiling
       set Acceptably_reporting_index         = -- *sigh* there's no GREATEST / LEAST operator in this DB...
            case    when pt.panel_reporters > 0  then	(
															case    when 200 < 100 * (Acceptably_reliable_households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters then 200
																	else       100 * (Acceptably_reliable_households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters
															end
														)
					else 0
			end
       from Vespa_PanMan_Scaling_Segment_Profiling
            left join #panel_totals as pt on Vespa_PanMan_Scaling_Segment_Profiling.panel = pt.panel
     -- Not dropping #panel_totals here because we still need it for the single variable summaries

     -- Still... What are we pulling out to report this? One graph for Vespa Live, one for
     -- each alternate...

     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_Scaling_Segment_Profiling
      where Acceptably_reporting_index is not null

     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'B02: Complete! (Indexing panels)', coalesce(@QA_catcher, -1)
     commit

     EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B02-1 DML command status: '||@@error
     
     /****************** B03: AGGREGATING TO VARIABLE VIEWS ******************/

     -- So this is the bit that's specific from one scaling build to the next; we need
     -- individual variables here. And because we don't want to introduce any bias from
     -- how we calculated the indices on the segments above, we'll do it from the account
     -- level stuff. But we still have to join in the lookups to get the IDs across the
     -- variables we want:

     -- (Now with improved normalisation, helped by the merging of reliable & somewhat
     -- reliable into one category...)

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'UNIVERSE' -- Name of variable being profiled
             ,1          -- Whether the variable is used for scaling or not (determintes the results sheet pull)
             ,ssl.universe
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.universe

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'REGION'   -- Name of variable being profiled
             ,1          -- Whether the variable is used for scaling or not (determintes the results sheet pull)
             ,ssl.isba_tv_region
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.isba_tv_region

     commit

     insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'HHCOMP'
             ,1
             ,ssl.hhcomposition
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.hhcomposition

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'PACKAGE'
             ,1
             ,ssl.package
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.package

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'TENURE'
             ,1
             ,ssl.tenure
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.tenure

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'BOXTYPE'
             ,1
             ,ssl.boxtype
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.boxtype

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'B03: Midway! (Scaling variables)'
     commit

     -- Then other things that we're not scaling by, but we'd still like for panel balance:
       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'VALUESEG'
             ,0          -- indicates we're not scaling by this, because these variables are pulled onto a different sheet
             ,nssl.value_segment
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.value_segment

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'MOSAIC'
             ,0
             ,nssl.Mosaic_segment -- Special treatment for the MOSAIC segment names gets handled at the end
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.Mosaic_segment

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'FINANCIALSTRAT'
             ,0
             ,nssl.Financial_strategy_segment
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.Financial_strategy_segment

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'ONNET'
             ,0
             ,case when nssl.is_OnNet = 1 then '1.) OnNet' else '2.) OffNet' end
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.is_OnNet

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'SKYGO'
             ,0
             ,case when nssl.uses_sky_go = 1 then '1.) Uses Sky Go' else '2.) No Sky Go' end
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.uses_sky_go

     commit
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from Vespa_PanMan_all_aggregated_results

     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'B03: Midway! (Aggregated variables)', coalesce(@QA_catcher, -1)

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (Panel cleanse)'
     commit

     -- Okay, now all of that is done, we can patch the index calculations into
     -- the whole lot at once (the variables got calculated further up when we
     -- did indices for each segment):
     update Vespa_PanMan_all_aggregated_results
        set Good_Household_Index = 
			case    when pt.panel_reporters > 0  then	(
															case    when 200 < 100 * (Acceptable_Households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters then 200
																	else       100 * (Acceptable_Households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters
															end
														)
					else 0
			end
       from Vespa_PanMan_all_aggregated_results
            left join #panel_totals as pt on Vespa_PanMan_all_aggregated_results.panel = pt.panel

     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (Balance indexing)'

     commit

     -- Oh, and also, we want to tack the full names onto the Experian 3rd party
     -- things...

     update Vespa_PanMan_all_aggregated_results
       set variable_value = case variable_value
           when '00' then '00: Families'
           when '01' then '01: Extended family'
           when '02' then '02: Extended household'
           when '03' then '03: Pseudo family'
           when '04' then '04: Single male'
           when '05' then '05: Single female'
           when '06' then '06: Male homesharers'
           when '07' then '07: Female homesharers'
           when '08' then '08: Mixed homesharers'
           when '09' then '09: Abbreviated male families'
           when '10' then '10: Abbreviated female families'
           when '11' then '11: Multi-occupancy dwelling'
           else 'U: Unclassified HHComp' end
     where aggregation_variable = 'HHCOMP'

     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (HHComposition update)'

     commit

     update Vespa_PanMan_all_aggregated_results
        set variable_value = case variable_value
         when 'A' then 'A: Alpha Territory'
         when 'B' then 'B: Professional Rewards'
         when 'C' then 'C: Rural Solitude'
         when 'D' then 'D: Small Town Diversity'
         when 'E' then 'E: Active Retirement'
         when 'F' then 'F: Suburban Mindsets'
         when 'G' then 'G: Careers and Kids'
         when 'H' then 'H: New Homemakers'
         when 'I' then 'I: Ex-Council Community'
         when 'J' then 'J: Claimant Cultures'
         when 'K' then 'K: Upper Floor Living'
         when 'L' then 'L: Elderly Needs'
         when 'M' then 'M: Industrial Heritage'
         when 'N' then 'N: Terraced Melting Pot'
         when 'O' then 'O: Liberal Opinions'
         else 'U: Unknown MOSAIC' end
     where aggregation_variable = 'MOSAIC'

     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (MOSAIC update)'

     commit

     update Vespa_PanMan_all_aggregated_results
        set variable_value = case variable_value
         when 'A' then 'A: Successful Start'
         when 'B' then 'B: Happy Housemates'
         when 'C' then 'C: Surviving Singles'
         when 'D' then 'D: On The Breadline'
         when 'E' then 'E: Flourishing Families'
         when 'F' then 'F: Credit Hungry Families'
         when 'G' then 'G: Gilt Edged Lifestyles'
         when 'H' then 'H: Mid Life Affluence'
         when 'I' then 'I: Modest Mid Years'
         when 'J' then 'J: Advancing Status'
         when 'K' then 'K: Ageing Workers'
         when 'L' then 'L: Wealthy Retirement'
         when 'M' then 'M: Elderly Deprivation'
         else 'U: Unknown FSS' end
     where aggregation_variable = 'FINANCIALSTRAT'

     execute citeam.logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (Financial strategy update)'

     commit

     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'B03: Complete! (Patched updates)'
    
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B03-1 DML command status: '||@@error
    
     /****************** B04: DATA COMPLETENESS ******************/

     -- So all we want to do here is end up with two numbers: we want to partition the
     -- Sky Base into "In a segment with zero reliably reporting boxes" - ie entirely
     -- unrepresented, and "In a segment with (some factor) of good reporting boxes or
     -- less" - ie poorly represented. Then the goal is to get these two numbers as low
     -- as possible, getting even coverage on those segments. Fortunately, we've now got
     -- the reliability flag on the "Vespa_PanMan_Scaling_Segment_Profiling" table. Though,
     -- we do only want to run this over the scaling variables, which means we have to
     -- group out the non-scaling segments and then rebuild the indics again.

     declare @sky_base_coverage              float
     declare @reliability_rating             float
     declare @households_reliably_reporting  int

     -- Summarise everything further into the scaling segment only build (and we also only
     -- care about Vespa here):
       select scaling_segment_id
             ,sum(Sky_Base_Households)               as Sky_Base_Households
             ,sum(Acceptably_reliable_households)    as Acceptably_reliable_households
             ,convert(decimal(6,2), null)            as Acceptably_reporting_index
         into #scaling_completeness_survey
         from Vespa_PanMan_Scaling_Segment_Profiling
        where Panel in ('VESPA','VESPA11')
     group by scaling_segment_id

     commit

     -- now add on the indices calculations:
     update #scaling_completeness_survey
        set Acceptably_reporting_index = 
			case    when pt.panel_reporters > 0  then	(
															case    when 200 < 100 * (Acceptably_reliable_households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters then 200
																	else       100 * (Acceptably_reliable_households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters
															end
														)
					else 0
			end
       from #scaling_completeness_survey
            left join #panel_totals as pt on pt.panel = 'VESPA'

     commit

     -- And now build the coverage metric:
     select @sky_base_coverage = sum(Sky_Base_Households) / convert(float, @total_sky_base)
       from #scaling_completeness_survey
      where Acceptably_reporting_index > 80

     if @sky_base_coverage is null
         set @sky_base_coverage = 0 -- that'd be bad, but we'll catch it...

     -- Ok, now secondary statistics: how big is the reliable panel?
     select @households_reliably_reporting = coalesce(panel_reporters,0)
       from #panel_totals
      where panel = 'VESPA'

     if @households_reliably_reporting is null
         set @households_reliably_reporting = 0

     -- Finally, how reliably is our reporting?
     select @reliability_rating = convert(float, @households_reliably_reporting) / count(1)
       from Vespa_PanMan_all_households
      where panel in ('VESPA','VESPA11')

     commit

     -- Oh hey we probably want to track these numbers over time too? So we can see them
     -- shrink? First check if there's already metrics in the table that we'd kill
     set @QA_catcher = -1
     select @QA_catcher = count(1) from vespa_analysts.Vespa_PanMan_Historic_Panel_Metrics where metric_date = @profiling_thursday
     if @QA_catcher <> 0
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'B04: Metric date colision! Old data will be destroyed!'

     commit

     delete from vespa_analysts.Vespa_PanMan_Historic_Panel_Metrics
      where metric_date = @profiling_thursday

     -- Heh, we might also have to manage this guy historically because we're now profiling
     -- at the back instead of the front of the analysis period, and we're also changing a
     -- lot of how the coverage metrics work etc. Might clip out a few recent items etc.
     insert into vespa_analysts.Vespa_PanMan_Historic_Panel_Metrics (
            metric_date
           ,sky_base_coverage
           ,reliability_rating
           ,households_reliably_reporting
     )
     values (
            @profiling_thursday
           ,@sky_base_coverage
           ,@reliability_rating
           ,@households_reliably_reporting
     )
     -- The rest of the metrics we'll push in during a later section

     -- Then we just need to pull out the top 24 items, and that's a rolling summary of the last 24 weeks. Is good!
     set @QA_catcher = -1
     set @QA_catcher = convert(int, @sky_base_coverage * @total_sky_base)

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'B04: Complete! (Data completeness)', coalesce(@QA_catcher, -1)
     commit
    
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B04-1 DML command status: '||@@error
    
     /****************** B05: REPORTING BOX SWING - HOW MUCH IS ONE BOX WORTH? ******************/

     -- This guy is just a profiling graph of Box Weight, ie, how much each box gets weighted
     -- up to the Sky Base. Good for another different measure of how the evenness of our panel
     -- cover. This guy also gives a pretty good indication of how we compare to BARB and other
     -- panels, in the sense that these weights indicate how much interpolation is taking place.

     -- We should really see the weight profile flatten out in the ideal balanced panel case,
     -- but right now we still have spikes at either end; tiny weights < 1 at one end weights
     -- of over 5k at the other, and this is with Scaling 2 involved.

     select sw.weighting
           ,rank() over (order by sw.weighting desc, si.account_number) as weight_rank -- subscriber_id just to make it unique
           ,convert(tinyint, null) as weighting_percentile
       into vespa_PanMan_08_ordered_weightings
       from vespa_analysts.SC2_weightings as sw
            inner join vespa_analysts.SC2_intervals as si on sw.scaling_day = @profiling_thursday
                                                         and sw.scaling_segment_ID = si.scaling_segment_ID
         -- Not joining by non_scaling_segment_ID here because this is all about the scaling weightings
                                                         and @profiling_thursday between si.reporting_starts and si.reporting_ends

     commit
     declare @reporting_accounts int
     declare @sample_diff        int

     select @reporting_accounts = count(1)
       from vespa_PanMan_08_ordered_weightings
     set @sample_diff = @reporting_accounts / 100 -- ints take care of the rounding

     commit

     delete from vespa_PanMan_08_ordered_weightings
      where mod(weight_rank, @sample_diff) <> 1

     -- We don't need to track what segment IDs they were, we get that from the indexing
     -- queries, this is just for general health of panel. But we do want to normalise the
     -- weight rank into the percentile:
     update vespa_PanMan_08_ordered_weightings
        set weighting_percentile = 100 - weight_rank / @sample_diff

     commit
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_PanMan_08_ordered_weightings

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'B05: Complete! (Box swing)', coalesce(@QA_catcher, -1)
     commit
    
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B05-1 DML command status: '||@@error
    
     /****************** B06: REDUNDANCY INDEXING INTO THE ALTERNATE PANELS ******************/

     -- Again just using the SBV and the most recent segmentation. Um, can we also do this
     -- directly from "Vespa_PanMan_Scaling_Segment_Profiling"? probably can, actually, but
     -- again we'll report only items that appear over / under indexed. 80 / 120 again.

     -- In fact, we just need to populate the column on that, we already have all the values
     -- we need to do that...

     -- Well, dunn know we want to handle this exactly, is another index on 60k segments going
     -- to be useful? Maybe we want another two basic measures? Though if we're using it for
     -- targeting, then yes, we want an index for each segment based on the distributions of
     -- boxes which report well...

     --update Vespa_PanMan_Scaling_Segment_Profiling
     --set Redundancy_Index =


     /* The example of calculating indices from above: just need to decide how we want to describe the totals (reliable Live into reliable 6+7?)
     update Vespa_PanMan_all_aggregated_results
     set
         Good_Household_Index           = 100 * (Acceptable_Households) * @total_sky_base
             / convert(float, Sky_Base_Households) / pt.panel_reporters
               end
     from Vespa_PanMan_all_aggregated_results
     inner join #panel_totals as pt
     on Vespa_PanMan_all_aggregated_results.panel = pt.panel
     */

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'B06: NYIP (Indexing vs Alternates)' --, coalesce(@QA_catcher, -1)
     commit

     /****************** B07: EVEN HIGHER LEVEL SUMMARIES OF SINGLE VARIABLES ******************/

     -- So we want a RAG status thing for each of the variables in our setup. We're going to
     -- calculate the total Euclidian distance of all indices to the ideal, normalise out the
     -- number of dimensions, and use this as our metric, so, small is good. Call it imbalance.

     -- The downside is we have to de-aggregate this to put it in the historic table. The upside
     -- is that we can start off doing everything normalised for all panels at once...

       select panel -- it gets denormalised in the extraction query though...
             ,case aggregation_variable
               when 'UNIVERSE'         then 'Universe'
               when 'REGION'           then 'Region'
               when 'HHCOMP'           then 'Household composition'
               when 'PACKAGE'          then 'Package'
               when 'TENURE'           then 'Tenure'
               when 'BOXTYPE'          then 'Box type'
               when 'VALUESEG'         then 'Value segment'
               when 'MOSAIC'           then 'MOSAIC'
               when 'FINANCIALSTRAT'   then 'FSS'
               when 'ONNET'            then 'OnNet / Offnet'
               when 'SKYGO'            then 'Sky Go users'
               else 'FAIL!'
              end as variable_name
             ,case aggregation_variable
               when 'UNIVERSE'         then 1
               when 'REGION'           then 2
               when 'HHCOMP'           then 3
               when 'PACKAGE'          then 4
               when 'TENURE'           then 5
               when 'BOXTYPE'          then 6
               when 'VALUESEG'         then 7
               when 'MOSAIC'           then 8
               when 'FINANCIALSTRAT'   then 9
               when 'ONNET'            then 10
               when 'SKYGO'            then 11
               else -1
              end as sequencer -- so the results go out into the excel thing in the right order
             ,sqrt(avg(
               (Good_Household_Index - 100) * (Good_Household_Index - 100)           )) as imbalance_rating
         into vespa_PanMan_09_traffic_lights
         from Vespa_PanMan_all_aggregated_results
     group by panel, aggregation_variable

     -- Indices? nah, it's a tiny table.

     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'B07: Complete! (Traffic lights)'
     commit
    
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'B07-1 DML command status: '||@@error
    
     /****************** D01: HIGH LEVEL OVERVIEW REPORT TABLES ******************/

     -- First the Vespa panel; yeah, ugly way of handling transposes, w/e
     select sum(Panel_Households) as result_value
           ,1 as sequencer -- We want the stuff going into subsequent rows, so...
       into vespa_PanMan_02_vespa_panel_overall
       from Vespa_PanMan_all_aggregated_results
      where panel in ('VESPA','VESPA11') and aggregation_variable = 'REGION' -- could use any variable

     insert into vespa_PanMan_02_vespa_panel_overall
     select sum(Acceptable_Households), 2
       from Vespa_PanMan_all_aggregated_results
      where panel in ('VESPA','VESPA11') and aggregation_variable = 'REGION'

     insert into vespa_PanMan_02_vespa_panel_overall
     select sum(Unreliable_Households), 3
       from Vespa_PanMan_all_aggregated_results
      where panel in ('VESPA','VESPA11') and aggregation_variable = 'REGION'

     insert into vespa_PanMan_02_vespa_panel_overall
     select sum(Zero_reporting_Households), 4
       from Vespa_PanMan_all_aggregated_results
      where panel in ('VESPA','VESPA11') and aggregation_variable = 'REGION'

     insert into vespa_PanMan_02_vespa_panel_overall
     select sum(Recently_enabled_households), 5
       from Vespa_PanMan_all_aggregated_results
      where panel in ('VESPA','VESPA11') and aggregation_variable = 'REGION'

     commit

     -- OK, now for Alt panel 6
     select sum(Panel_Households) as result_value
           ,1 as sequencer -- We want the stuff going into subsequent rows, so...
       into vespa_PanMan_03_panel_6_overall
       from Vespa_PanMan_all_aggregated_results
      where panel = 'ALT6' and aggregation_variable = 'REGION' -- could use any variable

     insert into vespa_PanMan_03_panel_6_overall
     select sum(Acceptable_Households), 2
       from Vespa_PanMan_all_aggregated_results
      where panel = 'ALT6' and aggregation_variable = 'REGION'

     insert into vespa_PanMan_03_panel_6_overall
     select sum(Unreliable_Households), 3
       from Vespa_PanMan_all_aggregated_results
      where panel = 'ALT6' and aggregation_variable = 'REGION'

     insert into vespa_PanMan_03_panel_6_overall
     select sum(Zero_reporting_Households), 4
       from Vespa_PanMan_all_aggregated_results
      where panel = 'ALT6' and aggregation_variable = 'REGION'

     insert into vespa_PanMan_03_panel_6_overall
     select sum(Recently_enabled_households), 5
       from Vespa_PanMan_all_aggregated_results
      where panel = 'ALT6' and aggregation_variable = 'REGION'

     commit

     -- Now for Alt panel 7:
     select sum(Panel_Households) as result_value
           ,1 as sequencer -- We want the stuff going into subsequent rows, so...
       into vespa_PanMan_04_panel_7_overall
       from Vespa_PanMan_all_aggregated_results
      where panel = 'ALT7' and aggregation_variable = 'REGION' -- could use any variable

     insert into vespa_PanMan_04_panel_7_overall
     select sum(Acceptable_Households), 2
       from Vespa_PanMan_all_aggregated_results
      where panel = 'ALT7' and aggregation_variable = 'REGION'

     insert into vespa_PanMan_04_panel_7_overall
     select sum(Unreliable_Households), 3
       from Vespa_PanMan_all_aggregated_results
      where panel = 'ALT7' and aggregation_variable = 'REGION'

     insert into vespa_PanMan_04_panel_7_overall
     select sum(Zero_reporting_Households), 4
       from Vespa_PanMan_all_aggregated_results
      where panel = 'ALT7' and aggregation_variable = 'REGION'

     insert into vespa_PanMan_04_panel_7_overall
     select sum(Recently_enabled_households), 5
       from Vespa_PanMan_all_aggregated_results
      where panel = 'ALT7' and aggregation_variable = 'REGION'

      -- since 20/02/2013, we're now keeping track of history of figures on this section
      -- aiming to spot trend over time on reporting metrics...
        declare @weekending date
        
        select  @weekending = (@profiling_thursday + 8) -- This is always been saturday based on how @profiling_thursday is derived
                                                        -- Until 28/02/2013
        
        if exists   (   
                        select  first *
                        from    vespa_analysts.vespa_panman_hist_summary
                        where   weekending = @weekending
                    )
            begin
                
                delete  from vespa_analysts.vespa_panman_hist_summary
                where   weekending = @weekending
                
                commit
                
            end
            
        insert  vespa_analysts.vespa_panman_hist_summary
        select  @weekending as weekending
                ,case 
                    when p12.sequencer = 1 then 'ac population'
                    when p12.sequencer = 2 then 'Panel ac report ok'
                    when p12.sequencer = 3 then 'Panel ac report unre'
                    when p12.sequencer = 4 then 'Panel ac report none'
                    when p12.sequencer = 5 then 'Panel ac recent enab'
                end as context
                ,p12.result_value           as dp
                ,ap6.result_value           as p6
                ,ap7.result_value           as p7
        from    vespa_analysts.vespa_PanMan_02_vespa_panel_overall          as p12
                inner join  vespa_analysts.vespa_PanMan_03_panel_6_overall  as ap6
                on  p12.sequencer = ap6.sequencer
                inner join vespa_analysts.vespa_PanMan_04_panel_7_overall   as ap7
                on  p12.sequencer = ap7.sequencer
        
        -- since 03/04/2013 we are now keeping track of history for Traffic lights...
        if exists   (
                        select  first *
                        from    vespa_analysts.Vespa_PanMan_hist_trafficlight
                        where   weekending = @weekending
                    )
            begin
                
                delete  from vespa_analysts.Vespa_PanMan_hist_trafficlight
                where   weekending = @weekending
                
                commit
                
            end
            
        insert  into vespa_analysts.Vespa_PanMan_hist_trafficlight
        select  @weekending as weekending
                ,variable_name
                ,sum(case when panel = 'VESPA' then imbalance_rating else 0 end) as vespa_imbalance
                ,sum(case when panel = 'ALT6'  then imbalance_rating else 0 end) as alt6_imbalance
                ,sum(case when panel = 'ALT7'  then imbalance_rating else 0 end) as alt7_imbalance
        from    vespa_analysts.vespa_PanMan_09_traffic_lights
        where   sequencer between 1 and 6
        group by variable_name
        
        commit 

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'D01: Complete! (Panel overviews)'
     commit
    
    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'D01-1 DML command status: '||@@error
    
     /****************** D02: ANALYSIS OF VESPA REPORTING QUALITY BY UNIVERSE ******************/

     -- This section follows a very similar structure to the panel overviews, but we're breaking
     -- down the Vespa panel in terms of the scaling universes. This is not super efficient as
     -- we're basically ulling single rows out of the aggregated results but whatever. We're
     -- still managing the sequencer here, so that's slightly useful.

     -- So these guys are named tables 42 through 44 because they're like the reporting quality
     -- breakdowns numberes 02-04, but they're specific to panel 4 like tables 40 and 41.

     select sum(Panel_Households) as result_value
          ,1 as sequencer -- We want the stuff going into subsequent rows, so...
       into vespa_PanMan_42_vespa_panel_single_box_HHs
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'A) Single box HH'

     insert into vespa_PanMan_42_vespa_panel_single_box_HHs
     select sum(Acceptable_Households), 2
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'A) Single box HH'

     insert into vespa_PanMan_42_vespa_panel_single_box_HHs
     select sum(Unreliable_Households), 3
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'A) Single box HH'

     insert into vespa_PanMan_42_vespa_panel_single_box_HHs
     select sum(Zero_reporting_Households), 4
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'A) Single box HH'

     insert into vespa_PanMan_42_vespa_panel_single_box_HHs
     select sum(Recently_enabled_households), 5
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'A) Single box HH'

     commit

     -- And now the dual box universe...
    /*
     select sum(Panel_Households) as result_value
          ,1 as sequencer -- We want the stuff going into subsequent rows, so...
      into vespa_PanMan_43_vespa_panel_dual_box_HHs
      from Vespa_PanMan_all_aggregated_results
     where panel = 'VESPA'
       and aggregation_variable = 'UNIVERSE'
       and variable_value = 'B) Dual box HH'

     insert into vespa_PanMan_43_vespa_panel_dual_box_HHs
     select sum(Acceptable_Households), 2
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'B) Dual box HH'

     insert into vespa_PanMan_43_vespa_panel_dual_box_HHs
     select sum(Unreliable_Households), 3
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'B) Dual box HH'

     insert into vespa_PanMan_43_vespa_panel_dual_box_HHs
     select sum(Zero_reporting_Households), 4
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'B) Dual box HH'

     insert into vespa_PanMan_43_vespa_panel_dual_box_HHs
     select sum(Recently_enabled_households), 5
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'B) Dual box HH'

     commit
        */ -- No longer used, left in for reference

     -- Finally the multi-box households

     select sum(Panel_Households) as result_value
          ,1 as sequencer -- We want the stuff going into subsequent rows, so...
      into vespa_PanMan_44_vespa_panel_multi_box_HHs
      from Vespa_PanMan_all_aggregated_results
     where panel = 'VESPA'
       and aggregation_variable = 'UNIVERSE'
       and variable_value = 'B) Multiple box HH'

     insert into vespa_PanMan_44_vespa_panel_multi_box_HHs
     select sum(Acceptable_Households), 2
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'B) Multiple box HH'

     insert into vespa_PanMan_44_vespa_panel_multi_box_HHs
     select sum(Unreliable_Households), 3
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'B) Multiple box HH'

     insert into vespa_PanMan_44_vespa_panel_multi_box_HHs
     select sum(Zero_reporting_Households), 4
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'B) Multiple box HH'

     insert into vespa_PanMan_44_vespa_panel_multi_box_HHs
     select sum(Recently_enabled_households), 5
       from Vespa_PanMan_all_aggregated_results
      where panel = 'VESPA'
        and aggregation_variable = 'UNIVERSE'
        and variable_value = 'B) Multiple box HH'

     -- done!

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'D02: Complete! (Vespa Universes)'
     commit

    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'D02-1 DML command status: '||@@error
    
    
     /****************** M01: BOX SELECTION - FIRST ROUND: UP TO STRATIFIED LIMIT ******************/

     /* The whole box selection thing isn't in play any more; we're doing kind of ad-hoc builds
     ** of migrations when we get around to it, but there's currently nothing about regularly
     ** tuning the panel or weekly balance tweaks or anything. ##11##

     -- So we need to create the three tables:
     --  * vespa_PanMan_11_panel_4_discontinuations
     --  * vespa_PanMan_12_panel_6_imports
     --  * vespa_PanMan_13_panel_7_imports
     -- These guys should end up with subscriber_id as primary keys.

     -- So this is our target panel sample size:
     declare @target_Vespa_panel_size        float   -- Since we're forming ratios with it etc
     select @target_Vespa_panel_size = count(1) from Vespa_PanMan_all_households
     -- eventualy wil be 500k, but for now we're keeping panel size constant
     commit

     -- So the only thing in there we don't have yet is... the rank of reporting quality
     -- withing each segment. (Over different segments, we want reporting quality, no rank,
     -- because we want to compare how much data we'd get back for each option.)

     select
         account_number
         ,rank() over (partition by scaling_segment_ID, non_scaling_segment_ID order by reporting_quality desc, accno_SHA1) as reporting_rank
     into #ranking_the_reporting
     from Vespa_PanMan_all_households

     commit
     create unique index fake_pk on #ranking_the_reporting (account_number)
     commit

     update Vespa_PanMan_all_households
     set reporting_rank = rr.reporting_rank
     from Vespa_PanMan_all_households
     inner join #ranking_the_reporting as rr
     on Vespa_PanMan_all_households.account_number = rr.account_number

     commit
     drop table #ranking_the_reporting
     commit

     -- So now we have the ranks, we can do the stratification sampling. The utility measures
     -- come after that, since they depend on the indexing of what we'd add in the first cycle
     -- of box selection.

     -- Clip out things which are past the segmentation stratification goals that we have:
     select ah.account_number
     into #first_round_HHs
     from Vespa_PanMan_all_households as ah
     inner join Vespa_PanMan_Scaling_Segment_Profiling as ssp
     on ah.scaling_segment_id = ssp.scaling_segment_id
     and ah.non_scaling_segment_id = ssp.non_scaling_segment_id
     where ah.reporting_rank <= (ssp.Sky_Base_Households * @target_Vespa_panel_size / @total_sky_base) -- Want to sample according to the stratifications
     and reporting_categorisation in ('Acceptable') -- but we only want the good households

     commit

     -- Decisions has been made! Push them back into table:
     create index fake_pk on #first_round_HHs (account_number)
     commit

     update Vespa_PanMan_all_households
     set selection = 'FIRST'
     from Vespa_PanMan_all_households
     inner join #first_round_HHs as frh
     on Vespa_PanMan_all_households.account_number = frh.account_number

     commit

     -- Count the marked boxes and move on

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from Vespa_PanMan_all_households
     where selection = 'FIRST'

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'M01: Complete! (Stratified selection)', coalesce(@QA_catcher, -1)
     commit

     */

     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'M01: NYIP! (Stratified selection)'
     commit

     /****************** M02: SECOND ROUND: FILLING IN WITH UTILITY ******************/

     /* As per comment at section M01, there's no automated weekly rebalancing in play yet. ##11##

     -- OK, how we're handlinhg this prioritisation:
     -- We're building a utility measure for each household based on:
     --      1. Sky Base population in the same segment
     --      2. Number of days of solid reporting in the last 30 days (ie all boxes reporting)
     --      3. Rank of reporting solidity across reporting boxes in that segment
     -- Then we have a bunch of perameters for how much weight each of those effects
     -- get, and the first round of selection happens on those weights up to the point
     -- of segmented stratification. But there will still be a bunch of segments that
     -- don't get enough boxes, so we'll adjust the utilities based on the indices of
     -- what's already selected and select again. We'll raise the threshhold each time,
     -- until we get up to the number of boxes we want on the Vespa panel.

     -- Yeah, for this weighting, smaller is better.

     declare @rank_de_utilitify_factor       float   -- How much to devalue ranking in comparison to reporting count
     declare @post_sampling_rejudge_factor   float   -- After round of stratified selection, adjust the utility based on this factor applied to the product of the indices (about 1 rather than about 100 though)
     -- What other measures do we want? What's the selection process?


     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from Vespa_PanMan_all_households
     where selection = 'SECOND'

     */

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'M02: NYIP! (Utility selection)'
     commit

     /****************** M03: SOME OTHER SELECTION ROUND BASED ON AS YET UNKNOWN MAGIC ******************/

     -- Not yet in play!

     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_all_households
      where selection = 'SECOND'

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'M03: NYIP! (Magical selection)'
     commit

     /****************** M04: CHOOSING BOX SWAP LIMITS AND FORMING LISTS ******************/

     /* Like the others in part M, this guy isn't live yet. Maybe it'll get un-commented
     ** sometime after we start to put time into establishing regular tuning work ##11##

     -- OK, so anything that's not marked as wanted gets thrown out:
     select sbv.account_number, sbv.subscriber_id
     into vespa_PanMan_11_panel_4_discontinuations
     from vespa_analysts.vespa_single_box_view as sbv
     inner join Vespa_PanMan_all_households as ah
     on sbv.account_number = ah.account_number
     where ah.panel = 'VESPA'
     and ah.selection is null or ah.selection not in ('FIRST', 'SECOND', 'THIRD') -- Might have an additional round of selection?

     -- Then pull in anything we decided we wanted from Alternate 6:
     select sbv.account_number, sbv.subscriber_id
     into vespa_PanMan_12_panel_6_imports
     from vespa_analysts.vespa_single_box_view as sbv
     inner join Vespa_PanMan_all_households as ah
     on sbv.account_number = ah.account_number
     where ah.panel = 'ALT6'
     and ah.selection in ('FIRST', 'SECOND', 'THIRD')

     -- Then the same for 7:
     select sbv.account_number, sbv.subscriber_id
     into vespa_PanMan_13_panel_7_imports
     from vespa_analysts.vespa_single_box_view as sbv
     inner join Vespa_PanMan_all_households as ah
     on sbv.account_number = ah.account_number
     where ah.panel = 'ALT7'
     and ah.selection in ('FIRST', 'SECOND', 'THIRD')

     -- And the results QA steps...
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from vespa_PanMan_11_panel_4_discontinuations
     -- Yes that's the same query as before, but we've clipped things out of it now...


     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'M04: Complete! (Make swap lists)', coalesce(@QA_catcher, -1)
     commit

     */

     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'M04: NYIP! (Make swap lists)'
     commit

     /****************** Q01: AUTOMATED QA! UNITS TESTS! ******************/

     -- Um... what are we testing thus far then? So far we're not bothering with checking if the
     -- numbers all fail because only the Vespa Live panel is in place...

     -- So we know exactly how many variables we're working with here: for scaling,
     --  * 4 universes (including non-scaling, whatever that is)
     --  * 16 isba regions... previously we had 14, have we got Ireland in here too now or something
     --  * 13 household compositions
     --  * 6 ranks of tenure
     --  * 9 packages
     --  * 14 box types
     -- Then for non-scaling:
     --  * 7 value segments
     --  * 16 MOSAIC segments
     --  * 14 financial strategy segments
     --  * 2 for Onnet/Offnet
     --  * 2 for Sky Go or not
     -- So that's a total of 103 single variable profiles per panel... (and 600 segments per active customer...)
     declare @panel_count tinyint

     select @panel_count = 103 * count (distinct panel)
       from Vespa_PanMan_all_aggregated_results

     -- and how many do we actually have?
     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_all_aggregated_results

     commit

     if @QA_catcher is null or @QA_catcher <> @panel_count
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'Q01a: Aggregated variable listing should have ' || convert(varchar(5),@panel_count) || ' items, instead has:', coalesce(@QA_catcher, -1)

     commit
     -------------------------------------------------------------------------------------


     -- Also, all of the index values should be populated:

     -- Okay so this guy is throwing up single instances of the Eire guy again. Apparently it's
     -- the only 'Eire' item in our customer base, or otherwise, the only one that survives the
     -- ROI exclusion checks. We'll kick it out of the table:
     delete from Vespa_PanMan_all_aggregated_results
      where aggregation_variable = 'REGION'
        and variable_value = 'Eire'
        and sky_base_households = 1
     -- That's specific enough to hit probably only our special case...

     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_all_aggregated_results
      where Good_Household_Index is null

     if @QA_catcher is null or @QA_catcher <> 0
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'Q01b: Aggregated variable listing has NULL indices!', coalesce(@QA_catcher, -1)

     set @QA_catcher = -1

     -- Output number is for a percentile graph, it should have exactly 101 items (as it includes both endpoints)
     select @QA_catcher = count(1)
       from vespa_PanMan_08_ordered_weightings

     if @QA_catcher is null or @QA_catcher <> 101
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'Q01c: Percentile graphs should have 101 items, instead has:', coalesce(@QA_catcher, -1)

     commit

     set @QA_catcher = -1

     -- We have 11 variables (6 scaling, 5 non-scaling) which should be getting traffic-lighted separately for each panel:
     select @QA_catcher = count(1)
       from vespa_PanMan_09_traffic_lights

     if @QA_catcher is null or @QA_catcher <> @TrafficLights_stdCount
      execute citeam.logger_add_event @PanMan_logging_ID, 2, 'Q01d: Traffic light listing should have '||convert(varchar(3),@TrafficLights_stdCount)||' items, instead has:', coalesce(@QA_catcher, -1)

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'Q01: Complete! (Dashboard QA)'
     commit

    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'Q01-1 DML command status: '||@@error
    
     /****************** Q03: END TO END CONTROL TOTALS ******************/

     -- Oh, and internal consistency stuff: let's just check that the totals we've got for all the
     -- cuts we're reporting match the totals we see in single box view...

       select panel, sum(panel_households) as hh_count
         into #qa_panel_tots
         from Vespa_PanMan_all_aggregated_results
        where aggregation_variable = 'REGION'
     group by panel

       select panel, count(distinct account_number) as hh_count
         into #qa_panel_raw
         from vespa_analysts.vespa_single_box_view
     group by panel

     -- But we'll check each of the known existing panels separately...
     declare @QA_panel_count  int
     declare @QA_output_count int

     -- First the Vespa:
     set @QA_catcher = -1

     select @QA_panel_count  = hh_count from #qa_panel_tots where panel = 'VESPA'
     select @QA_output_count = hh_count from #qa_panel_raw  where panel = 'VESPA'

     --set @QA_catcher = @panel_count - @output_count
     set @QA_catcher = @QA_panel_count - @QA_output_count

     -- Yeah, so this guy is failing by small amounts. There are things in Scaling that
     -- aren't "in" our vespa panel. But it's easily bounded by the CLASH! boxes, so
     -- that's probably what it is; scaling not isolating panel conflicts with Sky View.

     commit

     if @QA_catcher is null or @QA_catcher <> 0
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'Q02a: Control total failure for Vespa panel!', coalesce(@QA_catcher, -1)

     -- Panels 7 and 8: Trick question, they shouldn't be in play yet
     select @QA_panel_count  = hh_count from #qa_panel_tots where panel = 'ALT6'
     select @QA_output_count = hh_count from #qa_panel_raw  where panel = 'ALT6'

     if @QA_output_count <> 0 or @QA_panel_count <> 0
         -- Report on the output one because that's the one we've got our results I guess...
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'Q02b: Control total failure for ALT6 panel! (but... not expecting any at all?)', @QA_output_count

     -- Besides, panel management report will need further tweaking when 6 and 7 come online, so we'll just
     -- change this bit around when that gets here.

     set @QA_panel_count  = -1
     set @QA_output_count = -1

     select @QA_panel_count  = hh_count from #qa_panel_tots where panel = 'ALT7'
     select @QA_output_count = hh_count from #qa_panel_raw  where panel = 'ALT7'

     if @QA_output_count <> 0 or @QA_panel_count <> 0
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'Q02c: Control total failure for ALT7 panel! (but... not expecting any at all?)', @QA_output_count

     -- Are there other control totals we want to involve? Maybe later.

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'Q02: Complete! (Control totals)'
     commit

    EXECUTE citeam.logger_add_event @PanMan_logging_ID, 4, 'Q03-1 DML command status: '||@@error
    
     /****************** Q04: CONSISTENCY TESTING ON THE PANEL MIGRATION ******************/

     /* Yeah, panel balance iterations aren't in play so we're not testing the swap lists we're not buliding ##11##

     -- Okay, so the panel flux should net to zero (so the size of the live panel doesn't change)
     select @QA_catcher = count(1) from vespa_PanMan_12_panel_6_imports
     select @QA_catcher = @QA_catcher + count(1) from vespa_PanMan_13_panel_7_imports
     -- So that's the total boxes entering panel 4. The boxes leaving are:
     select @QA_catcher = @QA_catcher - count(1) from vespa_PanMan_11_panel_4_discontinuations

     -- Don't want any unplanned / unbalanced flux between Live & Alternate Day panels
     if @QA_catcher is null or @QA_catcher <> 0
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'Q04a: Non-zero Live Panel flux!', coalesce(@QA_catcher, -1)

     commit

     -- Also, the three lists should be disjoint of subscriber IDs:
     select @QA_catcher = count(1)
     from vespa_PanMan_11_panel_4_discontinuations as p4
     inner join vespa_PanMan_12_panel_6_imports as p6
     on p4.subscriber_id = p6.subscriber_id

     if @QA_catcher is null or @QA_catcher <> 0
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'Q04b: Box rejected from panel 4 and requested from 6!', coalesce(@QA_catcher, -1)

     commit

     -- The same check between 4 and 7
     select @QA_catcher = count(1)
     from vespa_PanMan_11_panel_4_discontinuations as p4
     inner join vespa_PanMan_13_panel_7_imports as p7
     on p4.subscriber_id = p7.subscriber_id

     if @QA_catcher is null or @QA_catcher <> 0
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'Q04c: Box rejected from panel 4 and requested from 7!', coalesce(@QA_catcher, -1)

     commit

     -- And again on the two lists of boxes coming in...
     select @QA_catcher = count(1)
     from vespa_PanMan_12_panel_6_imports as p6
     inner join vespa_PanMan_13_panel_7_imports as p7
     on p6.subscriber_id = p7.subscriber_id

     if @QA_catcher is null or @QA_catcher <> 0
         execute citeam.logger_add_event @PanMan_logging_ID, 2, 'Q04d: Same box requested from both alt panels!', coalesce(@QA_catcher, -1)

     commit
     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'Q04: Complete! (Panel migration QA)', coalesce(@QA_catcher, -1)
     commit

     */

     execute citeam.logger_add_event @PanMan_logging_ID, 3, 'Q04: NYIP! (Panel migration QA)'
     commit

     /****************** T01: PERMISSIONS! ******************/

     -- So pulls 1 through 3 are still going from core tables...
     grant select on vespa_PanMan_02_vespa_panel_overall         to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
     grant select on vespa_PanMan_03_panel_6_overall             to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
     grant select on vespa_PanMan_04_panel_7_overall             to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
     grant select on vespa_PanMan_08_ordered_weightings          to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
     grant select on vespa_PanMan_09_traffic_lights              to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
     --grant select on vespa_PanMan_11_panel_4_discontinuations    to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
     --grant select on vespa_PanMan_12_panel_6_imports             to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
     --grant select on vespa_PanMan_13_panel_7_imports             to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
     grant select on vespa_PanMan_42_vespa_panel_single_box_HHs  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
     -- grant select on vespa_PanMan_43_vespa_panel_dual_box_HHs    to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security
     grant select on vespa_PanMan_44_vespa_panel_multi_box_HHs   to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, vespa_group_low_security

     /****************** X01: AND WE'RE DONE! ******************/

     EXECUTE citeam.logger_add_event @PanMan_logging_ID, 3, 'PanMan: weekly refresh complete!'
     COMMIT

end;

commit;
go

-- And somethign else to clean up the junk that was built:
if object_id('PanMan_clear_transients') is not null
   drop procedure PanMan_clear_transients;

commit;
go

create procedure PanMan_clear_transients
as
begin
    -- For some reason, these guys needed the explicit schema references while inside a
    -- proc that was called by a different user. Weird.
    -- ##32## - are we recasting this so that the schema is automatically detected?
    delete from vespa_analysts.Vespa_PanMan_all_households
    delete from vespa_analysts.Vespa_PanMan_Scaling_Segment_Profiling
    delete from vespa_analysts.Vespa_PanMan_this_weeks_non_scaling_segmentation
    delete from vespa_analysts.Vespa_PanMan_all_aggregated_results
    delete from vespa_analysts.Vespa_PanMan_panel_redundancy_calculations
    if object_id( 'vespa_analysts.vespa_PanMan_02_vespa_panel_overall') is not null
        drop table vespa_analysts.vespa_PanMan_02_vespa_panel_overall
    if object_id( 'vespa_analysts.vespa_PanMan_03_panel_6_overall') is not null
        drop table vespa_analysts.vespa_PanMan_03_panel_6_overall
    if object_id( 'vespa_analysts.vespa_PanMan_04_panel_7_overall') is not null
        drop table vespa_analysts.vespa_PanMan_04_panel_7_overall
    if object_id( 'vespa_analysts.vespa_PanMan_08_ordered_weightings') is not null
        drop table vespa_analysts.vespa_PanMan_08_ordered_weightings
    if object_id( 'vespa_analysts.vespa_PanMan_09_traffic_lights') is not null
        drop table vespa_analysts.vespa_PanMan_09_traffic_lights
    if object_id( 'vespa_analysts.vespa_PanMan_11_panel_4_discontinuations') is not null
        drop table vespa_analysts.vespa_PanMan_11_panel_4_discontinuations
    if object_id( 'vespa_analysts.vespa_PanMan_12_panel_6_imports') is not null
        drop table vespa_analysts.vespa_PanMan_12_panel_6_imports
    if object_id( 'vespa_analysts.vespa_PanMan_13_panel_7_imports') is not null
        drop table vespa_analysts.vespa_PanMan_13_panel_7_imports
    if object_id( 'vespa_analysts.vespa_PanMan_42_vespa_panel_single_box_HHs') is not null
        drop table vespa_analysts.vespa_PanMan_42_vespa_panel_single_box_HHs
    if object_id( 'vespa_analysts.vespa_PanMan_43_vespa_panel_dual_box_HHs') is not null
        drop table vespa_analysts.vespa_PanMan_43_vespa_panel_dual_box_HHs
    if object_id( 'vespa_analysts.vespa_PanMan_44_vespa_panel_multi_box_HHs') is not null
        drop table vespa_analysts.vespa_PanMan_44_vespa_panel_multi_box_HHs
    if object_id( 'vespa_analysts.Vespa_PanMan_this_weeks_non_scaling_segmentation_bugfix') is not null
        drop table vespa_analysts.Vespa_PanMan_this_weeks_non_scaling_segmentation_bugfix
end;

commit;
go

grant execute on PanMan_make_report          to public;
grant execute on PanMan_clear_transients     to public;
-- Need the central scheduler thing to be able to call the procs. But it gets
-- run within the vespa_analytics account, so it doesn't mean that any random
-- public person can see what's in the resulting tables.

commit;
go

---
