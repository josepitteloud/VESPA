

if object_id(    'PanMan_make_report') is not null
   drop procedure PanMan_make_report;

commit;

drop procedure panman_make_report;
create procedure  PanMan_make_report -- execute PanMan_make_report
   @profiling_thursday         date    = null
as
begin

     -- ****************** A01: SETTING UP THE LOGGER ******************

     DECLARE @PanMan_logging_ID      bigint
     DECLARE @Refresh_identifier     varchar(40)
     declare @run_Identifier         varchar(20)
     declare @recent_profiling_date date
     DECLARE @TrafficLights_stdCount integer
     -- For putting control totals in prior to logging:
     DECLARE @QA_catcher             integer
  declare @exe_status       integer

     set @TrafficLights_stdCount = 11

     if lower(user) = 'kmandal'
         set @run_Identifier = 'VespaPanMan'
     else
         set @run_Identifier = 'PanMan test ' || upper(right(user,1)) || upper(left(user,2))

     set @Refresh_identifier = convert(varchar(10),today(),123) || ' PanMan refresh'
     -- execute logger_create_run @run_Identifier, @Refresh_identifier, @PanMan_logging_ID output

     commit
     -- execute logger_add_event @PanMan_logging_ID, 3, 'A01: Complete! (Report setup)'

     commit

     -- ****************** A02: TABLE RESETS ******************

     execute PanMan_clear_transients

     -- Well that was easy. But we're also going to check that it worked:

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from Vespa_PanMan_all_households

     if @QA_catcher is null or @QA_catcher <> 0
         -- execute logger_add_event @PanMan_logging_ID, 2, 'A02: failed to realise clean table!', @QA_catcher

     commit

     -- execute logger_add_event @PanMan_logging_ID, 3, 'A02: Complete! (Table resets)'
     commit

     -- ****************** A03: TIME BOUNDS ******************

     -- Might not use this for a whole lot, given how much gets done in the midway scaling tables
     if @profiling_thursday is null
     begin
         execute Regulars_Get_report_end_date @profiling_thursday output -- A Saturday
         set @profiling_thursday    = dateadd(day, -8, @profiling_thursday) -- Thursday of SAV flip data, but we're now profiling from the beginning of the period.
     end
     commit

     -- execute logger_add_event @PanMan_logging_ID, 3, 'A03: Complete! (Temporal bounds)'
     commit

     -- ****************** A04: DEPENDENCY COMPLETENESS CHECK ******************

     -- So this build relies strongly on the most recent scaling build complting. But as of
     -- Scaling 2, this is no longer managed through the scheduler and is instead a manual
     -- process, so we need another check to see if the manual flips have been completed for
     -- the week. Yeeeash. Why isn't it just in the scheduler? It'd easily fit, that's why
     -- Scaling 1 was rebuilt the way that it was...

     -- Update: Nope, the Scaling 2 build is now fully automated!



     select @recent_profiling_date = max(profiling_date)
from vespa_analysts.SC2_Sky_base_segment_snapshots

     select account_number,scaling_segment_id
       into #Scaling_weekly_sample
from vespa_analysts.SC2_Sky_base_segment_snapshots
      where profiling_date = @recent_profiling_date

     -- ****************** B01: SEGMENTING BY VARIABLES NOT USED IN SCALING ******************

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
 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01-1 DML command status: '||@@error
     commit
 -- Cronacle healthcheck...
 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01-1 DML command status: '||@@error

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from Vespa_PanMan_this_weeks_non_scaling_segmentation

     commit
     -- execute logger_add_event @PanMan_logging_ID, 4, 'B01: Population', coalesce(@QA_catcher, -1)
     commit

     -- ****************** B01a: VALUE SEGMENTS ******************

     -- First off the Value Segments data:
     update Vespa_PanMan_this_weeks_non_scaling_segmentation
        set value_segment = coalesce(value_seg, 'Bedding In') -- Anyone new is, by construction, new
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
            left join sk_prod.VALUE_SEGMENTS_DATA as vsd on Vespa_PanMan_this_weeks_non_scaling_segmentation.account_number = vsd.account_number

 -- Cronacle healthcheck...
 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01a-1 DML command status: '||@@error
     commit

 -- Cronacle healthcheck...
 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01a-2 DML command status: '||@@error

     -- Since it's a subsection, may as well control total it up
     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
      where value_segment <> 'Bedding In'

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B01a: Value segments', coalesce(@QA_catcher, -1)
     commit

     -- ****************** B01b: EXPERIAN: MOSAIC AND FINANCIAL STRATEGY SEGMENTS ******************

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
 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01b-1 DML command status: '||@@error

     commit
     create unique index fake_pk on #consumerview_lookup (cb_key_individual)
     commit

 -- Cronacle healthcheck...
 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01b-2 DML command status: '||@@error

     -- So in SAV the individual key ends up duplicated across a few accounts, and some of these account
     -- number duplicates even show different individual keys and tenure dates... whatever.
       select sav.account_number
             ,min(cl.consumerview_cb_row_id) as consumerview_cb_row_id -- this does bad things to the processing in the case that SAV is already broken. There's a longer suggested workaround coming from cust_subs_hist so we might be able to work that into SBV?
         into #consumerview_patch
         from sk_prod.cust_single_account_view as sav
              inner join #consumerview_lookup as cl on sav.cb_key_individual = cl.cb_key_individual
     group by sav.account_number

 -- Cronacle healthcheck...
 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01b-3 DML command status: '||@@error

     commit
     create unique index for_joining on #consumerview_patch (account_number)
     commit

 -- Cronacle healthcheck...
 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01b-4 DML command status: '||@@error

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
 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01b-5 DML command status: '||@@error

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

 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01b-6 DML command status: '||@@error

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

 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01b-7 DML command status: '||@@error

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

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B01b: Experian patch', coalesce(@QA_catcher, -1)
     commit

 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01b-8 DML command status: '||@@error

     -- ****************** B01c: ONNET AND OFFNET  ******************

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

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B01c: OnNet (vs OffNet)', coalesce(@QA_catcher, -1)
     commit
 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01c-1 DML command status: '||@@error
     -- ****************** B01d: SKY GO USERS  ******************

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

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B01d: Sky Go users', coalesce(@QA_catcher, -1)
     commit

 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01d-1 DML command status: '||@@error

     -- ****************** B01x: GETTING SEGMENTATION IDS  ******************

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
         -- execute logger_add_event @PanMan_logging_ID, 2, 'B01: Failure establishing non-scaling segmentation IDs!', coalesce(@QA_catcher, -1)

     commit

     -- OK, and that's this week's segmentation done!

     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_this_weeks_non_scaling_segmentation
      where non_scaling_segment_id is not null

     -- execute logger_add_event @PanMan_logging_ID, 3, 'B01: Complete! (Non-scaling segmentation)', coalesce(@QA_catcher, -1)
     commit

 -- execute logger_add_event @PanMan_logging_ID, 4, 'B01x-1 DML command status: '||@@error

     -- ****************** B02: INDEXING PANELS AGAINST THE SKY BASE ******************

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
             -- ,case when datediff(day, max(Enablement_date), @profiling_thursday) < 15      then 'Recently enabled'   [!!!]
             --       when min(logs_every_day_30d) = 1                                        then 'Acceptable'
             --       when min(logs_returned_in_30d) >= 25 or min(reporting_quality) >= 0.9   then 'Acceptable'
             --       when max(logs_returned_in_30d) = 0                                      then 'Zero reporting'
             --                                                                               else 'Unreliable'
             --       end
             ,case
                   when min(reporting_quality) > 0                                         then 'Acceptable'
                                                                                           else 'Other'
                   end
             ,min(reporting_quality)  -- Used much later in the box selection bit, but may as well build it now
             ,min(panel)              -- This guy should be unique per account, we test for that coming off SBV
         from Vespa_PanMan_SBV  -- vespa_analysts.vespa_single_box_view [!!!]
        where panel = 'VESPA'
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
             -- ,case when datediff(day, max(Enablement_date), @profiling_thursday) < 15      then 'Recently enabled'   [!!!]
             --       when min(logs_every_day_30d) = 1                                        then 'Acceptable'
             --       when min(logs_returned_in_30d) >= 25 or min(reporting_quality) >= 0.9   then 'Acceptable'
             --       when max(logs_returned_in_30d) = 0                                      then 'Zero reporting'
             --                                                                               else 'Unreliable'
             --       end
             ,case
                   when min(reporting_quality) > 0                                         then 'Acceptable'
                                                                                           else 'Other'
                   end
             ,min(reporting_quality)  -- Used much later in the box selection bit, but may as well build it now
             ,min(panel)              -- This guy should be unique per account, we test for that coming off SBV
         from Vespa_PanMan_SBV  -- vespa_analysts.vespa_single_box_view [!!!]
        where panel in ('ALT6','ALT7')
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
         -- execute logger_add_event @PanMan_logging_ID, 2, 'B02: Error populating returning households! (Could be account / panel conflict.)'

     commit
     -- execute logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Households generated)'
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

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (New hash list)', @QA_catcher
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
             -- execute logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Hashes batch processed)', @t * 4000
         commit

     end

     -- OK, so patch the new hashes onto the main table...
     update Vespa_PanMan_all_households
        set accno_SHA1 = sha1.accno_SHA1 -- Did some tests, MD5 actually takes *longer* than SHA1
       from Vespa_PanMan_all_households
            inner join #new_SHA1s as sha1 on Vespa_PanMan_all_households.account_number = sha1.account_number

     commit
     -- And also archive all those hashes so we don't calculate them again
--     insert into vespa_analysts.Vespa_PanMan_SHA1_archive
--     select account_number, accno_SHA1
--       from #new_SHA1s

     commit
     drop table #new_SHA1s
     drop table #hash_cache
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_all_households
      where accno_SHA1 is null

     if @QA_catcher is null or @QA_catcher <> 0
         -- execute logger_add_event @PanMan_logging_ID, 2, 'B02: Households without hashes!', coalesce(@QA_catcher, -1)

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Hashes tagged)'
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
     -- execute logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Segmentation applied)'
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
     -- execute logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Sky totals built)'
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
            cross join #panel_totals as pt -- want all the combinations of stuff

     commit
     -- execute logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Sky totals deployed)'
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
     -- execute logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Profiler built)'
     commit

     -- Patch in the scaling segment name from the lookup...
     update Vespa_PanMan_Scaling_Segment_Profiling
        set scaling_segment_name = ssl.scaling_segment_name
       from Vespa_PanMan_Scaling_Segment_Profiling
            inner join vespa_analysts.SC2_Segments_lookup_v2_0 as ssl on Vespa_PanMan_Scaling_Segment_Profiling.scaling_segment_ID = ssl.scaling_segment_ID

     update Vespa_PanMan_Scaling_Segment_Profiling
        set non_scaling_segment_name = nssl.non_scaling_segment_name
       from Vespa_PanMan_Scaling_Segment_Profiling
            inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on Vespa_PanMan_Scaling_Segment_Profiling.non_scaling_segment_ID = nssl.non_scaling_segment_ID

     commit
     -- execute logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Names imported)'
     commit

     -- Then yeah, that guy gets sucked out and he powers the various reporting views that we get.

     -- We do need the indices in-database though, since we make decisions based on them etc.
     declare @total_sky_base                 int
     -- With the new normalised structures, panel totals just go into a table...

     -- We need the size of the sky base for indexing calculations
     select @total_sky_base     = sum(Sky_Base_Households)
       from Vespa_PanMan_Scaling_Segment_Profiling
      where panel = 'VESPA'

     commit
     -- execute logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Totals set)'
     commit

     -- Now simplified because we'll only be dividing by things in cases where we've got
     -- the appropriate panel stuff in the table:
     update Vespa_PanMan_Scaling_Segment_Profiling
       set Acceptably_reporting_index         = -- *sigh* there's no GREATEST / LEAST operator in this DB...
             case when 200 < 100 * (Acceptably_reliable_households)   * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters then 200
                  else       100 * (Acceptably_reliable_households)   * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters
             end
       from Vespa_PanMan_Scaling_Segment_Profiling
            inner join #panel_totals as pt on Vespa_PanMan_Scaling_Segment_Profiling.panel = pt.panel
     -- Not dropping #panel_totals here because we still need it for the single variable summaries

     -- Still... What are we pulling out to report this? One graph for Vespa Live, one for
     -- each alternate...

     set @QA_catcher = -1

     select @QA_catcher = count(1)
       from Vespa_PanMan_Scaling_Segment_Profiling
      where Acceptably_reporting_index is not null

     -- execute logger_add_event @PanMan_logging_ID, 3, 'B02: Complete! (Indexing panels)', coalesce(@QA_catcher, -1)
     commit

  -- execute logger_add_event @PanMan_logging_ID, 4, 'B02-1 DML command status: '||@@error

     -- ****************** B03: AGGREGATING TO VARIABLE VIEWS ******************

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
              inner join vespa_analysts.SC2_Segments_lookup_v2_0 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
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
              inner join vespa_analysts.SC2_Segments_lookup_v2_0 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
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
              inner join vespa_analysts.SC2_Segments_lookup_v2_0 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
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
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp inner join vespa_analysts.SC2_Segments_lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
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
              inner join vespa_analysts.SC2_Segments_lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
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
              inner join vespa_analysts.SC2_Segments_lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.boxtype

     commit
     -- execute logger_add_event @PanMan_logging_ID, 3, 'B03: Midway! (Scaling variables)'
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

     -- execute logger_add_event @PanMan_logging_ID, 3, 'B03: Midway! (Aggregated variables)', coalesce(@QA_catcher, -1)

     commit
     -- execute logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (Panel cleanse)'
     commit

     -- Okay, now all of that is done, we can patch the index calculations into
     -- the whole lot at once (the variables got calculated further up when we
     -- did indices for each segment):
     update Vespa_PanMan_all_aggregated_results
        set Good_Household_Index = case when 200 < 100 * (Acceptable_Households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters then 200
                                        else       100 * (Acceptable_Households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters
                                   end
       from Vespa_PanMan_all_aggregated_results
            inner join #panel_totals as pt on Vespa_PanMan_all_aggregated_results.panel = pt.panel

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (Balance indexing)'

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

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (HHComposition update)'

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

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (MOSAIC update)'

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

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (Financial strategy update)'

     commit

     -- execute logger_add_event @PanMan_logging_ID, 3, 'B03: Complete! (Patched updates)'

 -- execute logger_add_event @PanMan_logging_ID, 4, 'B03-1 DML command status: '||@@error

     -- ****************** B04: DATA COMPLETENESS ******************

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
        where Panel = 'VESPA'
     group by scaling_segment_id

     commit

     -- now add on the indices calculations:
     update #scaling_completeness_survey
        set Acceptably_reporting_index = case when 200 < 100 * (Acceptably_reliable_households)   * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters then 200
                                              else       100 * (Acceptably_reliable_households)   * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters
                                         end
       from #scaling_completeness_survey
            inner join #panel_totals as pt on pt.panel = 'VESPA'

     commit

     -- And now build the coverage metric:
     select @sky_base_coverage = sum(Sky_Base_Households) / convert(float, @total_sky_base)
       from #scaling_completeness_survey
      where Acceptably_reporting_index > 80

     if @sky_base_coverage is null
         set @sky_base_coverage = 0 -- that'd be bad, but we'll catch it...

     -- Ok, now secondary statistics: how big is the reliable panel?
     select @households_reliably_reporting = panel_reporters
       from #panel_totals
      where panel = 'VESPA'

     if @households_reliably_reporting is null
         set @households_reliably_reporting = 0

     -- Finally, how reliably is our reporting?
     select @reliability_rating = convert(float, @households_reliably_reporting) / count(1)
       from Vespa_PanMan_all_households
      where panel = 'VESPA'

     commit

     -- Oh hey we probably want to track these numbers over time too? So we can see them
     -- shrink? First check if there's already metrics in the table that we'd kill
--     set @QA_catcher = -1
--     select @QA_catcher = count(1) from vespa_analysts.Vespa_PanMan_Historic_Panel_Metrics where metric_date = @profiling_thursday
--     if @QA_catcher <> 0
--         -- execute logger_add_event @PanMan_logging_ID, 2, 'B04: Metric date colision! Old data will be destroyed!'
--
--     commit

--     delete from vespa_analysts.Vespa_PanMan_Historic_Panel_Metrics
--      where metric_date = @profiling_thursday

     -- Heh, we might also have to manage this guy historically because we're now profiling
     -- at the back instead of the front of the analysis period, and we're also changing a
     -- lot of how the coverage metrics work etc. Might clip out a few recent items etc.
--     insert into vespa_analysts.Vespa_PanMan_Historic_Panel_Metrics (
--            metric_date
--           ,sky_base_coverage
--           ,reliability_rating
--           ,households_reliably_reporting
--     )
--     values (
--            @profiling_thursday
--           ,@sky_base_coverage
--           ,@reliability_rating
--           ,@households_reliably_reporting
--     )
     -- The rest of the metrics we'll push in during a later section

     -- Then we just need to pull out the top 24 items, and that's a rolling summary of the last 24 weeks. Is good!
     set @QA_catcher = -1
     set @QA_catcher = convert(int, @sky_base_coverage * @total_sky_base)

     commit
     -- execute logger_add_event @PanMan_logging_ID, 3, 'B04: Complete! (Data completeness)', coalesce(@QA_catcher, -1)
     commit

 -- execute logger_add_event @PanMan_logging_ID, 4, 'B04-1 DML command status: '||@@error

     -- ****************** B05: REPORTING BOX SWING - HOW MUCH IS ONE BOX WORTH? ******************

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
       into Vespa_PanMan_08_ordered_weightings
from vespa_analysts.sc2_weightings as sw
inner join vespa_analysts.sc2_intervals as si on sw.scaling_day = @profiling_thursday
                                                         and sw.scaling_segment_ID = si.scaling_segment_ID
         -- Not joining by non_scaling_segment_ID here because this is all about the scaling weightings
                                                         and @profiling_thursday between si.reporting_starts and si.reporting_ends

     commit
     declare @reporting_accounts int
     declare @sample_diff        int

     select @reporting_accounts = count(1)
       from Vespa_PanMan_08_ordered_weightings
     set @sample_diff = @reporting_accounts / 100 -- ints take care of the rounding

     commit

     delete from Vespa_PanMan_08_ordered_weightings
      where mod(weight_rank, @sample_diff) <> 1

     -- We don't need to track what segment IDs they were, we get that from the indexing
     -- queries, this is just for general health of panel. But we do want to normalise the
     -- weight rank into the percentile:
     update Vespa_PanMan_08_ordered_weightings
        set weighting_percentile = 100 - weight_rank / @sample_diff

     commit
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from Vespa_PanMan_08_ordered_weightings

     commit
     -- execute logger_add_event @PanMan_logging_ID, 3, 'B05: Complete! (Box swing)', coalesce(@QA_catcher, -1)
     commit

 -- execute logger_add_event @PanMan_logging_ID, 4, 'B05-1 DML command status: '||@@error

     -- ****************** B06: REDUNDANCY INDEXING INTO THE ALTERNATE PANELS ******************

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
     -- execute logger_add_event @PanMan_logging_ID, 3, 'B06: NYIP (Indexing vs Alternates)' --, coalesce(@QA_catcher, -1)
     commit

     -- ****************** B07: EVEN HIGHER LEVEL SUMMARIES OF SINGLE VARIABLES ******************

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
         into Vespa_PanMan_09_traffic_lights
         from Vespa_PanMan_all_aggregated_results
      where (aggregation_variable = 'BOXTYPE')
         or (aggregation_variable = 'FINANCIALSTRAT' and variable_value <> 'U: Unknown FSS')
         or (aggregation_variable = 'HHCOMP' and variable_value <> 'U: Unclassified HHComp')
         or (aggregation_variable = 'MOSAIC' and variable_value <> 'U: Unknown MOSAIC')
         or (aggregation_variable = 'ONNET')
         or (aggregation_variable = 'PACKAGE')
         or (aggregation_variable = 'REGION' and variable_value <> 'Not Defined')
         or (aggregation_variable = 'SKYGO')
         or (aggregation_variable = 'TENURE' and variable_value <> 'D) Unknown')
         or (aggregation_variable = 'UNIVERSE')
         or (aggregation_variable = 'VALUESEG')
     group by panel, aggregation_variable

     -- Indices? nah, it's a tiny table.

     -- execute logger_add_event @PanMan_logging_ID, 3, 'B07: Complete! (Traffic lights)'
     commit

 -- execute logger_add_event @PanMan_logging_ID, 4, 'B07-1 DML command status: '||@@error

     -- execute logger_add_event @PanMan_logging_ID, 3, 'PanMan: weekly refresh complete!'
     COMMIT

end;

commit;

-- And somethign else to clean up the junk that was built:
if object_id('PanMan_clear_transients') is not null
   drop procedure PanMan_clear_transients;

commit;

create procedure PanMan_clear_transients
as
begin
    -- For some reason, these guys needed the explicit schema references while inside a
    -- proc that was called by a different user. Weird.
    -- ##32## - are we recasting this so that the schema is automatically detected?
    delete from Vespa_PanMan_all_households
    delete from Vespa_PanMan_Scaling_Segment_Profiling
    delete from Vespa_PanMan_this_weeks_non_scaling_segmentation
    delete from Vespa_PanMan_all_aggregated_results
    delete from Vespa_PanMan_panel_redundancy_calculations
    if object_id( 'vespa_PanMan_02_vespa_panel_overall') is not null
        drop table vespa_PanMan_02_vespa_panel_overall
    if object_id( 'vespa_PanMan_03_panel_6_overall') is not null
        drop table vespa_PanMan_03_panel_6_overall
    if object_id( 'vespa_PanMan_04_panel_7_overall') is not null
        drop table vespa_PanMan_04_panel_7_overall
    if object_id( 'Vespa_PanMan_08_ordered_weightings') is not null
        drop table Vespa_PanMan_08_ordered_weightings
    if object_id( 'vespa_PanMan_09_traffic_lights') is not null
        drop table vespa_PanMan_09_traffic_lights
    if object_id( 'vespa_PanMan_11_panel_4_discontinuations') is not null
        drop table vespa_PanMan_11_panel_4_discontinuations
    if object_id( 'vespa_PanMan_12_panel_6_imports') is not null
        drop table vespa_PanMan_12_panel_6_imports
    if object_id( 'vespa_PanMan_13_panel_7_imports') is not null
        drop table vespa_PanMan_13_panel_7_imports
    if object_id( 'vespa_PanMan_42_vespa_panel_single_box_HHs') is not null
        drop table vespa_PanMan_42_vespa_panel_single_box_HHs
    if object_id( 'vespa_PanMan_43_vespa_panel_dual_box_HHs') is not null
        drop table vespa_PanMan_43_vespa_panel_dual_box_HHs
    if object_id( 'vespa_PanMan_44_vespa_panel_multi_box_HHs') is not null
        drop table vespa_PanMan_44_vespa_panel_multi_box_HHs
    if object_id( 'Vespa_PanMan_this_weeks_non_scaling_segmentation_bugfix') is not null
        drop table Vespa_PanMan_this_weeks_non_scaling_segmentation_bugfix
end;

commit;








execute PanMan_make_report '2013-04-25';
select * from Vespa_PanMan_09_traffic_lights
order by panel,sequencer


