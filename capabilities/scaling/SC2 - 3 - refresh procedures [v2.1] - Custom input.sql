

-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
--            This version uses a CUSTOM INPUT table - "Scaling2_00_Input"
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Scaling - Refresh procedures
**
** These procedures update the core scaling tables with new reporting data.
** There are a couple of fundamental changes to support scaling 2. Profiling
** gets done on a different loop (analytics 9PM once a week), everythign else
** gets done once a day in a separate loop. Next, panel membership decisions
** are isolated in their own stored procedure, because that might change too.
** Finally, all the calculations go one day at a time from the lists already
** built.
**
** Refer to:
**
**      http://rtci/Vespa1/Scaling.aspx
**      http://rtci/Vespa1/VIQ%20interim%20scaling.aspx
**
**
** Code sections: now adjusted to support VIQ internal interim! Or, VIQII.
** Might appear out of order, but the order will change around when the
** processes actually get run anyway, so it's not a big deal.
**
**      Part L:       Weekly profiling (run weekly in 9PM analytics queue)
**              L01 - Get account population
**              L02 - Pull together all the flags & variables
**              L03 - Assign scaling segment ID
**              L04 - Publishing segmentation to interface tables
**
**      Part A:       Virtual panel balance (or placeholder thereof)
**
**              A00 - Clean transient tables
**              A01 - Accounts reporting last week
**
**      Part B:       Building actual scaling weights!
**
**              B01 - Totals of base and panel by segment
**              B02 - Calculate segment weights
**              B03 - Publishing weights to interface tables
**
**      Part E:       Consistency tests on scaling numbers built by date
**
**              E01 - Total of base segments should match count of base
**              E02 - Weighting total matches Sky base
**
**      Part F:       Consistency tests over weekly builds
**
**              F01 - Intervals shouldn't overlap
**              F02 - Vespa panel should be contained within Sky base
**              F03 - Should not have any NULL segmentation IDs
**              F04 - ROI and non-standard accounts in Vespa Panel
**              F05 - Interval vs weighting consistency check
**              F06 - Convergence of Rim weighting method
**
**      Part I:
**              I01 - The central refresh procedure (defaults to one day)
**
**      Part P:
**              P01 - Permisisons!
**
** To do:
** 19. Update documentation (in all the places) for the Scaling 2 dev (who is
**      responsible for doing this?)
** 21. Update for Phase 2 data structure. Should only affect Fuzzy A on the
**      scaling data flow picture?
**
** Completed items:
** 16. New segmentation definitions from Sarah's work. Fuzzy Area B.
** 18. Implement new rim weighting mechanic. Fuzzy Area C.
** 15. Viewing definition updates - modularise so that it's easily modified
**      during virtual panel balance work. Fuzzy Area A.
** 24. Clean up sectioning in transposed code blocks
** 25. Drop temporary tables after final use in transposed code blocks
** 26. Newly transposed code blocks need the logger too eh?
** 27. Migrate in those QA queries too. Though, they just record basic stats,
**      we'll still need some tests indicating when to raise flag sthat they
**      didn't work out.
** 17. Update segmentations to use Experian instead of ILU & new BARB regions
** 23. Make sure to clear out any existing values already in any of the tables
**      of the same build dates. Except maybe the final delivery interface.
** 20. Sophisticated historic rebuild management including auditing, leading
**      into Plan AB+ because that's probably going to be how VIQ gets done.
**      [Update: no, we're just going to handle this manually, *maybe*]
**
** Remember also to search for ## and address all the dev points flagged there.
**
** Generally these guys are not mentioning vespa_analysts specifically so that
** we can throw together separate testing instances in our different schemas.
**
******************************************************************************/

/**************** PART L: WEEKLY SEGMENTATION BUILD ****************/

IF object_id('SC2_v2_1__do_weekly_segmentation_CUSTOM') IS NOT NULL THEN DROP PROCEDURE SC2_v2_1__do_weekly_segmentation_CUSTOM END IF;

create procedure SC2_v2_1__do_weekly_segmentation_CUSTOM
    @profiling_thursday         date = null         -- Day on which to do sky base profiling
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
as
begin

     declare @QA_catcher                 integer         -- For control totals along the way
     declare @tablespacename             varchar(40)

     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC2: Profiling Sky UK base as of ' || dateformat(@profiling_thursday,'yyyy-mm-dd') || '.'
     commit

     -- Clear out the processing tables and suchlike

     DELETE FROM SC2_scaling_weekly_sample
     COMMIT

     -- Decide when we're doing the profiling, if it's not passed in as a parameter
     if @profiling_thursday is null
     begin
         execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output  -- proc returns a Saturday
         set @profiling_thursday = @profiling_thursday - 2                               -- but we want a Thursday
     end
     commit

     -- Get us a refresh logging ID thing if one wasn't assigned
     if @Scale_refresh_logging_ID is null
     begin
         execute Regulars_whats_my_namespace @tablespacename output
         if @tablespacename = 'vespa_analysts'
             EXECUTE logger_create_run 'ScalingSegmentation'           , 'SC2: Segmentation build for ' || dateformat(@profiling_thursday, 'yyyy-mm-dd') || '.', @Scale_refresh_logging_ID output
         else
         begin
             set @tablespacename = coalesce(@tablespacename, user)
             EXECUTE logger_create_run 'SC2 Dev ' || @tablespacename || 'SC2: Segmentation build for  ' || dateformat(@profiling_thursday, 'yyyy-mm-dd') || '.', @Scale_refresh_logging_ID output
         end
     end
     commit

     -- So this bit is not stable for the VIQ builds since we can't delete weights from there,
     -- but for dev builds within analytics this is required.
     DELETE FROM SC2_Sky_base_segment_snapshots where profiling_date = @profiling_thursday
     commit

     /**************** L01: ESTABLISH POPULATION ****************/
     -- We need the segmentation over the whole Sky base so we can scale up

     -- Captures all active accounts in cust_subs_hist
     SELECT   account_number
             ,cb_key_household
             ,cb_key_individual
             ,current_short_description
             ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
             ,convert(bit, 0)  AS uk_standard_account
             ,convert(VARCHAR(20), NULL) AS isba_tv_region
       INTO #weekly_sample
       FROM cust_subs_hist
      WHERE subscription_sub_type IN ('DTV Primary Viewing')
        AND status_code IN ('AC','AB','PC')
        AND effective_from_dt    <= @profiling_thursday
        AND effective_to_dt      > @profiling_thursday
        AND effective_from_dt    <> effective_to_dt
        AND EFFECTIVE_FROM_DT    IS NOT NULL
        AND cb_key_household     > 0
        AND cb_key_household     IS NOT NULL
        AND cb_key_individual    IS NOT NULL
        AND account_number       IS NOT NULL
        AND service_instance_id  IS NOT NULL

     -- De-dupes accounts
     COMMIT
     DELETE FROM #weekly_sample WHERE rank > 1
     COMMIT

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #weekly_sample

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L01: Midway 1/2 (Weekly sample)', coalesce(@QA_catcher, -1)
     commit

     -- Create indices
     CREATE UNIQUE INDEX fake_pk ON #weekly_sample (account_number)
     CREATE INDEX for_package_join ON #weekly_sample (current_short_description)
     COMMIT

     -- Take out ROIs (Republic of Ireland) and non-standard accounts as these are not currently in the scope of Vespa
     UPDATE #weekly_sample
     SET
         uk_standard_account = CASE
             WHEN b.acct_type='Standard' AND b.account_number <>'?' AND b.pty_country_code ='GBR' THEN 1
             ELSE 0 END
         ,isba_tv_region = b.isba_tv_region
         -- Grab the cb_key_individual we need for consumerview linkage at the same time
         ,cb_key_individual = b.cb_key_individual
     FROM #weekly_sample AS a
     inner join cust_single_account_view AS b
     ON a.account_number = b.account_number

     COMMIT
     DELETE FROM #weekly_sample WHERE uk_standard_account=0
     COMMIT

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #weekly_sample

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L01: Complete! (Population)', coalesce(@QA_catcher, -1)
     commit

     -- hahaha, no, there are lots of key dupes here, we're just going to
     -- have to cull them during the piping into the VIQ build
     --CREATE UNIQUE INDEX for_ilu_joining ON #weekly_sample (cb_key_household)
     --commit

     /**************** L02: ASSIGN VARIABLES ****************/
     -- Since "h_household_composition" & "p_head_of_household" are now in two separate tables, an intemidiary table is created
     -- so both variables are available for ranking function in the next step
     SELECT
          cv.cb_key_household,
          cv.cb_key_family,
          cv.cb_key_individual,
          min(cv.cb_row_id) as cb_row_id,
          max(cv.h_household_composition) as h_household_composition,
          max(pp.p_head_of_household) as p_head_of_household
      INTO #cv_pp
      FROM EXPERIAN_CONSUMERVIEW cv,
           PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD pp
     WHERE cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
       AND cv.cb_key_individual is not null
     GROUP BY cv.cb_key_household, cv.cb_key_family, cv.cb_key_individual
     COMMIT

     CREATE LF INDEX idx1 on #cv_pp(p_head_of_household)
     CREATE HG INDEX idx2 on #cv_pp(cb_key_family)
     CREATE HG INDEX idx3 on #cv_pp(cb_key_individual)

     -- We grabbed the cb_key_individual mark from SAV in the previuos build, so
     -- now we need the ConsumerView treatment from the customer group wiki:
     SELECT   cb_key_individual
             ,cb_row_id
             ,rank() over(partition by cb_key_family     ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_fam
             ,rank() over(partition by cb_key_individual ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_ind
             ,h_household_composition -- may as well pull out the item we need given we're ranking and deleting
     INTO #cv_keys
     FROM #cv_pp
     WHERE cb_key_individual IS not NULL
       AND cb_key_individual <> 0

     -- This is a cleaned out version of http://mktskyportal/Campaign%20Handbook/ConsumerView.aspx
     -- since we only need the individual stuff for this linkage.

     commit
     DELETE FROM #cv_keys WHERE rank_fam != 1 AND rank_ind != 1
     commit

     CREATE INDEX index_ac on #cv_keys (cb_key_individual)
     COMMIT

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #cv_keys

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 1/7 (Consumerview Linkage)', coalesce(@QA_catcher, -1)
     commit

     -- Populate Package & ISBA TV Region

     INSERT INTO SC2_scaling_weekly_sample (
         account_number
         ,cb_key_household
         ,cb_key_individual
         ,universe
         ,isba_tv_region
         ,hhcomposition
         ,tenure
         ,num_mix
         ,mix_pack
         ,package
         ,boxtype
     )
     SELECT
         fbp.account_number
         ,fbp.cb_key_household
         ,fbp.cb_key_individual
         ,'A) Single box HH' -- universe
         ,fbp.isba_tv_region -- isba_tv_region
         ,'U'  -- hhcomposition
         ,'D) Unknown' -- tenure
         ,cel.Variety + cel.Knowledge + cel.Kids + cel.Style_Culture + cel.Music + cel.News_Events as num_mix
         ,CASE
                         WHEN Num_Mix IS NULL OR Num_Mix=0                           THEN 'Entertainment Pack'
                         WHEN (cel.variety=1 OR cel.style_culture=1)  AND Num_Mix=1  THEN 'Entertainment Pack'
                         WHEN (cel.variety=1 AND cel.style_culture=1) AND Num_Mix=2  THEN 'Entertainment Pack'
                         WHEN Num_Mix > 0                                            THEN 'Entertainment Extra'
                     END AS mix_pack -- Basic package has recently been split into the Entertainment and Entertainment Extra packs
         ,CASE
             WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN 'Top Tier'
             WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN 'Dual Sports'
             WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN 'Dual Movies'
             WHEN cel.prem_sports = 1 AND cel.prem_movies = 0 THEN 'Single Sports'
             WHEN cel.prem_sports = 0 AND cel.prem_movies = 1 THEN 'Single Movies'
             WHEN cel.prem_sports > 0 OR  cel.prem_movies > 0 THEN 'Other Premiums'
             WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Pack'  THEN 'Basic - Ent'
             WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Extra' THEN 'Basic - Ent Extra'
             ELSE                                                  'Basic - Ent' END -- package
         ,'D) FDB & No_secondary_box' -- boxtype
     FROM #weekly_sample AS fbp
     left join cust_entitlement_lookup AS cel
         ON fbp.current_short_description = cel.short_description
     WHERE fbp.cb_key_household IS NOT NULL
       AND fbp.cb_key_individual IS NOT NULL

     commit
     drop table #weekly_sample
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_scaling_weekly_sample

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 2/7 (Package & ISBA region)', coalesce(@QA_catcher, -1)
     commit

     -- HHcomposition

     UPDATE SC2_scaling_weekly_sample
     SET
         stws.hhcomposition = cv.h_household_composition
     FROM SC2_scaling_weekly_sample AS stws
     inner join #cv_keys AS cv
     ON stws.cb_key_individual = cv.cb_key_individual

     commit
     drop table #cv_keys
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_scaling_weekly_sample
     where hhcomposition <> 'U'

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 3/7 (HH composition)', coalesce(@QA_catcher, -1)
     commit

     -- Tenure

     -- Tenure has been grouped according to its relationship with viewing behaviour

     UPDATE SC2_scaling_weekly_sample t1
     SET
         tenure = CASE   WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  730 THEN 'A) 0-2 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <= 3650 THEN 'B) 3-10 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) > 3650 THEN  'C) 10 Years+'
                         ELSE 'D) Unknown'
                  END
     FROM cust_single_account_view sav
     WHERE t1.account_number=sav.account_number
     COMMIT

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_scaling_weekly_sample
     where tenure <> 'D) Unknown'

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 4/7 (Tenure)', coalesce(@QA_catcher, -1)
     commit

     -- Boxtype & Universe

     -- Boxtype is defined as the top two boxtypes held by a household ranked in the following order
     -- 1) HD, 2) HDx, 3) Skyplus, 4) FDB

     -- Capture all active boxes for this week
     SELECT    csh.service_instance_id
             , csh.account_number
             , subscription_sub_type
             , rank() over (PARTITION BY csh.service_instance_id ORDER BY csh.account_number, csh.cb_row_id desc) AS rank
       INTO #accounts -- drop table #accounts
       FROM cust_subs_hist as csh
             inner join SC2_scaling_weekly_sample AS ss ON csh.account_number = ss.account_number
      WHERE  csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription')     --the DTV sub Type
        AND csh.status_code IN ('AC','AB','PC')                  --Active Status Codes
        AND csh.effective_from_dt <= @profiling_thursday
        AND csh.effective_to_dt > @profiling_thursday
        AND csh.effective_from_dt<>effective_to_dt

     -- De-dupe active boxes
     DELETE FROM #accounts WHERE rank>1
     COMMIT

     -- Create indices on list of boxes
     CREATE UNIQUE hg INDEX idx1 ON #accounts(service_instance_id)
     CREATE hg INDEX idx2 ON #accounts(account_number)
     commit

     -- Identify HD & 1TB/2TB HD boxes
     SELECT  stb.service_instance_id
            ,SUM(CASE WHEN current_product_description LIKE '%HD%' THEN 1
                     ELSE 0
                  END) AS HD
            ,SUM(CASE WHEN x_description IN ('Amstrad HD PVR6 (1TB)', 'Amstrad HD PVR6 (2TB)') THEN 1
                     ELSE 0
                  END) AS HD1TB
     INTO #hda -- drop table #hda
     FROM CUST_SET_TOP_BOX AS stb INNER JOIN #accounts AS acc
                                                  ON stb.service_instance_id = acc.service_instance_id
     WHERE box_installed_dt <= @profiling_thursday
     AND box_replaced_dt   > @profiling_thursday
     AND current_product_description like '%HD%'
     GROUP BY stb.service_instance_id

     -- Create index on HD table
     COMMIT
     CREATE UNIQUE hg INDEX idx1 ON #hda(service_instance_id)
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #hda

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 5/7 (HD boxes)', coalesce(@QA_catcher, -1)
     commit

     SELECT  --acc.service_instance_id,
            acc.account_number
            ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
            ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
            ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
            ,MAX(CASE  WHEN #hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
            ,MAX(CASE  WHEN #hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
     INTO #scaling_box_level_viewing
     FROM cust_subs_hist AS csh
            INNER JOIN #accounts AS acc ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
            LEFT OUTER JOIN cust_entitlement_lookup cel
                            ON csh.current_short_description = cel.short_description
            LEFT OUTER JOIN #hda ON csh.service_instance_id = #hda.service_instance_id --< Links to the HD Set Top Boxes
      WHERE csh.effective_FROM_dt <= @profiling_thursday
        AND csh.effective_to_dt    > @profiling_thursday
        AND csh.status_code IN  ('AC','AB','PC')
        AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
        AND csh.effective_FROM_dt <> csh.effective_to_dt
     GROUP BY acc.service_instance_id ,acc.account_number

     commit
     drop table #accounts
     drop table #hda
     commit

     -- Identify boxtype of each box and whether it is a primary or a secondary box
     SELECT  tgt.account_number
            ,SUM(CASE WHEN MR=1 THEN 1 ELSE 0 END) AS mr_boxes
            ,MAX(CASE WHEN MR=0 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                      WHEN MR=0 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                      WHEN MR=0 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                      ELSE                                                                              1 END) AS pb -- FDB
            ,MAX(CASE WHEN MR=1 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                      WHEN MR=1 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                      WHEN MR=1 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                      ELSE                                                                              1 END) AS sb -- FDB
             ,convert(varchar(20), null) as universe
             ,convert(varchar(30), null) as boxtype
       INTO #boxtype_ac -- drop table #boxtype_ac
       FROM #scaling_box_level_viewing AS tgt
     GROUP BY tgt.account_number

     -- Create indices on box-level boxtype temp table
     COMMIT
     CREATE unique INDEX idx_ac ON #boxtype_ac(account_number)
     drop table #scaling_box_level_viewing
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #boxtype_ac

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6/7 (P/S boxes)', coalesce(@QA_catcher, -1)
     commit

     -- Build the combined flags
     update #boxtype_ac
     set universe = CASE WHEN mr_boxes = 0 THEN 'A) Single box HH'
                              ELSE 'B) Multiple box HH' END
         ,boxtype  =
             CASE WHEN       mr_boxes = 0 AND  pb =  3 AND sb =  1   THEN  'A) HDx & No_secondary_box'
                  WHEN       mr_boxes = 0 AND  pb =  4 AND sb =  1   THEN  'B) HD & No_secondary_box'
                  WHEN       mr_boxes = 0 AND  pb =  2 AND sb =  1   THEN  'C) Skyplus & No_secondary_box'
                  WHEN       mr_boxes = 0 AND  pb =  1 AND sb =  1   THEN  'D) FDB & No_secondary_box'
                  WHEN       mr_boxes > 0 AND  pb =  4 AND sb =  4   THEN  'E) HD & HD' -- If a hh has HD  then all boxes have HD (therefore no HD and HDx)
                  WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  3) OR (pb =  3 AND sb =  4)  THEN  'E) HD & HD'
                  WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  2) OR (pb =  2 AND sb =  4)  THEN  'F) HD & Skyplus'
                  WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  1) OR (pb =  1 AND sb =  4)  THEN  'G) HD & FDB'
                  WHEN       mr_boxes > 0 AND  pb =  3 AND sb =  3                            THEN  'H) HDx & HDx'
                  WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  2) OR (pb =  2 AND sb =  3)  THEN  'I) HDx & Skyplus'
                  WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  1) OR (pb =  1 AND sb =  3)  THEN  'J) HDx & FDB'
                  WHEN       mr_boxes > 0 AND  pb =  2 AND sb =  2                            THEN  'K) Skyplus & Skyplus'
                  WHEN       mr_boxes > 0 AND (pb =  2 AND sb =  1) OR (pb =  1 AND sb =  2)  THEN  'L) Skyplus & FDB'
                             ELSE   'M) FDB & FDB' END

     commit

     /* Now building this differently; Sybase 15 didn't like it at all, even had trouble killing
     ** the thread after the query is cancelled. This weirdness has been replicated in other schemas
     ** and on the QA server and has been raised to Sybase. Apparently it's a known bug and there's
     ** a patch coming in 15.4, but for the meantime this method remains commented out even though
     ** the workaround is amazingly ugly...
     UPDATE SC2_scaling_weekly_sample
     SET
         universe    = ac.universe
         ,boxtype    = ac.boxtype
         ,mr_boxes   = ac.mr_boxes
     FROM SC2_scaling_weekly_sample
     inner join #boxtype_ac AS ac
     on ac.account_number = SC2_scaling_weekly_sample.account_number
     */

     CREATE TABLE #SC2_weird_sybase_update_workaround (
          account_number                     VARCHAR(20)     primary key
         ,cb_key_household                   BIGINT          not null
         ,cb_key_individual                  BIGINT          not null
         ,consumerview_cb_row_id             BIGINT
         ,universe                           VARCHAR(20)                         -- Single, Dual or Multiple box household
         ,isba_tv_region                     VARCHAR(20)                         -- Scaling variable 1 : Region
         ,hhcomposition                      VARCHAR(2)      default 'U'         -- Scaling variable 2: Household composition
         ,tenure                             VARCHAR(15)     DEFAULT 'D) Unknown'-- Scaling variable 3: Tenure
         ,num_mix                            INT
         ,mix_pack                           VARCHAR(20)
         ,package                            VARCHAR(20)                         -- Scaling variable 4: Package
         ,boxtype                            VARCHAR(35)                         -- Scaling variable 5: Household boxtype (ranked)
         ,scaling_segment_id                 INT             DEFAULT NULL        -- segment scaling id for identifying segments
         ,mr_boxes                           INT
     --    ,complete_viewing                   TINYINT         DEFAULT 0           -- Flag for all accounts with complete viewing data
     )

     CREATE INDEX for_segment_identification_raw ON #SC2_weird_sybase_update_workaround
         (universe, isba_tv_region, hhcomposition, tenure, package, boxtype)     -- Might it be this one guy? this index rebuild making everything super slow? But it should be going in as a single atomic commit... but on inserts, it still only takes 55 sec...
     CREATE INDEX consumerview_joining ON #SC2_weird_sybase_update_workaround (consumerview_cb_row_id)
     CREATE INDEX for_grouping ON #SC2_weird_sybase_update_workaround (scaling_segment_ID)
     COMMIT

     insert into #SC2_weird_sybase_update_workaround (
          account_number
         ,cb_key_household
         ,cb_key_individual
         ,consumerview_cb_row_id
         ,universe
         ,isba_tv_region
         ,hhcomposition
         ,tenure
         ,num_mix
         ,mix_pack
         ,package
         ,boxtype
         ,mr_boxes
     )
     select
          sws.account_number
         ,sws.cb_key_household
         ,sws.cb_key_individual
         ,sws.consumerview_cb_row_id
         ,ac.universe
         ,sws.isba_tv_region
         ,sws.hhcomposition
         ,sws.tenure
         ,sws.num_mix
         ,sws.mix_pack
         ,sws.package
         ,ac.boxtype
         ,ac.mr_boxes
     from SC2_scaling_weekly_sample as sws
     inner join #boxtype_ac AS ac
     on ac.account_number = sws.account_number
     WHERE sws.cb_key_household IS NOT NULL
       AND sws.cb_key_individual IS NOT NULL

     -- This data is eventually going to go back into the SC2_scaling_weekly_sample,
     -- but there's some weird Sybase bug at the moment that means that updates don't
     -- work. And then the sessions can't be cancelled, for some bizarre reason.

     commit
     drop table #boxtype_ac
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #SC2_weird_sybase_update_workaround

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Complete! (Variables)', coalesce(@QA_catcher, -1)
     commit

      /**************** L03: ASSIGN SCALING SEGMENT ID ****************/

     -- The SC2_Segments_lookup table can be used to append a segment_id to
     -- the SC2_scaling_weekly_sample table by matching on universe and each of the
     -- five scaling variables (hhcomposition, isba_tv_region, package, boxtype and tenure)

     UPDATE #SC2_weird_sybase_update_workaround
        SET #SC2_weird_sybase_update_workaround.scaling_segment_ID = ssl.scaling_segment_ID
       FROM #SC2_weird_sybase_update_workaround
             inner join vespa_analysts.SC2_Segments_lookup_v2_1 AS ssl
                                  ON #SC2_weird_sybase_update_workaround.universe       = ssl.universe
                                 AND #SC2_weird_sybase_update_workaround.hhcomposition  = ssl.hhcomposition
                                 AND #SC2_weird_sybase_update_workaround.isba_tv_region = ssl.isba_tv_region
                                 AND #SC2_weird_sybase_update_workaround.Package        = ssl.Package
                                 AND #SC2_weird_sybase_update_workaround.boxtype        = ssl.boxtype
                                 AND #SC2_weird_sybase_update_workaround.tenure         = ssl.tenure

     COMMIT

     -- Just checked one manual build, none of these are null, it should all work fine.

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #SC2_weird_sybase_update_workaround
     where scaling_segment_ID is not null

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L03: Midway (Segment lookup)', coalesce(@QA_catcher, -1)
     commit

     -- Okay, no throw all of that back into the weekly sample table, because that's where
     -- the build expects it to be, were it not for that weird bug in Sybase:

     delete from SC2_scaling_weekly_sample
     commit

     insert into SC2_scaling_weekly_sample
     select *
     from #SC2_weird_sybase_update_workaround

     commit
     drop table #SC2_weird_sybase_update_workaround

     commit
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_scaling_weekly_sample
     where scaling_segment_ID is not null
     commit

     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L03: Complete! (Segment ID assignment)', coalesce(@QA_catcher, -1)
     commit

     /**************** L04: PUBLISHING INTO INTERFACE STRUCTURES ****************/

     -- First off we need the accounts and their scaling segmentation IDs: generating
     -- some 10M such records a week, but we'd be able to cull them once we've finished
     -- the associated scaling builds. Only need to maintain it while we still have
     -- historic builds to do.

     insert into SC2_Sky_base_segment_snapshots
     select
         account_number
         ,@profiling_thursday
         ,cb_key_household   -- This guy still needs to be added to SC2_scaling_weekly_sample
         ,scaling_segment_ID
         ,mr_boxes+1         -- Number of multiroom boxes plus 1 for the primary
     from SC2_scaling_weekly_sample
     where scaling_segment_ID is not null -- still perhaps with the weird account from Eire?

     commit
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_Sky_base_segment_snapshots
     where profiling_date = @profiling_thursday

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L04: Complete! (Segments published)', coalesce(@QA_catcher, -1)
     commit

     -- We were summarising into scaling_segment_id and sky base count, but now we're
     -- doing that later during the actual weights build rather than keeping all the
     -- differrent profile date builds concurrent, we can recover it easily from the
     -- weekly segmentation anyway.

     -- Don't need to separately track the build dates, since they're on the interface
     -- tables and we'll just rely on those. (Gives us no visiblity of overwrites, but
     -- hey, it's okay, those only happen when stuff is being muddled with anyway.)

     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC2: base segmentation complete!'
     commit

end; -- of procedure "SC2_v2_1__do_weekly_segmentation_CUSTOM"
commit;



/**************** PART A: PLACEHOLDER FOR VIRTUAL PANEL BALANCE ****************/

-- This section nominally decides which boxes are considered to be on the panel
-- for each day. There could be a bunch of influences here:
--   * Completeness of returned data in multiroom households
--   * Regularity of returned data for panel stability / box reliability
--   * Virtual panel balance decisions (using the wekly segmentation) - NYIP
-- The output is a table of account numbers and scaling segment IDs. Which is
-- the other reason why it depends on the segmentation build.
IF object_id('SC2_v2_1__prepare_panel_members_CUSTOM') IS NOT NULL THEN DROP PROCEDURE SC2_v2_1__prepare_panel_members_CUSTOM END IF;

create procedure SC2_v2_1__prepare_panel_members_CUSTOM
    @scaling_day                date                -- Day for which to do scaling
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
as
begin

     /**************** A00: CLEANING OUT ALL THE OLD STUFF ****************/

     delete from SC2_todays_panel_members
     commit

     /**************** A01: ACCOUNTS REPORTING LAST WEEK ****************/

     -- This code block is more jury-rigged in than the others because the structure
     -- has to change a bit to accomodate appropriate modularisation. And it'll all
     -- change again later when Phase 2 stuff gets rolled in. And probably further to
     -- acommodate this overnight batching thing, because we won't have data returned
     -- up to a week in the future.

     declare @profiling_date             date            -- The relevant Thursday of SAV flip etc
     declare @QA_catcher                 integer         -- For control totals along the way

     -- The weekly profiling is called in a different build, so we'll
     -- just grab the most recent one prior to the date we're scaling
     select @profiling_date = max(profiling_date)
     from SC2_Sky_base_segment_snapshots
     where profiling_date <= @scaling_day

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC2: Deciding panel members for ' || dateformat(@scaling_day,'yyyy-mm-dd') || ' using profiling of ' || dateformat(@profiling_date,'yyyy-mm-dd') || '.'
     commit

     -- Prepare to catch the week's worth of logs:
     create table #raw_logs_dump (
         account_number          varchar(20)         not null
         ,service_instance_id    varchar(30)         not null
     )
     commit

     -- In phase two, we don't have to worry about juggling things through the daily tables,
     -- so figuring out what's returned data is a lot easier.

     insert into #raw_logs_dump
     select distinct account_number, service_instance_id
     from Scaling2_00_Input
     where event_start_date_time_utc between dateadd(hour, 6, @scaling_day) and dateadd(hour, 30, @scaling_day)
     and panel_id = 12
     and account_number is not null
     and service_instance_id is not null

     commit
     create index some_key on #raw_logs_dump (account_number)
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #raw_logs_dump
     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'A01: Midway 1/2 (Log extracts)', coalesce(@QA_catcher, -1)
     commit

     select
         account_number
         ,count(distinct service_instance_id) as box_count
         ,convert(tinyint, null) as expected_boxes
         ,convert(int, null) as scaling_segment_id
     into #panel_options
     from #raw_logs_dump
     group by account_number

     commit
     create unique index fake_pk on #panel_options (account_number)
     drop table #raw_logs_dump
     commit

     -- Getting this list of accounts isn't enough, we also want to know if all the boxes
     -- of the household have returned data.

     update #panel_options
     set expected_boxes      = sbss.expected_boxes
         ,scaling_segment_id = sbss.scaling_segment_id
     from #panel_options
     inner join SC2_Sky_base_segment_snapshots as sbss
     on #panel_options.account_number = sbss.account_number
     where sbss.profiling_date = @profiling_date

     commit
     delete from SC2_todays_panel_members
     commit

     -- First moving the unique account numbers in...

     insert into SC2_todays_panel_members (account_number, scaling_segment_id)
     SELECT account_number, scaling_segment_id
     FROM #panel_options
     where expected_boxes >= box_count
     -- Might be more than we expect if NULL service_instance_ID's are distinct against
     -- populated ones (might get fixed later but for now the initial Phase 2 build
     -- doesn't populate them all yet)
     and scaling_segment_id is not null

     commit
     drop table #panel_options
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_todays_panel_members

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'A01: Complete! (Panel members)', coalesce(@QA_catcher, -1)
     commit

     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC2: panel members prepared!'
     commit

end; -- of procedure "SC2_v2_1__prepare_panel_members_CUSTOM"
commit;



IF object_id('SC2_v2_1__make_weights_CUSTOM') IS NOT NULL THEN DROP PROCEDURE SC2_v2_1__make_weights_CUSTOM END IF;

create procedure SC2_v2_1__make_weights_CUSTOM
    @scaling_day                date                -- Day for which to do scaling; this argument is mandatory
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
as
begin

     -- So by this point we're assuming that the Sky base segmentation is done
     -- (for a suitably recent item) and also that today's panel members have
     -- been established, and we're just going to go calculate these weights.

     DECLARE @cntr           INT
     DECLARE @iteration      INT
     DECLARE @cntr_var       SMALLINT
     DECLARE @scaling_var    VARCHAR(30)
     DECLARE @convergence    TINYINT
     DECLARE @sky_base       DOUBLE
     DECLARE @vespa_panel    DOUBLE
     DECLARE @sum_of_weights DOUBLE
     declare @profiling_date date
     declare @QA_catcher     bigint

     commit

     /**************** PART B01: GETTING TOTALS FOR EACH SEGMENT ****************/

     -- Figure out which profiling info we're using;
     select @profiling_date = max(profiling_date)
     from SC2_Sky_base_segment_snapshots
     where profiling_date <= @scaling_day

     commit

     -- Log the profiling date being used for the build
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC2: Making weights for ' || dateformat(@scaling_day,'yyyy-mm-dd') || ' using profiling of ' || dateformat(@profiling_date,'yyyy-mm-dd') || '.'
     commit

     -- First adding in the Sky base numbers
     delete from SC2_weighting_working_table
     commit

     INSERT INTO SC2_weighting_working_table (scaling_segment_id, sky_base_accounts)
     select scaling_segment_id, count(1)
     from SC2_Sky_base_segment_snapshots
     where profiling_date = @profiling_date
     group by scaling_segment_id

     commit

     -- Now tack on the universe flags; a special case of things coming out of the lookup

     update SC2_weighting_working_table
     set universe = sl.universe
     from SC2_weighting_working_table
     inner join vespa_analysts.SC2_Segments_lookup_v2_1 as sl
     on SC2_weighting_working_table.scaling_segment_id = sl.scaling_segment_id

     commit

     -- Mix in the Vespa panel counts as determined earlier
     select scaling_segment_id
         ,count(1) as panel_members
     into #segment_distribs
     from SC2_Todays_panel_members
     where scaling_segment_id is not null
     group by scaling_segment_id

     commit
     create unique index fake_pk on #segment_distribs (scaling_segment_id)
     commit

     -- It defaults to 0, so we can just poke values in
     update SC2_weighting_working_table
     set vespa_panel = sd.panel_members
     from SC2_weighting_working_table
     inner join #segment_distribs as sd
     on SC2_weighting_working_table.scaling_segment_id = sd.scaling_segment_id

     -- And we're done! log the progress.
     commit
     drop table #segment_distribs
     commit
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_weighting_working_table

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B01: Complete! (Segmentation totals)', coalesce(@QA_catcher, -1)
     commit

     /**************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/

     delete from SC2_category_subtotals where scaling_date = @scaling_day
     delete from SC2_metrics where scaling_date = @scaling_day
     commit

     -- Rim-weighting is an iterative process that iterates through each of the scaling variables
     -- individually until the category sum of weights converge to the population category subtotals

     SET @cntr           = 1
     SET @iteration      = 0
     SET @cntr_var       = 1
     SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr)

     -- The SC2_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
     -- the sky base.
     -- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
     -- to ensure convergence.

     -- arbitrary value to ensure convergence
     update SC2_weighting_working_table
     set vespa_panel = 0.000001
     where vespa_panel = 0

     commit

     -- Initialise working columns
     update SC2_weighting_working_table
     set sum_of_weights = vespa_panel

     commit

     -- The iterative part.
     -- This works by choosing a particular scaling variable and then summing across the categories
     -- of that scaling variable for the sky base, the vespa panel and the sum of weights.
     -- A Category weight is calculated by dividing the sky base subtotal by the vespa panel subtotal
     -- for that category.
     -- This category weight is then applied back to the segments table and the process repeats until
     -- the sum_of_weights in the category table converges to the sky base subtotal.

     -- Category Convergence is defined as the category sum of weights being +/- 3 away from the sky
     -- base category subtotal within 100 iterations.
     -- Overall Convergence for that day occurs when each of the categories has converged, or the @convergence variable = 0

     -- The @convergence variable represents how many categories did not converge.
     -- If the number of iterations = 100 and the @convergence > 0 then this means that the Rim-weighting
     -- has not converged for this particular day.
     -- In this scenario, the person running the code should send the results of the SC2_metrics for that
     -- week to analytics team for review. ## What exactly are we checking? can we automate any of it?

     WHILE @cntr <6
     BEGIN
             DELETE FROM SC2_category_working_table

             SET @cntr_var = 1
             WHILE @cntr_var < 6
             BEGIN
                         SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr_var

                         EXECUTE('
                         INSERT INTO SC2_category_working_table (universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights)
                             SELECT  srs.universe
                                    ,@scaling_var
                                    ,ssl.'||@scaling_var||'
                                    ,SUM(srs.sky_base_accounts)
                                    ,SUM(srs.vespa_panel)
                                    ,SUM(srs.sum_of_weights)
                             FROM SC2_weighting_working_table AS srs
                                     inner join vespa_analysts.SC2_Segments_lookup_v2_1 AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
                             GROUP BY srs.universe,ssl.'||@scaling_var||'
                             ORDER BY srs.universe
                         ')

                         SET @cntr_var = @cntr_var + 1
             END

             commit

             UPDATE SC2_category_working_table
             SET  category_weight = sky_base_accounts / sum_of_weights
                 ,convergence_flag = CASE WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 ELSE 1 END

             SELECT @convergence = SUM(convergence_flag) FROM SC2_category_working_table
             SET @iteration = @iteration + 1
             SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr

             EXECUTE('
             UPDATE SC2_weighting_working_table
             SET  SC2_weighting_working_table.category_weight = sc.category_weight
                 ,SC2_weighting_working_table.sum_of_weights  = SC2_weighting_working_table.sum_of_weights * sc.category_weight
             FROM SC2_weighting_working_table
                     inner join vespa_analysts.SC2_Segments_lookup_v2_1 AS ssl ON SC2_weighting_working_table.scaling_segment_id = ssl.scaling_segment_id
                     inner join SC2_category_working_table AS sc ON sc.value = ssl.'||@scaling_var||'
                                                                      AND sc.universe = ssl.universe
             ')

             commit

             IF @iteration = 100 OR @convergence = 0 SET @cntr = 6
             ELSE

             IF @cntr = 5  SET @cntr = 1
             ELSE
             SET @cntr = @cntr+1

     END

     commit
     -- This loop build took about 4 minutes. That's fine.

     -- Calculate segment weight and corresponding indices

     -- This section calculates the segment weight which is the weight that should be applied to viewing data
     -- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting


     SELECT @sky_base = SUM(sky_base_accounts) FROM SC2_weighting_working_table
     SELECT @vespa_panel = SUM(vespa_panel) FROM SC2_weighting_working_table
     SELECT @sum_of_weights = SUM(sum_of_weights) FROM SC2_weighting_working_table

     UPDATE SC2_weighting_working_table
     SET  segment_weight = sum_of_weights / vespa_panel
         ,indices_actual = 100*(vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
         ,indices_weighted = 100*(sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

     commit

     -- OK, now catch those cases where stuff diverged because segments weren't reperesented:
     update SC2_weighting_working_table
     set segment_weight  = 0.000001
     where vespa_panel   = 0.000001

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_weighting_working_table
     where segment_weight >= 0.001           -- Ignore the placeholders here to guarantee convergence

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Midway (Iterations)', coalesce(@QA_catcher, -1)
     commit

     -- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level

     INSERT INTO SC2_category_subtotals (scaling_date,universe,profile,value,sky_base_accounts,vespa_panel,category_weight
                                              ,sum_of_weights, convergence)
     SELECT  @scaling_day
             ,universe
             ,profile
             ,value
             ,sky_base_accounts
             ,vespa_panel
             ,category_weight
             ,sum_of_weights
             ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
     FROM SC2_category_working_table

     -- The SC2_metrics table contains metrics for a particular scaling date. It shows whether the
     -- Rim-weighting process converged for that day and the number of iterations. It also shows the
     -- maximum and average weight for that day and counts for the sky base and the vespa panel.

     commit

     -- Apparently it should be reviewed each week, but what are we looking for?

     INSERT INTO SC2_metrics (scaling_date, iterations, convergence, max_weight, av_weight,
                                  sum_of_weights, sky_base, vespa_panel, non_scalable_accounts)
     SELECT  @scaling_day
            ,@iteration
            ,@convergence
            ,MAX(segment_weight)
            ,sum(segment_weight * vespa_panel) / sum(vespa_panel)    -- gives the average weight by account (just uising AVG would give it average by segment id)
            ,SUM(segment_weight * vespa_panel)                       -- again need some math because this table has one record per segment id rather than being at acocunt level
            ,@sky_base
            ,sum(CASE WHEN segment_weight >= 0.001 THEN vespa_panel ELSE NULL END)
            ,sum(CASE WHEN segment_weight < 0.001  THEN vespa_panel ELSE NULL END)
     FROM SC2_weighting_working_table

     update SC2_metrics
        set sum_of_convergence = abs(sky_base - sum_of_weights)

     insert into SC2_non_convergences(scaling_date,scaling_segment_id, difference)
     select @scaling_day
           ,scaling_segment_id
           ,abs(sum_of_weights - sky_base_accounts)
       from SC2_weighting_working_table
      where abs((segment_weight * vespa_panel) - sky_base_accounts) > 3

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Complete (Calculate weights)', coalesce(@QA_catcher, -1)
     commit

     /**************** PART B03: PUBLISHING WEIGHTS INTO INTERFACE STRUCTURES ****************/

     -- Here is where that bit of interface code goes, including extending the intervals
     -- in the Scaling midway tables (which now happens one day ata time). Maybe this guy
     -- wants to go into a new and different stored procedure?

     -- Heh, this deletion process clears out everything *after* the scaling day, meaning we
     -- have to start from the beginning doing this processing... I guess we'll just manage
     -- the historical build like this. (This is because we'd otherwise have to manage adding
     -- additional records to the interval table when we re-run a day and break an interval
     -- that already exists, and that whole process would be annoying to manage.)

     -- Except we'll only nuke everything if we *rebuild* a day that's not already there.
     if (select count(1) from SC2_Weightings where scaling_day = @scaling_day) > 0
     begin
         delete from SC2_Weightings where scaling_day >= @scaling_day

         delete from SC2_Intervals where reporting_starts >= @scaling_day

         update SC2_Intervals set reporting_ends = dateadd(day, -1, @scaling_day) where reporting_ends >= @scaling_day
     end
     commit

     -- Part 1: Update the Vespa midway scaling tables. In Vespa Analysts? May as well
     -- also keep this in VIQ_prod too.
     insert into SC2_Weightings
     select
         @scaling_day
         ,scaling_segment_id
         ,vespa_panel
         ,sky_base_accounts
         ,segment_weight
         ,sum_of_weights
         ,indices_actual
         ,indices_weighted
         ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
     from SC2_weighting_working_table
     -- Might have to check that the filter on segment_weight doesn't leave any orphaned
     -- accounts about the place...

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_Weightings
     where scaling_day = @scaling_day

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 1/4 (Midway weights)', coalesce(@QA_catcher, -1)
     commit

     -- First off extend the intervals that are already in the table:

     update SC2_Intervals
     set reporting_ends = @scaling_day
     from SC2_Intervals
     inner join SC2_Todays_panel_members as tpm
     on SC2_Intervals.account_number         = tpm.account_number
     and SC2_Intervals.scaling_segment_ID    = tpm.scaling_segment_ID
     where reporting_ends = @scaling_day - 1

     -- Next step is adding in all the new intervals that don't appear
     -- as extensions on existing intervals. First off, isolate the
     -- intervals that got extended

     select account_number
     into #included_accounts
     from SC2_Intervals
     where reporting_ends = @scaling_day

     commit
     create unique index fake_pk on #included_accounts (account_number)
     commit

     -- Now having figured out what already went in, we can throw in the rest:
     insert into SC2_Intervals (
         account_number
         ,reporting_starts
         ,reporting_ends
         ,scaling_segment_ID
     )
     select
         tpm.account_number
         ,@scaling_day
         ,@scaling_day
         ,tpm.scaling_segment_ID
     from SC2_Todays_panel_members as tpm
     left join #included_accounts as ia
     on tpm.account_number = ia.account_number
     where ia.account_number is null -- we don't want to add things already in the intervals table

     commit
     drop table #included_accounts
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_Intervals where reporting_ends = @scaling_day

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 2/4 (Midway intervals)', coalesce(@QA_catcher, -1)
     commit

     -- Part 2: Update the VIQ interface table (which needs the household key thing)

     insert into VESPA_HOUSEHOLD_WEIGHTING
     select
         ws.account_number
         ,ws.cb_key_household
         ,@scaling_day
         ,wwt.segment_weight
         ,@batch_date
     from SC2_weighting_working_table as wwt
     inner join SC2_Sky_base_segment_snapshots as ws -- need this table to get the cb_key_household items
     on wwt.scaling_segment_id = ws.scaling_segment_id
     inner join SC2_Todays_panel_members as tpm
     on ws.account_number = tpm.account_number       -- Filter for today's panel only
     and ws.profiling_date = @profiling_date

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from VESPA_HOUSEHOLD_WEIGHTING
     where scaling_date = @scaling_day

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 3/4 (VIQ interface)', coalesce(@QA_catcher, -1)
     commit

     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Complete! (Publish weights)'
     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC2: Weights made for ' || dateformat(@scaling_day, 'yyyy-mm-dd')
     commit

end; -- of procedure "SC2_v2_1__make_weights_CUSTOM"
commit;



/**************** I01: MAIN SCALING REFRESH PROCEDURE ****************/
-- This guy is expected to be run every day immediately after the Vespa
-- data load batch of the previous night completes, and before the VIQ
-- load process (on relevant Fridays). It will only update the specified
-- day's weights, defaulting to yesterday if no date is supplied. It will
-- *not* perform any segmentation, it will only end up with the most
-- appropriate segmentation already built in the SC2_Sky_base_segment_snapshots
-- table.
IF object_id('SC2_v2_1__scale_Vespa_panel_CUSTOM') IS NOT NULL THEN DROP PROCEDURE SC2_v2_1__scale_Vespa_panel_CUSTOM END IF;

create procedure SC2_v2_1__scale_Vespa_panel_CUSTOM
    @scaling_day                date = dateadd(day, -1, today())    -- Date to make weights
    ,@batch_date                datetime = now()                    -- For knowing when we did stuff
    ,@Scale_refresh_logging_ID  bigint = null                       -- Optional: good for centralising logs on large builds
as
begin

    -- Um, there's basically nothing to do here except set up the logger, so then...
    if @Scale_refresh_logging_ID is null
    begin
        declare @tablespacename varchar(40)
        declare @log_title      varchar(40)

        -- Remember to install the "what's my tablespace?" reference procedure wherever this is deployed
        if object_id('Regulars_whats_my_namespace') is not null
            execute Regulars_whats_my_namespace @tablespacename output

        set @log_title =
          case
            when lower(@tablespacename) = 'viq_prod'        then 'VespaSC2Live'
            when lower(@tablespacename) = 'vespa_analysts'  then 'VespaSC2Dev'
            when @tablespacename is not null                then 'SC2:schema:' || @tablespacename
                                                            else 'SC2:user:' || user
          end

        -- The logger title is supposed to encode what the thing does, and is limited to 20 characters.

        EXECUTE logger_create_run @log_title, 'SC2: Scaling initiated ('|| dateformat(@scaling_day, 'yyyy-mm-dd') ||')', @Scale_refresh_logging_ID output

        -- If the variable is still NULL because the tablespace procedure isn't defined,
        -- it's probably a dev build somewhere.

    end
    -- if the log ID thing isn't null, it's assumed to already represent a
    -- valid logger run that's been initialised somewhere else.
    commit

    -- Do all the work!
    EXECUTE SC2_v2_1__prepare_panel_members_CUSTOM   @scaling_day, @batch_date, @Scale_refresh_logging_ID
    -- That guy is in a seperate procedure in case we want to mess with virtual
    -- panel balance; then we only need to release one sub-procedure.
    commit
    EXECUTE SC2_v2_1__make_weights_CUSTOM            @scaling_day, @batch_date, @Scale_refresh_logging_ID
    commit

    -- Maybe we'll have unit tests there too? We've got interfaces designed
    -- and they won't change across implementations.

    EXECUTE SC2_v2_1__daily_QA_units          @Scale_refresh_logging_ID
    commit
    execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC2: Scaling completed ('|| dateformat(@scaling_day, 'yyyy-mm-dd') ||')'
    commit

end; -- of procedure "SC2_v2_1__scale_Vespa_panel_CUSTOM"

commit;


/**************** P01: PERMISSIONS! ****************/
-- Because CITeam needs to be able to run these procs if they're
-- going to integrate with the scheduler. We need the prod GRANTs
-- for the live VIQII stuff (I think). Table permissions are over
-- in the other script though. Might revoke these permissions when
-- we actually get into the VIQ_prod situation.
--grant execute on SC2_v2_1__do_weekly_segmentation_CUSTOM     to CITeam, vespa_analysts, sk_prod, sk_prodreg;
--grant execute on SC2_v2_1__prepare_panel_members_CUSTOM      to CITeam, vespa_analysts, sk_prod, sk_prodreg;
--grant execute on SC2_v2_1__make_weights_CUSTOM               to CITeam, vespa_analysts, sk_prod, sk_prodreg;
--grant execute on SC2_v2_1__daily_QA_units             to CITeam, vespa_analysts, sk_prod, sk_prodreg;
--grant execute on SC2_v2_1__scale_Vespa_panel_CUSTOM          to CITeam, vespa_analysts, sk_prod, sk_prodreg;
--grant execute on SC2_v2_1__weekly_QA_units            to CITeam, vespa_analysts, sk_prod, sk_prodreg;

commit;












