
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Data_Quality_Vespa_Scaling_Checks
**
** This is the scaling metrics procedure which, when run, will calculate various metrics in relation
** to the VESPA base and the Sky Base
**
**  
** Refer also to:
**
**
** Code sections:
**      Part A: A01 - Populate initial table with base and some of key variables
**
**      Part B:       Data Updates
**              B01 - Value Segments
**              B02 - Panel Status
**              B03 - Household Box Composition and Boxtype
**
**      Part C:       Indexes Data
**              C01 - Create Indexes Table and add data
**		C02 - Insert total indexes data
**		C03 - Insert into the main final table
**
**      Part D:       Inserts into vespa repository table
**              D01 - Insert into panel 12 metrics into the repository table
**              D02 - Insert into sky metrics into the repository table
**              D03 - Insert indexes into the repository table
**
**
**
**
**
** Things done:
**
**
******************************************************************************/


if object_id('Data_Quality_Vespa_Scaling_Checks') is not null drop procedure Data_Quality_Vespa_Scaling_Checks
commit

go

create procedure Data_Quality_Vespa_Scaling_Checks
   @target_date        date = NULL     -- Date of data analyzed or date process run
    ,@CP2_build_ID     bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin


   declare @Scale_refresh_logging_ID     bigint
   declare @QA_catcher                 integer         -- For control totals along the way
   declare @tablespacename             varchar(40)



     DELETE FROM SC2_scaling_weekly_sample_viq_dq
     COMMIT

     -- So this bit is not stable for the VIQ builds since we can't delete weights from there,
     -- but for dev builds within analytics this is required.
--     DELETE FROM SC2_Sky_base_segment_snapshots_viq_dq where profiling_date = @target_date
--     commit

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
       FROM sk_prod.cust_subs_hist
      WHERE subscription_sub_type IN ('DTV Primary Viewing')
        AND status_code IN ('AC','AB','PC')
        AND effective_from_dt    <= @target_date
        AND effective_to_dt      > @target_date
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
     execute logger_add_event @CP2_build_ID, 3, 'L01: Midway 1/2 (Weekly sample)', coalesce(@QA_catcher, -1)
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
     inner join sk_prod.cust_single_account_view AS b
     ON a.account_number = b.account_number

     COMMIT
     DELETE FROM #weekly_sample WHERE uk_standard_account=0
     COMMIT

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #weekly_sample

     commit
     execute logger_add_event @CP2_build_ID, 3, 'L01: Complete! (Population)', coalesce(@QA_catcher, -1)
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
      FROM sk_prod.EXPERIAN_CONSUMERVIEW cv,
           sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD pp
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
     execute logger_add_event @CP2_build_ID, 3, 'L02: Midway 1/7 (Consumerview Linkage)', coalesce(@QA_catcher, -1)
     commit

     -- Populate Package & ISBA TV Region

     INSERT INTO SC2_scaling_weekly_sample_viq_dq (
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
     left join sk_prod.cust_entitlement_lookup AS cel
         ON fbp.current_short_description = cel.short_description
     WHERE fbp.cb_key_household IS NOT NULL
       AND fbp.cb_key_individual IS NOT NULL

     commit
     drop table #weekly_sample
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_scaling_weekly_sample_viq_dq

     commit
     execute logger_add_event @CP2_build_ID, 3, 'L02: Midway 2/7 (Package & ISBA region)', coalesce(@QA_catcher, -1)
     commit

     -- HHcomposition

     UPDATE SC2_scaling_weekly_sample_viq_dq
     SET
         stws.hhcomposition = cv.h_household_composition
     FROM SC2_scaling_weekly_sample_viq_dq AS stws
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
     execute logger_add_event @CP2_build_ID, 3, 'L02: Midway 3/7 (HH composition)', coalesce(@QA_catcher, -1)
     commit

     -- Tenure

     -- Tenure has been grouped according to its relationship with viewing behaviour

     UPDATE SC2_scaling_weekly_sample_viq_dq t1
     SET
         tenure = CASE   WHEN datediff(day,acct_first_account_activation_dt,@target_date) <=  730 THEN 'A) 0-2 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@target_date) <= 3650 THEN 'B) 3-10 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@target_date) > 3650 THEN  'C) 10 Years+'
                         ELSE 'D) Unknown'
                  END
     FROM sk_prod.cust_single_account_view sav
     WHERE t1.account_number=sav.account_number
     COMMIT

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_scaling_weekly_sample
     where tenure <> 'D) Unknown'

     commit
     execute logger_add_event @CP2_build_ID, 3, 'L02: Midway 4/7 (Tenure)', coalesce(@QA_catcher, -1)
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
       FROM sk_prod.cust_subs_hist as csh
             inner join SC2_scaling_weekly_sample_viq_dq AS ss ON csh.account_number = ss.account_number
      WHERE  csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription')     --the DTV sub Type
        AND csh.status_code IN ('AC','AB','PC')                  --Active Status Codes
        AND csh.effective_from_dt <= @target_date
        AND csh.effective_to_dt > @target_date
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
     FROM sk_prod.CUST_SET_TOP_BOX AS stb INNER JOIN #accounts AS acc
                                                  ON stb.service_instance_id = acc.service_instance_id
     WHERE box_installed_dt <= @target_date
     AND box_replaced_dt   > @target_date
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
     execute logger_add_event @CP2_build_ID, 3, 'L02: Midway 5/7 (HD boxes)', coalesce(@QA_catcher, -1)
     commit

     SELECT  --acc.service_instance_id,
            acc.account_number
            ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
            ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
            ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
            ,MAX(CASE  WHEN #hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
            ,MAX(CASE  WHEN #hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
     INTO #scaling_box_level_viewing
     FROM sk_prod.cust_subs_hist AS csh
            INNER JOIN #accounts AS acc ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
            LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel
                            ON csh.current_short_description = cel.short_description
            LEFT OUTER JOIN #hda ON csh.service_instance_id = #hda.service_instance_id --< Links to the HD Set Top Boxes
      WHERE csh.effective_FROM_dt <= @target_date
        AND csh.effective_to_dt    > @target_date
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
     execute logger_add_event @CP2_build_ID, 3, 'L02: Midway 6/7 (P/S boxes)', coalesce(@QA_catcher, -1)
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
     from SC2_scaling_weekly_sample_viq_dq as sws
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
     execute logger_add_event @CP2_build_ID, 3, 'L02: Complete! (Variables)', coalesce(@QA_catcher, -1)
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
     execute logger_add_event @CP2_build_ID, 3, 'L03: Midway (Segment lookup)', coalesce(@QA_catcher, -1)
     commit

     -- Okay, no throw all of that back into the weekly sample table, because that's where
     -- the build expects it to be, were it not for that weird bug in Sybase:

     delete from SC2_scaling_weekly_sample_viq_dq
     commit

     insert into SC2_scaling_weekly_sample_viq_dq
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

     execute logger_add_event @CP2_build_ID, 3, 'L03: Complete! (Segment ID assignment)', coalesce(@QA_catcher, -1)
     commit

     /**************** L04: PUBLISHING INTO INTERFACE STRUCTURES ****************/

     -- First off we need the accounts and their scaling segmentation IDs: generating
     -- some 10M such records a week, but we'd be able to cull them once we've finished
     -- the associated scaling builds. Only need to maintain it while we still have
     -- historic builds to do.

/*
     insert into SC2_Sky_base_segment_snapshots
     select
         account_number
         ,@target_date
         ,cb_key_household   -- This guy still needs to be added to SC2_scaling_weekly_sample
         ,scaling_segment_ID
         ,mr_boxes+1         -- Number of multiroom boxes plus 1 for the primary
     from SC2_scaling_weekly_sample
     where scaling_segment_ID is not null -- still perhaps with the weird account from Eire?

     commit
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC2_Sky_base_segment_snapshots
     where profiling_date = @target_date

     commit
     execute logger_add_event @CP2_build_ID, 3, 'L04: Complete! (Segments published)', coalesce(@QA_catcher, -1)
     commit
*/
     -- We were summarising into scaling_segment_id and sky base count, but now we're
     -- doing that later during the actual weights build rather than keeping all the
     -- differrent profile date builds concurrent, we can recover it easily from the
     -- weekly segmentation anyway.

     -- Don't need to separately track the build dates, since they're on the interface
     -- tables and we'll just rely on those. (Gives us no visiblity of overwrites, but
     -- hey, it's okay, those only happen when stuff is being muddled with anyway.)

     execute logger_add_event @CP2_build_ID, 3, 'SC2: base segmentation complete!'
     commit

--select count(1) from SC2_scaling_weekly_sample_viq_dq

delete from  scaling_cbi_panel

insert into scaling_cbi_panel 
select a.*, b.adjusted_event_start_date_vespa, b.calculated_scaling_weight
from SC2_scaling_weekly_sample_viq_dq a
inner join
sk_prod.viq_viewing_data_scaling b
on a.account_number = b.account_number
and b.adjusted_event_start_date_vespa = @target_date

commit

----------------------------------------------------------------get the percentage you need to scale up the segments by -------------------------------------------------
declare @sky_base_upscale bigint
declare @event_date date
declare @sky_base_count bigint
declare @upscale_value decimal (6,3)

--set @sky_base_count = sky_base_count

set @event_date = @target_date
set @sky_base_count = (select count(1) from SC2_scaling_weekly_sample_viq_dq)

set @sky_base_upscale = (select sky_base_upscale_total from data_quality_sky_base_upscale where event_date = @event_date)

set @upscale_value = (select 1.0 * @sky_base_upscale / @sky_base_count)

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

select * into #vespa_base_seg from
(select '101' metric_id, universe segment, count(distinct account_number) vespa_base, round(sum(calculated_Scaling_weight),0) coverage
from scaling_cbi_panel
group by '101', universe
union all
select '201' metric_id, isba_tv_region segment, count(distinct account_number) vespa_base, round(sum(calculated_Scaling_weight),0) coverage
from scaling_cbi_panel
group by '201', isba_tv_region
union all
select '301' metric_id,CASE WHEN hhcomposition = '00' THEN hhcomposition || ': Families'
                  WHEN hhcomposition = '01' THEN hhcomposition || ': Extended family'
                  WHEN hhcomposition = '02' THEN hhcomposition || ': Extended household'
                  WHEN hhcomposition = '03' THEN hhcomposition || ': Pseudo family'
                  WHEN hhcomposition = '04' THEN hhcomposition || ': Single male'
                  WHEN hhcomposition = '05' THEN hhcomposition || ': Single female'
                  WHEN hhcomposition = '06' THEN hhcomposition || ': Male homesharers'
                  WHEN hhcomposition = '07' THEN hhcomposition || ': Female homesharers'
                  WHEN hhcomposition = '08' THEN hhcomposition || ': Mixed homesharers'
                  WHEN hhcomposition = '09' THEN hhcomposition || ': Abbreviated male families'
                  WHEN hhcomposition = '10' THEN hhcomposition || ': Abbreviated female families'
                  WHEN hhcomposition = '11' THEN hhcomposition || ': Multi-occupancy dwelling'
                  WHEN hhcomposition = 'U' THEN hhcomposition || ': Unclassified HHComp'
                  ELSE hhcomposition
            END AS segment,
 count(distinct account_number) vespa_base, round(sum(calculated_Scaling_weight),0) coverage
from scaling_cbi_panel
group by '301', hhcomposition
union all
select '401' metric_id, tenure segment, count(distinct account_number) vespa_base, round(sum(calculated_Scaling_weight),0) coverage
from scaling_cbi_panel
group by '401', tenure
union all
select '501' metric_id, boxtype segment, count(distinct account_number) vespa_base, round(sum(calculated_Scaling_weight),0) coverage
from scaling_cbi_panel
group by '501', boxtype
union all
select '601' metric_id, package segment, count(distinct account_number) vespa_base, round(sum(calculated_Scaling_weight),0) coverage
from scaling_cbi_panel
group by '601', package)t


----------------------------------------------------------------------------------------------------------------------------------------

select * into #sky_base_seg from
(select '101' metric_id, universe segment, count(distinct account_number) sky_base_actual,
round(1.0 * count(distinct account_number) * @upscale_value,0) sky_base
from SC2_scaling_weekly_sample_viq_dq
group by '101', universe
union all
select '201' metric_id, isba_tv_region segment, count(distinct account_number) sky_base_actual,
round(1.0 * count(distinct account_number) * @upscale_value,0) sky_base
from SC2_scaling_weekly_sample_viq_dq
group by '201', isba_tv_region
union all
select '301' metric_id,CASE WHEN hhcomposition = '00' THEN hhcomposition || ': Families'
                  WHEN hhcomposition = '01' THEN hhcomposition || ': Extended family'
                  WHEN hhcomposition = '02' THEN hhcomposition || ': Extended household'
                  WHEN hhcomposition = '03' THEN hhcomposition || ': Pseudo family'
                  WHEN hhcomposition = '04' THEN hhcomposition || ': Single male'
                  WHEN hhcomposition = '05' THEN hhcomposition || ': Single female'
                  WHEN hhcomposition = '06' THEN hhcomposition || ': Male homesharers'
                  WHEN hhcomposition = '07' THEN hhcomposition || ': Female homesharers'
                  WHEN hhcomposition = '08' THEN hhcomposition || ': Mixed homesharers'
                  WHEN hhcomposition = '09' THEN hhcomposition || ': Abbreviated male families'
                  WHEN hhcomposition = '10' THEN hhcomposition || ': Abbreviated female families'
                  WHEN hhcomposition = '11' THEN hhcomposition || ': Multi-occupancy dwelling'
                  WHEN hhcomposition = 'U' THEN hhcomposition || ': Unclassified HHComp'
                  ELSE hhcomposition
            END AS segment,
 count(distinct account_number) sky_base_actual,
round(1.0 * count(distinct account_number) * @upscale_value,0) sky_base
from SC2_scaling_weekly_sample_viq_dq
group by '301', hhcomposition
union all
select '401' metric_id, tenure segment, count(distinct account_number) sky_base_actual,
round(1.0 * count(distinct account_number) * @upscale_value,0) sky_base
from SC2_scaling_weekly_sample_viq_dq
group by '401', tenure
union all
select '501' metric_id, boxtype segment, count(distinct account_number) sky_base_actual,
round(1.0 * count(distinct account_number) * @upscale_value,0) sky_base
from SC2_scaling_weekly_sample_viq_dq
group by '501', boxtype
union all
select '601' metric_id, package segment, count(distinct account_number) sky_base_actual,
round(1.0 * count(distinct account_number) * @upscale_value,0) sky_base
from SC2_scaling_weekly_sample_viq_dq
group by '601', package) t


delete from segmentation_index_viq_dq

insert into segmentation_index_viq_dq
select a.metric_id, a.segment, a.sky_base,a.sky_base_actual, b.vespa_base, b.coverage 
from #sky_base_seg a
left outer join
#vespa_base_seg b
on
a.metric_id = b.metric_id
and a.segment = b.segment


commit

------------------------------------------------------------------------------Household TYPE INDEX --------------------------------------------------------------------------------------

SELECT      SUM(sky_base) AS sb_cat_total
            , SUM(vespa_base) AS va_cat_total
            , SUM(coverage) AS sky_coverage_total
INTO #index1_hh
FROM        segmentation_index_viq_dq
WHERE       metric_id = '101'

-----------------------------------------------------------------
--Divide Sky base & Vespa panel accounts per segment category by
--total accounts from the previous query, to get percent to total
-----------------------------------------------------------------

SELECT      weight.metric_id
            , segment 
            ,'hhtype' metric_desc 
            , 1.0 * weight.sky_base/ in1.sb_cat_total AS sb_index
            , 1.0 * weight.vespa_base / in1.va_cat_total AS va_index
INTO #index2_hh
FROM        #index1_hh in1, segmentation_index_viq_dq AS weight
WHERE       weight.metric_id = '101'

select in2.*, 
CAST(ROUND((in2.va_index / in2.sb_index) * 100,2) AS NUMERIC(8,2)) as index_var
into #hhtype
from #index2_hh in2

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------Region TYPE INDEX --------------------------------------------------------------------------------------

SELECT      SUM(sky_base) AS sb_cat_total
            , SUM(vespa_base) AS va_cat_total
            , SUM(coverage) AS sky_coverage_total
INTO #index1_region
FROM        segmentation_index_viq_dq
WHERE       metric_id = '201'

-----------------------------------------------------------------
--Divide Sky base & Vespa panel accounts per segment category by
--total accounts from the previous query, to get percent to total
-----------------------------------------------------------------

SELECT      weight.metric_id           
            , segment 
            ,'region' metric_desc 
            , 1.0 * weight.sky_base/ in1.sb_cat_total AS sb_index
            , 1.0 * weight.vespa_base / in1.va_cat_total AS va_index
INTO #index2_region
FROM        #index1_region in1, segmentation_index_viq_dq AS weight
WHERE       weight.metric_id = '201'

select in2.*, 
CAST(ROUND((in2.va_index / in2.sb_index) * 100,2) AS NUMERIC(8,2)) as index_var
into #region
from #index2_region in2


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------Household Composition INDEX --------------------------------------------------------------------------------------

SELECT      SUM(sky_base) AS sb_cat_total
            , SUM(vespa_base) AS va_cat_total
            , SUM(coverage) AS sky_coverage_total
INTO #index1_hhcomp
FROM        segmentation_index_viq_dq
WHERE       metric_id = '301'

-----------------------------------------------------------------
--Divide Sky base & Vespa panel accounts per segment category by
--total accounts from the previous query, to get percent to total
-----------------------------------------------------------------

SELECT      weight.metric_id
            , segment 
            ,'hhcomposition' metric_desc 
            , 1.0 * weight.sky_base/ in1.sb_cat_total AS sb_index
            , 1.0 * weight.vespa_base / in1.va_cat_total AS va_index
INTO #index2_hhcomp
FROM        #index1_hhcomp in1, segmentation_index_viq_dq AS weight
WHERE       weight.metric_id = '301'

select in2.*, 
CAST(ROUND((in2.va_index / in2.sb_index) * 100,2) AS NUMERIC(8,2)) as index_var
into #hhcomp
from #index2_hhcomp in2


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------Tenure INDEX --------------------------------------------------------------------------------------

SELECT      SUM(sky_base) AS sb_cat_total
            , SUM(vespa_base) AS va_cat_total
            , SUM(coverage) AS sky_coverage_total
INTO #index1_tenure
FROM        segmentation_index_viq_dq
WHERE       metric_id = '401'

-----------------------------------------------------------------
--Divide Sky base & Vespa panel accounts per segment category by
--total accounts from the previous query, to get percent to total
-----------------------------------------------------------------

SELECT      weight.metric_id
            , segment 
            ,'tenure' metric_desc 
            , 1.0 * weight.sky_base/ in1.sb_cat_total AS sb_index
            , 1.0 * weight.vespa_base / in1.va_cat_total AS va_index
INTO #index2_tenure
FROM        #index1_tenure in1, segmentation_index_viq_dq AS weight
WHERE       weight.metric_id = '401'

select in2.*, 
CAST(ROUND((in2.va_index / in2.sb_index) * 100,2) AS NUMERIC(8,2)) as index_var
into #tenure
from #index2_tenure in2



-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------BOX TYPE INDEX --------------------------------------------------------------------------------------

SELECT      SUM(sky_base) AS sb_cat_total
            , SUM(vespa_base) AS va_cat_total
            , SUM(coverage) AS sky_coverage_total
INTO #index1_box
FROM        segmentation_index_viq_dq
WHERE       metric_id = '501'

-----------------------------------------------------------------
--Divide Sky base & Vespa panel accounts per segment category by
--total accounts from the previous query, to get percent to total
-----------------------------------------------------------------

SELECT      weight.metric_id
            , segment 
            ,'boxtype' metric_desc 
            , 1.0 * weight.sky_base/ in1.sb_cat_total AS sb_index
            , 1.0 * weight.vespa_base / in1.va_cat_total AS va_index
INTO #index2_box
FROM        #index1_box in1, segmentation_index_viq_dq AS weight
WHERE       weight.metric_id = '501'

select in2.*, 
CAST(ROUND((in2.va_index / in2.sb_index) * 100,2) AS NUMERIC(8,2)) as index_var
into #Box_Type
from #index2_box in2

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------BOX TYPE INDEX --------------------------------------------------------------------------------------

SELECT      SUM(sky_base) AS sb_cat_total
            , SUM(vespa_base) AS va_cat_total
            , SUM(coverage) AS sky_coverage_total
INTO #index1_package
FROM        segmentation_index_viq_dq
WHERE       metric_id = '601'

-----------------------------------------------------------------
--Divide Sky base & Vespa panel accounts per segment category by
--total accounts from the previous query, to get percent to total
-----------------------------------------------------------------

SELECT      weight.metric_id
            , segment 
            ,'package' metric_desc 
            , 1.0 * weight.sky_base/ in1.sb_cat_total AS sb_index
            , 1.0 * weight.vespa_base / in1.va_cat_total AS va_index
INTO #index2_package
FROM        #index1_package in1, segmentation_index_viq_dq AS weight
WHERE       weight.metric_id = '601'

select in2.*, 
CAST(ROUND((in2.va_index / in2.sb_index) * 100,2) AS NUMERIC(8,2)) as index_var
into #tv_package
from #index2_package in2
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


delete from scaling_variables_viq_dq 

insert into scaling_variables_viq_dq 
SELECT segment.*, scal_var.index_var, scal_var.metric_desc
FROM (
SELECT * FROM #tv_package
UNION
SELECT * FROM #Box_Type
UNION
SELECT * FROM #tenure
UNION
SELECT * FROM #hhcomp
UNION
SELECT * FROM #region
UNION
SELECT * FROM #hhtype
) AS Scal_Var,
segmentation_index_viq_dq segment
where scal_var.metric_id = segment.metric_id
and lower(scal_var.segment) = lower(segment.segment)

commit



--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------insert vespa results into repository--------------------------------------------------------------------

insert into data_quality_vespa_REPOSITORY
(dq_run_id, viewing_data_date, dq_vm_id, metric_result, metric_tolerance_amber, metric_tolerance_red, metric_rag, load_timestamp)
select @CP2_build_ID,@target_date,a.dq_vm_id, b.metric_value, a.metric_tolerance_amber, a.metric_tolerance_red,
metric_benchmark_check(metric_value, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag,
getdate()
from
data_quality_vespa_metrics a,
(select vespa_base  metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||12||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name
from scaling_variables_viq_dq) b
where lower(a.metric_short_name) = lower(b.metric_short_name)
and a.current_flag = 1

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'Metrics for vespa results added for '||cast (@target_date as varchar(20))

---------------------------------D02 - Insert into sky metrics into the repository table------------------------------------------------------

insert into data_quality_vespa_REPOSITORY
(dq_run_id, viewing_data_date, dq_vm_id, metric_result, metric_tolerance_amber, metric_tolerance_red, metric_rag, load_timestamp)
select @CP2_build_ID,@target_date,a.dq_vm_id, b.metric_value, a.metric_tolerance_amber, a.metric_tolerance_red,
metric_benchmark_check(metric_value, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag,
getdate()
from
data_quality_vespa_metrics a,
(select sky_base metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'sky'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name
from scaling_variables_viq_dq) b
where lower(a.metric_short_name) = lower(b.metric_short_name)
and a.current_flag = 1

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'Metrics for sky metrics upscaled added for '||cast (@target_date as varchar(20))

--------------------------------Insert Sky Base Actual------------------------------------------------------------

---------------------------------D02 - Insert into sky metrics into the repository table------------------------------------------------------

insert into data_quality_vespa_REPOSITORY
(dq_run_id, viewing_data_date, dq_vm_id, metric_result, metric_tolerance_amber, metric_tolerance_red, metric_rag, load_timestamp)
select @CP2_build_ID,@target_date,a.dq_vm_id, b.metric_value, a.metric_tolerance_amber, a.metric_tolerance_red,
metric_benchmark_check(metric_value, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag,
getdate()
from
data_quality_vespa_metrics a,
(select sky_base_actual metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'sky_actual'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name
from scaling_variables_viq_dq) b
where lower(a.metric_short_name) = lower(b.metric_short_name)
and a.current_flag = 1

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'Metrics for sky metrics actual added for '||cast (@target_date as varchar(20))

-----------------------------------------------------------------------------------------------------------------

-------------------------------D03 - Insert indexes into the repository table-------------------------------------

insert into data_quality_vespa_REPOSITORY
(dq_run_id, viewing_data_date, dq_vm_id, metric_result, metric_tolerance_amber, metric_tolerance_red, metric_rag, load_timestamp)
select @CP2_build_ID,@target_date,a.dq_vm_id, b.metric_value, a.metric_tolerance_amber, a.metric_tolerance_red,
metric_benchmark_check(metric_value, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag,
getdate()
from
data_quality_vespa_metrics a,
(select index_var metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'index'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_')metric_short_name
from scaling_variables_viq_dq) b
where lower(a.metric_short_name) = lower(b.metric_short_name)
and a.current_flag = 1

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'Metrics for indexes added for '||cast (@target_date as varchar(20))

--------------------------------Insert Vespa  Base Coverage------------------------------------------------------------

insert into data_quality_vespa_REPOSITORY
(dq_run_id, viewing_data_date, dq_vm_id, metric_result, metric_tolerance_amber, metric_tolerance_red, metric_rag, load_timestamp)
select @CP2_build_ID,@target_date,a.dq_vm_id, b.metric_value, a.metric_tolerance_amber, a.metric_tolerance_red,
metric_benchmark_check(metric_value, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag,
getdate()
from
data_quality_vespa_metrics a,
(select coverage metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'coverage'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name
from scaling_variables_viq_dq) b
where lower(a.metric_short_name) = lower(b.metric_short_name)
and a.current_flag = 1

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'Metrics for vespa base coverage added for '||cast (@target_date as varchar(20))



EXECUTE logger_add_event @CP2_build_ID , 3,'Metrics for Sky Base added for '||cast (@target_date as varchar(20))


EXECUTE logger_add_event @CP2_build_ID , 3,'Scaling Metrics End for Date :'||cast (@target_date as varchar(20))

end

go


grant execute on Data_Quality_Vespa_Scaling_Checks to  sk_prodreg
