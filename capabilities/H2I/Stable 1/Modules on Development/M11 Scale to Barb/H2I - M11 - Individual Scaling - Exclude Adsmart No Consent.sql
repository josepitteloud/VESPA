
/*
exec V289_M11_01_SC3_v1_1__do_weekly_segmentation '2015-02-05', 99, '2014-07-29' -- thurs, logid, batch date

exec V289_M11_02_SC3_v1_1__prepare_panel_members '2015-02-05', '2015-02-07', '2014-07-29', 99 -- thurs, scaling, batch date, logid
exec V289_M11_03_SC3I_v1_1__add_individual_data '2015-02-05', '2014-07-29', 99  -- Thur, Batch logid

-- exec V289_M11_04_SC3I_v1_1__make_weights '2013-09-23', '2014-07-22', 23
exec V289_M11_04_SC3I_v1_1__make_weights_BARB '2015-02-05', '2015-02-07', '2014-07-29', 99 -- Thur, Scaling, Batch Date, logid

select * from thompsonja.z_logger_events
*/

/***********************************************************************************************************************************************
************************************************************************************************************************************************
******* M11: SKYVIEW INDIVIDUAL AND HOUSEOLD LEVEL SCALING SCRIPT                                                                             *******
************************************************************************************************************************************************
***********************************************************************************************************************************************/


--- Skyview scaling uses 2 of the Scaling 3.0 procedures. See the repository for more details
-- \Git_repository\Vespa\ad_hoc\V154 - Scaling 3.0\Vespa Analysts - SC3\SC3 - 3 - refresh procedures [v1.1].sql

-- These procs prepare the Skybase accounts (to be done once a week for a Thursday) and valid Vespa accounts (to be run each day)
--        SC3_v1_1__do_weekly_segmentation  SKYVIEW VERSION: V289_M11_01_SC3_v1_1__do_weekly_segmentation
--        SC3_v1_1__prepare_panel_members   SKYVIEW VERSION: V289_M11_02_SC3_v1_1__prepare_panel_members


--- A new procedure has been written to add individual level data to the scaling tables
--     V289_M11_03_SC3I_v1_1__add_individual_data

--- An existing Scaling 3.0 proc has been ammended to work for SkyView
-- This proc calculates the weights using a RIM Weighting process
--         SC3_v1_1__make_weights           SKYVIEW VERSION: V289_M11_04_SC3I_v1_1__make_weights




/**************** PART L: WEEKLY SEGMENTATION BUILD ****************/

create or replace procedure V289_M11_01_SC3_v1_1__do_weekly_segmentation
    @profiling_thursday         date = null         -- Day on which to do sky base profiling
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
as
begin

     declare @QA_catcher                 integer         -- For control totals along the way
     declare @tablespacename             varchar(40)
     COMMIT -- (^_^)

         
     -- CREATE OF REPLACE VARIABLE @QA_catcher                 integer;         -- For control totals along the way
     -- CREATE OF REPLACE VARIABLE @tablespacename             varchar(40);
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Profiling Sky UK base as of ' || dateformat(@profiling_thursday,'yyyy-mm-dd') || '.'

     -- Clear out the processing tables and suchlike

     DELETE FROM SC3_scaling_weekly_sample
     COMMIT -- (^_^)

     -- Decide when we're doing the profiling, if it's not passed in as a parameter
     if @profiling_thursday is null
     begin
         --execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output  -- proc returns a Saturday
         --set @profiling_thursday = @profiling_thursday - 2                               -- but we want a Thursday

                select @profiling_thursday = dateformat((now() - datepart(weekday,now()))-2, 'YYYY-MM-DD')
     end
     COMMIT -- (^_^)

     -- Get us a refresh logging ID thing if one wasn't assigned
--      if @Scale_refresh_logging_ID is null
--      begin
--          execute Regulars_whats_my_namespace @tablespacename output
--          if @tablespacename = 'vespa_analysts'
--              EXECUTE logger_create_run 'ScalingSegmentation'           , 'SC3: Segmentation build for ' || dateformat(@profiling_thursday, 'yyyy-mm-dd') || '.', @Scale_refresh_logging_ID output
--          else
--          begin
--              set @tablespacename = coalesce(@tablespacename, user)
--              EXECUTE logger_create_run 'SC3 Dev ' || @tablespacename || 'SC3: Segmentation build for  ' || dateformat(@profiling_thursday, 'yyyy-mm-dd') || '.', @Scale_refresh_logging_ID output
--          end
--      end
     COMMIT -- (^_^)

     -- So this bit is not stable for the VIQ builds since we can't delete weights from there,
     -- but for dev builds within analytics this is required.
     DELETE FROM SC3_Sky_base_segment_snapshots where profiling_date = @profiling_thursday
     COMMIT -- (^_^)

     /**************** L01: ESTABLISH POPULATION ****************/
     -- We need the segmentation over the whole Sky base so we can scale up


     -- Captures all active accounts in cust_subs_hist
     SELECT   account_number
             ,cb_key_household
             ,cb_key_individual
             ,current_short_description
             ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
             ,convert(bit, 0)  AS uk_standard_account
             ,convert(VARCHAR(30), NULL) AS isba_tv_region
       INTO #weekly_sample
       FROM /*sk_prod.*/cust_subs_hist
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
     COMMIT -- (^_^)
     DELETE FROM #weekly_sample WHERE rank > 1
     COMMIT -- (^_^)

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #weekly_sample

     COMMIT -- (^_^)
     --execute logger_add_event @Scale_refresh_logging_ID, 3, 'L01: Midway 1/2 (Weekly sample)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- Create indices
     CREATE UNIQUE INDEX fake_pk ON #weekly_sample (account_number)
         COMMIT -- (^_^)
     CREATE INDEX for_package_join ON #weekly_sample (current_short_description)
     COMMIT -- (^_^)

     -- Take out ROIs (Republic of Ireland) and non-standard accounts as these are not currently in the scope of Vespa
     UPDATE #weekly_sample
     SET
         uk_standard_account = CASE
             WHEN b.acct_type='Standard' AND b.account_number <>'?' AND b.pty_country_code ='GBR' THEN 1
             ELSE 0 END
        -- Insert SC3 TV regions
         ,isba_tv_region = case
                when b.isba_tv_region = 'Border' then'NI, Scotland & Border'
                when b.isba_tv_region = 'Central Scotland' then'NI, Scotland & Border'
                when b.isba_tv_region = 'East Of England' then'Wales & Midlands'
                when b.isba_tv_region = 'HTV Wales' then'Wales & Midlands'
                when b.isba_tv_region = 'HTV West' then'South England'
                when b.isba_tv_region = 'London' then'London'
                when b.isba_tv_region = 'Meridian (exc. Channel Islands)' then'South England'
                when b.isba_tv_region = 'Midlands' then'Wales & Midlands'
                when b.isba_tv_region = 'North East' then'North England'
                when b.isba_tv_region = 'North Scotland' then'NI, Scotland & Border'
                when b.isba_tv_region = 'North West' then'North England'
                when b.isba_tv_region = 'Not Defined' then'Not Defined'
                when b.isba_tv_region = 'South West' then'South England'
                when b.isba_tv_region = 'Ulster' then'NI, Scotland & Border'
                when b.isba_tv_region = 'Yorkshire' then'North England'
                else 'Not Defined'
          end
--          ,isba_tv_region = b.isba_tv_region
         -- Grab the cb_key_individual we need for consumerview linkage at the same time
         ,cb_key_individual = b.cb_key_individual
     FROM #weekly_sample AS a
     inner join /*sk_prod.*/cust_single_account_view AS b
     ON a.account_number = b.account_number

     COMMIT -- (^_^)
     DELETE FROM #weekly_sample WHERE uk_standard_account=0
     COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from #weekly_sample

     COMMIT -- (^_^)
     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'L01: Complete! (Population)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- hahaha, no, there are lots of key dupes here, we're just going to
     -- have to cull them during the piping into the VIQ build
     --CREATE UNIQUE INDEX for_ilu_joining ON #weekly_sample (cb_key_household)
     --COMMIT -- (^_^)

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
      FROM /*sk_prod.*/EXPERIAN_CONSUMERVIEW cv,
           /*sk_prod.*/PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD pp
     WHERE cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
       AND cv.cb_key_individual is not null
     GROUP BY cv.cb_key_household, cv.cb_key_family, cv.cb_key_individual
     COMMIT -- (^_^)

     CREATE LF INDEX idx1 on #cv_pp(p_head_of_household)
         COMMIT -- (^_^)
     CREATE HG INDEX idx2 on #cv_pp(cb_key_family)
         COMMIT -- (^_^)
     CREATE HG INDEX idx3 on #cv_pp(cb_key_individual)
         COMMIT -- (^_^)

     -- We grabbed the cb_key_individual mark from SAV in the previuos build, so
     -- now we need the ConsumerView treatment from the customer group wiki:
     -- Update for SC3 build. Use case statement to consolidate old scaling segments into new.

     -- Approx 25% of accounts do not match when using cb_key_individual meaning these default to D) Uncassified
     -- Instead match at household level

/*     SELECT   cb_key_individual
             ,cb_row_id
             ,rank() over(partition by cb_key_family     ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_fam
             ,rank() over(partition by cb_key_individual ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_ind
             ,case
                     when h_household_composition = '00' then 'A) Families'
                     when h_household_composition = '01' then 'A) Families'
                     when h_household_composition = '02' then 'A) Families'
                     when h_household_composition = '03' then 'A) Families'
                     when h_household_composition = '04' then 'B) Singles'
                     when h_household_composition = '05' then 'B) Singles'
                     when h_household_composition = '06' then 'C) Homesharers'
                     when h_household_composition = '07' then 'C) Homesharers'
                     when h_household_composition = '08' then 'C) Homesharers'
                     when h_household_composition = '09' then 'A) Families'
                     when h_household_composition = '10' then 'A) Families'
                     when h_household_composition = '11' then 'C) Homesharers'
                     when h_household_composition = 'U'  then 'D) Unclassified HHComp'
                     else 'D) Unclassified HHComp'
             end as h_household_composition
--              ,h_household_composition -- may as well pull out the item we need given we're ranking and deleting
     INTO #cv_keys
     FROM #cv_pp
     WHERE cb_key_individual IS not NULL
       AND cb_key_individual <> 0
*/

     SELECT   cb_key_household
             ,cb_row_id
             ,rank() over(partition by cb_key_family     ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_fam
             ,rank() over(partition by cb_key_household ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_hhd
             ,case
                     when h_household_composition = '00' then 'A) Families'
                     when h_household_composition = '01' then 'A) Families'
                     when h_household_composition = '02' then 'A) Families'
                     when h_household_composition = '03' then 'A) Families'
                     when h_household_composition = '04' then 'B) Singles'
                     when h_household_composition = '05' then 'B) Singles'
                     when h_household_composition = '06' then 'C) Homesharers'
                     when h_household_composition = '07' then 'C) Homesharers'
                     when h_household_composition = '08' then 'C) Homesharers'
                     when h_household_composition = '09' then 'A) Families'
                     when h_household_composition = '10' then 'A) Families'
                     when h_household_composition = '11' then 'C) Homesharers'
                     when h_household_composition = 'U'  then 'D) Unclassified HHComp'
                     else 'D) Unclassified HHComp'
             end as h_household_composition
--              ,h_household_composition -- may as well pull out the item we need given we're ranking and deleting
     INTO #cv_keys
     FROM #cv_pp
--     WHERE cb_key_individual IS not NULL
--       AND cb_key_individual <> 0
     WHERE cb_key_household IS not NULL
     AND cb_key_household <> 0
         COMMIT -- (^_^)






     -- This is a cleaned out version of http://mktskyportal/Campaign%20Handbook/ConsumerView.aspx
     -- since we only need the individual stuff for this linkage.

--     DELETE FROM #cv_keys WHERE rank_fam != 1 AND rank_ind != 1
       DELETE FROM #cv_keys WHERE rank_fam != 1 AND rank_hhd != 1
     COMMIT -- (^_^)

--     CREATE INDEX index_ac on #cv_keys (cb_key_individual)
       CREATE INDEX index_ac on #cv_keys (cb_key_household)
     COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from #cv_keys

     COMMIT -- (^_^)
     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 1/8 (Consumerview Linkage)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- Populate Package & ISBA TV Region

     INSERT INTO SC3_scaling_weekly_sample (
         account_number
         ,cb_key_household
         ,cb_key_individual
         ,universe    --scaling variables removed. Use later to set no_of_stbs
         ,sky_base_universe  -- Need to include this as they form part of a big index
         ,vespa_universe  -- Need to include this as they form part of a big index
         ,isba_tv_region
         ,hhcomposition
         ,tenure
         ,num_mix
         ,mix_pack
         ,package
         ,boxtype
         ,no_of_stbs
         ,hd_subscription
         ,pvr
     )
     SELECT
         fbp.account_number
         ,fbp.cb_key_household
         ,fbp.cb_key_individual
         ,'A) Single box HH' -- universe
         ,'Not adsmartable'  -- sky_base_universe
         ,'Non-Vespa'   -- Vespa Universe
         ,fbp.isba_tv_region -- isba_tv_region
         ,'D)'  -- hhcomposition
         ,'D) Unknown' -- tenure
         ,cel.Variety + cel.Knowledge + cel.Kids + cel.Style_Culture + cel.Music + cel.News_Events as num_mix
         ,CASE
                         WHEN Num_Mix IS NULL OR Num_Mix=0                           THEN 'Entertainment Pack'
                         WHEN (cel.variety=1 OR cel.style_culture=1)  AND Num_Mix=1  THEN 'Entertainment Pack'
                         WHEN (cel.variety=1 AND cel.style_culture=1) AND Num_Mix=2  THEN 'Entertainment Pack'
                         WHEN Num_Mix > 0                                            THEN 'Entertainment Extra'
                     END AS mix_pack -- Basic package has recently been split into the Entertainment and Entertainment Extra packs
         ,CASE
             WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN 'Movies & Sports' --'Top Tier'
             WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN 'Sports' --'Dual Sports'
             WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN 'Movies' --'Dual Movies'
             WHEN cel.prem_sports = 1 AND cel.prem_movies = 0 THEN 'Sports' --'Single Sports'
             WHEN cel.prem_sports = 0 AND cel.prem_movies = 1 THEN 'Movies' --'Single Movies'
             WHEN cel.prem_sports > 0 OR  cel.prem_movies > 0 THEN 'Movies & Sports' --'Other Premiums'
             WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Pack'  THEN 'Basic' --'Basic - Ent'
             WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Extra' THEN 'Basic' --'Basic - Ent Extra'
             ELSE 'Basic' END --                                                  'Basic - Ent' END -- package
          ,'D) FDB & No_secondary_box' -- boxtype
          ,'Single' --no_of_stbs
          ,'No' --hd_subscription
          ,'No' --pvr
     FROM #weekly_sample AS fbp
     left join /*sk_prod.*/cust_entitlement_lookup AS cel
         ON fbp.current_short_description = cel.short_description
     WHERE fbp.cb_key_household IS NOT NULL
       AND fbp.cb_key_individual IS NOT NULL

     COMMIT -- (^_^)
     drop table #weekly_sample
     COMMIT -- (^_^)

     -- Populate sky_base_universe according to SQL code used to find adsmartable bozes in weekly reports
     select  account_number
            ,case
                when flag = 1 and cust_viewing_data_capture_allowed = 'Y' then 'Adsmartable with consent'
                when flag = 1 and cust_viewing_data_capture_allowed <> 'Y' then 'Adsmartable but no consent'
                else 'Not adsmartable'
                end as sky_base_universe
        into  #cv_sbu
        from (
                 select  sav.account_number as account_number, adsmart.flag, cust_viewing_data_capture_allowed
                    from    (
                                select      distinct account_number, cust_viewing_data_capture_allowed
                                     from   /*sk_prod.*/CUST_SINGLE_ACCOUNT_VIEW
                                    where   CUST_ACTIVE_DTV = 1                     -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
                                      and   pty_country_code = 'GBR'
                            )as sav
                                    left join       (
                                            ----------------------------------------------------------
                                            -- B03: Flag Adsmartable boxes based on Adsmart definition
                                            ----------------------------------------------------------
                                                select  account_number
                                                                ,max(   CASE    WHEN x_pvr_type ='PVR6'                                 THEN 1
                                                                                WHEN x_pvr_type ='PVR5'                                 THEN 1
                                                                                WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung'  THEN 1
                                                                                WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'     THEN 1
                                                                                ELSE 0
                                                                                END) AS flag
                                                from    (
                                                        --------------------------------------------------------------------------
                                                        -- B02: Extracting Active Boxes per account (one line per box per account)
                                                        --------------------------------------------------------------------------
                                                        select  *
                                                        from    (
                                                                --------------------------------------------------------------------
                                                                -- B01: Ranking STB based on service instance id to dedupe the table
                                                                --------------------------------------------------------------------
                                                                Select  account_number
                                                                                ,x_pvr_type
                                                                                ,x_personal_storage_capacity
                                                                                ,currency_code
                                                                                ,x_manufacturer
                                                                                ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
                                                                from    /*sk_prod.*/CUST_SET_TOP_BOX

                                                        )       as base
                                                        where   active_flag = 1

                                                )       as active_boxes
                                        where   currency_code = 'GBP'
                                        group   by      account_number

                                    )       as adsmart
                                    on      sav.account_number = adsmart.account_number
        ) as sub1
     COMMIT -- (^_^)

     UPDATE SC3_scaling_weekly_sample
     SET
         stws.sky_base_universe = cv.sky_base_universe
     FROM SC3_scaling_weekly_sample AS stws
     inner join #cv_sbu AS cv
     ON stws.account_number = cv.account_number
         COMMIT -- (^_^)

--      -- Update vespa universe from SC3_account_universe
--      UPDATE SC3_scaling_weekly_sample
--      SET    samp.vespa_universe     = acc.vespa_universe
--      FROM   SC3_scaling_weekly_sample samp
--      INNER JOIN SC3_account_universe acc
--      ON    samp.account_number = acc.account_number

     -- Update vespa universe
     UPDATE SC3_scaling_weekly_sample
     SET    vespa_universe     =
              case
                when sky_base_universe = 'Not adsmartable' then 'Vespa not Adsmartable'
                when sky_base_universe = 'Adsmartable with consent' then 'Vespa adsmartable'
                when sky_base_universe = 'Adsmartable but no consent' then 'Vespa but no consent'
                else 'Non-Vespa'
              end
        COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3_scaling_weekly_sample
     where sky_base_universe is not null and vespa_universe is not null

     COMMIT -- (^_^)
     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 2a/8 (Accounts with no universe)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     delete from SC3_scaling_weekly_sample
     where sky_base_universe is null or vespa_universe is null
         COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3_scaling_weekly_sample

     COMMIT -- (^_^)
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 2/8 (Package & ISBA region)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- HHcomposition

     UPDATE SC3_scaling_weekly_sample
     SET
         stws.hhcomposition = cv.h_household_composition
     FROM SC3_scaling_weekly_sample AS stws
     inner join #cv_keys AS cv
     -- ON stws.cb_key_individual = cv.cb_key_individual
     ON stws.cb_key_household = cv.cb_key_household

     COMMIT -- (^_^)
     drop table #cv_keys
     COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3_scaling_weekly_sample
     where left(hhcomposition, 2) <> 'D)'

     COMMIT -- (^_^)
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 3/8 (HH composition)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- Tenure

     -- Tenure has been grouped according to its relationship with viewing behaviour

     UPDATE SC3_scaling_weekly_sample t1
     SET
         tenure = CASE   WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  730 THEN 'A) 0-2 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <= 3650 THEN 'B) 3-10 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) > 3650 THEN  'C) 10 Years+'
                         ELSE 'D) Unknown'
                  END
     FROM /*sk_prod.*/cust_single_account_view sav
     WHERE t1.account_number=sav.account_number
     COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3_scaling_weekly_sample
     where tenure <> 'D) Unknown'
         COMMIT -- (^_^)

     -- Added SC3 line to remove Unknown tenure
     delete from SC3_scaling_weekly_sample
     where tenure = 'D) Unknown'
         COMMIT -- (^_^)

     -- Added SC3 line to remove Not Defined region
     delete from SC3_scaling_weekly_sample
     where isba_tv_region = 'Not Defined'


     COMMIT -- (^_^)
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 4/8 (Tenure)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- Boxtype & Universe

     -- Boxtype is defined as the top two boxtypes held by a household ranked in the following order
     -- 1) HD, 2) HDx, 3) Skyplus, 4) FDB

     -- Capture all active boxes for this week
     SELECT    csh.service_instance_id
             , csh.account_number
             , subscription_sub_type
             , rank() over (PARTITION BY csh.service_instance_id ORDER BY csh.account_number, csh.cb_row_id desc) AS rank
       INTO #accounts -- drop table #accounts
       FROM /*sk_prod.*/cust_subs_hist as csh
             inner join SC3_scaling_weekly_sample AS ss ON csh.account_number = ss.account_number
      WHERE  csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription')     --the DTV sub Type
        AND csh.status_code IN ('AC','AB','PC')                  --Active Status Codes
        AND csh.effective_from_dt <= @profiling_thursday
        AND csh.effective_to_dt > @profiling_thursday
        AND csh.effective_from_dt<>effective_to_dt
        COMMIT -- (^_^)

     -- De-dupe active boxes
     DELETE FROM #accounts WHERE rank>1
     COMMIT -- (^_^)

     -- Create indices on list of boxes
     CREATE hg INDEX idx1 ON #accounts(service_instance_id)
         COMMIT -- (^_^)
     CREATE hg INDEX idx2 ON #accounts(account_number)
     COMMIT -- (^_^)

     -- Identify HD & 1TB/2TB HD boxes
     SELECT  stb.service_instance_id
            ,SUM(CASE WHEN current_product_description LIKE '%HD%' THEN 1
                     ELSE 0
                  END) AS HD
            ,SUM(CASE WHEN x_description IN ('Amstrad HD PVR6 (1TB)', 'Amstrad HD PVR6 (2TB)') THEN 1
                     ELSE 0
                  END) AS HD1TB
     INTO #hda -- drop table #hda
     FROM /*sk_prod.*/CUST_SET_TOP_BOX AS stb INNER JOIN #accounts AS acc
                                                  ON stb.service_instance_id = acc.service_instance_id
     WHERE box_installed_dt <= @profiling_thursday
     AND box_replaced_dt   > @profiling_thursday
     AND current_product_description like '%HD%'
     GROUP BY stb.service_instance_id

     -- Create index on HD table
     COMMIT -- (^_^)
     CREATE UNIQUE hg INDEX idx1 ON #hda(service_instance_id)
     COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from #hda

     COMMIT -- (^_^)
--   execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 5/8 (HD boxes)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- Identify PVR boxes
     SELECT  acc.account_number
            ,MAX(CASE WHEN x_box_type LIKE '%Sky+%' THEN 'Yes'
                     ELSE 'No'
                  END) AS PVR
     INTO #pvra -- drop table #pvra
     FROM /*sk_prod.*/CUST_SET_TOP_BOX AS stb INNER JOIN #accounts AS acc
                                                  ON stb.service_instance_id = acc.service_instance_id
     WHERE box_installed_dt <= @profiling_thursday
     AND box_replaced_dt   > @profiling_thursday
     GROUP by acc.account_number

     -- Create index on PVR table
     COMMIT -- (^_^)
     CREATE hg INDEX pvidx1 ON #pvra(account_number)
     COMMIT -- (^_^)

     -- PVR
     UPDATE SC3_scaling_weekly_sample
     SET
         stws.pvr = cv.pvr
     FROM SC3_scaling_weekly_sample AS stws
     inner join #pvra AS cv
     ON stws.account_number = cv.account_number
         COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from #pvra

     COMMIT -- (^_^)
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6/8 (PVR boxes)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

       -- Set default value when account cannot be found
      update SC3_scaling_weekly_sample
         set pvr = case
                when sky_base_universe like 'Adsmartable%' then 'Yes'
                else 'No'
         end
       where pvr is null
      COMMIT -- (^_^)

     --Further check to ensure that when PVR is No then the box is Not Adsmartable
     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3_scaling_weekly_sample
     where pvr = 'No' and sky_base_universe like 'Adsmartable%'

     COMMIT -- (^_^)
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6a/8 (Non-PVR boxes which are adsmartable)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

       -- Update PVR when PVR says 'No' and universe is an adsmartable one.
      update SC3_scaling_weekly_sample
         set pvr = 'Yes'
       where pvr = 'No' and sky_base_universe like 'Adsmartable%'
      COMMIT -- (^_^)

     SELECT  --acc.service_instance_id,
            acc.account_number
            ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
            ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
            ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
            ,MAX(CASE  WHEN #hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
            ,MAX(CASE  WHEN #hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
     INTO #scaling_box_level_viewing
     FROM /*sk_prod.*/cust_subs_hist AS csh
            INNER JOIN #accounts AS acc ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
            LEFT OUTER JOIN /*sk_prod.*/cust_entitlement_lookup cel
                            ON csh.current_short_description = cel.short_description
            LEFT OUTER JOIN #hda ON csh.service_instance_id = #hda.service_instance_id --< Links to the HD Set Top Boxes
      WHERE csh.effective_FROM_dt <= @profiling_thursday
        AND csh.effective_to_dt    > @profiling_thursday
        AND csh.status_code IN  ('AC','AB','PC')
        AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
        AND csh.effective_FROM_dt <> csh.effective_to_dt
     GROUP BY acc.service_instance_id ,acc.account_number

     COMMIT -- (^_^)
     drop table #accounts
         COMMIT -- (^_^)
     drop table #hda
     COMMIT -- (^_^)

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
     COMMIT -- (^_^)
     CREATE unique INDEX idx_ac ON #boxtype_ac(account_number)
         COMMIT -- (^_^)
     drop table #scaling_box_level_viewing
     COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from #boxtype_ac

     COMMIT -- (^_^)
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6/8 (P/S boxes)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

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

     COMMIT -- (^_^)

     /* Now building this differently; Sybase 15 didn't like it at all, even had trouble killing
     ** the thread after the query is cancelled. This weirdness has been replicated in other schemas
     ** and on the QA server and has been raised to Sybase. Apparently it's a known bug and there's
     ** a patch coming in 15.4, but for the meantime this method remains commented out even though
     ** the workaround is amazingly ugly...pvr
     UPDATE SC3_scaling_weekly_sample
     SET
         universe    = ac.universe
         ,boxtype    = ac.boxtype
         ,mr_boxes   = ac.mr_boxes
     FROM SC3_scaling_weekly_sample
     inner join #boxtype_ac AS ac
     on ac.account_number = SC3_scaling_weekly_sample.account_number
     */

     CREATE TABLE #SC3_weird_sybase_update_workaround (
          account_number                     VARCHAR(20)     primary key not null
         ,cb_key_household                   BIGINT          not null
         ,cb_key_individual                  BIGINT          not null
         ,consumerview_cb_row_id             BIGINT          null
         ,universe                           VARCHAR(30)     null                    -- Single or multiple box household. Reused for no_of_stbs
         ,sky_base_universe                  VARCHAR(30)     null                    -- Not adsmartable, Adsmartable with consent, Adsmartable but no consent household
         ,vespa_universe                     VARCHAR(30)     null                    -- Non-Vespa, Not Adsmartable, Vespa with consent, vespa but no consent household
         ,weighting_universe                 VARCHAR(30)     null                    -- Used when finding appropriate scaling segment - see note
         ,isba_tv_region                     VARCHAR(30)     null                    -- Scaling variable 1 : Region
         ,hhcomposition                      VARCHAR(2)      default 'D)'         not null -- Scaling variable 2: Household composition
         ,tenure                             VARCHAR(15)     DEFAULT 'D) Unknown' not null-- Scaling variable 3: Tenure
         ,num_mix                            INT             null
         ,mix_pack                           VARCHAR(20)     null
         ,package                            VARCHAR(20)     null                    -- Scaling variable 4: Package
         ,boxtype                            VARCHAR(35)     null                    -- Old Scaling variable 5: Household boxtype split into no_of_stbs, hd_subscription and pvr.
         ,no_of_stbs                         VARCHAR(15)     null                    -- Scaling variable 5: No of set top boxes
         ,hd_subscription                    VARCHAR(5)      null                    -- Scaling variable 6: HD subscription
         ,pvr                                VARCHAR(5)      null                    -- Scaling variable 7: Is the box pvr capable?
         ,population_scaling_segment_id      INT             DEFAULT NULL null       -- segment scaling id for identifying segments
         ,vespa_scaling_segment_id           INT             DEFAULT NULL null       -- segment scaling id for identifying segments
         ,mr_boxes                           INT                null
     --    ,complete_viewing                   TINYINT         DEFAULT 0           -- Flag for all accounts with complete viewing data
     )
         COMMIT -- (^_^)

     CREATE INDEX for_segment_identification_temp1 ON #SC3_weird_sybase_update_workaround (isba_tv_region)      COMMIT -- (^_^)
     CREATE INDEX for_segment_identification_temp2 ON #SC3_weird_sybase_update_workaround (hhcomposition)       COMMIT -- (^_^)
     CREATE INDEX for_segment_identification_temp3 ON #SC3_weird_sybase_update_workaround (tenure)      COMMIT -- (^_^)
     CREATE INDEX for_segment_identification_temp4 ON #SC3_weird_sybase_update_workaround (package)     COMMIT -- (^_^)
     CREATE INDEX for_segment_identification_temp5 ON #SC3_weird_sybase_update_workaround (boxtype)     COMMIT -- (^_^)
     CREATE INDEX consumerview_joining ON #SC3_weird_sybase_update_workaround (consumerview_cb_row_id)  COMMIT -- (^_^)
     CREATE INDEX for_temping1 ON #SC3_weird_sybase_update_workaround (population_scaling_segment_id)   COMMIT -- (^_^)
     CREATE INDEX for_temping2 ON #SC3_weird_sybase_update_workaround (vespa_scaling_segment_id)        COMMIT -- (^_^)

     insert into #SC3_weird_sybase_update_workaround (
          account_number
         ,cb_key_household
         ,cb_key_individual
         ,consumerview_cb_row_id
         ,universe
         ,sky_base_universe
         ,vespa_universe
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
         ,sky_base_universe
         ,vespa_universe
         ,sws.isba_tv_region
         ,sws.hhcomposition
         ,sws.tenure
         ,sws.num_mix
         ,sws.mix_pack
         ,sws.package
         ,ac.boxtype
         ,ac.mr_boxes
     from SC3_scaling_weekly_sample as sws
     inner join #boxtype_ac AS ac
     on ac.account_number = sws.account_number
     WHERE sws.cb_key_household IS NOT NULL
       AND sws.cb_key_individual IS NOT NULL
         COMMIT -- (^_^)

     -- Update SC3 scaling variables in #SC3_weird_sybase_update_workaround according to Scaling 3.0 variables
     update #SC3_weird_sybase_update_workaround sws
             set sws.pvr = ac.pvr
            from #pvra AS ac
           where ac.account_number = sws.account_number

     -- This data is eventually going to go back into the SC3_scaling_weekly_sample,
     -- but there's some weird Sybase bug at the moment that means that updates don't
     -- work. And then the sessions can't be cancelled, for some bizarre reason.

     COMMIT -- (^_^)
     drop table #boxtype_ac
     COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from #SC3_weird_sybase_update_workaround

     COMMIT -- (^_^)
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Complete! (Variables)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

      /**************** L03: ASSIGN SCALING SEGMENT ID ****************/

     -- The SC3_Segments_lookup table can be used to append a segment_id to
     -- the SC3_scaling_weekly_sample table by matching on universe and each of the
     -- seven scaling variables (hhcomposition, isba_tv_region, package, boxtype, tenure, no_of_stbs, hd_subscription and pvr)

     -- Commented out code is for when we were looking to create a proxy group using adsmartable accounts to mimic those adsmartable
     -- accounts that had not given viewing consent. Code is kept here jsut in case we need to revert back to this method.

--      update #SC3_weird_sybase_update_workaround
--      SET    samp.sky_base_universe  = acc.sky_base_universe
--            ,samp.vespa_universe     = acc.vespa_universe
--      FROM   #SC3_weird_sybase_update_workaround samp
--      INNER JOIN SC3_account_universe acc
--      ON    samp.account_number = acc.account_number

     --Set default sky_base_universe, if, for some reason, it is null
     UPDATE #SC3_weird_sybase_update_workaround
        SET  sky_base_universe = 'Not adsmartable'
                where sky_base_universe is null
        COMMIT -- (^_^)

     UPDATE #SC3_weird_sybase_update_workaround
        SET  vespa_universe = 'Non-Vespa'
                where sky_base_universe is null
        COMMIT -- (^_^)

     UPDATE #SC3_weird_sybase_update_workaround
        SET  weighting_universe = 'Not adsmartable'
                where weighting_universe is null
        COMMIT -- (^_^)

--      UPDATE #SC3_weird_sybase_update_workaround
--         SET  weighting_universe = case
--                 when vespa_universe = 'Vespa but no consent' then 'Adsmartable but no consent'
--                 else sky_base_universe
--                 end

      -- Set default value when account cannot be found
      update #SC3_weird_sybase_update_workaround
         set pvr = case
                when sky_base_universe like 'Adsmartable%' then 'Yes'
                else 'No'
         end
       where pvr is null
      COMMIT -- (^_^)

       -- Update PVR when PVR says 'No' and universe is an adsmartable one.
      update #SC3_weird_sybase_update_workaround
         set pvr = 'Yes'
       where pvr = 'No' and sky_base_universe like 'Adsmartable%'
      COMMIT -- (^_^)

     update #SC3_weird_sybase_update_workaround
             set no_of_stbs =
             case
                when Universe like '%Single%' then 'Single'
                when Universe like '%Multiple%' then 'Multiple'
                else 'Single'
                end
        COMMIT -- (^_^)

     update #SC3_weird_sybase_update_workaround
             set hd_subscription =
             case
                when boxtype like 'B)%' or boxtype like 'E)%' or boxtype like 'F)%' or boxtype like 'G)%' then 'Yes'
                else 'No'
                end

     COMMIT -- (^_^)

     UPDATE #SC3_weird_sybase_update_workaround
        SET #SC3_weird_sybase_update_workaround.population_scaling_segment_ID = ssl.scaling_segment_ID
       FROM #SC3_weird_sybase_update_workaround
             inner join vespa_analysts.SC3_Segments_lookup_v1_1 AS ssl
                                  ON trim(lower(#SC3_weird_sybase_update_workaround.sky_base_universe)) = trim(lower(ssl.sky_base_universe))
                                 AND left(#SC3_weird_sybase_update_workaround.hhcomposition, 2)  = left(ssl.hhcomposition, 2)
                                 AND left(#SC3_weird_sybase_update_workaround.isba_tv_region, 20) = left(ssl.isba_tv_region, 20)
                                 AND #SC3_weird_sybase_update_workaround.Package        = ssl.Package
                                 AND left(#SC3_weird_sybase_update_workaround.tenure, 2)         = left(ssl.tenure, 2)
                                 AND #SC3_weird_sybase_update_workaround.no_of_stbs     = ssl.no_of_stbs
                                 AND #SC3_weird_sybase_update_workaround.hd_subscription = ssl.hd_subscription
                                 AND #SC3_weird_sybase_update_workaround.pvr            = ssl.pvr
        COMMIT -- (^_^)

     UPDATE #SC3_weird_sybase_update_workaround
        SET vespa_scaling_segment_id = population_scaling_segment_ID
        COMMIT -- (^_^)

--      UPDATE #SC3_weird_sybase_update_workaround
--         SET #SC3_weird_sybase_update_workaround.vespa_scaling_segment_id = ssl.scaling_segment_ID
--        FROM #SC3_weird_sybase_update_workaround
--              inner join vespa_analysts.SC3_Segments_lookup_v1_1 AS ssl
--                                   ON trim(lower(#SC3_weird_sybase_update_workaround.weighting_universe)) = trim(lower(ssl.sky_base_universe))
--                                  AND left(#SC3_weird_sybase_update_workaround.hhcomposition, 2)  = left(ssl.hhcomposition, 2)
--                                  AND left(#SC3_weird_sybase_update_workaround.isba_tv_region, 20) = left(ssl.isba_tv_region, 20)
--                                  AND #SC3_weird_sybase_update_workaround.Package        = ssl.Package
--                                  AND left(#SC3_weird_sybase_update_workaround.tenure, 2)         = left(ssl.tenure, 2)
--                                  AND #SC3_weird_sybase_update_workaround.no_of_stbs     = ssl.no_of_stbs
--                                  AND #SC3_weird_sybase_update_workaround.hd_subscription = ssl.hd_subscription
--                                  AND #SC3_weird_sybase_update_workaround.pvr            = ssl.pvr

     COMMIT -- (^_^)

     -- Just checked one manual build, none of these are null, it should all work fine.

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from #SC3_weird_sybase_update_workaround
     where population_scaling_segment_ID is not null

     COMMIT -- (^_^)
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L03a: Midway (Population Segment lookup)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from #SC3_weird_sybase_update_workaround
     where vespa_scaling_segment_id is not null

     COMMIT -- (^_^)
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L03b: Midway (Weighting Segment lookup)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- Okay, no throw all of that back into the weekly sample table, because that's where
     -- the build expects it to be, were it not for that weird bug in Sybase:

     delete from SC3_scaling_weekly_sample
     COMMIT -- (^_^)

     insert into SC3_scaling_weekly_sample
     select *
     from #SC3_weird_sybase_update_workaround

     COMMIT -- (^_^)
     drop table #SC3_weird_sybase_update_workaround

     COMMIT -- (^_^)
     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3_scaling_weekly_sample
     where population_scaling_segment_ID is not null and vespa_scaling_segment_id is not null
     COMMIT -- (^_^)

--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L03: Complete! (Segment ID assignment)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     /**************** L04: PUBLISHING INTO INTERFACE STRUCTURES ****************/

     -- First off we need the accounts and their scaling segmentation IDs: generating
     -- some 10M such records a week, but we'd be able to cull them once we've finished
     -- the associated scaling builds. Only need to maintain it while we still have
     -- historic builds to do.

     insert into SC3_Sky_base_segment_snapshots
     select
         account_number
         ,@profiling_thursday
         ,cb_key_household   -- This guy still needs to be added to SC3_scaling_weekly_sample
         ,population_scaling_segment_id
         ,vespa_scaling_segment_id
         ,mr_boxes+1         -- Number of multiroom boxes plus 1 for the primary
     from SC3_scaling_weekly_sample
     where population_scaling_segment_id is not null and vespa_scaling_segment_id is not null -- still perhaps with the weird account from Eire?

     COMMIT -- (^_^)
     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3_Sky_base_segment_snapshots
     where profiling_date = @profiling_thursday

     COMMIT -- (^_^)
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L04: Complete! (Segments published)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- We were summarising into scaling_segment_id and sky base count, but now we're
     -- doing that later during the actual weights build rather than keeping all the
     -- differrent profile date builds concurrent, we can recover it easily from the
     -- weekly segmentation anyway.

     -- Don't need to separately track the build dates, since they're on the interface
     -- tables and we'll just rely on those. (Gives us no visiblity of overwrites, but
     -- hey, it's okay, those only happen when stuff is being muddled with anyway.)

--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: base segmentation complete!'
     COMMIT -- (^_^)

end; -- of procedure "V289_M11_01_SC3_v1_1__do_weekly_segmentation"
commit;


/**************** PART A: PLACEHOLDER FOR VIRTUAL PANEL BALANCE ****************/

-- This section nominally decides which boxes are considered to be on the panel
-- for each day. There could be a bunch of influences here:
--   * Completeness of returned data in multiroom households
--   * Regularity of returned data for panel stability / box reliability
--   * Virtual panel balance decisions (using the wekly segmentation) - NYIP
-- The output is a table of account numbers and scaling segment IDs. Which is
-- the other reason why it depends on the segmentation build.

create OR REPLACE procedure V289_M11_02_SC3_v1_1__prepare_panel_members
     @profiling_date            date                  -- Thursday to use for scaling
    ,@scaling_day                date                -- Day for which to do scaling
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
as
begin

        -- -- Test input arguments
        -- CREATE OR REPLACE VARIABLE   @profiling_date date    =       '2015-02-05';
    -- CREATE OR REPLACE VARIABLE       @scaling_day    date    =       '2015-02-06';
    -- CREATE OR REPLACE VARIABLE       @batch_date             datetime        =       now();
    -- CREATE OR REPLACE VARIABLE       @Scale_refresh_logging_ID       bigint  =       3874;

        -- DECLARE      @profiling_date date    =       '2015-02-05'
    -- DECLARE  @scaling_day    date    =       '2015-02-06'
    -- DECLARE  @batch_date             datetime = now()
    -- DECLARE  @Scale_refresh_logging_ID       bigint = 3874
        -- COMMIT


        /**************** A00: CLEANING OUT ALL THE OLD STUFF ****************/

     truncate table SC3_todays_panel_members
     COMMIT -- (^_^)

     /**************** A01: ACCOUNTS REPORTING LAST WEEK ****************/

     -- This code block is more jury-rigged in than the others because the structure
     -- has to change a bit to accomodate appropriate modularisation. And it'll all
     -- change again later when Phase 2 stuff gets rolled in. And probably further to
     -- acommodate this overnight batching thing, because we won't have data returned
     -- up to a week in the future.

     --declare @profiling_date             date            -- The relevant Thursday of SAV flip etc
     declare @QA_catcher                 integer         -- For control totals along the way
     -- CREATE OR REPLACE VARIABLE @QA_catcher                 integer;         -- For control totals along the way
         COMMIT -- (^_^)

     -- The weekly profiling is called in a different build, so we'll
     -- just grab the most recent one prior to the date we're scaling
/*     select @profiling_date = max(profiling_date)
     from SC3_Sky_base_segment_snapshots
     where profiling_date <= @scaling_day
*/

        /***************************** A01.1: Initialising extract variables ************************/
                
                
    declare @dp_tname   varchar(50)
        declare @query          varchar(5000)
        declare @from_dt        integer
        declare @to_dt          integer
        COMMIT
        
    -- CREATE OR REPLACE VARIABLE @dp_tname     varchar(50);
        -- CREATE OR REPLACE VARIABLE @query            varchar(5000);
        -- CREATE OR REPLACE VARIABLE @from_dt  integer;
        -- CREATE OR REPLACE VARIABLE @to_dt            integer;

        
        set @dp_tname = 'SK_PROD.VESPA_DP_PROG_VIEWED_'
        COMMIT -- (^_^)
        select  @from_dt        = cast((dateformat(dateadd(hour, 6, @scaling_day),'YYYYMMDD')||'00') as integer)
        COMMIT -- (^_^)
        select  @to_dt          = cast((dateformat(dateadd(hour, 30, @scaling_day),'YYYYMMDD')||'23') as integer)
        COMMIT -- (^_^)

        set @dp_tname = @dp_tname||datepart(year,@scaling_day)||right(('00'||cast(datepart(month,@scaling_day) as varchar(2))),2) 
     COMMIT -- (^_^)
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Deciding panel members for ' || dateformat(@scaling_day,'yyyy-mm-dd') || ' using profiling of ' || dateformat(@profiling_date,'yyyy-mm-dd') || '.'
     COMMIT -- (^_^)

        -- Prepare to catch the week's worth of logs:
        create table #raw_logs_dump_temp(
                        account_number                  varchar(20)             not null
                ,       service_instance_id             varchar(30)             not null
        )
        COMMIT -- (^_^)

     -- In phase two, we don't have to worry about juggling things through the daily tables,
     -- so figuring out what's returned data is a lot easier.
         set @query =   'insert into #raw_logs_dump_temp '||
                                        'select         distinct '||
                                                                'account_number '||
                                                                ',service_instance_id '||
                                        'from '||@dp_tname||
                                        ' where dk_event_start_datehour_dim between '||@from_dt||' and '||@to_dt||
                                        ' and           (panel_id = 12 or panel_id = 11) '||
                                        'and    account_number is not null '||
                                        'and    service_instance_id is not null '
        COMMIT -- (^_^)
                                        
        execute(@query)
     COMMIT -- (^_^)

     create hg index idx1 on #raw_logs_dump_temp (account_number)       COMMIT -- (^_^)
     create hg index idx2 on #raw_logs_dump_temp (service_instance_id)  COMMIT -- (^_^)


     create table #raw_logs_dump (
         account_number          varchar(20)         not null
         ,service_instance_id    varchar(30)         not null
     )
     COMMIT -- (^_^)

        insert into     #raw_logs_dump
        select  distinct
                        account_number
                ,       service_instance_id
        from    #raw_logs_dump_temp
        COMMIT -- (^_^)

     create index some_key on #raw_logs_dump (account_number)
     COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from #raw_logs_dump
     COMMIT -- (^_^)
--     execute logger_add_event @Scale_refresh_logging_ID, 3, 'A01: Midway 1/2 (Log extracts)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

        select
                        account_number
                ,       count(distinct service_instance_id)             as      box_count
                ,       convert(tinyint, null)                                  as      expected_boxes
                ,       convert(int, null)                                              as      scaling_segment_id
        into    #panel_options
        from    #raw_logs_dump
        group by        account_number
        COMMIT -- (^_^)

     create unique index fake_pk on #panel_options (account_number)
         COMMIT -- (^_^)

     drop table #raw_logs_dump
     COMMIT -- (^_^)


     -- Getting this list of accounts isn't enough, we also want to know if all the boxes
     -- of the household have returned data.

        update  #panel_options
        set
                        expected_boxes      =   sbss.expected_boxes
                ,       scaling_segment_id      =       sbss.vespa_scaling_segment_id
        from
                                        #panel_options
                inner join      SC3_Sky_base_segment_snapshots  as      sbss            on      #panel_options.account_number = sbss.account_number
        where   sbss.profiling_date     =       @profiling_date
        COMMIT -- (^_^)


        truncate table SC3_todays_panel_members
        COMMIT -- (^_^)

     -- First moving the unique account numbers in...

        insert into     SC3_todays_panel_members(
                        account_number
                ,       scaling_segment_id
                )
        SELECT
                        account_number
                ,       scaling_segment_id
        FROM    #panel_options
        where
                                expected_boxes >= box_count
                -- Might be more than we expect if NULL service_instance_ID's are distinct against
                -- populated ones (might get fixed later but for now the initial Phase 2 build
                -- doesn't populate them all yet)
                and             scaling_segment_id is not null
        COMMIT -- (^_^)

        -- Clean up
        drop table #panel_options
        COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3_todays_panel_members
     COMMIT -- (^_^)
     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'A01: Complete! (Panel members)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: panel members prepared!'
     COMMIT -- (^_^)

end; -- of procedure "V289_M11_02_SC3_v1_1__prepare_panel_members"
commit;




--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--- Adds individual level data in some of the scaling tables for Skyview before the Rim Weighting is applied


create or replace procedure V289_M11_03_SC3I_v1_1__add_individual_data
    @profiling_thursday                date                -- Day on which to do sky base profiling
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
as
begin

        -- -- Test input arguments
    -- create or replace variable @profiling_thursday   date    =       '2015-02-05';
    -- create or replace variable @batch_date   datetime        =       now();
    -- create or replace variable @Scale_refresh_logging_ID  bigint = 3874;

    -- declare  @profiling_thursday     date    =       '2015-02-05'
    -- declare  @batch_date     datetime        =       now()
    -- declare  @Scale_refresh_logging_ID  bigint = 3874
        -- commit



        declare @QA_catcher                 integer         -- For control totals along the way
        COMMIT -- (^_^)



        -- Clean up data from the current target profiling Thursday
        delete from SC3I_Sky_base_segment_snapshots
        where profiling_date = @profiling_thursday
        COMMIT -- (^_^)


        --- Skybase segments
        -- We can convert the segments from Scaling 3.0 into Skyview scaling segments
        insert into SC3I_Sky_base_segment_snapshots(
                account_number
        ,       profiling_date
        ,       HH_person_number
        ,       population_scaling_segment_id
        ,       vespa_scaling_segment_id
        ,       expected_boxes
                )
        select
                        b.account_number
                ,       b.profiling_date
                ,       d.HH_person_number
                ,       l_sc3i.scaling_segment_id
                ,       l_sc3i.scaling_segment_id
                ,       b.expected_boxes
        from
                                        SC3_Sky_base_segment_snapshots                          as      b
                inner join      V289_M08_SKY_HH_composition                             as      d               on      b.account_number                                                        =       d.account_number
                inner join      vespa_analysts.SC3_Segments_lookup_v1_1         as      l_sc3   on      b.population_scaling_segment_id                         =       l_sc3.scaling_segment_id
                inner join      vespa_analysts.SC3i_Segments_lookup_v1_1        as      l_sc3i  -- on l_sc3.sky_base_universe = l_sc3i.sky_base_universe -- NOT USED IN THIS VERSION
                                                                                                                                                        on      l_sc3.isba_tv_region                                            =       l_sc3i.isba_tv_region
                                                                                                                                                        -- and l_sc3.hhcomposition = l_sc3i.hhcomposition -- NOT USED IN THIS VERSION
                                                                                                                                                        and     l_sc3.package                                                           =       l_sc3i.package
                                                                                                                                                        and     d.person_head                                                           =       l_sc3i.head_of_hhd
                                                                                                                                                        -- and d.person_gender = l_sc3i.gender -- NOT USED IN THIS VERSION
                                                                                                                                                        and     d.person_gender || ' ' || d.person_ageband      =       l_sc3i.age_band -- combine age and gender into a single attribute
                                                                                                                                                        -- and  l_sc3i.viewed_tv                                                        =       'Y' -- most people watch TV and for SKy Base we can't differentiate between the viewers and non-viewers. Will deal with non-viewers later
                                                                                                                                                        and     case
                                                                                                                                                                        when    d.household_size > 7    then    '8+'
                                                                                                                                                                        else                                                                    cast(d.household_size as varchar(2))
                                                                                                                                                                end                                                                                     =       l_sc3i.hh_size
        where   b.profiling_date = @profiling_thursday
        COMMIT -- (^_^)

        /* IN this version we are not exlcuding any segments as this has already been taken care of in the segment definitions

        --- We want to exclude some segments (and therefor accounts within these segments) from scaling to improve effective sample size
        --- This will only effects segments which have low numbers of accounts
        select distinct account_number
        into #t1
        from SC3I_Sky_base_segment_snapshots b inner join vespa_analysts.SC3i_Segments_lookup_v1_1 l on b.population_scaling_segment_id = l.scaling_segment_id
        where (gender = 'U' and age_band <> '0-19') -- exclude U gender except for 0-19 (almost all 0-19 are U)
        --        or hhcomposition = 'D) Unclassified HHComp' -- high numbers of zero vespa segemnts driving lower effective sample size
        --        or l.sky_base_universe = 'Adsmartable but no consent' -- Oct test data has very few Adsmart No Consent on the panel so exclude
        --        or (age_band = '20-24' and hhcomposition = 'B) Singles') -- Small segments
        --        or (age_band = '65+' and hhcomposition = 'C) Homesharers') -- Small segments

        COMMIT -- (^_^)

        create hg index ind1 on #t1(account_number)
        COMMIT -- (^_^)


        -- Delete the excluded accounts
        delete from SC3I_Sky_base_segment_snapshots
        from SC3I_Sky_base_segment_snapshots b inner join #t1 t
        on b.account_number = t.account_number
        COMMIT -- (^_^)

        */

        set @QA_catcher = -1
        COMMIT -- (^_^)

        select @QA_catcher = count(1)
        from SC3I_Sky_base_segment_snapshots
        COMMIT -- (^_^)

        -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'M11_03: Skybase Individuals', coalesce(@QA_catcher, -1)
        COMMIT -- (^_^)




        -- Ensure only accounts and individuals on Vespa extract is used
        select  account_number
        into    #vespa_viewer_accounts
        from    V289_M10_session_individuals -- changed from V289_M07_dp_data so only include accounts that make it through the whole H2I process
        group by        account_number
        COMMIT -- (^_^)

        create hg index hg1 on #vespa_viewer_accounts(account_number)
        COMMIT -- (^_^)


        -- Now get the individuals that've been assigned viewing
        select
                        account_number
                ,       hh_person_number
        into    #vespa_viewer_individuals
        from    V289_M10_session_individuals -- changed from V289_M07_dp_data so only include accounts that make it through the whole H2I process
        group by
                        account_number
                ,       hh_person_number
        COMMIT -- (^_^)

        create hg index hg1 on #vespa_viewer_individuals(account_number)
        COMMIT -- (^_^)
        create hg index hg2 on #vespa_viewer_individuals(hh_person_number)
        COMMIT -- (^_^)


        -- Pick up all individuals in an account on the panel
        select
                        m08.account_number
                ,       m08.hh_person_number
                -- , 'Y' as viewed_tv
                ,       case
                                when    ind.hh_person_number    is not null             then    'Yes'
                                else    'NV - Viewing HHD'
                        end as viewed_tv
                ,       m08.person_head
                ,       m08.person_gender || ' ' || m08.person_ageband          as      age_band
                ,       case
                                when    m08.household_size > 7  then    '8+'
                                else                                                                    cast(m08.household_size as varchar(2))
                        end                                                                                                     as      hhsize_capped

        into    #t3
        from
                V289_M08_SKY_HH_composition             as      m08
                inner join #vespa_viewer_accounts        as      acc on m08.account_number = acc.account_number
                left join #vespa_viewer_individuals     as      ind on      acc.account_number              =       ind.account_number       -- left join back onto post-individual assignment results so that we can identify viewers/non-viewers
                                                                                                                                and     m08.account_number              =       ind.account_number
                                                                                                                                and     m08.hh_person_number    =       ind.hh_person_number
        where
                m08.panel_flag = 1
                and m08.nonviewer_household = 0



        -- Now pick up all individuals in NonViewer HHD accounts (not on the panel)
        insert into #t3
        select
                        m08.account_number
                ,       m08.hh_person_number
                ,       'NV - NonView HHD' as viewed_tv
                ,       m08.person_head
                ,       m08.person_gender || ' ' || m08.person_ageband          as      age_band
                ,       case
                                when    m08.household_size > 7  then    '8+'
                                else                                                                    cast(m08.household_size as varchar(2))
                        end                                                                                                     as      hhsize_capped
        from
                V289_M08_SKY_HH_composition             as      m08
        where
                m08.panel_flag = 0
                and m08.nonviewer_household = 1






        /*      -- We only have unique account-individuals now, so the group-by does nothing here
        group by
                        m08.account_number
                ,       m08.hh_person_number
                ,       viewed_tv
        */
        COMMIT -- (^_^)

        create hg index ind1 on #t3(account_number)
        COMMIT -- (^_^)
        create lf index ind2 on #t3(hh_person_number)
        COMMIT -- (^_^)


        
        -- Get Vespa Viewers (and non-viewers) and attach their new SC3I scaling segments
        truncate table SC3I_Todays_panel_members
        COMMIT -- (^_^)

        /* -- Old version to be deleted...
        insert into SC3I_Todays_panel_members
        select
                        p.account_number
                ,       d.HH_person_number
                ,       l_sc3i.scaling_segment_id
        from
                                        SC3_Todays_panel_members                                        as      p       -- HYT take out and replace with t3 only
                inner join      V289_M08_SKY_HH_composition                                     as      d               on p.account_number                                                             =       d.account_number -- HYT get required individual data in #t3 generation
                inner join      vespa_analysts.SC3_Segments_lookup_v1_1         as      l_sc3   on p.scaling_segment_id                                                 =       l_sc3.scaling_segment_id -- HYT take this out as well
                inner join      vespa_analysts.SC3i_Segments_lookup_v1_1        as      l_sc3i  -- on l_sc3.sky_base_universe                                   =       l_sc3i.sky_base_universe -- NOT USED IN THIS VERSION
                                                                                                                                                        on l_sc3.isba_tv_region                                                 =       l_sc3i.isba_tv_region
                                                                                                                                                        -- and l_sc3.hhcomposition                                              =       l_sc3i.hhcomposition -- NOT USED IN THIS VERSION
                                                                                                                                                        and l_sc3.package                                                               =       l_sc3i.package
                                                                                                                                                        and d.person_head                                                               =       l_sc3i.head_of_hhd
                                                                                                                                                        -- and d.person_gender                                                  =       l_sc3i.gender  -- NOT USED IN THIS VERSION
                                                                                                                                                        and     d.person_gender || ' ' || d.person_ageband      =       l_sc3i.age_band -- combine age and gender into a single attribute
                                                                                                                                                        -- and  l_sc3i.viewed_tv                                                        =       'Y' -- by definition all these guys watched tv .Will deal with non-viewers later
                                                                                                                                                        and     case
                                                                                                                                                                        when    d.household_size > 7    then    '8+'
                                                                                                                                                                        else                                                                    cast(d.household_size as varchar(2))
                                                                                                                                                                end                                                                                     =       l_sc3i.hh_size
                 inner join     #t3                                                                                     as      t               on      p.account_number                                                        =       t.account_number
                                                                                                                                                        and     d.hh_person_number                                                      =       t.hh_person_number
                                                                                                                                                        and     l_sc3i.viewed_tv                                                        =       t.viewed_tv
        COMMIT -- (^_^)
        */


        -- First deal with real panel members
        insert into SC3I_Todays_panel_members
        select
                        t.account_number
                ,       t.HH_person_number
                ,       l_sc3i.scaling_segment_id
        from
                                        #t3                                                                                     as      t
                inner join  SC3_Todays_panel_members                                    as      p               on  t.account_number                                                    =   p.account_number                    -- we need this join to bring in the old scaling segment ID
                inner join      vespa_analysts.SC3_Segments_lookup_v1_1         as      l_sc3   on      p.scaling_segment_id                                            =       l_sc3.scaling_segment_id        -- this join to bring in the old scaling segments
                inner join      vespa_analysts.SC3i_Segments_lookup_v1_1        as      l_sc3i  on      l_sc3.isba_tv_region                                            =       l_sc3i.isba_tv_region           -- finally, match the attributes to give us the new SC3I segment IDs
                                                                                                                                                        and l_sc3.package                                                               =       l_sc3i.package
                                                                                                                                                        and t.person_head                                                               =       l_sc3i.head_of_hhd
                                                                                                                                                        and     t.age_band                                                                      =       l_sc3i.age_band
                                                                                                                                                        and     t.hhsize_capped                                                         =       l_sc3i.hh_size
                                                                                                                                                        and     t.viewed_tv                                                                     =       l_sc3i.viewed_tv
        where   t.viewed_tv in ('Yes', 'NV - Viewing HHD')
        COMMIT -- (^_^)



        -- Now add those selected to be NV HHDS
        insert into SC3I_Todays_panel_members
        select
                        t.account_number
                ,       t.HH_person_number
                ,       p.population_scaling_segment_id as scaling_segment_id
        from
                                        #t3                                                                                     as      t
                inner join  SC3I_Sky_base_segment_snapshots                                    as      p               on  t.account_number                                                    =   p.account_number                    -- we need this join to bring in the old scaling segment ID
                                                                                                                        and t.hh_person_number = p.hh_person_number
                inner join  vespa_analysts.SC3i_Segments_lookup_v1_1 l on p.population_scaling_segment_id = l.scaling_segment_id
         where   t.viewed_tv = 'NV - NonView HHD'
         and l.viewed_tv = 'NV - NonViewing HHD'
         and p.profiling_date = @profiling_thursday
        COMMIT -- (^_^)







        /* IN this version we are not exlcuding any segments as this has already been taken care of in the segment definitions

        --- We want to exclude some segments (and therefor accounts within these segments) from scaling to improve effective sample size
        --- This will only effects segments which have low numbers of accounts
        select distinct account_number
        into #t2
        from SC3I_Todays_panel_members p inner join vespa_analysts.SC3i_Segments_lookup_v1_1 l on p.scaling_segment_id = l.scaling_segment_id
        where (gender = 'U' and age_band <> '0-19') -- exclude U gender except for 0-19 (almost all 0-19 are U)
        --        or hhcomposition = 'D) Unclassified HHComp' -- high numbers of zero vespa segemnts driving lower effective sample size
        --        or l.sky_base_universe = 'Adsmartable but no consent' -- Oct test data has very few Adsmart No Consent on the panel so exclude
        --        or (age_band = '20-24' and hhcomposition = 'B) Singles') -- Small segments
        --        or (age_band = '65+' and hhcomposition = 'C) Homesharers') -- Small segments
        COMMIT -- (^_^)

        create hg index ind1 on #t2(account_number)
        COMMIT -- (^_^)

        -- Delete the excluded accounts
        delete from SC3I_Todays_panel_members
        from SC3I_Todays_panel_members p inner join #t2 t
        on p.account_number = t.account_number
        COMMIT -- (^_^)

        */


        set @QA_catcher = -1
        COMMIT -- (^_^)

        select @QA_catcher = count(1)
        from SC3I_Sky_base_segment_snapshots
        COMMIT -- (^_^)
        -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'M11_03: Panel Individuals', coalesce(@QA_catcher, -1)
        COMMIT -- (^_^)


end; -- of procedure "V289_M11_03_SC3I_v1_1__add_individual_data"
commit;


-----------------------------------------------------------------------------------------------------------------------------------------




-- IF object_id('V289_M11_04_SC3I_v1_1__make_weights') IS NOT NULL THEN DROP PROCEDURE V289_M11_04_SC3I_v1_1__make_weights END IF;

-- create procedure V289_M11_04_SC3I_v1_1__make_weights
    -- @scaling_day                date                -- Day for which to do scaling; this argument is mandatory
    -- ,@batch_date                datetime = now()    -- Day on which build was kicked off
    -- ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
-- as
-- begin


        -- -- Only need these if we can't get to execute as a Proc
-- /*        declare @scaling_day  date
        -- declare @batch_date date
        -- declare @Scale_refresh_logging_ID bigint
        -- set @scaling_day = '2013-09-26'
        -- set @batch_date = '2014-07-10'
        -- set @Scale_refresh_logging_ID = 5
-- */


     -- -- So by this point we're assuming that the Sky base segmentation is done
     -- -- (for a suitably recent item) and also that today's panel members have
     -- -- been established, and we're just going to go calculate these weights.

     -- DECLARE @cntr           INT
     -- DECLARE @iteration      INT
     -- DECLARE @cntr_var       SMALLINT
     -- DECLARE @scaling_var    VARCHAR(30)
     -- DECLARE @scaling_count  SMALLINT
     -- DECLARE @convergence    TINYINT
     -- DECLARE @sky_base       DOUBLE
     -- DECLARE @vespa_panel    DOUBLE
     -- DECLARE @sum_of_weights DOUBLE
     -- declare @profiling_date date
     -- declare @QA_catcher     bigint

     -- commit



     -- /**************** PART B01: GETTING TOTALS FOR EACH SEGMENT ****************/

     -- -- Figure out which profiling info we're using;
     -- select @profiling_date = max(profiling_date)
     -- from SC3I_Sky_base_segment_snapshots
     -- where profiling_date <= @scaling_day

     -- commit

     -- -- Log the profiling date being used for the build
      -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Making weights for ' || dateformat(@scaling_day,'yyyy-mm-dd') || ' using profiling of ' || dateformat(@profiling_date,'yyyy-mm-dd') || '.'
      -- commit

     -- -- First adding in the Sky base numbers
     -- delete from SC3I_weighting_working_table
     -- commit

     -- INSERT INTO SC3I_weighting_working_table (scaling_segment_id, sky_base_accounts)
     -- select population_scaling_segment_id, count(1)
     -- from SC3I_Sky_base_segment_snapshots
     -- where profiling_date = @profiling_date
     -- group by population_scaling_segment_id

     -- commit


-- /**************** update SC3I_weighting_working_table
-- -- Keep the totals for age/gender groups the same but apply Barb %

        -- -- Get SkyBase ageband totals
        -- select
                -- 'age_band' as profile
                -- ,age_band as value
                -- ,count(1) as weighted_population
                -- ,9.999 as percent_of_total_pop
        -- into
                -- #skybase_age_gender_totals
        -- from
                -- SC3I_weighting_working_table w
             -- inner join
                -- vespa_analysts.SC3i_Segments_lookup_v1_1 l
             -- on w.scaling_segment_id = l.scaling_segment_id
        -- group by
                -- profile
                -- ,value
                -- ,weighted_population

        -- -- Get SkyBase ageband totals
        -- insert into #skybase_age_gender_totals
        -- select
                -- 'gender' as profile
                -- ,gender as value
                -- ,count(1) as weighted_population
        -- into
                -- #skybase_age_gender_totals
        -- from
                -- SC3I_weighting_working_table w
             -- inner join
                -- vespa_analysts.SC3i_Segments_lookup_v1_1 l
             -- on w.scaling_segment_id = l.scaling_segment_id
        -- group by
                -- profile
                -- ,value
                -- ,weighted_population

        -- -- Calculate the % of Sky base by age group and by gender group
        -- update #skybase_age_gender_totals sb
        -- set percent_of_total_pop = weighted_population / tot_weighted_population
        -- from
                -- (select profile, sum(weighted_population) as tot_weighted_population
                -- from #skybase_age_gender_totals
                -- group by profile) summary
        -- where sb.profile = summary.profile



-- TABLE #barb_age_gender_weighted_population
        -- profile                 e.g. ageband
        -- value                   e.g 34-45
        -- weighted_population     e.g 12,000,000
        -- percent_of_total_pop    e.g. 0.151


        -- -- Calculate the adjustment to apply to age and gender groups so that they have same Barb profile
        -- select sb.profile, value, (bb.percent_of_total_pop / sb.percent_of_total_pop) as sky_adjust
        -- into #skybase_adjust_for_barb
        -- from
                -- #skybase_age_gender_totals sb
             -- inner join
                -- #barb_age_gender_weighted_population bb
             -- on sb.profile = bb.profile and sb.value = bb.value






        -- update SC3I_weighting_working_table w
        -- set sky_base_accounts =
        -- from

        -- select scaling_segment_id,
        -- from
                -- #skybase_age_gender_totals sb
             -- inner join
                -- #barb_age_gender_weighted_population bb





-- */





     -- -- Now tack on the universe flags; a special case of things coming out of the lookup

     -- update SC3I_weighting_working_table
     -- set sky_base_universe = sl.sky_base_universe
     -- from SC3I_weighting_working_table
-- --      inner join vespa_analysts.SC2_Segments_lookup_v1_1 as sl
     -- inner join vespa_analysts.SC3i_Segments_lookup_v1_1 as sl
     -- on SC3I_weighting_working_table.scaling_segment_id = sl.scaling_segment_id

     -- commit

     -- -- Mix in the Vespa panel counts as determined earlier
     -- select scaling_segment_id
         -- ,count(1) as panel_members
     -- into #segment_distribs
     -- from SC3I_Todays_panel_members
     -- where scaling_segment_id is not null
     -- group by scaling_segment_id

     -- commit
     -- create unique index fake_pk on #segment_distribs (scaling_segment_id)
     -- commit

     -- -- It defaults to 0, so we can just poke values in
     -- update SC3I_weighting_working_table
     -- set vespa_panel = sd.panel_members
     -- from SC3I_weighting_working_table
     -- inner join #segment_distribs as sd
     -- on SC3I_weighting_working_table.scaling_segment_id = sd.scaling_segment_id

     -- -- And we're done! log the progress.
     -- commit
     -- drop table #segment_distribs
     -- commit
     -- set @QA_catcher = -1

     -- select @QA_catcher = count(1)
     -- from SC3I_weighting_working_table

     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B01: Complete! (Segmentation totals)', coalesce(@QA_catcher, -1)
     -- commit






     -- /**************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/

     -- delete from SC3I_category_subtotals where scaling_date = @scaling_day
     -- delete from SC3I_metrics where scaling_date = @scaling_day
     -- commit

     -- -- Rim-weighting is an iterative process that iterates through each of the scaling variables
     -- -- individually until the category sum of weights converge to the population category subtotals

     -- SET @cntr           = 1
     -- SET @iteration      = 0
     -- SET @cntr_var       = 1
-- --      SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr)
     -- SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC3I_Variables_lookup_v1_1 WHERE id = @cntr)
     -- SET @scaling_count  = (SELECT COUNT(scaling_variable) FROM vespa_analysts.SC3I_Variables_lookup_v1_1)

     -- -- The SC3I_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
     -- -- the sky base.
     -- -- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
     -- -- to ensure convergence.

     -- -- arbitrary value to ensure convergence
     -- update SC3I_weighting_working_table
     -- set vespa_panel = 0.000001
     -- where vespa_panel = 0

     -- commit

     -- -- Initialise working columns
     -- update SC3I_weighting_working_table
     -- set sum_of_weights = vespa_panel

     -- commit

     -- -- The iterative part.
     -- -- This works by choosing a particular scaling variable and then summing across the categories
     -- -- of that scaling variable for the sky base, the vespa panel and the sum of weights.
     -- -- A Category weight is calculated by dividing the sky base subtotal by the vespa panel subtotal
     -- -- for that category.
     -- -- This category weight is then applied back to the segments table and the process repeats until
     -- -- the sum_of_weights in the category table converges to the sky base subtotal.

     -- -- Category Convergence is defined as the category sum of weights being +/- 3 away from the sky
     -- -- base category subtotal within 100 iterations.
     -- -- Overall Convergence for that day occurs when each of the categories has converged, or the @convergence variable = 0

     -- -- The @convergence variable represents how many categories did not converge.
     -- -- If the number of iterations = 100 and the @convergence > 0 then this means that the Rim-weighting
     -- -- has not converged for this particular day.
     -- -- In this scenario, the person running the code should send the results of the SC3I_metrics for that
     -- -- week to analytics team for review. ## What exactly are we checking? can we automate any of it?

     -- WHILE @cntr <= @scaling_count
     -- BEGIN
             -- DELETE FROM SC3I_category_working_table

             -- SET @cntr_var = 1
             -- WHILE @cntr_var <= @scaling_count
             -- BEGIN
                         -- SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC3I_Variables_lookup_v1_1 WHERE id = @cntr_var

                         -- EXECUTE('
                         -- INSERT INTO SC3I_category_working_table (sky_base_universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights)
                             -- SELECT  srs.sky_base_universe
                                    -- ,@scaling_var
                                    -- ,ssl.'||@scaling_var||'
                                    -- ,SUM(srs.sky_base_accounts)
                                    -- ,SUM(srs.vespa_panel)
                                    -- ,SUM(srs.sum_of_weights)
                             -- FROM SC3I_weighting_working_table AS srs
                                     -- inner join vespa_analysts.SC3i_Segments_lookup_v1_1 AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
                             -- GROUP BY srs.sky_base_universe,@scaling_var,ssl.'||@scaling_var||'
                             -- ORDER BY srs.sky_base_universe
                         -- ')

                         -- SET @cntr_var = @cntr_var + 1
             -- END

             -- commit

             -- UPDATE SC3I_category_working_table
             -- SET  category_weight = sky_base_accounts / sum_of_weights
                 -- ,convergence_flag = CASE WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 ELSE 1 END

             -- SELECT @convergence = SUM(convergence_flag) FROM SC3I_category_working_table
             -- SET @iteration = @iteration + 1
             -- SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC3I_Variables_lookup_v1_1 WHERE id = @cntr

             -- EXECUTE('
             -- UPDATE SC3I_weighting_working_table
             -- SET  SC3I_weighting_working_table.category_weight = sc.category_weight
                 -- ,SC3I_weighting_working_table.sum_of_weights  = SC3I_weighting_working_table.sum_of_weights * sc.category_weight
             -- FROM SC3I_weighting_working_table
                     -- inner join vespa_analysts.SC3i_Segments_lookup_v1_1 AS ssl ON SC3I_weighting_working_table.scaling_segment_id = ssl.scaling_segment_id
                     -- inner join SC3I_category_working_table AS sc ON sc.value = ssl.'||@scaling_var||'
                                                                      -- AND sc.sky_base_universe = ssl.sky_base_universe
                                                                                                                                          -- AND sc.profile=@scaling_var
             -- ')

             -- commit

             -- IF @iteration = 100 OR @convergence = 0 SET @cntr = (@scaling_count + 1)
             -- ELSE

             -- IF @cntr = @scaling_count  SET @cntr = 1
             -- ELSE
             -- SET @cntr = @cntr+1

     -- END

     -- commit
     -- -- This loop build took about 4 minutes. That's fine.

     -- -- Calculate segment weight and corresponding indices

     -- -- This section calculates the segment weight which is the weight that should be applied to viewing data
     -- -- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting


     -- SELECT @sky_base = SUM(sky_base_accounts) FROM SC3I_weighting_working_table
     -- SELECT @vespa_panel = SUM(vespa_panel) FROM SC3I_weighting_working_table
     -- SELECT @sum_of_weights = SUM(sum_of_weights) FROM SC3I_weighting_working_table

     -- UPDATE SC3I_weighting_working_table
     -- SET  segment_weight = sum_of_weights / vespa_panel
         -- ,indices_actual = 100*(vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
         -- ,indices_weighted = 100*(sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

     -- commit

     -- -- OK, now catch those cases where stuff diverged because segments weren't reperesented:
     -- update SC3I_weighting_working_table
     -- set segment_weight  = 0.000001
     -- where vespa_panel   = 0.000001

     -- commit

     -- set @QA_catcher = -1

     -- select @QA_catcher = count(1)
     -- from SC3I_weighting_working_table
     -- where segment_weight >= 0.001           -- Ignore the placeholders here to guarantee convergence

     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Midway (Iterations)', coalesce(@QA_catcher, -1)
     -- commit

     -- -- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level

     -- INSERT INTO SC3I_category_subtotals (scaling_date,sky_base_universe,profile,value,sky_base_accounts,vespa_panel,category_weight
                                              -- ,sum_of_weights, convergence)
     -- SELECT  @scaling_day
             -- ,sky_base_universe
             -- ,profile
             -- ,value
             -- ,sky_base_accounts
             -- ,vespa_panel
             -- ,category_weight
             -- ,sum_of_weights
             -- ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
     -- FROM SC3I_category_working_table

     -- -- The SC3I_metrics table contains metrics for a particular scaling date. It shows whether the
     -- -- Rim-weighting process converged for that day and the number of iterations. It also shows the
     -- -- maximum and average weight for that day and counts for the sky base and the vespa panel.

     -- commit

     -- -- Apparently it should be reviewed each week, but what are we looking for?

     -- INSERT INTO SC3I_metrics (scaling_date, iterations, convergence, max_weight, av_weight,
                                  -- sum_of_weights, sky_base, vespa_panel, non_scalable_accounts)
     -- SELECT  @scaling_day
            -- ,@iteration
            -- ,@convergence
            -- ,MAX(segment_weight)
            -- ,sum(segment_weight * vespa_panel) / sum(vespa_panel)    -- gives the average weight by account (just uising AVG would give it average by segment id)
            -- ,SUM(segment_weight * vespa_panel)                       -- again need some math because this table has one record per segment id rather than being at acocunt level
            -- ,@sky_base
            -- ,sum(CASE WHEN segment_weight >= 0.001 THEN vespa_panel ELSE NULL END)
            -- ,sum(CASE WHEN segment_weight < 0.001  THEN vespa_panel ELSE NULL END)
     -- FROM SC3I_weighting_working_table

     -- update SC3I_metrics
        -- set sum_of_convergence = abs(sky_base - sum_of_weights)

     -- insert into SC3I_non_convergences(scaling_date,scaling_segment_id, difference)
     -- select @scaling_day
           -- ,scaling_segment_id
           -- ,abs(sum_of_weights - sky_base_accounts)
       -- from SC3I_weighting_working_table
      -- where abs((segment_weight * vespa_panel) - sky_base_accounts) > 3

     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Complete (Calculate weights)', coalesce(@QA_catcher, -1)
     -- commit



     -- /**************** PART B03: PUBLISHING WEIGHTS INTO INTERFACE STRUCTURES ****************/

     -- -- Here is where that bit of interface code goes, including extending the intervals
     -- -- in the Scaling midway tables (which now happens one day ata time). Maybe this guy
     -- -- wants to go into a new and different stored procedure?

     -- -- Heh, this deletion process clears out everything *after* the scaling day, meaning we
     -- -- have to start from the beginning doing this processing... I guess we'll just manage
     -- -- the historical build like this. (This is because we'd otherwise have to manage adding
     -- -- additional records to the interval table when we re-run a day and break an interval
     -- -- that already exists, and that whole process would be annoying to manage.)

     -- -- Except we'll only nuke everything if we *rebuild* a day that's not already there.
     -- if (select count(1) from SC3I_Weightings where scaling_day = @scaling_day) > 0
     -- begin
         -- delete from SC3I_Weightings where scaling_day = @scaling_day

         -- delete from SC3I_Intervals where reporting_starts = @scaling_day

         -- update SC3I_Intervals set reporting_ends = dateadd(day, -1, @scaling_day) where reporting_ends >= @scaling_day
     -- end
     -- commit

     -- -- Part 1: Update the Vespa midway scaling tables. In Vespa Analysts? May as well
     -- -- also keep this in VIQ_prod too.
     -- insert into SC3I_Weightings
     -- select
         -- @scaling_day
         -- ,scaling_segment_id
         -- ,vespa_panel
         -- ,sky_base_accounts
         -- ,segment_weight
         -- ,sum_of_weights
         -- ,indices_actual
         -- ,indices_weighted
         -- ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
     -- from SC3I_weighting_working_table
     -- -- Might have to check that the filter on segment_weight doesn't leave any orphaned
     -- -- accounts about the place...

     -- commit

     -- set @QA_catcher = -1

     -- select @QA_catcher = count(1)
     -- from SC3I_Weightings
     -- where scaling_day = @scaling_day

     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 1/4 (Midway weights)', coalesce(@QA_catcher, -1)
     -- commit

     -- -- First off extend the intervals that are already in the table:

-- /*
     -- update SC3I_Intervals
     -- set reporting_ends = @scaling_day
     -- from SC3I_Intervals
     -- inner join SC3I_Todays_panel_members as tpm
     -- on SC3I_Intervals.account_number         = tpm.account_number
     -- and SC3I_Intervals.scaling_segment_ID    = tpm.scaling_segment_ID
     -- where reporting_ends = @scaling_day - 1

     -- -- Next step is adding in all the new intervals that don't appear
     -- -- as extensions on existing intervals. First off, isolate the
     -- -- intervals that got extended

     -- select account_number
     -- into #included_accounts
     -- from SC3I_Intervals
     -- where reporting_ends = @scaling_day

     -- commit
     -- create unique index fake_pk on #included_accounts (account_number)
     -- commit

     -- -- Now having figured out what already went in, we can throw in the rest:
     -- insert into SC3I_Intervals (
         -- account_number
         -- ,HH_person_number
         -- ,reporting_starts
         -- ,reporting_ends
         -- ,scaling_segment_ID
     -- )
     -- select
         -- tpm.account_number
         -- ,HH_person_number
         -- ,@scaling_day
         -- ,@scaling_day
         -- ,tpm.scaling_segment_ID
     -- from SC3I_Todays_panel_members as tpm
     -- left join #included_accounts as ia
     -- on tpm.account_number = ia.account_number
     -- where ia.account_number is null -- we don't want to add things already in the intervals table


     -- commit
     -- drop table #included_accounts
     -- commit
-- */
     -- set @QA_catcher = -1

     -- select @QA_catcher = count(1)
     -- from SC3I_Intervals where reporting_ends = @scaling_day

     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 2/4 (Midway intervals)', coalesce(@QA_catcher, -1)
     -- commit

     -- -- Part 2: Update the VIQ interface table (which needs the household key thing)
     -- if (select count(1) from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING where scaling_date = @scaling_day) > 0
     -- begin
         -- delete from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING where scaling_date = @scaling_day
     -- end
     -- commit

     -- insert into V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
     -- select
         -- ws.account_number
         -- ,ws.HH_person_number
         -- ,@scaling_day
         -- ,wwt.segment_weight
         -- ,@batch_date
     -- from SC3I_weighting_working_table as wwt
     -- inner join SC3I_Sky_base_segment_snapshots as ws -- need this table to get the cb_key_household items
     -- on wwt.scaling_segment_id = ws.population_scaling_segment_id
     -- inner join SC3I_Todays_panel_members as tpm
     -- on ws.account_number = tpm.account_number       -- Filter for today's panel only
     -- and ws.profiling_date = @profiling_date

     -- commit

     -- set @QA_catcher = -1

     -- select @QA_catcher = count(1)
     -- from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
     -- where scaling_date = @scaling_day

     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 3/4 (VIQ interface)', coalesce(@QA_catcher, -1)
     -- commit

     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Complete! (Publish weights)'
     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Weights made for ' || dateformat(@scaling_day, 'yyyy-mm-dd')
     -- commit

-- end; -- of procedure "V289_M11_04_SC3I_v1_1__make_weights"
-- commit;


/*******************************************************************************************************/


create or replace procedure V289_M11_04_SC3I_v1_1__make_weights_BARB
    @profiling_date             date                -- Thursday profilr date
    ,@scaling_day                date                -- Day for which to do scaling; this argument is mandatory
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
as
begin


        /*
        -- Only need these if we can't get to execute as a Proc
                declare @scaling_day  date
        declare @batch_date date
        declare @Scale_refresh_logging_ID bigint
        set @scaling_day = '2013-09-26'
        set @batch_date = '2014-07-10'
        set @Scale_refresh_logging_ID = 5
                COMMIT
        */
        -- CREATE OR REPLACE VARIABLE @profiling_date             date  =       '2015-02-05';
    -- CREATE OR REPLACE VARIABLE @scaling_day                date =    '2015-02-06';
    -- CREATE OR REPLACE VARIABLE @batch_date                datetime = now();
    -- CREATE OR REPLACE VARIABLE @Scale_refresh_logging_ID  bigint = 4131;

        -- SET @profiling_date  =       '2015-02-05'
    -- SET @scaling_day =       '2015-02-06'
    -- SET      @Scale_refresh_logging_ID       = 4131
        -- COMMIT



     -- So by this point we're assuming that the Sky base segmentation is done
     -- (for a suitably recent item) and also that today's panel members have
     -- been established, and we're just going to go calculate these weights.

     DECLARE @cntr           INT
     DECLARE @iteration      INT
     DECLARE @cntr_var       SMALLINT
     DECLARE @scaling_var    VARCHAR(30)
     DECLARE @scaling_count  SMALLINT
     DECLARE @convergence    TINYINT
     DECLARE @sky_base       DOUBLE
     DECLARE @vespa_panel    DOUBLE
     DECLARE @sum_of_weights DOUBLE
--     declare @profiling_date date
     declare @QA_catcher     bigint
     COMMIT -- (^_^)

     -- CREATE OR REPLACE VARIABLE @cntr           INT;
     -- CREATE OR REPLACE VARIABLE @iteration      INT;
     -- CREATE OR REPLACE VARIABLE @cntr_var       SMALLINT;
     -- CREATE OR REPLACE VARIABLE @scaling_var    VARCHAR(30);
     -- CREATE OR REPLACE VARIABLE @scaling_count  SMALLINT;
     -- CREATE OR REPLACE VARIABLE @convergence    TINYINT;
     -- CREATE OR REPLACE VARIABLE @sky_base       DOUBLE;
     -- CREATE OR REPLACE VARIABLE @vespa_panel    DOUBLE;
     -- CREATE OR REPLACE VARIABLE @sum_of_weights DOUBLE;
-- --     CREATE OR REPLACE VARIABLE @profiling_date date;
     -- CREATE OR REPLACE VARIABLE @QA_catcher     bigint;


     /**************** PART B01: GETTING TOTALS FOR EACH SEGMENT ****************/

     -- Figure out which profiling info we're using;
/*     select @profiling_date = max(profiling_date)
     from SC3I_Sky_base_segment_snapshots
     where profiling_date <= @scaling_day

     COMMIT -- (^_^)
*/
     -- Log the profiling date being used for the build
      -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Making weights for ' || dateformat(@scaling_day,'yyyy-mm-dd') || ' using profiling of ' || dateformat(@profiling_date,'yyyy-mm-dd') || '.'
      COMMIT -- (^_^)

        -- First adding in the Sky base numbers
        truncate table SC3I_weighting_working_table
        COMMIT -- (^_^)

        INSERT INTO     SC3I_weighting_working_table(
                        scaling_segment_id
                ,       sky_base_accounts
        )
        select
                        population_scaling_segment_id
                ,       count(1)
        from    SC3I_Sky_base_segment_snapshots
        where   profiling_date  =       @profiling_date
        group by        population_scaling_segment_id
        COMMIT -- (^_^)

                -- Rebase the Sky Base so that it matches EDM processes
        -- The easiest way to do this is to use the total wieghts derived in VIQ_VIEWING_DATA_SCALING

        -- Get Skybase size according to this process
        select  count(distinct account_number) as base_total
        into    #base
        from    SC3I_Sky_base_segment_snapshots
        where   profiling_date  =       @profiling_date

        -- Get Skybase size according to EDM
        select max(adjusted_event_start_date_vespa) as edm_latest_scaling_date
        into   #latest_date
        from   sk_prod.VIQ_VIEWING_DATA_SCALING
        COMMIT -- (^_^)

        select case when @scaling_day <= edm_latest_scaling_date then @scaling_day else edm_latest_scaling_date end as sky_base_universe_date
        into #use_date
        from #latest_date
        COMMIT -- (^_^)

        select  sum(calculated_scaling_weight) as edm_total
        into    #viq_scaling
        from    sk_prod.VIQ_VIEWING_DATA_SCALING
        cross join #use_date
        where   adjusted_event_start_date_vespa = sky_base_universe_date
        COMMIT -- (^_^)


        update SC3I_weighting_working_table    as      w
        set             w.sky_base_accounts     = w.sky_base_accounts * v.edm_total / b.base_total
        from            #base b, #viq_scaling v
        COMMIT -- (^_^)

        /**************** update SC3I_weighting_working_table
        -- Re-scale Sky base to Barb age/gender totals
        -- Will only rescale to barb households that have any viewing data for the day being scaled
        -- and NOT the barb base
        */

        -- Get individuals from Barb who have viewed tv for the processing day
        select
                        household_number
                ,       person_number
        into    #barb_viewers
        from    skybarb_fullview
        where   date(start_time_of_session)     =       @scaling_day
        group by
                        household_number
                ,       person_number
        COMMIT -- (^_^)

        create hg index ind1 on #barb_viewers(household_number) COMMIT -- (^_^)
        create lf index ind2 on #barb_viewers(person_number) COMMIT -- (^_^)



        -- Now reduce that to the corresponding households that've registered some viewing
        select  household_number
        into    #barb_hhd_viewers
        from    #barb_viewers
        group by        household_number
        COMMIT -- (^_^)

        create hg index ind1 on #barb_hhd_viewers(household_number)
        COMMIT -- (^_^)



        -- Get details on individuals from Sky households in BARB (no date-dependency here)
        select
                        h.house_id                              as      household_number
                ,       h.person                                as      person_number
                ,       h.age
                ,       case
                                when age <= 19                  then 'U'
                                when h.sex = 'Male'     then 'M'
                                when h.sex = 'Female'   then 'F'
                        end                                             as      gender
                ,       case
                                when age <= 11                          then '0-11'
                                WHEN age BETWEEN 12 AND 19      then '12-19'
                                WHEN age BETWEEN 20 AND 24      then '20-24'
                                WHEN age BETWEEN 25 AND 34      then '25-34'
                                WHEN age BETWEEN 35 AND 44      then '35-44'
                                WHEN age BETWEEN 45 AND 64      then '45-64'
                                WHEN age >= 65                          then '65+'
                        end                                             as      ageband
                ,       cast(h.head as char(1)) as      head_of_hhd
                ,       w.processing_weight             as      processing_weight
        into    #barb_inds_with_sky
        from
                                        skybarb                 as      h
                inner join      barb_weights    as      w       on      h.house_id      =       w.household_number
                                                                                        and     h.person        =       w.person_number
        COMMIT -- (^_^)





        -------------- Summarise Barb Data

        -- Define the default matrix of ageband, hhsize (and now viewed_tv) to deal with empty Barb segments
        select
                        hh_size
                ,       age_band
                ,       viewed_tv
                ,       head_of_hhd
                ,       1.0                     as      default_weight
        into    #default_m
        from
                                        (
                                                select  distinct hh_size
                                                from    vespa_analysts.SC3i_Segments_lookup_v1_1
                                        )       as      a
                cross join      (
                                                select  distinct age_band
                                                from    vespa_analysts.SC3i_Segments_lookup_v1_1
                                        )       as      b
                cross join      (
                                                select  distinct viewed_tv
                                                from    vespa_analysts.SC3i_Segments_lookup_v1_1
                                        )       as      c
                cross join      (
                                                select  distinct head_of_hhd
                                                from    vespa_analysts.SC3i_Segments_lookup_v1_1
                                        )       as      d
        COMMIT -- (^_^)



        -- Add BARB individual weights from the available segments, aggregated by gender-age/viewed-tv/hhsize attributes
        truncate table V289_M11_04_Barb_weighted_population
        COMMIT -- (^_^)

        insert into V289_M11_04_Barb_weighted_population(
                        ageband
                -- ,    gender
                ,       viewed_tv
                ,       head_of_hhd
                ,       hh_size
                ,       barb_weight
                )
        select
                        (
                                case
                                        when    ageband = '0-19'        then    'U'
                                        else                                                            gender
                                end
                        )       || ' ' || ageband                                                               as      gender_ageband -- now adapted to be a gender-age attribute
                -- ,    'A'                                                                                             as      gender  -- simply a dummy variable now
                -- ,    'Y' as viewed_tv
                ,       case
                                when v.household_number is not null         then    'Yes'
                                when v_hhd.household_number is not null     then    'NV - Viewing HHD'
                                else                                                'NV - NonViewing HHD'
                        end   as      viewed_tv       -- Derive viewer/non-viewer flag
                ,       i.head_of_hhd -- this has also been fixed                       as      head_of_hhd     -- '9'
                ,       z.hh_gr                                                                                         as      hh_size
                ,       sum(processing_weight)                                                          as      barb_weight
        from
                                        #barb_inds_with_sky     as      i       -- Sky individuals in BARB
                /*
                inner join      #barb_hhd_viewers       as      hv      -- Filter for households that have reported viewing     -- HYT don't want this inner join as we want non-viewers as well
                                                                                                on      i.household_number      =       hv.household_number
                */
                inner join      (       -- Cap household sizes
                                                select
                                                                household_number
                                                        ,       case
                                                                        when hhsize < 8         then    cast(hhsize as varchar(2))
                                                                        else                                            '8+'
                                                                end             as hh_gr
                                                from    (
                                                                        select  -- First calculate the household sizes
                                                                                        household_number
                                                                                ,       count(1) as hhsize
                                                                        from    barb_weights
                                                                        group by        household_number
                                                                )       as      w
                                        )                                       as      z       on      i.household_number      =       z.household_number
                left join       #barb_viewers           as      v       on      i.household_number      =       v.household_number
                                                                                                and     i.person_number         =       v.person_number
                left join       #barb_hhd_viewers v_hhd                 on      i.household_number      =       v_hhd.household_number
        group by
                        gender_ageband
                -- ,    gender
                ,       viewed_tv
                ,       head_of_hhd
                ,       hh_size
        COMMIT -- (^_^)

        -- Clean up
        drop table #barb_inds_with_sky
        COMMIT -- (^_^)

        -- Set the dummy variable here
        update V289_M11_04_Barb_weighted_population
        set gender = 'A'
        COMMIT -- (^_^)


        -- Now add default weights for any segments that were missing in the above
        insert into     V289_M11_04_Barb_weighted_population(
                        ageband
                ,       gender
                ,       viewed_tv
                ,       head_of_hhd
                ,       hh_size
                ,       barb_weight
                )
        select
                        m.age_band
                ,       'A'
                -- ,    'Y'
                ,       m.viewed_tv
                ,       m.head_of_hhd   --      '9'
                ,       m.hh_size
                ,       default_weight
        from
                                        #default_m                                                              as      m
                left join       V289_M11_04_Barb_weighted_population    as      b       on      m.hh_size               =       b.hh_size
                                                                                                                                        and     m.age_band              =       b.ageband
                                                                                                                                        and     m.viewed_tv             =       b.viewed_tv
                                                                                                                                        and     m.head_of_hhd   =       b.head_of_hhd
        where   b.hh_size is null
        COMMIT -- (^_^)


        /* -- Some checks...
        select top 20 * from V289_M11_04_Barb_weighted_population;
        ageband gender  viewed_tv       head_of_hhd     hh_size barb_weight
        A 25-34 A       Y       9       4       486293.5
        A 45-64 A       Y       9       4       1161471.4
        U 0-19  A       Y       9       4       1440588.4
        A 45-64 A       Y       9       2       1843330.6
        A 45-64 A       Y       9       1       633042.6

        select sum(barb_weight) from V289_M11_04_Barb_weighted_population; -- 23,356,984

        select
                        viewed_tv
                ,   sum(barb_weight)    as  SOW
        from V289_M11_04_Barb_weighted_population
        group by viewed_tv
        ;
        viewed_tv       SOW
        N       4,538,490.3
        Y       18,818,493.7

        */



        ----

/**************** NOT NEEDED IF WE ARE SCALING TO BARB NUMBERS ******************

        --- Calculate the percentage of viewers and non-viewers by Barb segments
        select           a.ageband
                        ,a.head_of_hhd
                        ,a.hh_size
                        ,a.viewed_tv
                        ,a.v_weight / b.tot_barb as viewed_pc
        into            #viewed_pc
        from
                        (select ageband, head_of_hhd, hh_size, viewed_tv, sum(barb_weight) as v_weight
                         from V289_M11_04_Barb_weighted_population
                         group by ageband, head_of_hhd, hh_size, viewed_tv) a
        inner join
                        (select ageband, head_of_hhd, hh_size, sum(barb_weight) as tot_barb
                         from V289_M11_04_Barb_weighted_population
                         group by ageband, head_of_hhd, hh_size) b on a.ageband = b.ageband and a.head_of_hhd = b.head_of_hhd and a.hh_size = b.hh_size



        -- All Skybase has been set to tv viewers
        -- This will rescale them to Barb viewers by age gender group

        update  SC3I_weighting_working_table    as      w
        set             w.sky_base_accounts     =       w.sky_base_accounts * p.viewed_pc
        from            vespa_analysts.SC3i_Segments_lookup_v1_1        as      l
        cross join      #viewed_pc p
        where           w.scaling_segment_id    =       l.scaling_segment_id
        and             l.age_band              =       p.ageband
        and             l.head_of_hhd           =       p.head_of_hhd
        and             l.hh_size               =       p.hh_size
        and             l.viewed_tv             =       p.viewed_tv


        COMMIT -- (^_^)

*/


        -- Calculate SKy Base Total by Barb Segment
        select          l.age_band
                        ,l.head_of_hhd
                        ,l.hh_size
                        ,l.viewed_tv
                        ,sum(w.sky_base_accounts) as tot_base_accounts
        into            #base_totals
        from            SC3I_weighting_working_table w
        inner join      vespa_analysts.SC3i_Segments_lookup_v1_1 l
                                on w.scaling_segment_id    =       l.scaling_segment_id
        group by        l.age_band
                        ,l.head_of_hhd
                        ,l.hh_size
                        ,l.viewed_tv
        commit -- (^_^)


        update  SC3I_weighting_working_table    as      w
        set             w.sky_base_accounts     =       w.sky_base_accounts * barb_weight / cast(b.tot_base_accounts as float)
        from            vespa_analysts.SC3i_Segments_lookup_v1_1        as      l
        cross join      #base_totals b
        cross join      V289_M11_04_Barb_weighted_population p
        where           w.scaling_segment_id    =       l.scaling_segment_id
        and             l.age_band              =       b.age_band
        and             l.head_of_hhd           =       b.head_of_hhd
        and             l.hh_size               =       b.hh_size
        and             l.viewed_tv             =       b.viewed_tv
        and             l.age_band              =       p.ageband
        and             l.head_of_hhd           =       p.head_of_hhd
        and             l.hh_size               =       p.hh_size
        and             l.viewed_tv             =       p.viewed_tv

        commit -- (^_^)




        /***********************************************/


     -- Now tack on the universe flags; a special case of things coming out of the lookup

     update     SC3I_weighting_working_table
     set        sky_base_universe = sl.sky_base_universe
     from
                                        SC3I_weighting_working_table
                 -- inner join vespa_analysts.SC2_Segments_lookup_v1_1 as sl
                 inner join vespa_analysts.SC3i_Segments_lookup_v1_1    as      sl              on      SC3I_weighting_working_table.scaling_segment_id =       sl.scaling_segment_id
     COMMIT -- (^_^)

     -- Mix in the Vespa panel counts as determined earlier
     select
                        scaling_segment_id
                ,       count(1) as panel_members
     into       #segment_distribs
     from       SC3I_Todays_panel_members
     where      scaling_segment_id is not null
     group by   scaling_segment_id

     COMMIT -- (^_^)
     create unique index fake_pk on #segment_distribs (scaling_segment_id)
     COMMIT -- (^_^)

     -- It defaults to 0, so we can just poke values in
     update     SC3I_weighting_working_table
     set        vespa_panel = sd.panel_members
     from
                                        SC3I_weighting_working_table
                inner join      #segment_distribs       as      sd              on      SC3I_weighting_working_table.scaling_segment_id =       sd.scaling_segment_id

     -- And we're done! log the progress.
     COMMIT -- (^_^)
     drop table #segment_distribs
     COMMIT -- (^_^)
     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3I_weighting_working_table
     COMMIT -- (^_^)

     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B01: Complete! (Segmentation totals)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)






     /**************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/

     delete from SC3I_category_subtotals 
         where scaling_date = @scaling_day
         COMMIT -- (^_^)
         
     delete from SC3I_metrics
         where scaling_date = @scaling_day
     COMMIT -- (^_^)

     -- Rim-weighting is an iterative process that iterates through each of the scaling variables
     -- individually until the category sum of weights converge to the population category subtotals

     SET @cntr                  = 1     COMMIT -- (^_^)
     SET @iteration             = 0     COMMIT -- (^_^)
     SET @cntr_var              = 1     COMMIT -- (^_^)
--      SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr)  COMMIT -- (^_^)

        SET @scaling_var        =       (
                                                                SELECT  scaling_variable
                                                                FROM    vespa_analysts.SC3I_Variables_lookup_v1_1
                                                                WHERE   id = @cntr
                                                        )
        COMMIT -- (^_^)
        
        SET @scaling_count      =       (
                                                                SELECT  COUNT(scaling_variable)
                                                                FROM    vespa_analysts.SC3I_Variables_lookup_v1_1
                                                        )
        COMMIT -- (^_^)

        
        
     -- The SC3I_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
     -- the sky base.
     -- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
     -- to ensure convergence.

     -- arbitrary value to ensure convergence
     update     SC3I_weighting_working_table
     set        vespa_panel = 0.000001
     where      vespa_panel = 0
     COMMIT -- (^_^)

     -- Initialise working columns
     update     SC3I_weighting_working_table
     set        sum_of_weights = vespa_panel
     COMMIT -- (^_^)

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
     -- In this scenario, the person running the code should send the results of the SC3I_metrics for that
     -- week to analytics team for review. ## What exactly are we checking? can we automate any of it?

        WHILE @cntr <= @scaling_count
        BEGIN
        
                TRUNCATE TABLE SC3I_category_working_table
                COMMIT

                SET @cntr_var = 1
                COMMIT
                
                WHILE @cntr_var <= @scaling_count
                        BEGIN
                        
                                SELECT  @scaling_var = scaling_variable
                                FROM    vespa_analysts.SC3I_Variables_lookup_v1_1
                                WHERE   id = @cntr_var
                                COMMIT

                                EXECUTE('
                                        INSERT INTO SC3I_category_working_table(
                                                        sky_base_universe
                                                ,       profile
                                                ,       value
                                                ,       sky_base_accounts
                                                ,       vespa_panel
                                                ,       sum_of_weights
                                                )
                                        SELECT 
                                                        srs.sky_base_universe
                                                ,       @scaling_var
                                                ,       ssl.'||@scaling_var||'
                                                ,       SUM(srs.sky_base_accounts)
                                                ,       SUM(srs.vespa_panel)
                                                ,       SUM(srs.sum_of_weights)
                                        FROM
                                                                        SC3I_weighting_working_table AS srs
                                                inner join      vespa_analysts.SC3i_Segments_lookup_v1_1        AS      ssl             ON      srs.scaling_segment_id  =       ssl.scaling_segment_id
                                        GROUP BY
                                                        srs.sky_base_universe
                                                ,       @scaling_var
                                                ,       ssl.'||@scaling_var||'
                                        ORDER BY
                                                        srs.sky_base_universe
                                        ')
                                COMMIT
                                
                                SET     @cntr_var       =       @cntr_var + 1
                                COMMIT
                        END -- while @cntr_var <= @scaling_count
                COMMIT

                UPDATE  SC3I_category_working_table
                SET
                                category_weight         =       sky_base_accounts / sum_of_weights
                        ,       convergence_flag        =       CASE
                                                                                        WHEN abs(sky_base_accounts - sum_of_weights) < 3        THEN    0
                                                                                        ELSE                                                                                                            1
                                                                                END
                COMMIT

                SELECT  @convergence = SUM(convergence_flag)
                FROM    SC3I_category_working_table
                COMMIT
                
                SET @iteration = @iteration + 1
                COMMIT
                
                SELECT  @scaling_var = scaling_variable
                FROM    vespa_analysts.SC3I_Variables_lookup_v1_1
                WHERE   id = @cntr
                COMMIT

                EXECUTE('
                        UPDATE  SC3I_weighting_working_table
                        SET
                                        SC3I_weighting_working_table.category_weight    =       sc.category_weight
                                ,       SC3I_weighting_working_table.sum_of_weights             =       SC3I_weighting_working_table.sum_of_weights * sc.category_weight
                        FROM
                                                        SC3I_weighting_working_table
                                inner join      vespa_analysts.SC3i_Segments_lookup_v1_1        AS      ssl             ON      SC3I_weighting_working_table.scaling_segment_id =       ssl.scaling_segment_id
                                inner join      SC3I_category_working_table                                     AS      sc              ON      sc.value                                =       ssl.'||@scaling_var||'
                                                                                                                                                                        AND     sc.sky_base_universe    =       ssl.sky_base_universe
                                                                                                                                                                        AND     sc.profile                              =       @scaling_var
                        ')

                COMMIT

                IF      (@iteration = 100 OR @convergence = 0)
                        SET @cntr = @scaling_count + 1
                ELSE
                        IF      @cntr   =       @scaling_count
                                SET     @cntr   =       1
                        ELSE
                                SET     @cntr   =       @cntr + 1
                COMMIT

        END
        COMMIT -- (^_^)

        -- This loop build took about 4 minutes. That's fine. 
        -- HYT 2.5 mins

     -- Calculate segment weight and corresponding indices

     -- This section calculates the segment weight which is the weight that should be applied to viewing data
     -- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting


        SELECT
                        @sky_base = SUM(sky_base_accounts)
                ,       @vespa_panel = SUM(vespa_panel)
                ,       @sum_of_weights = SUM(sum_of_weights)
        FROM SC3I_weighting_working_table
        COMMIT -- (^_^)

        UPDATE  SC3I_weighting_working_table
        SET
                        segment_weight          =       sum_of_weights / vespa_panel
                ,       indices_actual          =       100*(vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
                ,       indices_weighted        =       100*(sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)
        COMMIT -- (^_^)

     -- OK, now catch those cases where stuff diverged because segments weren't reperesented:
     update SC3I_weighting_working_table
     set segment_weight  = 0.000001
     where vespa_panel   = 0.000001
     COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3I_weighting_working_table
     where segment_weight >= 0.001           -- Ignore the placeholders here to guarantee convergence
     COMMIT -- (^_^)
     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Midway (Iterations)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level

        INSERT INTO     SC3I_category_subtotals(
                        scaling_date
                ,       sky_base_universe
                ,       profile
                ,       value
                ,       sky_base_accounts
                ,       vespa_panel
                ,       category_weight
                ,       sum_of_weights
                ,       convergence
                )
        SELECT
                        @scaling_day
                ,       sky_base_universe
                ,       profile
                ,       value
                ,       sky_base_accounts
                ,       vespa_panel
                ,       category_weight
                ,       sum_of_weights
                ,       case
                                when abs(sky_base_accounts - sum_of_weights) > 3        then    1
                                else                                                                                                            0
                        end
        FROM    SC3I_category_working_table
        COMMIT -- (^_^)
         
     -- The SC3I_metrics table contains metrics for a particular scaling date. It shows whether the
     -- Rim-weighting process converged for that day and the number of iterations. It also shows the
     -- maximum and average weight for that day and counts for the sky base and the vespa panel.

     COMMIT -- (^_^)

     -- Apparently it should be reviewed each week, but what are we looking for?

     INSERT INTO        SC3I_metrics(
                        scaling_date
                ,       iterations
                ,       convergence
                ,       max_weight
                ,       av_weight
                ,       sum_of_weights
                ,       sky_base
                ,       vespa_panel
                ,       non_scalable_accounts
                )
     SELECT
                        @scaling_day
                ,       @iteration
                ,       @convergence
                ,       MAX(segment_weight)
                ,       sum(segment_weight * vespa_panel) / sum(vespa_panel)    -- gives the average weight by account (just uising AVG would give it average by segment id)
                ,       SUM(segment_weight * vespa_panel)                       -- again need some math because this table has one record per segment id rather than being at acocunt level
                ,       @sky_base
                ,       sum     (
                                        CASE
                                                WHEN segment_weight >= 0.001    THEN    vespa_panel
                                                ELSE                                                                    NULL
                                        END
                                )
                ,       sum     (
                                        CASE
                                                WHEN segment_weight < 0.001             THEN    vespa_panel
                                                ELSE                                                                    NULL
                                        END
                                )
     FROM       SC3I_weighting_working_table
         COMMIT -- (^_^)

        update  SC3I_metrics
        set     sum_of_convergence = abs(sky_base - sum_of_weights)
        COMMIT -- (^_^)

        insert into     SC3I_non_convergences(
                        scaling_date
                ,       scaling_segment_id
                ,       difference
                )
        select
                        @scaling_day
                ,       scaling_segment_id
                ,       abs(sum_of_weights - sky_base_accounts)
        from    SC3I_weighting_working_table
        where abs((segment_weight * vespa_panel) - sky_base_accounts) > 3
        COMMIT -- (^_^)
        
        -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Complete (Calculate weights)', coalesce(@QA_catcher, -1)
        COMMIT -- (^_^)



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
     if (select count(1) from SC3I_Weightings where scaling_day = @scaling_day) > 0
     begin
         delete from SC3I_Weightings where scaling_day = @scaling_day

         delete from SC3I_Intervals where reporting_starts = @scaling_day

         update SC3I_Intervals set reporting_ends = dateadd(day, -1, @scaling_day) where reporting_ends >= @scaling_day
     end
     COMMIT -- (^_^)

        -- Part 1: Update the Vespa midway scaling tables. In Vespa Analysts? May as well
        -- also keep this in VIQ_prod too.
        insert into SC3I_Weightings
        select
                        @scaling_day
                ,       scaling_segment_id
                ,       vespa_panel
                ,       sky_base_accounts
                ,       segment_weight
                ,       sum_of_weights
                ,       indices_actual
                ,       indices_weighted
                ,       case
                                when abs(sky_base_accounts - sum_of_weights) > 3        then    1
                                else                                                                                                            0
                        end
        from    SC3I_weighting_working_table
        COMMIT -- (^_^)
     -- Might have to check that the filter on segment_weight doesn't leave any orphaned
     -- accounts about the place...


     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3I_Weightings
     where scaling_day = @scaling_day
     COMMIT -- (^_^)

     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 1/4 (Midway weights)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- First off extend the intervals that are already in the table:
/*
     update SC3I_Intervals
     set reporting_ends = @scaling_day
     from SC3I_Intervals
     inner join SC3I_Todays_panel_members as tpm
     on SC3I_Intervals.account_number         = tpm.account_number
     and SC3I_Intervals.scaling_segment_ID    = tpm.scaling_segment_ID
     where reporting_ends = @scaling_day - 1

     -- Next step is adding in all the new intervals that don't appear
     -- as extensions on existing intervals. First off, isolate the
     -- intervals that got extended

     select account_number
     into #included_accounts
     from SC3I_Intervals
     where reporting_ends = @scaling_day

     COMMIT -- (^_^)
     create unique index fake_pk on #included_accounts (account_number)
     COMMIT -- (^_^)

     -- Now having figured out what already went in, we can throw in the rest:
     insert into SC3I_Intervals (
         account_number
         ,HH_person_number
         ,reporting_starts
         ,reporting_ends
         ,scaling_segment_ID
     )
     select
         tpm.account_number
         ,HH_person_number
         ,@scaling_day
         ,@scaling_day
         ,tpm.scaling_segment_ID
     from SC3I_Todays_panel_members as tpm
     left join #included_accounts as ia
     on tpm.account_number = ia.account_number
     where ia.account_number is null -- we don't want to add things already in the intervals table


     COMMIT -- (^_^)
     drop table #included_accounts
     COMMIT -- (^_^)
*/
     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3I_Intervals where reporting_ends = @scaling_day

     COMMIT -- (^_^)
     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 2/4 (Midway intervals)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- Part 2: Update the VIQ interface table (which needs the household key thing)
        truncate table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
        COMMIT -- (^_^)


/*
    insert into V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
        select
                        ws.account_number
                ,       ws.HH_person_number
                ,       @scaling_day
                ,       wwt.segment_weight
                ,       @batch_date
        from
                                SC3I_weighting_working_table    as      wwt
        inner join      SC3I_Sky_base_segment_snapshots as      ws -- need this table to get the cb_key_household items -- this currently introduces duplicates
                                                                                                                        on      wwt.scaling_segment_id  =       ws.population_scaling_segment_id
        inner join      SC3I_Todays_panel_members               as      tpm             on      ws.account_number               =       tpm.account_number       -- Filter for today's panel only
                                                                                                                        and     ws.hh_person_number             =       tpm.hh_person_number
                                                                                                                        and     ws.profiling_date               =       @profiling_date
        COMMIT -- (^_^)
*/
    insert into V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING(
                        account_number
                ,       HH_person_number
                ,       scaling_date
                ,       scaling_weighting
                ,       build_date
                )
        select
                        tpm.account_number
                ,       tpm.HH_person_number
                ,       @scaling_day
                ,       wwt.segment_weight
                ,       @batch_date
        from
                                        SC3I_Todays_panel_members       as  tpm
                inner join      SC3I_weighting_working_table    as      wwt             on      tpm.scaling_segment_id  =       wwt.scaling_segment_id
        COMMIT -- (^_^)

        
     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
     where scaling_date = @scaling_day

     COMMIT -- (^_^)
     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 3/4 (VIQ interface)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Complete! (Publish weights)'
     COMMIT -- (^_^)
     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Weights made for ' || dateformat(@scaling_day, 'yyyy-mm-dd')
     COMMIT -- (^_^)

end; -- of procedure "V289_M11_04_SC3I_v1_1__make_weights_BARB"
commit;



