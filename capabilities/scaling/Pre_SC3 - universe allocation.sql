/******************************************************************************
**
** Project Vespa: Pre Scaling - universe allocation procedure
**
** This procedure takes all of the active accounts and allocates them into a Sky
** base universe, and then, if appropriate, a Vespa universe. This procedure
** should be used only once, or when accounts need to have their universes reallocated.
**
**      Part A:       universe allocation
**
**              A00 - Create table
**              A01 - Get all active accounts
**              A02 - Universe allocation
**
**

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
**
******************************************************************************/

/**************** PART A: UNIVERSE ALLOCATION *******************/
IF object_id('SC3_v1_1__do_universe_allocation') IS NOT NULL THEN DROP PROCEDURE SC3_v1_1__do_universe_allocation END IF;

create procedure SC3_v1_1__do_universe_allocation
     @profiling_thursday        date = null        -- Day on which to do sky base profiling
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
as
begin

     declare @QA_catcher                 integer         -- For control totals along the way


/**************** PART A00 - CREATE TABLE *******************/
     IF object_id('SC3_account_universe') IS NOT NULL DROP TABLE SC3_account_universe
     create table SC3_account_universe (
         account_number             varchar(20)     not null
        ,scaling_segment_ID         int             default 0       -- links to the segments lookup table
        ,adsmart_capable            tinyint         default 0       -- Does the account have an adsmart capable box; 1 for yes, 0 for no
        ,adsmartable                tinyint         default 0       -- Does the box have an adsmartable box; 1 for yes, 0 for no. I.E. Is it adsmart_capable and do we have viewing consent
        ,cust_viewing_data_capture_allowed    varchar(5)      default 'N'     -- Is viewing captur allowed; Yes or No.
        ,sky_base_universe          varchar(30)     default null    -- Which sky base universe do they belong to; Not Adsmartable, Adsmartable but no viewing consent, Adsmartable with viewing consent
        ,vespa_universe             varchar(30)     default null    -- If appropriate, which vespa universe should the account be allocated to; Not Adsmartable, Vespa but no viewing consent, Vespa adsmartable
        ,random_number              double                          -- Used to decide how to split the adsmartable accounts into the appropriate vespa universe.
        ,isba_tv_region             varchar(30)    DEFAULT 'Not Defined'
        ,hhcomposition              varchar(30)    DEFAULT 'D) Unclassified HHComp'
        ,tenure                     varchar(30)    DEFAULT 'D) Unknown'
        ,package                    varchar(30)    DEFAULT 'Basic'
        ,no_of_stbs                 varchar(30)    DEFAULT 'Single'
        ,hd_subscription            varchar(30)    DEFAULT 'No'
        ,pvr                        varchar(30)    DEFAULT 'No'

     )

create hg index acc_idx1 on SC3_account_universe(account_number)
create hg index ran_idx2 on SC3_account_universe(random_number)
create hg index ran_idx3 on SC3_account_universe(scaling_segment_ID)
create hg index ran_idx4 on SC3_account_universe(sky_base_universe)
create hg index ran_idx5 on SC3_account_universe(isba_tv_region)
create hg index ran_idx6 on SC3_account_universe(hhcomposition)
create hg index ran_idx7 on SC3_account_universe(tenure)
create hg index ran_idx8 on SC3_account_universe(package)
create hg index ran_idx9 on SC3_account_universe(no_of_stbs)
create hg index ran_idx10 on SC3_account_universe(hd_subscription)
create hg index ran_idx11 on SC3_account_universe(pvr)

grant select on SC3_account_universe to vespa_group_low_security, sk_prodreg

commit


/**************** PART A01 - GET ALL ACTIVE ACCOUNTS (and part of A02) *******************/
-- Get account numbers and Sky base universe for each account in the Sky base
     insert into SC3_account_universe(
                         account_number
                        ,adsmart_capable
                        ,adsmartable
                        ,sky_base_universe
                        ,cust_viewing_data_capture_allowed
                )
     select  account_number
            ,case
                when flag = 1 then 1
                else 0
                end as adsmart_capable
            ,case
                when flag = 1 and cust_viewing_data_capture_allowed = 'Y' then 1
                else 0
                end as adsmartable
            ,case
                when adsmartable = 1 then 'Adsmartable with consent'
                when adsmart_capable = 1 and cust_viewing_data_capture_allowed <> 'Y' then 'Adsmartable but no consent'
                else 'Not adsmartable'
                end as sky_base_universe
            ,cust_viewing_data_capture_allowed
        from (
                 select  sav.account_number as account_number, adsmart.flag, cust_viewing_data_capture_allowed
                    from    (
                                select      distinct account_number, cust_viewing_data_capture_allowed
                                     from   sk_prod.CUST_SINGLE_ACCOUNT_VIEW
                                    where   CUST_ACTIVE_DTV = 1                     -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
                                      and   pty_country_code = 'GBR'
                    --                   and   cust_viewing_data_capture_allowed = 'Y' -- [ ENABLE/DISABLE this criteria to consider viewing consent ]
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
                                                                from    sk_prod.CUST_SET_TOP_BOX

                                                        )       as base
                                                        where   active_flag = 1

                                                )       as active_boxes
                                        where   currency_code = 'GBP'
                                        group   by      account_number

                                    )       as adsmart
                                    on      sav.account_number = adsmart.account_number
        ) as sub1
commit


     /**************** L01: ESTABLISH POPULATION ****************/
     -- We need the segmentation over the whole Sky base so we can scale up
     -- Captures all active accounts in cust_subs_hist
     if object_id('#weekly_sample') is not null drop table #weekly_sample
     SELECT   account_number
             ,cb_key_household
             ,cb_key_individual
             ,current_short_description
             ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
             ,convert(bit, 0)  AS uk_standard_account
             ,convert(VARCHAR(30), NULL) AS isba_tv_region
       INTO #weekly_sample
       FROM sk_prod.cust_subs_hist
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
     execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L01: Midway 1/2 (Weekly sample)', coalesce(@QA_catcher, -1)
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
        -- Insert SC3 TV regions
         ,isba_tv_region = case
                when b.isba_tv_region = 'Border' then'NI, Scotland & Border'
                when b.isba_tv_region = 'Central Scotland' then'NI, Scotland & Border'
                when b.isba_tv_region = 'East Of England' then'Wales & Midlands'
                when b.isba_tv_region = 'HTV Wales' then'Wales & Midlands'
                when b.isba_tv_region = 'HTV West' then'South England'
                when b.isba_tv_region = 'London' then'London'
                when b.isba_tv_region = 'Meridian (exc. Chann' then'South England'
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
     execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L01: Complete! (Population)', coalesce(@QA_catcher, -1)
     commit

     -- Find accounts that are in SC3_account_universe (coming from single_account_view) but not
     select @QA_catcher = count(1)
     from SC3_account_universe
     where account_number not in (select account_number from #weekly_sample)

     commit
     execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Missing accounts:', coalesce(@QA_catcher, -1)
     commit

     -- Drop these accounts
     delete from SC3_account_universe where account_number not in (select account_number from #weekly_sample)

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
     -- Update for SC3 build. Use case statement to consolidate old scaling segments into new.
     SELECT   cb_key_individual
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
     execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 1/8 (Consumerview Linkage)', coalesce(@QA_catcher, -1)
     commit

     -- Populate Package & ISBA TV Region
     UPDATE SC3_account_universe
     SET a.package = b.package, a.isba_tv_region = b.isba_tv_region
     FROM SC3_account_universe AS a
     INNER JOIN (SELECT
          fbp.account_number
         ,CASE
             WHEN cel.prem_sports > 0 OR  cel.prem_movies > 0 THEN 'Movies & Sports' --'Other Premiums'
             WHEN cel.prem_sports > 0 AND cel.prem_movies = 0 THEN 'Sports' --'Dual Sports'
             WHEN cel.prem_sports = 0 AND cel.prem_movies > 0 THEN 'Movies' --'Dual Movies'
             WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 THEN 'Basic' --'Basic - Ent'
             ELSE 'Basic' END AS package  --                                                  'Basic - Ent' END -- package
          ,isba_tv_region
     FROM #weekly_sample AS fbp
     left join sk_prod.cust_entitlement_lookup AS cel
         ON fbp.current_short_description = cel.short_description
     WHERE fbp.cb_key_household IS NOT NULL
       AND fbp.cb_key_individual IS NOT NULL) as b
     ON a.account_number = b.account_number


     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_account_universe

     commit
     execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 2/8 (Package & ISBA region)', coalesce(@QA_catcher, -1)
     commit

     -- HHcomposition
     UPDATE SC3_account_universe
     SET stws.hhcomposition = sub1.h_household_composition
     FROM SC3_account_universe AS stws
     inner join (select account_number, h_household_composition
                   from #weekly_sample as ws
             inner join #cv_keys AS cv
     ON ws.cb_key_individual = cv.cb_key_individual
     ) sub1
     on stws.account_number = sub1.account_number

     -- Revert null hhcomposition to default (Unknown)
     UPDATE SC3_account_universe
        SET hhcomposition = 'D) Unclassified HHComp'
      WHERE hhcomposition is null

     commit
     drop table #cv_keys
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_account_universe
     where left(hhcomposition, 2) <> 'D)'

     commit
     execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 3/8 (HH composition)', coalesce(@QA_catcher, -1)
     commit

     -- Tenure

     -- Tenure has been grouped according to its relationship with viewing behaviour

     UPDATE SC3_account_universe t1
     SET
         tenure = CASE   WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  730 THEN 'A) 0-2 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <= 3650 THEN 'B) 3-10 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) > 3650 THEN  'C) 10 Years+'
                         ELSE 'D) Unknown'
                  END
     FROM sk_prod.cust_single_account_view sav
     WHERE t1.account_number=sav.account_number
     COMMIT

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_account_universe
     where tenure <> 'D) Unknown'

     -- Added SC3 line to remove Unknown tenure
     delete from SC3_account_universe
     where tenure = 'D) Unknown'

     commit
     execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 4/8 (Tenure)', coalesce(@QA_catcher, -1)
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
             inner join SC3_account_universe AS ss ON csh.account_number = ss.account_number
      WHERE  csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription')     --the DTV sub Type
        AND csh.status_code IN ('AC','AB','PC')                  --Active Status Codes
        AND csh.effective_from_dt <= @profiling_thursday
        AND csh.effective_to_dt > @profiling_thursday
        AND csh.effective_from_dt<>effective_to_dt

     -- De-dupe active boxes
     DELETE FROM #accounts WHERE rank>1
     COMMIT

     -- Create indices on list of boxes
     CREATE hg INDEX idx1 ON #accounts(service_instance_id)
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
     execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 5/8 (HD boxes)', coalesce(@QA_catcher, -1)
     commit

     -- Identify PVR boxes
     SELECT  acc.account_number
            ,MAX(CASE WHEN x_box_type LIKE '%Sky+%' THEN 'Yes'
                     ELSE 'No'
                  END) AS PVR
     INTO #pvra -- drop table #pvra
     FROM sk_prod.CUST_SET_TOP_BOX AS stb INNER JOIN #accounts AS acc
                                                  ON stb.service_instance_id = acc.service_instance_id
     WHERE box_installed_dt <= @profiling_thursday
     AND box_replaced_dt   > @profiling_thursday
     GROUP by acc.account_number

     -- Create index on PVR table
     COMMIT
     CREATE UNIQUE hg INDEX pvidx1 ON #pvra(account_number)
     commit

     -- PVR
     UPDATE SC3_account_universe
     SET
         stws.pvr = cv.pvr
     FROM SC3_account_universe AS stws
     inner join #pvra AS cv
     ON stws.account_number = cv.account_number

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_account_universe
     where pvr is not null

     commit
     execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6/8 (PVR boxes)', coalesce(@QA_catcher, -1)
     commit

       -- Set default value when account cannot be found
      update SC3_account_universe
         set pvr = case
                when sky_base_universe like 'Adsmartable%' then 'Yes'
                when sky_base_universe not like 'Adsmartable%' then 'No'
         end
       where pvr is null
      commit

     --Further check to ensure that when PVR is No then the box is Not Adsmartable
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_account_universe
     where pvr = 'No' and sky_base_universe like 'Adsmartable%'

     commit
     execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6a/8 (Non-PVR boxes which are adsmartable)', coalesce(@QA_catcher, -1)
     commit

       -- Update PVR when PVR says 'No' and universe is an adsmartable one.
      update SC3_account_universe
         set pvr = 'Yes'
       where pvr = 'No' and sky_base_universe like 'Adsmartable%'
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
     execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6/8 (P/S boxes)', coalesce(@QA_catcher, -1)
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

        update      SC3_account_universe a set
          no_of_stbs = case
                 when b.Universe like '%Single%' then 'Single'
                 when b.Universe like '%Multiple%' then 'Multiple'
                 else 'Single'
                 end
         ,hd_subscription = case
                when boxtype like 'B)%' or boxtype like 'E)%' or boxtype like 'F)%' or boxtype like 'G)%' then 'Yes'
                else 'No'
                end
         from   #boxtype_ac b
        where   a.account_number = b.account_number

          set @QA_catcher = -1

          select @QA_catcher = count(1)
          from SC3_account_universe
          where no_of_stbs is not null

          commit
          execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 7a/8 (No. of set top boxes)', coalesce(@QA_catcher, -1)
          commit

           -- Following on from SC2 set default when value is not known
          update SC3_account_universe
             set no_of_stbs = 'Single'
           where no_of_stbs is null

          set @QA_catcher = -1

          select @QA_catcher = count(1)
          from SC3_account_universe
          where hd_subscription is not null

          commit
          execute citeam.logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 7b/8 (HD Subscription)', coalesce(@QA_catcher, -1)
          commit

           -- Following on from SC2 set default when value is not known
          update SC3_account_universe
             set hd_subscription = 'No'
           where hd_subscription is null

/**************** PART A02 - UNIVERSE ALLOCATION *******************/

/**************** Part a - Initial allocation *******************/

-- Need to do the scaling segment in two parts. Need to proportion of Adsmartable who have
-- not given consent. Firstly, we ignore the 'Adsmartable but no consent' universe

     update SC3_account_universe a
        set a.scaling_segment_id = b.scaling_segment_id
       from vespa_analysts.SC3_Segments_Lookup_v1_1 b
      where b.sky_base_universe = case
                when a.sky_base_universe = 'Not adsmartable' then a.sky_base_universe
                else 'Adsmartable with consent'
                 end
        and a.isba_tv_region = b.isba_tv_region
        and left(a.hhcomposition, 2) = left(b.hhcomposition, 2)
        and left(a.tenure, 2) = left(b.tenure, 2)
        and a.package = b.package
        and a.no_of_stbs = b.no_of_stbs
        and a.hd_subscription = b.hd_subscription
        and a.pvr = b.pvr

     delete from SC3_account_universe
      where scaling_segment_id = 0

/**************** Part b - Vespa allocation *******************/

-- Set a random number which will be used to allocate the Vespa universe
        update SC3_account_universe
              set random_number = RAND(NUMBER(*)*(DATEPART(MS,NOW())+1))
              where sky_base_universe = 'Adsmartable with consent'
        commit

        IF object_id('SC3_consent_table') IS NOT NULL DROP TABLE SC3_consent_table
        select   scaling_segment_id
                ,sum(adsmart_capable) as no_of_adsmart_capable_accounts
                ,sum(adsmartable) as no_of_adsmart_accounts
                ,case when no_of_adsmart_capable_accounts > 0 then 1.0*no_of_adsmart_accounts/no_of_adsmart_capable_accounts else 0 end as adsmartable_ratio
            into SC3_consent_table
            from SC3_account_universe
        group by scaling_segment_id
        commit

        update SC3_account_universe a
              set vespa_universe =
              case
                when cust_viewing_data_capture_allowed = 'N' then 'Non-Vespa'
                when adsmartable = 0 then 'Vespa not Adsmartable'
                when adsmartable = 1 then 'Vespa adsmartable'
                else 'Non-Vespa'
              end
        commit

        update      SC3_account_universe
            set     vespa_universe = 'Vespa but no consent'
           from     SC3_account_universe a
      inner join    SC3_consent_table b
            on      b.scaling_segment_id = a.scaling_segment_id
            where   adsmartable = 1
            and     random_number >= adsmartable_ratio
        commit

/**************** Part c - Final allocation *******************/
-- Now update the scaling_segment_id with the actual values

     update SC3_account_universe a
        set a.scaling_segment_id = b.scaling_segment_id
       from vespa_analysts.SC3_Segments_Lookup_v1_1 b
      where a.sky_base_universe = b.sky_base_universe
        and a.isba_tv_region = b.isba_tv_region
        and left(a.hhcomposition, 2) = left(b.hhcomposition, 2)
        and left(a.tenure, 2) = left(b.tenure, 2)
        and a.package = b.package
        and a.no_of_stbs = b.no_of_stbs
        and a.hd_subscription = b.hd_subscription
        and a.pvr = b.pvr


/**** Part d - Put account numbers and vespa universe into a stand alone table ****/
-- Need to decide if thsi table should be created in the table creation part of the code
-- Also need to decide if we require 'Non-Vespa' accounts in here.

select       account_number, vespa_universe
        into SC3_vespa_universe
        from SC3_account_universe
       where vespa_universe <> 'Non-Vespa'
       --Is this final line required?

end -- of procedure "SC3_v1_1__do_universe_allocation"

commit;
go

-- SC3_v1_1__do_universe_allocation




