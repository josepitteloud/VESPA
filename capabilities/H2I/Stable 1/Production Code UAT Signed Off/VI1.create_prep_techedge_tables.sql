/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                                                 Skyview H2I
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson          (Jason.Thompson@skyiq.co.uk)
                                                                                ,Hoi Yu Tang            (HoiYu.Tang@skyiq.co.uk)
                                                                                ,Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
                                                                              ,Jose Loureda           (Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:

        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin

**Business Brief:

        This Module produces the final individual level viewing table that will be sent to TechEdge

**Module:

        M13: Create output tables for TechEdge
                        M13.0 - Initialising Environment
                        M13.1 - Transpose Individuals to Columns
                        M13.2 - Final Viewing Output Table
                        M13.3 - Final Individual Table

--------------------------------------------------------------------------------------------------------------
*/





---------------------------------








-----------------------------------
-- M13.0 - Initialising Environment
-----------------------------------

create or replace procedure ${SQLFILE_ARG001}.V289_M13_Create_Final_TechEdge_Output_Tables
        @proc_date  date    =   null
as begin

        MESSAGE cast(now() as timestamp)||' | Begining M13.0 - Initialising Environment' TO CLIENT
        commit -- ; --(^_^)

        declare @person_loop int                        commit -- ; --(^_^)
        declare @sql_text varchar(10000)        commit -- ; --(^_^)




        create table #V289_M13_individual_viewing_working_table (
                        event_id                                bigint                  null
                        ,pk_viewing_prog_instance_fact  bigint                  null
                        ,overlap_batch                  int                             null
                        ,account_number                 varchar(20)             null
                        ,subscriber_id                  numeric(10)             null
                        ,service_key                    int                             null
                        ,parent_service_key             int                             null
                        ,HD_flag                                int                             null
                        ,event_start_date_time  timestamp               null
                        ,event_end_date_time    timestamp               null
                        ,barb_min_start_date_time_utc   timestamp       null
                        ,barb_min_end_date_time_utc             timestamp       null
                        ,event_start_date_BARB  timestamp               null
                        ,barb_min_start_date    timestamp               null
                        ,live_recorded                  varchar(8)              not     null
                        ,viewing_type_flag              int             not null default 0
                        ,person_1                               smallint                null    default 0
                        ,person_2                               smallint                null    default 0
                        ,person_3                               smallint                null    default 0
                        ,person_4                               smallint                null    default 0
                        ,person_5                               smallint                null    default 0
                        ,person_6                               smallint                null    default 0
                        ,person_7                               smallint                null    default 0
                        ,person_8                               smallint                null    default 0
                        ,person_9                               smallint                null    default 0
                        ,person_10                              smallint                null    default 0
                        ,person_11                              smallint                null    default 0
                        ,person_12                              smallint                null    default 0
                        ,person_13                              smallint                null    default 0
                        ,person_14                              smallint                null    default 0
                        ,person_15                              smallint                null    default 0
                        ,person_16                              smallint                null    default 0
        )
        commit -- ; --(^_^)

        create hg index hg1 on #V289_M13_individual_viewing_working_table(event_id)     commit -- ; --(^_^)
        create hg index hg2 on #V289_M13_individual_viewing_working_table(overlap_batch)        commit -- ; --(^_^)
        create hg index hg3 on #V289_M13_individual_viewing_working_table(account_number)       commit -- ; --(^_^)
        create hg index hg4 on #V289_M13_individual_viewing_working_table(subscriber_id)        commit -- ; --(^_^)
        create hg index hg5 on #V289_M13_individual_viewing_working_table(service_key)  commit -- ; --(^_^)
        create hg index hg6 on #V289_M13_individual_viewing_working_table(parent_service_key)   commit -- ; --(^_^)
        create hg index hg7 on #V289_M13_individual_viewing_working_table(event_start_date_time)        commit -- ; --(^_^)
        create hg index hg8 on #V289_M13_individual_viewing_working_table(event_end_date_time)  commit -- ; --(^_^)
        create hg index hg9 on #V289_M13_individual_viewing_working_table(barb_min_start_date_time_utc) commit -- ; --(^_^)
        create hg index hg10 on #V289_M13_individual_viewing_working_table(barb_min_end_date_time_utc)  commit -- ; --(^_^)
        create hg index hg11 on #V289_M13_individual_viewing_working_table(pk_viewing_prog_instance_fact)       commit -- ; --(^_^)
        create hg index hg12 on #V289_M13_individual_viewing_working_table(event_start_date_BARB)       commit -- ; --(^_^)
        create hg index hg13 on #V289_M13_individual_viewing_working_table(barb_min_start_date) commit -- ; --(^_^)
        create lf index lf1 on #V289_M13_individual_viewing_working_table(HD_flag)      commit -- ; --(^_^)
        create lf index lf2 on #V289_M13_individual_viewing_working_table(live_recorded)        commit -- ; --(^_^)
        commit -- ; --(^_^)


        MESSAGE cast(now() as timestamp)||' | @ M13.0: Initialising Environment DONE' TO CLIENT
        commit -- ; --(^_^)




        -----------------------------------
        -- M13.1 - Transpose Individuals to Columns
        -----------------------------------

        MESSAGE cast(now() as timestamp)||' | Begining M13.1 - Create individual viewing working table...' TO CLIENT
        commit -- ; --(^_^)

        truncate table #V289_M13_individual_viewing_working_table
        commit -- ; --(^_^)

        -- Populate the working viewing table with a single copy of each viewing event and overlap batches where relevant
        -- This will do linear only (i.e. not Pull vod)
        insert into     #V289_M13_individual_viewing_working_table(
                        event_id
                ,       pk_viewing_prog_instance_fact
                ,       overlap_batch
                ,       account_number
                ,       subscriber_id
                ,       service_key
                ,       parent_service_key
                ,       HD_flag
                ,       event_start_date_time
                ,       event_end_date_time
                ,       barb_min_start_date_time_utc
                ,       barb_min_end_date_time_utc
                ,       event_start_date_BARB
                ,       barb_min_start_date
                ,       live_recorded
                ,       viewing_type_flag
                )
        select
                        dp_raw.dth_event_id
                ,       dp.event_id
                ,       dp.overlap_batch
                ,       dp.account_number
                ,       dp.subscriber_id
                ,       dp_raw.service_key
                ,       cmap.parent_service_key
                ,       case    cmap.[format]
                                when    'HD'    then    1
                                when    '3D'    then    1
                                else                                    0
                        end
                ,       dp.event_Start_utc
                ,       dp.event_end_utc
                ,       dp.barb_min_start_date_time_utc
                ,       dp.barb_min_end_date_time_utc
                ,       case
                                when    datepart(hour,dp.event_Start_utc) < 2   then    dateadd(dd,-1,date(dp.event_Start_utc))
                                else                                                                                                    date(dp.event_Start_utc)
                        end
                ,       case
                                when    datepart(hour,dp.barb_min_start_date_time_utc) < 2  then    dateadd(dd,-1,date(dp.barb_min_start_date_time_utc))
                                else                                                                                                                                    date(dp.barb_min_start_date_time_utc)
                        end
                ,       dp_raw.live_recorded
                ,       dp.viewing_type_flag
        from
                                        v289_M06_dp_raw_data                                                                            as      dp_raw
                inner join      V289_M07_dp_data                                                                                        as      dp              on      dp_raw.pk_viewing_prog_instance_fact    =       dp.event_id
                inner join      vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES          as      cmap    on      dp_raw.service_key                                              =       cmap.service_key
        where dp.barb_min_start_date_time_utc is not null -- We do not want to pass non minute attributed events to TE
        and   dp.event_Start_utc between cmap.effective_from and cmap.effective_to -- otherwise get duplicated rows in TE output
        commit -- ; --(^_^)


        -- Populate the working viewing table with a single copy of each viewing event and overlap batches where relevant
        -- This will do Pull vod
        insert into     #V289_M13_individual_viewing_working_table(
                        event_id
                ,       pk_viewing_prog_instance_fact
                ,       overlap_batch
                ,       account_number
                ,       subscriber_id
                ,       service_key
                ,       parent_service_key
                ,       HD_flag
                ,       event_start_date_time
                ,       event_end_date_time
                ,       barb_min_start_date_time_utc
                ,       barb_min_end_date_time_utc
                ,       event_start_date_BARB
                ,       barb_min_start_date
                ,       live_recorded
                ,       viewing_type_flag
                )
        select
                        dp_raw.dth_event_id
                ,       dp.event_id
                ,       dp.overlap_batch
                ,       dp.account_number
                ,       dp.subscriber_id
                ,       dp_raw.service_key
                ,       cmap.parent_service_key
                ,       case    cmap.[format]
                                when    'HD'    then    1
                                when    '3D'    then    1
                                else                                    0
                        end
                ,       dp.event_Start_utc
                ,       dp.event_end_utc
                ,       dp.barb_min_start_date_time_utc
                ,       dp.barb_min_end_date_time_utc
                ,       case
                                when    datepart(hour,dp.event_Start_utc) < 2   then    dateadd(dd,-1,date(dp.event_Start_utc))
                                else                                                                                                    date(dp.event_Start_utc)
                        end
                ,       case
                                when    datepart(hour,dp.barb_min_start_date_time_utc) < 2  then    dateadd(dd,-1,date(dp.barb_min_start_date_time_utc))
                                else                                                                                                                                    date(dp.barb_min_start_date_time_utc)
                        end
                ,       'RECORDED' --dp_raw.live_recorded
                ,       dp.viewing_type_flag
        from
                                        v289_M17_vod_raw_data                                                                            as      dp_raw
                inner join      V289_M07_dp_data                                                                                        as      dp              on      dp_raw.pk_viewing_prog_instance_fact    =       dp.event_id
                inner join      vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES          as      cmap    on      dp_raw.service_key                                              =       cmap.service_key
        where dp.barb_min_start_date_time_utc is not null -- We do not want to pass non minute attributed events to TE
        and   dp.event_Start_utc between cmap.effective_from and cmap.effective_to -- otherwise get duplicated rows in TE output                                                                                                                                                                          and cast(dp_raw.event_Start_date_time_utc as date) between cmap.effective_from and cmap.effective_to
        commit -- ; --(^_^)



        -- Loop through each possible person (max 16) in a hhd and add their viewing

        MESSAGE cast(now() as timestamp)||' | Begining M13.1 - Iterate over individuals and add their viewing...' TO CLIENT
        commit -- ; --(^_^)

        -- Individuals with a weight
        select  account_number, hh_person_number
        into    #scale_accs
        from    V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
        where   scaling_date = @proc_date
        commit

        create  hg index ind1 on #scale_accs(account_number)
        create  hg index ind2 on #scale_accs(hh_person_number)
        commit



        set @person_loop = 1
        commit -- ; --(^_^)

        while @person_loop <= 16
        begin

                        -- update events with person where overlap_batch match (i.e. overlap_batch is not null)
                        set     @sql_text =     '
                                update  #V289_M13_individual_viewing_working_table m13
                                set             person_' || @person_loop || ' = 1
                                from    V289_M10_session_individuals m10
                                inner join #scale_accs m11 on m10.account_number = m11.account_number
                                                           and  m10.hh_person_number = m11.hh_person_number
                                where
                                                m13.pk_viewing_prog_instance_fact = m10.event_id
                                        and     m13.overlap_batch = m10.overlap_batch
                                        and m10.hh_person_number = ' || @person_loop

                        commit

                        execute (@sql_text)
                        commit

                        -- now update when overlap_batch is null (i.e. the event is not overlapping another in same hhd which is most of them)
                        set     @sql_text =     '
                                update  #V289_M13_individual_viewing_working_table m13
                                set             person_' || @person_loop || ' = 1
                                from    V289_M10_session_individuals m10
                                inner join #scale_accs m11 on m10.account_number = m11.account_number
                                                           and  m10.hh_person_number = m11.hh_person_number
                                where
                                                m13.pk_viewing_prog_instance_fact = m10.event_id
                                        and     m13.overlap_batch is null
                                        and     m10.overlap_batch is null
                                        and     m10.hh_person_number = ' || @person_loop

                        commit

                        execute (@sql_text)
                        commit

                        set @person_loop = @person_loop + 1
                        commit
        end
        commit -- ; --(^_^)




        -----------------------------------
        -- M13.2 - Final Viewing Output Table
        -----------------------------------

        MESSAGE cast(now() as timestamp)||' | Begining M13.2 - Update final individual viewing output table...' TO CLIENT
        commit -- ; --(^_^)

        -- This will need re-working to make sure we get the right data
        -- Also needs to be MA version for start/end times. Have cheated here for now

        truncate table ${SQLFILE_ARG001}.V289_M13_individual_viewing_live_vosdal
        commit -- ; --(^_^)

        insert into     V289_M13_individual_viewing_live_vosdal(
                                                SUBSCRIBER_ID
                                                ,ACCOUNT_NUMBER
                                                ,STB_BROADCAST_START_TIME
                                                ,STB_BROADCAST_END_TIME
                                                ,STB_EVENT_START_TIME
                                                ,TIMESHIFT
                                                ,service_key    -- field name OK, but should be parent_service_key
                                                ,Platform_flag
                                                ,Original_Service_key   -- field name OK, but should be service_key
                                                ,AdSmart_flag
                                                ,DTH_VIEWING_EVENT_ID -- will populate with pk_viewing_prog_instance_fact
                                                ,person_1
                                                ,person_2
                                                ,person_3
                                                ,person_4
                                                ,person_5
                                                ,person_6
                                                ,person_7
                                                ,person_8
                                                ,person_9
                                                ,person_10
                                                ,person_11
                                                ,person_12
                                                ,person_13
                                                ,person_14
                                                ,person_15
                                                ,person_16
                                                )
        select          m13.subscriber_id
                                        ,m13.account_number
                                        ,barb_min_start_date_time_utc
                                        ,barb_min_end_date_time_utc
                                        ,event_start_date_time
                                        ,CASE
                                                        WHEN    live_recorded = 'LIVE'  THEN    0
                                                        ELSE    (
                                                                                (
                                                                                        case
                                                                                                when (event_start_date_BARB - barb_min_start_date) < 0 then 0
                                                                                                else (event_start_date_BARB - barb_min_start_date)
                                                                                        end
                                                                                )
                                                                                + 1
                                                                        )
                                                        END AS TIMESHIFT
                                        ,parent_service_key
                                        ,HD_flag
                                        ,service_key
                                        ,0
                                        ,pk_viewing_prog_instance_fact -- event_id
                                        ,person_1
                                        ,person_2
                                        ,person_3
                                        ,person_4
                                        ,person_5
                                        ,person_6
                                        ,person_7
                                        ,person_8
                                        ,person_9
                                        ,person_10
                                        ,person_11
                                        ,person_12
                                        ,person_13
                                        ,person_14
                                        ,person_15
                                        ,person_16
        from            #V289_M13_individual_viewing_working_table m13
        where           person_1 + person_2 + person_3 + person_4 + person_5 + person_6 + person_7 + person_8 + person_9 + person_10
                                                                                                                                        + person_11 + person_12 + person_13 + person_14 + person_15 + person_16 > 0
                        and (TIMESHIFT < 2 and viewing_type_flag = 0)
        commit -- ; --(^_^)





        truncate table ${SQLFILE_ARG001}.V289_M13_individual_viewing_timeshift_pullvod
        commit -- ; --(^_^)

        insert into     V289_M13_individual_viewing_live_vosdal( -- ALL VIEWING TO GO INTO A SINGLE TABLE FOR TE --V289_M13_individual_viewing_timeshift_pullvod(
                                                SUBSCRIBER_ID
                                                ,ACCOUNT_NUMBER
                                                ,STB_BROADCAST_START_TIME
                                                ,STB_BROADCAST_END_TIME
                                                ,STB_EVENT_START_TIME
                                                ,TIMESHIFT
                                                ,service_key    -- field name OK, but should be parent_service_key
                                                ,Platform_flag
                                                ,Original_Service_key   -- field name OK, but should be service_key
                                                ,AdSmart_flag
                                                ,DTH_VIEWING_EVENT_ID -- will populate with pk_viewing_prog_instance_fact
                                                ,person_1
                                                ,person_2
                                                ,person_3
                                                ,person_4
                                                ,person_5
                                                ,person_6
                                                ,person_7
                                                ,person_8
                                                ,person_9
                                                ,person_10
                                                ,person_11
                                                ,person_12
                                                ,person_13
                                                ,person_14
                                                ,person_15
                                                ,person_16
                                                )
        select          m13.subscriber_id
                                        ,m13.account_number
                                        ,barb_min_start_date_time_utc
                                        ,barb_min_end_date_time_utc
                                        ,event_start_date_time
                                        ,CASE
                                                        WHEN    live_recorded = 'LIVE'  THEN    0
                                                        ELSE    (
                                                                                (
                                                                                        case
                                                                                                when (event_start_date_BARB - barb_min_start_date) < 0 then 0
                                                                                                else (event_start_date_BARB - barb_min_start_date)
                                                                                        end
                                                                                )
                                                                                + 1
                                                                        )
                                                        END AS TIMESHIFT
                                        ,parent_service_key
                                        ,HD_flag
                                        ,service_key
                                        ,0
                                        ,pk_viewing_prog_instance_fact -- event_id
                                        ,person_1
                                        ,person_2
                                        ,person_3
                                        ,person_4
                                        ,person_5
                                        ,person_6
                                        ,person_7
                                        ,person_8
                                        ,person_9
                                        ,person_10
                                        ,person_11
                                        ,person_12
                                        ,person_13
                                        ,person_14
                                        ,person_15
                                        ,person_16
        from            #V289_M13_individual_viewing_working_table m13
        where           person_1 + person_2 + person_3 + person_4 + person_5 + person_6 + person_7 + person_8 + person_9 + person_10
                                                                                                                                        + person_11 + person_12 + person_13 + person_14 + person_15 + person_16 > 0
                        and (TIMESHIFT > 1 or viewing_type_flag = 1)
        commit -- ; --(^_^)

                
        MESSAGE cast(now() as timestamp)||' | M13.2 - Update subscriber_is 99 for Pull VOD events' TO CLIENT
                
                SELECT  b.account_number 
                                ,CONVERT (integer,min(si_external_identifier)) as subscriber_id
                                ,CONVERT (bit, max(case when si_service_instance_type = 'Primary DTV' then 1 else 0 end)) as primary_box
                INTO    #subscriber_details
                FROM    CUST_SERVICE_INSTANCE as b
                INNER JOIN V289_M13_individual_viewing_timeshift_pullvod as base        ON   base.account_number = b.account_number AND base.subscriber_id = 99
                WHERE    si_service_instance_type = 'Primary DTV'
                        AND @proc_date BETWEEN effective_from_dt AND effective_to_dt
                GROUP BY b.account_number 
                
                COMMIT 
                CREATE HG INDEX wef ON #subscriber_details(account_number)
                COMMIT
                
                UPDATE V289_M13_individual_viewing_timeshift_pullvod
                SET a.subscriber_id = b.subscriber_id
                FROM V289_M13_individual_viewing_timeshift_pullvod AS a 
                JOIN #subscriber_details as b ON a.account_number = b.account_number
                WHERE a.subscriber_id = 99 
                
                MESSAGE cast(now() as timestamp)||' | M13.2 - Subscriber_is 99 updated: '||@@rowcount  TO CLIENT
                COMMIT


        -----------------------------------
        -- M13.3 - Final Individual Table
        -----------------------------------

        MESSAGE cast(now() as timestamp)||' | Begining M13.3 - Update final individual details table...' TO CLIENT
        commit -- ; --(^_^)

        truncate table ${SQLFILE_ARG001}.V289_M13_individual_details
        commit -- ; --(^_^)

        insert into     V289_M13_individual_details     (
                                                                                                        account_number
                                                                                                ,       person_number
                                                                                                ,       ind_scaling_weight
                                                                                                ,       gender
                                                                                                ,       age_band
                                                                                                ,       head_of_hhd
                                                                                                ,       hhsize
                                                                                        )
        select
                        hh.account_number
                ,       hh.hh_person_number
                ,       w.scaling_weighting
                ,       case
                                when hh.person_gender = 'M' then 1
                                when hh.person_gender = 'F' then 2
                                else 99
                        end             as      gender
                ,       case
                                when hh.person_ageband = '0-11'     then 0 -- between 0 and 11 then 0
                                when hh.person_ageband = '12-19'    then 1 -- between 12 and 19 then 1
                                when hh.person_ageband = '20-24'    then 2 --  between 20 and 24 then 2
                                when hh.person_ageband = '25-34'    then 3 -- between 25 and 34 then 3
                                when hh.person_ageband = '35-44'    then 4 -- between 35 and 44 then 4
                                when hh.person_ageband = '45-64'    then 5 --  between 45 and 64 then 5
                                when hh.person_age >= 65 then 6
                                else 99
                        end             as      age_band
                ,       hh.person_head
                ,       hh.household_size
        from
                                        V289_M08_sky_hh_composition                     as      hh
                inner join      V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING  as      w       on      hh.account_number       =       w.account_number
                                                                                                                                                                                and     hh.HH_person_number     =       w.HH_person_number

        WHERE   (
                                                hh.panel_flag                           =       1
                                        or      hh.nonviewer_household  =       1
                                )
        commit -- ; --(^_^)

        ---------------------------------------------------------------------------------------

end; -- procedure
GO
commit;



